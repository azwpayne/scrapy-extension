"""Duplicate filter component for scrapy-extension.

This module provides a Scrapy dupefilter component using backend set interfaces.
"""

from __future__ import annotations

import logging
import sys
from collections import OrderedDict, deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, Lock, RLock, get_ident
from typing import TYPE_CHECKING, Any, Literal, Protocol
from weakref import ReferenceType, WeakSet, ref

from scrapy import Spider
from twisted.internet.defer import Deferred

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import (
    DEFAULT_MEMORY_MAXSIZE,
    MemoryMembershipFilter,
)
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.exceptions.base import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.utils._config import (
    get_bool_setting,
    parse_float_setting,
    parse_int_setting,
)
from scrapy_extension.utils.identity import (
    DEFAULT_DUPEFILTER_KEY_TEMPLATE,
    project_name_from_spider,
    resolve_identity_template,
)
from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from scrapy.http import Request
    from scrapy.settings import Settings

    from scrapy_extension.backends.connectors import (
        ConnectionManager,
        ConnectionManagerLease,
    )

    class _Fingerprinter(Protocol):
        """Duck type for Scrapy's request fingerprinter.

        Mirrors ``scrapy.http.request.RequestFingerprinter`` (and any custom
        ``REQUEST_FINGERPRINTER_CLASS``) — the ``fingerprint(request) -> bytes``
        contract. Used so ``BackendDupeFilter`` can honor a configured custom
        fingerprinter instead of always defaulting to the module function.
        """

        def fingerprint(self, request: Request) -> bytes: ...


logger = logging.getLogger(__name__)


def _cleanup_factory_filter_and_manager(
    membership_filter: MembershipFilter | None,
    manager: Any | None,
    manager_lease: Any | None,
    *,
    owns_manager: bool,
) -> BaseException | None:
    """Abort an unpublished filter in dependency order.

    Construction has no returned object to carry a retry callback. Try the filter
    twice (covering effect-then-raise) and then release the exact manager acquire;
    a persistent filter failure takes the explicit lossy construction-abort path.
    The connection layer retains a failed exact release for a later registry retry.
    """
    cleanup_error: BaseException | None = None
    if membership_filter is not None:
        try:
            membership_filter.close()
        except BaseException as exc:
            cleanup_error = exc
            try:
                membership_filter.close()
            except BaseException:
                pass
    if owns_manager and manager is not None:
        from scrapy_extension.backends.connectors import release_manager_acquire

        try:
            if manager_lease is not None:
                release_manager_acquire(manager_lease, exact=True)
            else:
                release_manager_acquire(manager)
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
    return cleanup_error


# Module-level warn-once flag for the cuckoo-filter-full degradation (Theme C,
# R7-A). Mirrors the factory.py:31 ``_warned`` pattern so a long-running crawl
# doesn't have its log spammed by per-request filter-full signals: the first
# time the cuckoo filter exhausts ``_MAX_KICKS`` we warn once per process,
# bump ``dupefilter/filter_full`` on every occurrence, and treat the overflow
# item as NOT-seen (allow enqueue). Tests reset this for isolation.
_filter_full_warned: bool = False

# Module-level warn-once flag for the transient-backend-error degradation (Risk 4).
# Mirrors ``_filter_full_warned``: a long-running crawl shouldn't have its log
# spammed by per-request transient-outage signals. The first time the SetBackend
# raises BackendConnectionError or the circuit breaker rejects a call we warn
# once per process, bump ``errors/dedup`` on every occurrence via the monitor,
# and treat the item as NOT-seen (allow enqueue — a duplicate fetch during a
# transient outage is strictly better than a crashed crawl). Tests reset this
# for isolation.
_backend_error_warned: bool = False

# Compensation has a distinct warning because its risk is a surviving remote
# marker that suppresses a URL, rather than an admitted duplicate fetch.
_forget_backend_error_warned: bool = False
# These process-wide latches are advisory diagnostics, but their decision is
# shared by concurrent filter calls. Serialize the test-and-set; never hold this
# lock while logging or invoking a monitor.
_diagnostic_state_lock = Lock()

_DEDUP_BACKEND_FAILURE_MESSAGE = "Dedup backend is unavailable."
_DEDUP_CIRCUIT_BREAKER_NAME = "dedup"

# Non-removable filters (notably Bloom and Cuckoo) cannot compensate a successful
# add after the scheduler's later queue push fails. Keep a bounded, one-shot retry
# allowance per fingerprint instead. 1,024 limits failure-path memory while
# covering a useful transient queue-outage window. At capacity, new marker
# admission is backpressured so an existing failed-work recovery is never evicted.
_DEFAULT_RETRY_ALLOWANCE_LIMIT = 1_024

# Volatile queue strategies need a process-local dedup shadow rather than a
# persistent marker. Bound it so a remote Set + local routing strategy does not
# duplicate the entire crawl frontier in process memory. Eviction admits replay
# but cannot lose queued work.
_DEFAULT_VOLATILE_MARKER_LIMIT = 65_536

# A slow or stuck custom monitor must not turn the non-waiting telemetry FIFO
# into an unbounded memory sink. Overflow drops whole decision batches (never a
# partial hit/miss + saturation pair); deduplication state remains authoritative.
_DEFAULT_MONITOR_EVENT_LIMIT = 1_024

_MonitorHook = Literal[
    "on_dedup_hit",
    "on_dedup_miss",
    "on_error",
    "on_filter_full",
    "on_filter_saturation",
]
_PendingMonitorEvent = tuple[_MonitorHook, tuple[object, ...]]
_MonitorEvent = tuple[_MonitorHook, tuple[object, ...], object]
_ContinuationDiagnostic = Literal[
    "filter_full",
    "backend_error",
    "forget_backend_error",
    "retry_allowance_overflow",
]
_LifecycleState = Literal["new", "opening", "open", "clearing", "closing", "closed"]
_NEW: _LifecycleState = "new"
_OPENING: _LifecycleState = "opening"
_OPEN: _LifecycleState = "open"
_CLEARING: _LifecycleState = "clearing"
_CLOSING: _LifecycleState = "closing"
_CLOSED: _LifecycleState = "closed"


def _static_backend_error(exc: BaseException) -> BaseException:
    """Return a fixed package error without exposing the driving exception."""
    if type(exc) is CircuitBreakerOpenError:
        return CircuitBreakerOpenError(_DEDUP_CIRCUIT_BREAKER_NAME)
    return BackendConnectionError(_DEDUP_BACKEND_FAILURE_MESSAGE)


@dataclass(slots=True, eq=False, repr=False)
class _StrongReference:
    """Callable reference fallback for hostile/non-weak-referenceable objects."""

    target: object

    def __call__(self) -> object:
        return self.target


@dataclass(frozen=True, slots=True, eq=False)
class _MonitorFenceToken:
    """Hook/drainer liveness derived from an invocation-unique local token."""

    thread_id: int
    local_name: str

    @property
    def active(self) -> bool:
        """Whether a live owner frame still holds this exact token identity."""
        try:
            frame = sys._current_frames().get(self.thread_id)  # noqa: SLF001
        except Exception:  # noqa: BLE001 - an audit hook must fail scheduling open
            return False
        while frame is not None:
            try:
                if frame.f_locals.get(self.local_name) is self:
                    return True
            except Exception:  # noqa: BLE001 - stale telemetry cannot reject work
                return False
            try:
                frame = frame.f_back
            except Exception:  # noqa: BLE001 - hostile frame audit must fail open
                return False
        return False


@dataclass(slots=True, eq=False, repr=False)
class _DedupReservation:
    """Opaque intent to publish a marker after a durable queue push."""

    fingerprint: bytes
    epoch: int
    owner: object
    request: object
    fingerprint_text: str


@dataclass(slots=True, eq=False)
class _LegacyReservation:
    """One invocation-scoped receipt for Scrapy's boolean dupefilter protocol."""

    fingerprint: bytes
    epoch: int
    owner: object
    request_ref: Callable[[], Any]
    fingerprint_text: str
    consumed: bool = False
    consumer_thread_id: int | None = None
    settling: bool = False
    allowance_slot_reserved: bool = False


@dataclass(slots=True, eq=False)
class _PendingLegacyAddIntent:
    """One pre-publish intent for a legacy (boolean protocol) marker write.

    Registered under the lifecycle lock before the ``filter.add`` attempt so a
    BaseException anywhere in the add→receipt-publish window can be compensated
    with ``forget`` semantics (one exact retry allowance) instead of stranding a
    possibly-written marker with no recovery path — a permanent ghost
    fingerprint that suppresses the URL for the rest of the process.
    """

    fingerprint: bytes
    slot_reserved: bool = False


@dataclass(frozen=True, slots=True)
class _OperationLease:
    """One admitted filter operation, fenced to one lifecycle epoch."""

    epoch: int
    owner: object
    thread_id: int


@dataclass(frozen=True, slots=True)
class DedupDecision:
    """Atomic scheduler decision for the bundled duplicate filter.

    ``observational`` marks re-entry by the exact request whose monitor callback
    is active. It is not a business duplicate and must not enqueue or settle a
    broker token. ``reservation`` is an invocation-scoped intent to publish a
    marker after the queue accepts the request.
    """

    seen: bool
    reservation: object | None = None
    observational: bool = False


class DupeFilterLifecycleUnavailable(QueueError, RuntimeError):
    """A request arrived while the dupefilter stopped admission."""

    def __init__(self, operation: str, state: _LifecycleState) -> None:
        super().__init__(
            f"dupefilter {operation} unavailable while lifecycle is {state}",
            operation=operation,
        )


class BackendDupeFilter:
    """Scrapy duplicate filter using a pluggable membership-filter strategy.

    Delegates duplicate detection to a
    :class:`~scrapy_extension.dupefilter.filters.base.MembershipFilter`. The
    default strategy is ``SetMembershipFilter`` (exact, cross-worker,
    byte-identical to the previous hardcoded ``SetBackend`` behavior); other
    strategies (memory, bloom, cuckoo) are selected via ``SCRAPY_DEDUP_STRATEGY``
    (wired in ``from_settings``).

    Attributes:
        connection_manager: The connection manager for backend access. ``None``
            for in-process strategies built without a backend.
        key: The key for the fingerprints set / filter scope.
        debug: Whether to log filtered requests.
    """

    def __init__(
        self,
        connection_manager: ConnectionManager | None,
        key: str = DEFAULT_DUPEFILTER_KEY_TEMPLATE,
        *,
        debug: bool = False,
        fingerprinter: _Fingerprinter | None = None,
        membership_filter: MembershipFilter | None = None,
        monitor: Monitor | None = None,
        clear_on_open: bool = False,
        owns_connection_manager: bool = True,
        connection_manager_lease: ConnectionManagerLease | None = None,
    ) -> None:
        """Initialize the dupefilter.

        Args:
            connection_manager: Connection manager for backend access. May be
                ``None`` when an in-process ``membership_filter`` is supplied.
            key: Key for the fingerprints set / filter scope. May contain the
                literal placeholders ``"{project}"`` and ``"{spider}"``; when
                present they are substituted at :meth:`open` time so each
                project/spider gets its own dedup scope.
            debug: Whether to log filtered requests.
            fingerprinter: Optional Scrapy request fingerprinter. When provided
                (normally threaded from ``crawler.request_fingerprinter`` via
                ``from_crawler``), fingerprints respect a configured
                ``REQUEST_FINGERPRINTER_CLASS``. When ``None``, falls back to
                ``scrapy.utils.request.fingerprint`` — which is byte-identical to
                the default fingerprinter, so omitting this is fully backward-
                compatible (verified R45).
            membership_filter: Optional membership-filter strategy. When ``None``
                (default), a ``SetMembershipFilter`` is built from the connection
                manager and key — preserving the pre-strategy behavior exactly.
                Pass a custom filter (memory, bloom, cuckoo, ...) to override.
            monitor: Optional observability monitor. When ``None`` (default),
                :class:`~scrapy_extension.monitor.NullMonitor` (no-op). Wired to a
                :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` in
                :meth:`from_crawler` when ``crawler.stats`` is available, so dedup
                hit/miss stats are default-on. Emitted hooks are additive.
            clear_on_open: When True, :meth:`open` clears any prior fingerprints
                before the run begins (C5 fix). Default False → zero compat break
                (re-running a spider sees the prior run's fingerprints, as before).
            owns_connection_manager: Whether :meth:`close` releases the supplied
                manager. Defaults to True for factory-created standalone
                dupefilters; composite owners can pass False and release their
                single shared acquire after all borrowed components are closed.
        """
        if connection_manager is None and connection_manager_lease is not None:
            raise ValueError("connection_manager_lease requires connection_manager")
        self.connection_manager = connection_manager
        self.key = key
        self.debug = debug
        self.clear_on_open = clear_on_open
        self._fingerprinter = fingerprinter
        self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
        # Use explicit None checks: MembershipFilter defines __len__, so an empty
        # filter can be falsy. A manager is required only for the default set filter.
        if membership_filter is not None:
            self._filter: MembershipFilter = membership_filter
        elif connection_manager is not None:
            self._filter = SetMembershipFilter(connection_manager, key)
        else:
            raise ConfigurationError(
                "BackendDupeFilter requires a connection manager when no "
                "membership filter strategy is supplied.",
                setting_name="SCRAPY_DEDUP_STRATEGY",
            )
        self._retry_allowances: OrderedDict[bytes, None] = OrderedDict()
        self._retry_allowance_lock = Lock()
        self._retry_allowance_limit = _DEFAULT_RETRY_ALLOWANCE_LIMIT
        self._retry_allowance_overflow_warned = False
        # Slots are reserved before a non-transactional marker is added.  A full
        # ledger therefore backpressures *new* marker admission instead of
        # evicting an allowance whose probabilistic marker cannot be removed.
        self._retry_allowance_slots_reserved = 0
        # A false request_seen result does not always mean a fingerprint was
        # reserved: filter-full and transient-outage degradation deliberately
        # admit without writing. Track only genuine new reservations (and
        # one-shot retry allowances) by Request identity so the scheduler can
        # compensate a later failed push without deleting an unrelated marker.
        # Weak membership prevents callers that use request_seen directly from
        # retaining Request objects indefinitely.
        self._pending_reservations: WeakSet[Request] = WeakSet()
        self._manager_released = False
        self._owns_connection_manager = owns_connection_manager
        self._connection_manager_lease = connection_manager_lease
        self._release_owner_token: object | None = None
        self._direct_release_owner = object()
        # A composite owner (currently BackendScheduler) may register its stable
        # lifecycle token before open.  That token is an explicit alias for the
        # direct owner, allowing it to resume failed-open cleanup without making
        # arbitrary release tokens interchangeable.
        self._release_owner_aliases: list[object] = []
        self._lifecycle_lock = RLock()
        self._lifecycle_condition = Condition(self._lifecycle_lock)
        # Membership-filter calls are serialized without holding the lifecycle
        # lock. This preserves the thread-safety of in-process filters while
        # allowing lifecycle transitions to observe and drain admitted calls.
        self._filter_operation_lock = RLock()
        self._lifecycle_state: _LifecycleState = _NEW
        self._active_operations = 0
        self._active_operations_by_epoch: dict[int, int] = {}
        self._active_operation_threads: dict[int, int] = {}
        self._lifecycle_transition_thread_id: int | None = None
        self._close_requested = False
        # Operations enqueue complete telemetry batches under the lifecycle lock.
        # One elected caller drains this shared FIFO outside the lock; peers never
        # wait for that drainer. This preserves enqueue order and the monitor's
        # historical single-caller contract without making a re-entrant callback
        # deadlock on another request_seen call. Transactional miss telemetry is
        # settled after push, so its order is outcome order rather than initial
        # membership-check order.
        self._monitor_events: deque[_MonitorEvent] = deque()
        self._monitor_drain_token: _MonitorFenceToken | None = None
        self._monitor_event_limit = _DEFAULT_MONITOR_EVENT_LIMIT
        self._monitor_overflow_warned = False
        # Scheduler calls retain an opaque commit intent until their queue push
        # succeeds or fails. The marker is published only on commit, so a failed or
        # crashed push cannot leave a ghost fingerprint. Receipts are keyed by
        # identity and fenced by the lifecycle epoch.
        self._active_reservations: dict[int, _DedupReservation] = {}
        self._reservations_by_owner: dict[int, _DedupReservation] = {}
        # A Request can be submitted more than once, including concurrently.  Each
        # table is consequently a FIFO of weak receipts, not a single value keyed
        # by Request id.  Handoffs retain the consumed receipt only until the
        # scheduler reports queue success or calls forget().
        self._legacy_reservations: dict[int, deque[_LegacyReservation]] = {}
        self._legacy_handoffs: dict[int, deque[_LegacyReservation]] = {}
        # Legacy marker writes publish their invocation receipt only after
        # ``filter.add`` returns.  A pre-publish intent registered under the
        # lifecycle lock lets a BaseException in that window convert the
        # invocation's reserved admission slot into one retry allowance
        # (``forget`` semantics) instead of stranding a possibly-written ghost
        # marker with no recovery path.
        self._legacy_add_intents: dict[int, list[_PendingLegacyAddIntent]] = {}
        # A transition retires all old receipts.  Keep only weak Request identity
        # tombstones so a late forget cannot select a replacement-generation
        # pending receipt for the same long-lived Request object.
        self._legacy_retired_requests: WeakSet[Request] = WeakSet()
        self._reservation_epoch = 0
        # A process-local queue strategy cannot safely publish into a persistent
        # membership backend: a hard crash would lose the queued item but retain
        # the marker. Keep a lifecycle-local shadow instead. It filters repeated
        # work in this process and disappears with the volatile queue on crash.
        self._volatile_fingerprints: OrderedDict[bytes, None] = OrderedDict()
        self._volatile_fingerprint_limit = _DEFAULT_VOLATILE_MARKER_LIMIT
        self._volatile_fingerprint_overflow_warned = False
        # During a monitor hook, direct re-entry (including a joined worker thread)
        # with the exact originating Request is observational. Calls made after the
        # hook returns are ordinary dedup operations; monitors must not launch
        # detached request_seen calls. A different Request with the same fingerprint
        # is always independent work.
        self._active_monitor_requests: dict[
            int,
            tuple[Callable[[], object | None], set[_MonitorFenceToken]],
        ] = {}
        # A custom MembershipFilter may synchronously or cross-thread re-enter
        # request_seen from its add/contains callback. The exact originating
        # Request is observational during that callback, just as it is for a
        # monitor hook; this avoids recursive filter calls and callback/join
        # deadlocks without suppressing a different Request with the same key.
        self._active_filter_requests: dict[int, object] = {}
        self._opened = False
        self._opened_spider: Spider | None = None
        self._opening = False
        self._open_owner_token: _MonitorFenceToken | None = None
        self._closed = False
        self._closing = False
        self._release_in_progress = False
        self._release_thread_id: int | None = None
        self._clear_in_progress = False
        self._filter_released = False
        # A MemoryMembershipFilter can emit saturation from inside ``add``. Keep
        # that internal callback on a NullMonitor while the filter is owned here;
        # request_seen records the same event and dispatches it only after releasing
        # the lifecycle lock, alongside Bloom/Cuckoo saturation.
        self._set_filter_monitor()

    def _set_filter_monitor(self) -> None:
        """Prevent built-in filter callbacks from escaping the lifecycle lock."""
        if isinstance(self._filter, MemoryMembershipFilter):
            self._filter.set_monitor(NullMonitor())

    @contextmanager
    def _filter_operation_scope(self, request: object) -> Iterator[None]:
        """Serialize one filter callback and fence exact-request re-entry."""
        with self._filter_operation_lock:
            request_id = id(request)
            with self._lifecycle_condition:
                previous = self._active_filter_requests.get(request_id)
                self._active_filter_requests[request_id] = request
            try:
                yield
            finally:
                with self._lifecycle_condition:
                    if previous is None:
                        if self._active_filter_requests.get(request_id) is request:
                            del self._active_filter_requests[request_id]
                    else:
                        self._active_filter_requests[request_id] = previous

    def _set_lifecycle_state_locked(self, state: _LifecycleState) -> None:
        """Publish one lifecycle state while retaining legacy boolean mirrors."""
        self._lifecycle_state = state
        self._opened = state == _OPEN
        self._opening = state == _OPENING
        self._closing = state == _CLOSING
        self._closed = state == _CLOSED

    def _operation_unavailable_error(self, operation: str) -> RuntimeError:
        """Build a stable error for an operation rejected by admission."""
        return DupeFilterLifecycleUnavailable(operation, self._lifecycle_state)

    @contextmanager
    def _admit_operation(
        self,
        operation: str,
        *,
        reservation: _DedupReservation | _LegacyReservation | None = None,
    ) -> Iterator[_OperationLease | None]:
        """Admit one filter call or settle one exact old-epoch reservation."""
        with self._lifecycle_condition:
            if reservation is None:
                if self._lifecycle_state not in {_NEW, _OPEN}:
                    raise self._operation_unavailable_error(operation)
                epoch = self._reservation_epoch
            elif not self._reservation_is_current_locked(reservation):
                yield None
                return
            elif self._lifecycle_state not in {_NEW, _OPEN} and operation != "rollback":
                # A commit/forget arriving after a transition has stopped
                # admission must not call the old filter while clear/close/open
                # owns the callback boundary. The queued item remains safe to
                # replay; rollback is bookkeeping-only and may still settle.
                self._discard_any_reservation_locked(reservation)
                yield None
                return
            else:
                epoch = reservation.epoch
            owner = object()
            thread_id = get_ident()
            lease = _OperationLease(epoch, owner, thread_id)
            self._active_operations += 1
            self._active_operations_by_epoch[epoch] = (
                self._active_operations_by_epoch.get(epoch, 0) + 1
            )
            self._active_operation_threads[thread_id] = (
                self._active_operation_threads.get(thread_id, 0) + 1
            )
        try:
            yield lease
        finally:
            with self._lifecycle_condition:
                self._active_operations -= 1
                epoch_count = self._active_operations_by_epoch.get(lease.epoch, 0)
                if epoch_count <= 1:
                    self._active_operations_by_epoch.pop(lease.epoch, None)
                else:
                    self._active_operations_by_epoch[lease.epoch] = epoch_count - 1
                thread_count = self._active_operation_threads.get(lease.thread_id, 0)
                if thread_count <= 1:
                    self._active_operation_threads.pop(lease.thread_id, None)
                else:
                    self._active_operation_threads[lease.thread_id] = thread_count - 1
                self._lifecycle_condition.notify_all()

    def _reservation_is_current_locked(
        self,
        reservation: _DedupReservation | _LegacyReservation,
    ) -> bool:
        """Return whether a receipt is still owned by the current epoch."""
        if reservation.epoch != self._reservation_epoch:
            return False
        if isinstance(reservation, _DedupReservation):
            return self._active_reservations.get(id(reservation)) is reservation
        request = reservation.request_ref()
        if request is None:
            return False
        for table in (self._legacy_reservations, self._legacy_handoffs):
            receipts = table.get(id(request), ())
            if any(candidate is reservation for candidate in receipts):
                return True
        return False

    def _wait_for_quiescence_locked(
        self,
        *,
        include_reservations: bool,
        include_legacy_reservations: bool = True,
    ) -> None:
        """Wait until admitted calls and selected receipts have settled."""
        if self._active_operation_threads.get(get_ident(), 0):
            raise RuntimeError(
                "dupefilter lifecycle transition re-entered an active operation"
            )
        while self._active_operations or (
            include_reservations
            and (
                self._active_reservations
                or (include_legacy_reservations and self._legacy_reservations)
            )
        ):
            self._lifecycle_condition.wait()

    def _discard_any_reservation_locked(
        self,
        reservation: _DedupReservation | _LegacyReservation,
    ) -> None:
        """Discard either receipt kind at a lifecycle transition boundary."""
        if isinstance(reservation, _DedupReservation):
            self._discard_reservation(reservation)
        else:
            self._remove_legacy_reservation_locked(reservation)

    @staticmethod
    def _legacy_request_is(
        reservation: _LegacyReservation,
        request: object,
    ) -> bool:
        return reservation.request_ref() is request

    @staticmethod
    def _remove_receipt_from_table_locked(
        table: dict[int, deque[_LegacyReservation]],
        reservation: _LegacyReservation,
    ) -> bool:
        """Remove one identity-specific receipt from a weak receipt table."""
        request = reservation.request_ref()
        if request is None:
            return False
        request_id = id(request)
        receipts = table.get(request_id)
        if receipts is None:
            return False
        for index, candidate in enumerate(receipts):
            if candidate is reservation:
                del receipts[index]
                if not receipts:
                    table.pop(request_id, None)
                return True
        return False

    def _remove_legacy_reservation_locked(
        self,
        reservation: _LegacyReservation,
    ) -> None:
        self._remove_receipt_from_table_locked(self._legacy_reservations, reservation)
        self._remove_receipt_from_table_locked(self._legacy_handoffs, reservation)
        request = reservation.request_ref()
        if request is not None and id(request) not in self._legacy_reservations:
            self._pending_reservations.discard(request)
        if reservation.allowance_slot_reserved:
            # This counter is protected by the lifecycle lock.  It is deliberately
            # not released through _retry_allowance_lock here: all retry-state
            # operations use lifecycle -> retry lock order, never the reverse.
            reservation.allowance_slot_reserved = False
            if self._retry_allowance_slots_reserved:
                self._retry_allowance_slots_reserved -= 1
        self._lifecycle_condition.notify_all()

    def _legacy_request_collected(
        self,
        request_id: int,
        request_ref: ReferenceType[Any],
    ) -> None:
        """Drop weak legacy receipt state when Scrapy releases its Request."""
        with self._lifecycle_condition:
            if request_ref() is not None:
                retired = self._legacy_retired_requests
                request = request_ref()
                if request is not None and request_id == id(request):
                    retired.discard(request)
            for table in (self._legacy_reservations, self._legacy_handoffs):
                receipts = table.get(request_id)
                if receipts is None:
                    continue
                retained = deque(
                    receipt
                    for receipt in receipts
                    if receipt.request_ref is not request_ref
                )
                removed = len(receipts) - len(retained)
                if removed:
                    for receipt in receipts:
                        if (
                            receipt.request_ref is request_ref
                            and receipt.allowance_slot_reserved
                        ):
                            receipt.allowance_slot_reserved = False
                            if self._retry_allowance_slots_reserved:
                                self._retry_allowance_slots_reserved -= 1
                if retained:
                    table[request_id] = retained
                else:
                    table.pop(request_id, None)
            self._lifecycle_condition.notify_all()

    def _legacy_reservation_for_request_locked(
        self,
        request: object,
        *,
        thread_id: int | None = None,
        handoff_only: bool = False,
        pending_only: bool = False,
    ) -> _LegacyReservation | None:
        """Select one unsettled receipt without touching sibling invocations."""
        request_id = id(request)
        tables: tuple[dict[int, deque[_LegacyReservation]], ...]
        if handoff_only:
            tables = (self._legacy_handoffs,)
        elif pending_only:
            tables = (self._legacy_reservations,)
        else:
            # A consumed scheduler handoff belongs to its caller until it is
            # settled. Prefer that exact thread, then permit a legacy caller that
            # has no thread affinity to settle the oldest handoff.
            tables = (self._legacy_handoffs, self._legacy_reservations)
        for table in tables:
            for receipt in table.get(request_id, ()):
                if receipt.request_ref() is not request or receipt.settling:
                    continue
                if (
                    thread_id is not None
                    and table is self._legacy_handoffs
                    and receipt.consumer_thread_id not in (None, thread_id)
                ):
                    continue
                return receipt
        if not handoff_only and not pending_only:
            # If a different thread owns every handoff, do not accidentally settle
            # one of those receipts; the pending table remains a safe fallback only
            # for a not-yet-consumed direct request_seen receipt.
            return None
        return None

    def _new_legacy_reservation_locked(
        self,
        request: Request,
        fingerprint: bytes,
        fingerprint_text: str,
        *,
        allowance_slot_reserved: bool,
    ) -> _LegacyReservation:
        """Register one weak legacy receipt without retaining the Request."""
        request_id = id(request)

        def collected(dead_ref: ReferenceType[Any]) -> None:
            self._legacy_request_collected(request_id, dead_ref)

        try:
            request_ref: Callable[[], Any] = ref(request, collected)
        except Exception:  # noqa: BLE001 - retain cleanup ownership fail-open
            request_ref = _StrongReference(request)
        reservation = _LegacyReservation(
            fingerprint,
            self._reservation_epoch,
            object(),
            request_ref,
            fingerprint_text,
            allowance_slot_reserved=allowance_slot_reserved,
        )
        self._legacy_reservations.setdefault(request_id, deque()).append(reservation)
        self._pending_reservations.add(request)
        return reservation

    def _register_legacy_add_intent_locked(
        self,
        request: Request,
        encoded_fingerprint: bytes,
    ) -> _PendingLegacyAddIntent:
        """Record one pre-publish legacy admission intent under the lifecycle lock.

        The boolean protocol writes its marker before the invocation receipt is
        published. This registration is the legacy counterpart of the
        transactional owner intent: it gives the BaseException compensation the
        request identity and fingerprint needed to decide whether a marker may
        have been written without a receipt.
        """
        intent = _PendingLegacyAddIntent(encoded_fingerprint)
        self._legacy_add_intents.setdefault(id(request), []).append(intent)
        return intent

    def _retire_legacy_add_intent_locked(
        self,
        request: Request,
        intent: _PendingLegacyAddIntent | None,
    ) -> None:
        """Drop one published intent; its slot ownership has moved on.

        Runs in the same lifecycle-lock section as the receipt publication, so
        after this point a late interruption no longer compensates this
        invocation: the published receipt (or, for admissions without a marker,
        nothing) owns the recovery path.
        """
        if intent is None:
            return
        intent.slot_reserved = False
        intents = self._legacy_add_intents.get(id(request))
        if intents is None:
            return
        try:
            intents.remove(intent)
        except ValueError:
            # A lifecycle boundary already cleared the intent table; the
            # admission-slot ownership was retired with that generation.
            return
        if not intents:
            del self._legacy_add_intents[id(request)]

    def _emit_monitor(self, event: _MonitorEvent) -> None:
        """Dispatch one recorded hook outside locks and isolate ordinary errors."""
        hook_name, args, origin_request = event
        event_token = _MonitorFenceToken(get_ident(), "event_token")
        primary: BaseException | None = None
        monitor_failed = False
        try:
            with self._lifecycle_lock:
                request_id = id(origin_request)
                active = self._active_monitor_requests.get(request_id)
                if active is None or active[0]() is not origin_request:
                    active_tokens: set[_MonitorFenceToken] = set()
                    self._active_monitor_requests[request_id] = (
                        self._monitor_origin_ref(origin_request),
                        active_tokens,
                    )
                else:
                    active_tokens = active[1]
                active_tokens.add(event_token)
            try:
                hook: Callable[..., None] = getattr(self._monitor, hook_name)
                hook(*args)
            except Exception:  # noqa: BLE001 - telemetry must not alter dedup state
                monitor_failed = True
        except BaseException as exc:
            primary = exc
        cleanup_error: BaseException | None = None
        try:
            with self._lifecycle_lock:
                active = self._active_monitor_requests.get(id(origin_request))
                if active is not None and active[0]() is origin_request:
                    active[1].discard(event_token)
                    if not active[1]:
                        del self._active_monitor_requests[id(origin_request)]
        except BaseException as exc:
            cleanup_error = exc

        if cleanup_error is not None:
            # A custom mapping or asynchronous interruption may fail the normal
            # ``get`` path. Try direct indexing once so an origin Request that never
            # retries is not retained indefinitely; never remove another live token.
            try:
                active = self._active_monitor_requests[id(origin_request)]
                if active[0]() is origin_request:
                    active[1].discard(event_token)
                    if not active[1]:
                        del self._active_monitor_requests[id(origin_request)]
            except BaseException:
                pass
            if primary is None:
                raise cleanup_error
            try:
                logger.debug(
                    "Failed to clear monitor observer fence while preserving signal"
                )
            except BaseException:
                pass
        if primary is not None:
            raise primary
        if monitor_failed:
            # The monitor's caught exception has now unwound. A custom logging
            # handler therefore cannot inspect it through ``sys.exc_info()``.
            try:
                logger.debug("Dupefilter monitor hook raised; ignored")
            except BaseException:
                # The duplicate-filter decision and any reservation were already
                # linearized before monitor delivery. A broken logging handler must
                # not turn an advisory monitor failure into a false request failure.
                pass

    def _monitor_origin_ref(
        self,
        origin_request: object,
    ) -> Callable[[], object | None]:
        """Create a weak origin reference that removes an abandoned fence entry.

        Requests are weak-referenceable in normal Scrapy operation.  A custom
        request proxy or a hostile weakref/audit hook must not turn advisory
        monitor bookkeeping into a failed dedup decision, though, so retain a
        short-lived callable strong reference as a fail-open fallback.
        """
        request_id = id(origin_request)
        try:
            owner_ref: ReferenceType[BackendDupeFilter] | None = ref(self)
        except Exception:  # noqa: BLE001 - telemetry bookkeeping is fail-open
            owner_ref = None

        def remove_stale_origin(dead_ref: ReferenceType[object]) -> None:
            if owner_ref is None:
                return
            owner = owner_ref()
            if owner is None:
                return
            try:
                with owner._lifecycle_lock:
                    active = owner._active_monitor_requests.get(request_id)
                    if active is not None and active[0] is dead_ref:
                        del owner._active_monitor_requests[request_id]
            except BaseException:
                # A weakref callback runs during arbitrary interpreter teardown;
                # losing this optional cleanup is safer than raising from GC.
                pass

        try:
            return ref(origin_request, remove_stale_origin)
        except Exception:  # noqa: BLE001 - fail open for hostile object proxies
            return _StrongReference(origin_request)

    def _queue_monitor_events_unlocked(
        self,
        origin_request: object,
        pending_events: list[_PendingMonitorEvent],
    ) -> tuple[_MonitorFenceToken | None, bool]:
        """Append one complete telemetry batch while lifecycle state is locked."""
        monitor_events: list[_MonitorEvent] = [
            (hook_name, args, origin_request) for hook_name, args in pending_events
        ]
        should_warn_overflow = False
        if len(self._monitor_events) + len(monitor_events) <= self._monitor_event_limit:
            self._monitor_events.extend(monitor_events)
        elif not self._monitor_overflow_warned:
            self._monitor_overflow_warned = True
            should_warn_overflow = True
        drain_token: _MonitorFenceToken | None = None
        if (
            self._monitor_drain_token is not None
            and not self._monitor_drain_token.active
        ):
            self._monitor_drain_token = None
        if self._monitor_events and self._monitor_drain_token is None:
            # The caller assigns the returned token to its ``drain_token`` local
            # before releasing ``_lifecycle_lock``. Liveness therefore follows the
            # complete operation frame without a fallible finally/cleanup window.
            drain_token = _MonitorFenceToken(get_ident(), "drain_token")
            self._monitor_drain_token = drain_token
        return drain_token, should_warn_overflow

    def _dispatch_queued_monitor_events(
        self,
        drain_token: _MonitorFenceToken | None,
        should_warn_overflow: bool,
    ) -> None:
        """Run an elected telemetry drain outside lifecycle locks."""
        if should_warn_overflow:
            self._warn_monitor_overflow()
        if drain_token is not None:
            self._drain_monitor_events(drain_token)

    @staticmethod
    def _emit_continuation_diagnostics(
        diagnostics: list[_ContinuationDiagnostic],
    ) -> None:
        """Emit fixed degradation warnings after their failure suites unwind."""
        for diagnostic in diagnostics:
            try:
                if diagnostic == "filter_full":
                    logger.warning(
                        "Dedup membership filter is full (filter_full); degrading — overflow "
                        "requests will be treated as not-seen and may re-fetch. Increase the "
                        "filter capacity or switch to an exact dedup strategy "
                        "(SCRAPY_DEDUP_STRATEGY=set). This filter_full warning fires once per "
                        "process; subsequent overflows are counted via the "
                        "dupefilter/filter_full stat only."
                    )
                elif diagnostic == "backend_error":
                    logger.warning(
                        "Dedup backend transiently unavailable; degrading — requests will be "
                        "treated as not-seen and may re-fetch until the backend recovers. This "
                        "warning fires once per process; subsequent transient backend errors "
                        "are counted via the errors/dedup stat only."
                    )
                elif diagnostic == "forget_backend_error":
                    logger.warning(
                        "Dedup backend transiently unavailable while compensating a failed "
                        "queue push; the remote marker may survive. A one-shot retry "
                        "allowance was granted so the fingerprint can be re-crawled once. "
                        "This warning fires once per process; subsequent failures during "
                        "forget are counted via the errors/dedup stat only."
                    )
                else:
                    logger.warning(
                        "Dedup retry allowance capacity is exhausted; backpressuring new "
                        "marker admission instead of evicting failed-work recovery state."
                    )
            except BaseException:
                # The graceful-miss outcome was already selected before reporting it.
                # A logging handler cannot turn the advisory warning into a crawl error.
                pass

    def _drain_monitor_events(self, token: _MonitorFenceToken) -> None:
        """Drain the event-enqueue-ordered FIFO as its sole consumer."""
        while True:
            with self._lifecycle_lock:
                if self._monitor_drain_token is not token:
                    return
                if not self._monitor_events:
                    self._monitor_drain_token = None
                    return
                event = self._monitor_events.popleft()
            self._emit_monitor(event)

    def _warn_monitor_overflow(self) -> None:
        """Log the bounded best-effort drop once without changing a decision."""
        try:
            logger.warning(
                "Duplicate-filter monitor backlog would exceed the %s-event limit; "
                "dropping complete telemetry batches until the active drainer catches "
                "up. Duplicate-filter decisions are unaffected.",
                self._monitor_event_limit,
            )
        except BaseException:
            # This warning is emitted after the queue decision has been linearized.
            # Logging handlers are outside the dedup control plane, including when
            # they raise KeyboardInterrupt/SystemExit themselves.
            return

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        connection_manager: ConnectionManager | None = None,
        fingerprinter: _Fingerprinter | None = None,
        monitor: Monitor | None = None,
        owns_connection_manager: bool | None = None,
        key_override: str | None = None,
    ) -> BackendDupeFilter:
        """Create dupefilter from Scrapy settings.

        Backend selection: ``SCRAPY_SET_BACKEND_TYPE`` /
        ``SCRAPY_SET_BACKEND_SETTINGS`` override the global
        ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the dedup set
        can bind to a different backend than the queue or storage pipeline
        (multi-backend coexistence). Unset → falls back to the global keys.

        Args:
            settings: Scrapy settings object.

        Returns:
            A new BackendDupeFilter instance.
        """
        from scrapy_extension.backends.connectors import (
            ConnectionManager,
            resolve_backend_config,
        )
        from scrapy_extension.dupefilter.filters.bloom_filter import (
            _BloomFilterAllocationError,
        )
        from scrapy_extension.dupefilter.filters.cuckoo_filter import (
            _CuckooFingerprintSizeError,
        )
        from scrapy_extension.dupefilter.filters.factory import (
            DedupeStrategy,
            build_membership_filter,
        )

        raw_strategy = settings.get("SCRAPY_DEDUP_STRATEGY", DedupeStrategy.SET.value)
        if raw_strategy is None or raw_strategy == "":
            raw_strategy = DedupeStrategy.SET.value
        try:
            strategy = DedupeStrategy(raw_strategy)
        except ValueError as e:
            valid = ", ".join(repr(m.value) for m in DedupeStrategy)
            raise ConfigurationError(
                f"Invalid SCRAPY_DEDUP_STRATEGY {raw_strategy!r}. Valid: {valid}.",
                setting_name="SCRAPY_DEDUP_STRATEGY",
                setting_value=str(raw_strategy),
            ) from e
        manager = connection_manager if strategy is DedupeStrategy.SET else None
        manager_lease = None
        owns_manager = (
            owns_connection_manager if owns_connection_manager is not None else True
        )
        if strategy is DedupeStrategy.SET and manager is None:
            backend_type, backend_settings = resolve_backend_config(
                settings,
                type_key="SCRAPY_SET_BACKEND_TYPE",
                settings_key="SCRAPY_SET_BACKEND_SETTINGS",
                required_capabilities={"set"},
                component_name="set",
            )
            manager_lease = ConnectionManager.acquire_lease(
                backend_type=backend_type,
                settings=backend_settings,
            )
            manager = manager_lease.manager
        membership_filter: MembershipFilter | None = None
        factory_failure: BaseException | None = None
        try:
            key = (
                settings.get(
                    "SCRAPY_DUPEFILTER_KEY",
                    DEFAULT_DUPEFILTER_KEY_TEMPLATE,
                )
                if key_override is None
                else key_override
            )
            # getpriority() distinguishes an absent setting from an explicitly stored
            # None; Settings.get(name, default) intentionally treats both alike.
            memory_maxsize = (
                settings.get("SCRAPY_DEDUP_MEMORY_MAXSIZE")
                if settings.getpriority("SCRAPY_DEDUP_MEMORY_MAXSIZE") is not None
                else DEFAULT_MEMORY_MAXSIZE
            )
            if memory_maxsize is not None:
                memory_maxsize = parse_int_setting(
                    memory_maxsize,
                    "SCRAPY_DEDUP_MEMORY_MAXSIZE",
                    minimum=1,
                )
            bloom_capacity = parse_int_setting(
                settings.get("SCRAPY_DEDUP_BLOOM_CAPACITY", 1_000_000),
                "SCRAPY_DEDUP_BLOOM_CAPACITY",
                minimum=1,
            )
            bloom_error_rate = parse_float_setting(
                settings.get("SCRAPY_DEDUP_BLOOM_ERROR_RATE", 0.001),
                "SCRAPY_DEDUP_BLOOM_ERROR_RATE",
                minimum=0.0,
                maximum=1.0,
                minimum_exclusive=True,
                maximum_exclusive=True,
            )
            cuckoo_capacity = parse_int_setting(
                settings.get("SCRAPY_DEDUP_CUCKOO_CAPACITY", 1_000_000),
                "SCRAPY_DEDUP_CUCKOO_CAPACITY",
                minimum=1,
            )
            cuckoo_error_rate = parse_float_setting(
                settings.get("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", 0.001),
                "SCRAPY_DEDUP_CUCKOO_ERROR_RATE",
                minimum=0.0,
                maximum=1.0,
                minimum_exclusive=True,
                maximum_exclusive=True,
            )
            strict = get_bool_setting(
                settings,
                "SCRAPY_DEDUP_STRICT",
            )
            debug = get_bool_setting(
                settings,
                "DUPEFILTER_DEBUG",
            )
            clear_on_open = get_bool_setting(
                settings,
                "SCRAPY_DUPEFILTER_CLEAR_ON_OPEN",
            )
            if not isinstance(key, str):
                raise ConfigurationError(
                    f"SCRAPY_DUPEFILTER_KEY must be a string, got {key!r}.",
                    setting_name="SCRAPY_DUPEFILTER_KEY",
                    setting_value=key,
                )
            try:
                _validate_key_name(
                    key.replace("{spider}", "spider").replace("{project}", "project"),
                    "SCRAPY_DUPEFILTER_KEY",
                )
            except ValueError as exc:
                raise ConfigurationError(
                    str(exc),
                    setting_name="SCRAPY_DUPEFILTER_KEY",
                    setting_value=key,
                ) from exc
            try:
                membership_filter = build_membership_filter(
                    strategy,
                    manager,
                    key=key,
                    memory_maxsize=memory_maxsize,
                    bloom_capacity=bloom_capacity,
                    bloom_error_rate=bloom_error_rate,
                    cuckoo_capacity=cuckoo_capacity,
                    cuckoo_error_rate=cuckoo_error_rate,
                    strict=strict,
                )
            except ConfigurationError:
                raise
            except _BloomFilterAllocationError as exc:
                # Sizing depends on both Bloom knobs. Attribute a budget failure
                # to the sole explicit knob when possible; if both were supplied,
                # capacity remains the actionable allocation-control setting.
                capacity_setting = "SCRAPY_DEDUP_BLOOM_CAPACITY"
                error_rate_setting = "SCRAPY_DEDUP_BLOOM_ERROR_RATE"
                constructor_setting = (
                    error_rate_setting
                    if settings.getpriority(capacity_setting) is None
                    and settings.getpriority(error_rate_setting) is not None
                    else capacity_setting
                )
                raise ConfigurationError(
                    f"Invalid {constructor_setting}: {exc}",
                    setting_name=constructor_setting,
                    setting_value=settings.get(constructor_setting),
                ) from exc
            except _CuckooFingerprintSizeError as exc:
                constructor_setting = "SCRAPY_DEDUP_CUCKOO_ERROR_RATE"
                raise ConfigurationError(
                    f"Invalid {constructor_setting}: {exc}",
                    setting_name=constructor_setting,
                    setting_value=settings.get(constructor_setting),
                ) from exc
            except (TypeError, ValueError, OverflowError) as exc:
                constructor_setting = {
                    DedupeStrategy.MEMORY: "SCRAPY_DEDUP_MEMORY_MAXSIZE",
                    DedupeStrategy.BLOOM: "SCRAPY_DEDUP_BLOOM_CAPACITY",
                    DedupeStrategy.CUCKOO: "SCRAPY_DEDUP_CUCKOO_CAPACITY",
                }.get(strategy, "SCRAPY_DEDUP_STRATEGY")
                raise ConfigurationError(
                    f"Invalid {constructor_setting}: {exc}",
                    setting_name=constructor_setting,
                    setting_value=settings.get(constructor_setting),
                ) from exc
            return cls(
                connection_manager=manager,
                key=key,
                debug=debug,
                fingerprinter=fingerprinter,
                membership_filter=membership_filter,
                monitor=monitor,
                clear_on_open=clear_on_open,
                owns_connection_manager=owns_manager,
                connection_manager_lease=manager_lease,
            )
        except BaseException as exc:
            factory_failure = exc
        assert factory_failure is not None
        cleanup_error = _cleanup_factory_filter_and_manager(
            membership_filter,
            manager,
            manager_lease,
            owns_manager=owns_manager,
        )
        if cleanup_error is not None:
            try:
                logger.error("Failed to clean up dupefilter after factory failure")
            except BaseException:
                pass
        raise factory_failure

    def _abort_factory_failure(self) -> BaseException | None:
        """Close an unpublished candidate without hiding its factory error."""
        return _cleanup_factory_filter_and_manager(
            self._filter,
            self.connection_manager,
            self._connection_manager_lease,
            owns_manager=self._owns_connection_manager,
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> BackendDupeFilter:
        """Create dupefilter from crawler.

        Threads ``crawler.request_fingerprinter`` so the dupefilter honors a
        configured ``REQUEST_FINGERPRINTER_CLASS`` (otherwise fingerprints are
        byte-identical to the default — see ``__init__``).

        Args:
            crawler: The Scrapy crawler instance.

        Returns:
            A new BackendDupeFilter instance.
        """
        dupefilter = cls.from_settings(crawler.settings)
        factory_failure: BaseException | None = None
        try:
            dupefilter._fingerprinter = getattr(crawler, "request_fingerprinter", None)
            # Default-on observability: wire a ScrapyStatsMonitor when crawler.stats is
            # available so dedup hit/miss counts show up on the Scrapy stats dump
            # without an explicit ``monitor=`` kwarg. Additive — existing stats untouched.
            stats = getattr(crawler, "stats", None)
            if stats is not None:
                dupefilter._monitor = ScrapyStatsMonitor(stats)
                dupefilter._set_filter_monitor()
                # R25-F: thread the monitor into the dedup ConnectionManager when
                # this strategy has one. In-process filters emit their own hooks and
                # intentionally acquire no backend manager.
                if dupefilter.connection_manager is not None:
                    dupefilter.connection_manager.set_monitor(dupefilter._monitor)
            return dupefilter
        except BaseException as exc:
            factory_failure = exc
        assert factory_failure is not None
        cleanup_error = dupefilter._abort_factory_failure()
        if cleanup_error is not None:
            try:
                logger.error(
                    "Failed to clean up dupefilter after crawler factory failure"
                )
            except BaseException:
                pass
        raise factory_failure

    def open(self, spider: Spider | None = None) -> None:
        """Open one filter generation without admitting concurrent requests."""
        open_owner = _MonitorFenceToken(get_ident(), "open_owner")
        with self._lifecycle_condition:
            if self._lifecycle_state in {_CLOSED, _CLOSING}:
                raise RuntimeError("dupefilter is closing or closed")
            if self._lifecycle_state in {_OPENING, _CLEARING}:
                current_owner = self._open_owner_token
                if current_owner is not None and current_owner.active:
                    raise RuntimeError("dupefilter open is already in progress")
                raise RuntimeError(
                    "dupefilter lifecycle transition is already in progress"
                )
            if self._lifecycle_state == _OPEN:
                if spider is self._opened_spider:
                    return
                raise RuntimeError("dupefilter is already open for a different spider")
            self._set_lifecycle_state_locked(_OPENING)
            self._open_owner_token = open_owner
            self._close_requested = False
            try:
                # Drain every call admitted before the opening boundary before
                # retiring its local receipts.  Otherwise a request finishing
                # between the reset and the wait could publish an old-generation
                # reservation after the reset and let a late commit affect the new
                # run.
                self._wait_for_quiescence_locked(include_reservations=False)
                self._clear_retry_allowances()
            except BaseException:
                self._open_owner_token = None
                self._set_lifecycle_state_locked(_NEW)
                raise

        open_failure: BaseException | None = None
        try:
            if spider is not None:
                _validate_key_name(spider.name, field_name="spider.name")
                self._resolve_spider_key(spider)
            with self._filter_operation_lock:
                self._filter.open()
                if self.clear_on_open:
                    self._filter.clear()
        except BaseException as exc:
            open_failure = exc

        if open_failure is None and self.clear_on_open:
            with self._lifecycle_condition:
                # clear-on-open is a successful clear boundary, so retire all
                # pre-open receipts and local shadow state only after the filter
                # callback has completed.  The opening boundary already advanced
                # the epoch; avoid a second generation bump for the successful
                # clear while still dropping any local state produced by setup.
                self._clear_retry_allowances(
                    advance_epoch=False,
                    clear_reservations=True,
                )

        if open_failure is not None:
            cleanup_failed = False
            reserved = False
            try:
                with self._lifecycle_condition:
                    reserved = self._reserve_release_locked(
                        self._direct_release_owner,
                        get_ident(),
                        opening_owner=open_owner,
                    )
                if reserved:
                    self._run_reserved_release()
            except BaseException:
                cleanup_failed = True
            if cleanup_failed:
                try:
                    logger.error("Failed to clean up dupefilter after open failure")
                except BaseException:
                    pass
            raise open_failure

        with self._lifecycle_condition:
            if self._open_owner_token is not open_owner:
                raise RuntimeError("dupefilter open ownership changed")
            self._opened_spider = spider
            self._open_owner_token = None
            self._set_lifecycle_state_locked(_OPEN)
            self._lifecycle_condition.notify_all()

    def _open_authoritative_async(
        self,
        spider: Spider | None = None,
        *,
        timeout: float,
    ) -> tuple[Deferred[None], Deferred[None]]:
        """Return authoritative and bounded views of synchronous ``open``."""
        from scrapy_extension.utils.reactor import defer_to_thread_ordered

        return defer_to_thread_ordered(
            self.open,
            spider,
            timeout=timeout,
            operation="dupefilter open",
        )

    def open_async(
        self,
        spider: Spider | None = None,
        *,
        timeout: float = 5.0,
    ) -> Deferred[None]:
        """Open off-reactor and return only the caller-bounded Deferred."""
        operation, bounded = self._open_authoritative_async(
            spider,
            timeout=timeout,
        )
        try:
            operation.addErrback(lambda _failure: None)
        except BaseException:
            # The operation remains authoritative even when a provider-specific
            # Deferred rejects this best-effort observer registration.
            pass
        return bounded

    def _clear_authoritative_async(
        self, *, timeout: float
    ) -> tuple[Deferred[None], Deferred[None]]:
        """Return authoritative and bounded views of synchronous ``clear``."""
        from scrapy_extension.utils.reactor import defer_to_thread_ordered

        return defer_to_thread_ordered(
            self.clear,
            timeout=timeout,
            operation="dupefilter clear",
        )

    def clear_async(self, *, timeout: float = 5.0) -> Deferred[None]:
        """Clear off-reactor and return only the caller-bounded Deferred."""
        operation, bounded = self._clear_authoritative_async(timeout=timeout)
        try:
            operation.addErrback(lambda _failure: None)
        except BaseException:
            # The operation remains authoritative even when a provider-specific
            # Deferred rejects this best-effort observer registration.
            pass
        return bounded

    def _release_authoritative_async(
        self,
        owner_token: object,
        reason: str,
        *,
        timeout: float,
    ) -> tuple[Deferred[None], Deferred[None]]:
        """Return authoritative and bounded views of one exact release."""
        from scrapy_extension.utils.reactor import defer_to_thread_ordered

        return defer_to_thread_ordered(
            self.release,
            owner_token,
            reason,
            timeout=timeout,
            operation="dupefilter close",
        )

    def release_async(
        self,
        owner_token: object,
        reason: str,
        *,
        timeout: float = 5.0,
    ) -> Deferred[None]:
        """Release off-reactor and return only the caller-bounded Deferred."""
        operation, bounded = self._release_authoritative_async(
            owner_token,
            reason,
            timeout=timeout,
        )
        try:
            operation.addErrback(lambda _failure: None)
        except BaseException:
            # The operation remains authoritative even when a provider-specific
            # Deferred rejects this best-effort observer registration.
            pass
        return bounded

    def _close_authoritative_async(
        self,
        reason: str,
        *,
        timeout: float,
    ) -> tuple[Deferred[None], Deferred[None]]:
        """Return authoritative and bounded views of direct ``close``."""
        from scrapy_extension.utils.reactor import defer_to_thread_ordered

        return defer_to_thread_ordered(
            self.close,
            reason,
            timeout=timeout,
            operation="dupefilter close",
        )

    def _resolve_spider_key(self, spider: Spider) -> None:
        """Substitute identity placeholders in :attr:`key`, propagating
        to the underlying membership filter.

        No-op when the key does not contain the placeholder. Only backend-backed
        filters (those exposing a mutable ``key`` attribute, e.g.
        :class:`SetMembershipFilter`) receive the propagated update; in-process
        filters (memory/bloom/cuckoo) ignore the key entirely, so the placeholder
        has no effect there — consistent with their per-process scope.
        """
        if "{spider}" not in self.key and "{project}" not in self.key:
            return
        templated = self.key
        resolved = resolve_identity_template(
            templated,
            spider_name=spider.name,
            project_name=project_name_from_spider(spider),
        )
        try:
            _validate_key_name(resolved, "SCRAPY_DUPEFILTER_KEY")
        except ValueError as exc:
            raise ConfigurationError(
                str(exc),
                setting_name="SCRAPY_DUPEFILTER_KEY",
                setting_value=templated,
            ) from exc
        self.key = resolved
        # Propagate to backend-backed filters that expose a writable ``key``. The
        # filter was built from the same templated key, so only rewrite when its
        # key still equals the templated form — a caller who passed a custom
        # filter with a different key is not silently overwritten.
        if getattr(self._filter, "key", None) == templated:
            self._filter.key = resolved  # type: ignore[attr-defined]

    def close(self, reason: str) -> None:
        """Close the dupefilter and its membership filter."""
        self.release(self._direct_release_owner, reason)

    def release(self, owner_token: object, reason: str) -> None:
        """Retryably close for one exact owner without holding lifecycle locks."""
        del reason
        with self._lifecycle_lock:
            reserved = self._reserve_release_locked(owner_token, get_ident())
        if reserved:
            self._run_reserved_release()

    def _authorize_release_owner_alias(self, owner_token: object) -> None:
        """Authorize one composite owner to resume direct-owner cleanup.

        Registration is identity-based and intentionally private: it grants only
        the supplied stable token access to this lifecycle's release reservation.
        """
        with self._lifecycle_lock:
            if self._closed:
                return
            if any(alias is owner_token for alias in self._release_owner_aliases):
                return
            self._release_owner_aliases.append(owner_token)

    def _release_owners_are_aliases(self, first: object, second: object) -> bool:
        """Return whether two tokens are the direct owner and an authorized alias."""
        if first is second:
            return True
        aliases = self._release_owner_aliases
        return (
            first is self._direct_release_owner
            and any(alias is second for alias in aliases)
        ) or (
            second is self._direct_release_owner
            and any(alias is first for alias in aliases)
        )

    def _reserve_release_locked(
        self,
        owner_token: object,
        thread_id: int,
        *,
        opening_owner: _MonitorFenceToken | None = None,
    ) -> bool:
        """Atomically stop admission and reserve one close attempt."""
        if self._lifecycle_state == _CLOSED:
            return False
        if self._lifecycle_state == _OPENING:
            open_owner = self._open_owner_token
            if opening_owner is not None and open_owner is opening_owner:
                # A failed opener transfers its still-live transition directly to
                # cleanup. No peer can observe an idle lifecycle between them.
                self._open_owner_token = None
            elif open_owner is not None and open_owner.active:
                raise RuntimeError("dupefilter open is already in progress")
            else:
                # The opening frame has unwound without terminal publication.
                # Reclaim package state for close without replaying filter.open().
                self._open_owner_token = None
        if (
            self._lifecycle_transition_thread_id == thread_id
            and self._clear_in_progress
        ):
            raise RuntimeError("dupefilter close re-entered an active clear")
        if self._active_operation_threads.get(thread_id, 0):
            raise RuntimeError("dupefilter close re-entered an active operation")
        if self._release_owner_token is None:
            self._release_owner_token = owner_token
        elif not self._release_owners_are_aliases(
            self._release_owner_token, owner_token
        ):
            raise RuntimeError("dupefilter close is owned by another caller")
        if self._release_in_progress:
            if self._release_thread_id == thread_id:
                return False
            raise RuntimeError("dupefilter close is already in progress")
        self._release_in_progress = True
        self._release_thread_id = thread_id
        self._close_requested = self._lifecycle_state == _CLEARING
        self._opened_spider = None
        self._set_lifecycle_state_locked(_CLOSING)
        self._lifecycle_condition.notify_all()
        return True

    def _run_reserved_release(self) -> None:
        """Execute a previously reserved close attempt outside the lifecycle lock."""
        try:
            self._close_locked()
        finally:
            with self._lifecycle_condition:
                self._release_in_progress = False
                self._release_thread_id = None
                self._lifecycle_condition.notify_all()

    def _close_locked(self) -> None:
        """Run reserved filter/manager callbacks outside ``_lifecycle_lock``."""
        with self._lifecycle_condition:
            while self._clear_in_progress:
                if self._lifecycle_transition_thread_id == get_ident():
                    raise RuntimeError("dupefilter close re-entered an active clear")
                self._lifecycle_condition.wait()
            self._wait_for_quiescence_locked(include_reservations=False)
            # Retire the old epoch only after every admitted filter call has
            # returned. Late commits/forgets now become
            # harmless no-ops instead of mutating the next generation.
            self._clear_retry_allowances()
        primary_error: BaseException | None = None
        secondary_release_failed = False
        if not self._filter_released:
            try:
                self._filter.close()
            except BaseException as exc:
                primary_error = exc
            else:
                with self._lifecycle_lock:
                    self._filter_released = True
        if (
            self._filter_released
            and self._owns_connection_manager
            and self.connection_manager is not None
            and not self._manager_released
        ):
            try:
                if self._connection_manager_lease is not None:
                    self._connection_manager_lease.release()
                else:
                    self.connection_manager.close()
            except BaseException as exc:
                if primary_error is None:
                    primary_error = exc
                else:
                    # Defer logging until the exception handler has unwound: a
                    # custom handler may inspect sys.exc_info() even when the
                    # LogRecord itself has no explicit exc_info payload.
                    secondary_release_failed = True
            else:
                with self._lifecycle_lock:
                    self._manager_released = True
        if secondary_release_failed:
            try:
                logger.error(
                    "ConnectionManager close failed while preserving filter close error"
                )
            except BaseException:
                pass
        with self._lifecycle_lock:
            if self._filter_released and (
                not self._owns_connection_manager
                or self.connection_manager is None
                or self._manager_released
            ):
                self._close_requested = False
                self._set_lifecycle_state_locked(_CLOSED)
                # Terminal close has no future monitor dispatch. Drop retained
                # Request references and invalidate any interrupted drainer.
                self._monitor_events.clear()
                self._monitor_drain_token = None
        if primary_error is not None:
            raise primary_error

    def clear(self) -> None:
        """Clear one generation after admission and receipt quiescence."""
        with self._lifecycle_condition:
            if self._lifecycle_state in {_CLOSED, _CLOSING}:
                raise RuntimeError("dupefilter is closing or closed")
            if self._lifecycle_state in {_OPENING, _CLEARING}:
                raise RuntimeError(
                    "dupefilter lifecycle transition is already in progress"
                )
            prior_state = self._lifecycle_state
            self._set_lifecycle_state_locked(_CLEARING)
            self._clear_in_progress = True
            self._lifecycle_transition_thread_id = get_ident()
            succeeded = False
            try:
                self._wait_for_quiescence_locked(include_reservations=False)
            except BaseException:
                self._clear_in_progress = False
                self._lifecycle_transition_thread_id = None
                self._set_lifecycle_state_locked(prior_state)
                self._lifecycle_condition.notify_all()
                raise

        try:
            with self._filter_operation_lock:
                self._filter.clear()
            succeeded = True
        finally:
            with self._lifecycle_condition:
                if succeeded:
                    # Publish only after remote clear succeeds; every receipt from
                    # the old epoch has already settled at this point.
                    self._clear_retry_allowances()
                self._clear_in_progress = False
                self._lifecycle_transition_thread_id = None
                if self._close_requested:
                    self._set_lifecycle_state_locked(_CLOSING)
                else:
                    self._set_lifecycle_state_locked(prior_state)
                self._lifecycle_condition.notify_all()

    def log(self, request: Request, spider: Spider) -> None:
        """Log a filtered request.

        Args:
            request: The filtered request.
            spider: The spider instance.
        """
        if self.debug:
            try:
                logger.debug(
                    "Filtered duplicate request: %s",
                    request.url,
                    extra={"spider": spider},
                )
            except BaseException:
                # Scrapy calls this only after the duplicate decision has already
                # been returned. A broken logging handler must not retroactively turn
                # that completed decision into a false processing failure.
                pass

    def request_seen(self, request: Request) -> bool:
        """Check a request through Scrapy's boolean duplicate-filter contract."""
        decision = self._request_seen_decision(
            request,
            transactional=False,
        )
        return decision.seen

    # Preserve the original stable hook identity so the scheduler can detect a
    # direct class-level monkeypatch. Instance and subclass overrides already
    # have closer declaration ranks; this also covers tests/integrations that
    # patch ``BackendDupeFilter.request_seen`` on the class itself.
    _atomic_protocol_request_seen = request_seen

    def request_seen_with_reservation(
        self,
        request: Request,
        owner: object | None = None,
    ) -> DedupDecision:
        """Return one invocation's transactional scheduler decision.

        The caller supplies a unique owner intent before entering this method. A
        read-only reservation is published against that intent before consulting
        membership, closing the callee-return/caller-assignment interruption
        window. No marker is recorded for a miss yet: a failed push calls
        :meth:`rollback_reservation`, while a durable push calls
        :meth:`commit_reservation`. The public :meth:`request_seen` API remains
        Scrapy's boolean contract.

        Returns:
            An atomic decision containing seen state, an optional opaque rollback
            receipt, and monitor-observer status.
        """
        if owner is None:
            owner = object()
        return self._request_seen_decision(
            request,
            transactional=True,
            owner=owner,
        )

    def _request_seen_decision(
        self,
        request: Request,
        *,
        transactional: bool,
        owner: object | None = None,
    ) -> DedupDecision:
        """Admit one decision, call the filter unlocked, then publish telemetry."""
        pending_monitor_events: list[_PendingMonitorEvent] = []
        pending_diagnostics: list[_ContinuationDiagnostic] = []
        drain_token: _MonitorFenceToken | None = None
        should_warn_overflow = False
        reservation: _DedupReservation | None = None
        published_owner: object | None = None
        legacy_slot_reserved = False
        legacy_add_intent: _PendingLegacyAddIntent | None = None
        seen = False
        try:
            with self._admit_operation("request_seen") as operation:
                assert operation is not None  # nosec B101
                try:
                    with self._lifecycle_condition:
                        fingerprint = self.request_fingerprint(request)
                        encoded_fingerprint = fingerprint.encode()

                        request_id = id(request)
                        active_monitor = self._active_monitor_requests.get(request_id)
                        if (
                            active_monitor is not None
                            and active_monitor[0]() is request
                        ):
                            active_tokens = active_monitor[1]
                            for stale_token in tuple(active_tokens):
                                if not stale_token.active:
                                    active_tokens.discard(stale_token)
                            if active_tokens:
                                return DedupDecision(seen=True, observational=True)
                            del self._active_monitor_requests[request_id]
                        elif active_monitor is not None:
                            # Dead weak origin or recycled object id: stale telemetry state
                            # must never suppress an unrelated Request.
                            del self._active_monitor_requests[request_id]

                        if self._active_filter_requests.get(request_id) is request:
                            return DedupDecision(seen=True, observational=True)

                        if transactional:
                            assert owner is not None  # nosec B101
                            existing = self._reservations_by_owner.get(id(owner))
                            if existing is not None and existing.owner is owner:
                                raise RuntimeError(
                                    "duplicate-filter owner intent is already active"
                                )
                            reservation = _DedupReservation(
                                encoded_fingerprint,
                                operation.epoch,
                                owner,
                                request,
                                fingerprint,
                            )
                            self._active_reservations[id(reservation)] = reservation
                            self._reservations_by_owner[id(owner)] = reservation
                            published_owner = owner
                        else:
                            # Pre-publish intent for the legacy boolean protocol:
                            # a BaseException between the marker write and the
                            # receipt publication is compensated from this
                            # registration (see
                            # ``_compensate_interrupted_legacy_add_locked``),
                            # mirroring the transactional owner-intent protocol.
                            legacy_add_intent = self._register_legacy_add_intent_locked(
                                request,
                                encoded_fingerprint,
                            )

                    # The filter/backend callback is deliberately outside the lifecycle
                    # lock. The separate operation lock keeps local filter strategies
                    # thread-safe without preventing lifecycle admission from draining.
                    with self._filter_operation_scope(request):
                        if transactional:
                            seen = self._request_seen_for_scheduler_unlocked(
                                fingerprint,
                                encoded_fingerprint,
                                pending_monitor_events,
                                pending_diagnostics,
                            )
                            reservation_state: Literal["added", "allowance"] | None = (
                                None
                            )
                        else:
                            assert legacy_add_intent is not None  # nosec B101
                            seen, reservation_state, legacy_slot_reserved = (
                                self._request_seen_unlocked(
                                    request,
                                    fingerprint,
                                    encoded_fingerprint,
                                    pending_monitor_events,
                                    pending_diagnostics,
                                    intent=legacy_add_intent,
                                )
                            )

                    with self._lifecycle_condition:
                        if transactional and seen:
                            assert reservation is not None  # nosec B101
                            self._discard_reservation(reservation)
                            reservation = None
                        elif not transactional:
                            if reservation_state is not None:
                                self._new_legacy_reservation_locked(
                                    request,
                                    encoded_fingerprint,
                                    fingerprint,
                                    allowance_slot_reserved=legacy_slot_reserved,
                                )
                                legacy_slot_reserved = False
                            self._retire_legacy_add_intent_locked(
                                request,
                                legacy_add_intent,
                            )
                        drain_token, should_warn_overflow = (
                            self._queue_monitor_events_unlocked(
                                request,
                                pending_monitor_events,
                            )
                        )
                except BaseException:
                    with self._lifecycle_condition:
                        if transactional:
                            self._compensate_interrupted_decision(
                                reservation,
                                published_owner,
                            )
                        else:
                            self._compensate_interrupted_legacy_add_locked(
                                request,
                                legacy_add_intent,
                            )
                    raise

            self._emit_continuation_diagnostics(pending_diagnostics)
            self._dispatch_queued_monitor_events(
                drain_token,
                should_warn_overflow,
            )
        except BaseException:
            # Preserve a published legacy receipt when process-control telemetry
            # interrupts delivery.  Scrapy/custom callers may still consume and
            # settle it after handling the signal; silently compensating here would
            # make the public boolean protocol lose that retry handoff.
            raise

        return DedupDecision(
            seen=seen,
            reservation=reservation,
        )

    def _request_seen_unlocked(
        self,
        request: Request,
        fingerprint: str,
        encoded_fingerprint: bytes,
        monitor_events: list[_PendingMonitorEvent],
        diagnostics: list[_ContinuationDiagnostic],
        intent: _PendingLegacyAddIntent,
    ) -> tuple[bool, Literal["added", "allowance"] | None, bool]:
        """Check if a request has been seen before.

        Args:
            request: The request to check.
            intent: This invocation's pre-publish intent. ``slot_reserved`` is
                maintained at every admission-slot ownership change so the
                caller's BaseException compensation can decide exactly whether
                a marker may have been written without a receipt.

        Returns:
            ``(seen, reservation_state, slot_reserved)`` for this invocation only.
        """
        del request

        if encoded_fingerprint in self._volatile_fingerprints:
            monitor_events.append(("on_dedup_hit", (fingerprint,)))
            return True, None, False

        # Non-removable filters retain their original bit/fingerprint after a
        # failed queue push. ``forget`` grants exactly one retry miss; deletion
        # under the shared lock is the linearization point, so concurrent callers
        # cannot consume the same allowance twice. The underlying retained marker
        # makes every other caller a duplicate before and after that one retry.
        if self._consume_retry_allowance(encoded_fingerprint):
            intent.slot_reserved = True
            monitor_events.append(("on_dedup_miss", (fingerprint,)))
            return False, "allowance", True

        # Never add a new non-transactional marker when every bounded recovery
        # slot is already owned by a failed push.  Probe membership first so a
        # previously durable marker remains a duplicate; an unseen item is
        # admitted without a marker and can therefore never become an
        # unreachable failed-work ghost.
        if not self._reserve_retry_allowance_slot():
            if self._note_retry_allowance_backpressure():
                diagnostics.append("retry_allowance_overflow")
            try:
                already_seen = encoded_fingerprint in self._filter
            except NotImplementedError as exc:
                raise RuntimeError(
                    "Configured backend does not support set/duplicate filtering; "
                    "use a backend with SetBackend or disable BackendDupeFilter."
                ) from exc
            except (BackendConnectionError, CircuitBreakerOpenError) as exc:
                self._handle_backend_error(
                    fingerprint,
                    exc,
                    monitor_events,
                    diagnostics,
                )
                return False, None, False
            if already_seen:
                monitor_events.append(("on_dedup_hit", (fingerprint,)))
                return True, None, False
            monitor_events.append(("on_dedup_miss", (fingerprint,)))
            return False, None, False

        intent.slot_reserved = True

        try:
            added = self._filter.add(encoded_fingerprint)
        except NotImplementedError as exc:
            self._release_retry_allowance_slot()
            intent.slot_reserved = False
            raise RuntimeError(
                "Configured backend does not support set/duplicate filtering; "
                "use a backend with SetBackend or disable BackendDupeFilter."
            ) from exc
        except FilterFull:
            # Membership-filter-full graceful degradation (Theme C, R7-A).
            #
            # ``CuckooMembershipFilter.add`` raises ``FilterFull`` once it exhausts
            # ``_MAX_KICKS`` (filter past capacity) — a correct low-level signal.
            # For a crawler, a dead spider is worse than a duplicate fetch: Scrapy
            # and the downstream pipeline handle occasional duplicates, but a crashed
            # long-running crawl loses all in-flight progress. So at this layer we
            # degrade gracefully: warn once per process (module-level
            # ``_filter_full_warned``, mirrors factory.py:31 ``_warned``), emit
            # ``monitor.on_filter_full()`` so a wired stats collector increments
            # ``dupefilter/filter_full`` via the monitor contract (no private-attr
            # reach), and treat the overflow item as NOT-seen (allow enqueue). Dedup
            # stays effective within capacity; overflow items may re-fetch — strictly
            # better than crashing. This arm is intentionally separate from the
            # ``NotImplementedError`` arm above (different meaning: unsupported vs.
            # full). ``FilterFull`` is caught by TYPE (not by string-matching the
            # message), so the cuckoo layer is free to reword its message without
            # silently disabling this guard.
            self._release_retry_allowance_slot()
            intent.slot_reserved = False
            self._handle_filter_full(fingerprint, monitor_events, diagnostics)
            return False, None, False
        except (BackendConnectionError, CircuitBreakerOpenError) as exc:
            # Transient-backend-error graceful degradation (Risk 4).
            #
            # A transient Redis/MongoDB/ES outage raises BackendConnectionError, and
            # an already-tripped circuit rejects the call with
            # CircuitBreakerOpenError. Left uncaught either propagates to the Scrapy
            # engine and crashes the crawl — contradicting the codebase's documented
            # "a dead spider is worse than a duplicate fetch" philosophy. Mirror the
            # FilterFull arm: warn once per process, emit
            # ``monitor.on_error("dedup", safe_error)`` so a wired collector
            # increments ``errors/dedup``, and degrade to not-seen (allow the request
            # through).
            # The tradeoff is possible duplicate fetches during the outage window —
            # strictly better than crawl death. Distinct from the NotImplementedError
            # arm (unsupported backend, still raises RuntimeError) and the FilterFull
            # arm (filter at capacity).
            self._release_retry_allowance_slot()
            intent.slot_reserved = False
            self._handle_backend_error(fingerprint, exc, monitor_events, diagnostics)
            return False, None, False
        except BaseException:
            # A process-control interruption may land after the backend write
            # took effect but before ``add`` returns. The invocation's reserved
            # admission slot is intentionally retained on the intent so the
            # caller's BaseException compensation converts it into one
            # ``forget``-semantics retry allowance; releasing it here would
            # strand a possibly-written marker as a permanent ghost.
            raise

        # add() returns True when the item was newly added; a duplicate maps to False.
        seen = not added
        # Memory historically emitted its at-cap signal inside ``add`` before the
        # outer miss hook, and skipped it for the duplicate early-return. Preserve
        # that cadence and ordering while deferring the callback outside the lock.
        is_memory_filter = isinstance(self._filter, MemoryMembershipFilter)
        saturation_event: _PendingMonitorEvent | None = None
        if not is_memory_filter or added:
            sat = getattr(self._filter, "saturation", None)
            if sat is not None:
                # Cuckoo's long-public ``capacity`` is physical slot count. Its
                # configured target is the actionable monitoring denominator;
                # other filters continue to use their existing capacity property.
                cap = getattr(
                    self._filter,
                    "configured_capacity",
                    getattr(self._filter, "capacity", None),
                )
                saturation_event = (
                    "on_filter_saturation",
                    (len(self._filter), cap),
                )
        if is_memory_filter and saturation_event is not None:
            monitor_events.append(saturation_event)
        if seen:
            monitor_events.append(("on_dedup_hit", (fingerprint,)))
        else:
            monitor_events.append(("on_dedup_miss", (fingerprint,)))
        # U2 operability: if the filter exposes saturation (Cuckoo, Bloom, and a
        # bounded Memory filter at cap), emit the leading fill-ratio signal. This
        # costs one property read plus one queued event and lets operators see the
        # filter approaching full (e.g. >0.9) before FilterFull ever fires.
        # Set filters do not expose ``saturation`` and stay silent. The Memory
        # case was queued above to preserve its insertion-only event order.
        if not is_memory_filter and saturation_event is not None:
            monitor_events.append(saturation_event)
        if not added:
            self._release_retry_allowance_slot()
            intent.slot_reserved = False
        return seen, "added" if added else None, added

    def _request_seen_for_scheduler_unlocked(
        self,
        fingerprint: str,
        encoded_fingerprint: bytes,
        monitor_events: list[_PendingMonitorEvent],
        diagnostics: list[_ContinuationDiagnostic],
    ) -> bool:
        """Read dedup state without publishing a pre-queue marker.

        The scheduler may enqueue concurrent duplicates, but a failed push or hard
        crash can never strand a marker that has no durable queue item. The later
        :meth:`commit_reservation` call publishes the marker after queue success.
        """
        if encoded_fingerprint in self._volatile_fingerprints:
            monitor_events.append(("on_dedup_hit", (fingerprint,)))
            return True

        try:
            seen = encoded_fingerprint in self._filter
        except NotImplementedError as exc:
            raise RuntimeError(
                "Configured backend does not support set/duplicate filtering; "
                "use a backend with SetBackend or disable BackendDupeFilter."
            ) from exc
        except (BackendConnectionError, CircuitBreakerOpenError) as exc:
            self._handle_backend_error(
                fingerprint,
                exc,
                monitor_events,
                diagnostics,
                include_miss=False,
            )
            return False

        if seen:
            monitor_events.append(("on_dedup_hit", (fingerprint,)))
            if not isinstance(self._filter, MemoryMembershipFilter):
                saturation = getattr(self._filter, "saturation", None)
                if saturation is not None:
                    capacity = getattr(
                        self._filter,
                        "configured_capacity",
                        getattr(self._filter, "capacity", None),
                    )
                    monitor_events.append(
                        ("on_filter_saturation", (len(self._filter), capacity))
                    )
        return seen

    def commit_reservation(self, reservation: object) -> None:
        """Publish a marker only after the owning queue push is durable."""
        if not isinstance(reservation, _DedupReservation):
            raise TypeError("invalid duplicate-filter reservation receipt")
        pending_monitor_events: list[_PendingMonitorEvent] = []
        pending_diagnostics: list[_ContinuationDiagnostic] = []
        drain_token: _MonitorFenceToken | None = None
        should_warn_overflow = False
        with self._admit_operation("commit", reservation=reservation) as operation:
            if operation is None:
                return
            try:
                with self._filter_operation_scope(reservation.request):
                    try:
                        added = self._filter.add(reservation.fingerprint)
                    except FilterFull:
                        self._handle_filter_full(
                            reservation.fingerprint_text,
                            pending_monitor_events,
                            pending_diagnostics,
                        )
                    except (BackendConnectionError, CircuitBreakerOpenError) as exc:
                        self._handle_backend_error(
                            reservation.fingerprint_text,
                            exc,
                            pending_monitor_events,
                            pending_diagnostics,
                        )
                    else:
                        saturation_event: _PendingMonitorEvent | None = None
                        is_memory_filter = isinstance(
                            self._filter,
                            MemoryMembershipFilter,
                        )
                        if not is_memory_filter or added:
                            saturation = getattr(self._filter, "saturation", None)
                            if saturation is not None:
                                capacity = getattr(
                                    self._filter,
                                    "configured_capacity",
                                    getattr(self._filter, "capacity", None),
                                )
                                saturation_event = (
                                    "on_filter_saturation",
                                    (len(self._filter), capacity),
                                )
                        if is_memory_filter and saturation_event is not None:
                            pending_monitor_events.append(saturation_event)
                        pending_monitor_events.append(
                            ("on_dedup_miss", (reservation.fingerprint_text,))
                        )
                        if not is_memory_filter and saturation_event is not None:
                            pending_monitor_events.append(saturation_event)
            finally:
                with self._lifecycle_condition:
                    self._discard_reservation(reservation)
                    drain_token, should_warn_overflow = (
                        self._queue_monitor_events_unlocked(
                            reservation.request,
                            pending_monitor_events,
                        )
                    )
        self._emit_continuation_diagnostics(pending_diagnostics)
        self._dispatch_queued_monitor_events(drain_token, should_warn_overflow)

    def commit_volatile_reservation(self, reservation: object) -> None:
        """Publish a lifecycle-local marker for a process-local queue push."""
        if not isinstance(reservation, _DedupReservation):
            raise TypeError("invalid duplicate-filter reservation receipt")
        drain_token: _MonitorFenceToken | None = None
        should_warn_overflow = False
        should_warn_marker_overflow = False
        with self._admit_operation("commit", reservation=reservation) as operation:
            if operation is None:
                return
            with self._lifecycle_condition:
                fingerprint = reservation.fingerprint
                if fingerprint in self._volatile_fingerprints:
                    self._volatile_fingerprints.move_to_end(fingerprint)
                else:
                    if (
                        len(self._volatile_fingerprints)
                        >= self._volatile_fingerprint_limit
                    ):
                        self._volatile_fingerprints.popitem(last=False)
                        if not self._volatile_fingerprint_overflow_warned:
                            self._volatile_fingerprint_overflow_warned = True
                            should_warn_marker_overflow = True
                    self._volatile_fingerprints[fingerprint] = None
                self._discard_reservation(reservation)
                drain_token, should_warn_overflow = self._queue_monitor_events_unlocked(
                    reservation.request,
                    [("on_dedup_miss", (reservation.fingerprint_text,))],
                )
        self._dispatch_queued_monitor_events(drain_token, should_warn_overflow)
        if should_warn_marker_overflow:
            try:
                logger.warning(
                    "Volatile queue dedup shadow reached the %d-entry bound; evicting "
                    "the oldest marker may admit safe at-least-once replay",
                    self._volatile_fingerprint_limit,
                )
            except BaseException:
                pass

    def rollback_reservation(self, reservation: object) -> None:
        """Discard one uncommitted intent; no membership mutation has occurred."""
        if not isinstance(reservation, _DedupReservation):
            raise TypeError("invalid duplicate-filter reservation receipt")
        drain_token: _MonitorFenceToken | None = None
        should_warn_overflow = False
        with self._admit_operation("rollback", reservation=reservation) as operation:
            if operation is None:
                return
            with self._lifecycle_condition:
                self._discard_reservation(reservation)
                drain_token, should_warn_overflow = self._queue_monitor_events_unlocked(
                    reservation.request,
                    [("on_dedup_miss", (reservation.fingerprint_text,))],
                )
        self._dispatch_queued_monitor_events(drain_token, should_warn_overflow)

    def rollback_reservation_intent(self, owner: object) -> None:
        """Discard a receipt whose return handoff was interrupted.

        No caller observed a miss decision, so this cleanup intentionally emits no
        monitor event. Keeping it side-effect-free also prevents monitor re-entry
        while an outer lifecycle-lock frame is still active.
        """
        with self._lifecycle_condition:
            reservation = self._reservations_by_owner.get(id(owner))
            if reservation is None or reservation.owner is not owner:
                return
            self._discard_reservation(reservation)

    def _compensate_interrupted_decision(
        self,
        reservation: _DedupReservation | None,
        owner: object | None,
    ) -> None:
        """Discard an unreturned intent without telemetry or membership writes."""
        try:
            with self._lifecycle_lock:
                if reservation is not None:
                    self._discard_reservation(reservation)
                    return
                if owner is not None:
                    owner_reservation = self._reservations_by_owner.get(id(owner))
                    if (
                        owner_reservation is not None
                        and owner_reservation.owner is owner
                    ):
                        self._discard_reservation(owner_reservation)
        except BaseException:
            # This best-effort cleanup commonly runs while an outer primary error is
            # active. Logging here would expose that raw exception to custom Handler
            # implementations through ``sys.exc_info()`` without improving recovery.
            return

    def _compensate_interrupted_legacy_add_locked(
        self,
        request: Request,
        intent: _PendingLegacyAddIntent | None,
    ) -> None:
        """Compensate one interrupted legacy admission with a retry path.

        A process-control interruption anywhere between the marker write
        (``filter.add``) and the receipt publication cannot tell whether the
        backend write landed. When the invocation still owns its reserved
        admission slot and no receipt was published, the slot is converted into
        exactly one ``forget``-semantics retry allowance: the next matching
        ``request_seen`` returns a miss (consuming the allowance) instead of
        being permanently suppressed by a ghost marker, while every later
        caller remains a duplicate against the retained marker. Requires the
        lifecycle lock; mirrors ``_compensate_interrupted_decision`` for the
        boolean protocol.
        """
        if intent is None:
            return
        intents = self._legacy_add_intents.get(id(request))
        if intents is not None:
            try:
                intents.remove(intent)
            except ValueError:
                # A lifecycle boundary already cleared the intent table; the
                # interrupted generation cannot own a live admission slot.
                return
            if not intents:
                del self._legacy_add_intents[id(request)]
        if not intent.slot_reserved:
            # No marker could have been written by this invocation (volatile
            # hit, backpressured probe, degradation, or duplicate admission).
            return
        receipt = self._legacy_reservation_for_request_locked(
            request,
            pending_only=True,
        )
        if (
            receipt is not None
            and receipt.fingerprint == intent.fingerprint
            and receipt.allowance_slot_reserved
        ):
            # The receipt publication completed and already owns the slot; the
            # scheduler's settle/forget path remains the recovery owner.
            return
        self._grant_retry_allowance(intent.fingerprint, reserved_slot=True)

    def _discard_reservation(self, reservation: _DedupReservation) -> None:
        """Forget one receipt without mutating membership state."""
        if self._active_reservations.get(id(reservation)) is reservation:
            del self._active_reservations[id(reservation)]
        owner_reservation = self._reservations_by_owner.get(id(reservation.owner))
        if owner_reservation is reservation:
            del self._reservations_by_owner[id(reservation.owner)]
        self._lifecycle_condition.notify_all()

    def consume_reservation(self, request: Request) -> bool:
        """Consume exactly one pending legacy receipt for ``request``.

        The boolean return is retained for Scrapy/custom-filter compatibility.  A
        per-request FIFO and the caller thread's handoff identity ensure that a
        repeated or concurrent call cannot consume a sibling invocation's receipt.
        """
        with self._lifecycle_condition:
            if request not in self._pending_reservations:
                return False
            reservation = self._legacy_reservation_for_request_locked(
                request,
                pending_only=True,
            )
            if reservation is None or reservation.epoch != self._reservation_epoch:
                if id(request) not in self._legacy_reservations:
                    self._pending_reservations.discard(request)
                return False
            self._remove_receipt_from_table_locked(
                self._legacy_reservations,
                reservation,
            )
            # A current scheduler handoff supersedes any old-generation tombstone
            # for this Request; subsequent forget() calls now select this exact
            # handoff rather than being conservatively ignored.
            self._legacy_retired_requests.discard(request)
            reservation.consumed = True
            reservation.consumer_thread_id = get_ident()
            self._legacy_handoffs.setdefault(id(request), deque()).append(reservation)
            if id(request) not in self._legacy_reservations:
                self._pending_reservations.discard(request)
            self._lifecycle_condition.notify_all()
            return True

    def settle_reservation(self, request: Request) -> bool:
        """Remove one successful legacy scheduler handoff without filter I/O.

        This is intentionally additive: generic third-party dupefilters that do
        not expose it remain on the historical ``request_seen``/``forget`` path.
        """
        with self._lifecycle_condition:
            reservation = self._legacy_reservation_for_request_locked(
                request,
                thread_id=get_ident(),
                handoff_only=True,
            )
            if reservation is None or reservation.epoch != self._reservation_epoch:
                return False
            self._remove_legacy_reservation_locked(reservation)
            return True

    def forget(self, request: Request) -> None:
        """Compensate a new fingerprint whose subsequent queue push failed.

        Filters with exact atomic deletion remove the reservation immediately.
        Filters such as Bloom and Cuckoo that raise ``NotImplementedError`` retain
        their marker and receive one bounded retry allowance instead. A transient
        backend or circuit-breaker failure during removal receives the same allowance,
        reports one static monitor error, and does not strand a ghost marker without a
        retry path. The next matching :meth:`request_seen` atomically consumes that
        allowance and returns a miss;
        a successful queue push consumes no further state, while another push
        failure calls ``forget`` again and re-arms one allowance.

        Allowances are unique per fingerprint and capped at 1,024 entries. At the
        cap, new non-transactional marker admission is backpressured instead of
        evicting an existing failed-work recovery. Insertion and consumption share
        one lock, giving concurrent callers a single linearization order and
        ensuring no allowance can admit two queue pushes.

        Args:
            request: The request whose newly-added fingerprint must be compensated.
        """
        pending_monitor_events: list[_PendingMonitorEvent] = []
        pending_diagnostics: list[_ContinuationDiagnostic] = []
        drain_token: _MonitorFenceToken | None = None
        should_warn_overflow = False
        with self._lifecycle_condition:
            if self._lifecycle_state in {_CLOSED}:
                raise RuntimeError("dupefilter is closing or closed")
            reservation = self._legacy_reservation_for_request_locked(
                request,
                thread_id=get_ident(),
                handoff_only=True,
            )
            if reservation is None and request in self._legacy_retired_requests:
                # This Request has an old-generation receipt that was cancelled by
                # a transition.  Do not reinterpret the late forget as compensation
                # for a replacement-generation pending marker.
                return
            if reservation is None:
                reservation = self._legacy_reservation_for_request_locked(
                    request,
                    pending_only=True,
                )
            if reservation is None or reservation.epoch != self._reservation_epoch:
                # A receipt from a retired epoch is intentionally a no-op. In
                # particular, never remove a same-fingerprint marker from the new
                # generation on behalf of a late scheduler compensation.
                return
            reservation.settling = True

        with self._admit_operation("forget", reservation=reservation) as operation:
            if operation is None:
                return
            retry_allowance_needed = False
            try:
                with self._filter_operation_scope(request):
                    try:
                        self._filter.remove(reservation.fingerprint)
                    except NotImplementedError:
                        retry_allowance_needed = True
                    except (BackendConnectionError, CircuitBreakerOpenError) as exc:
                        retry_allowance_needed = True
                        self._handle_forget_backend_error(
                            exc,
                            pending_monitor_events,
                            pending_diagnostics,
                        )
                    except BaseException:
                        # A control interruption may occur after a remote remove
                        # took effect or before it did.  Keep one retry path and
                        # re-raise the control signal after the receipt is settled.
                        retry_allowance_needed = True
                        raise
            finally:
                if retry_allowance_needed:
                    # A receipt-owned slot makes this conversion exact and cannot
                    # evict another failed-work allowance.
                    self._grant_retry_allowance(
                        reservation.fingerprint,
                        reserved_slot=reservation.allowance_slot_reserved,
                        reservation=reservation,
                    )
                with self._lifecycle_condition:
                    self._remove_legacy_reservation_locked(reservation)
                    drain_token, should_warn_overflow = (
                        self._queue_monitor_events_unlocked(
                            request,
                            pending_monitor_events,
                        )
                    )
        self._emit_continuation_diagnostics(pending_diagnostics)
        self._dispatch_queued_monitor_events(drain_token, should_warn_overflow)

    def _reserve_retry_allowance_slot(self) -> bool:
        """Reserve bounded recovery capacity before adding a legacy marker."""
        with self._lifecycle_condition:
            with self._retry_allowance_lock:
                if (
                    len(self._retry_allowances) + self._retry_allowance_slots_reserved
                    >= self._retry_allowance_limit
                ):
                    return False
                self._retry_allowance_slots_reserved += 1
                return True

    def _note_retry_allowance_backpressure(self) -> bool:
        """Elect one post-decision warning for a full recovery ledger."""
        with self._lifecycle_condition:
            with self._retry_allowance_lock:
                if self._retry_allowance_overflow_warned:
                    return False
                self._retry_allowance_overflow_warned = True
                return True

    def _release_retry_allowance_slot_locked(self) -> None:
        """Release a pre-admission slot while the lifecycle lock is held."""
        if self._retry_allowance_slots_reserved:
            self._retry_allowance_slots_reserved -= 1

    def _release_retry_allowance_slot(self) -> None:
        """Release a pre-admission slot outside filter callbacks."""
        with self._lifecycle_condition:
            self._release_retry_allowance_slot_locked()

    def _grant_retry_allowance(
        self,
        fingerprint: bytes,
        *,
        reserved_slot: bool = False,
        reservation: _LegacyReservation | None = None,
    ) -> bool:
        """Insert one exact recovery allowance without evicting another.

        A reserved slot is normally carried by the legacy receipt.  The
        ``reserved_slot=False`` path is retained for older/custom callers; when
        the ledger is full it refuses admission and leaves the underlying marker
        untouched rather than silently forgetting how to recover it.
        """
        warn_overflow = False
        inserted = False
        with self._lifecycle_condition:
            with self._retry_allowance_lock:
                if fingerprint in self._retry_allowances:
                    self._retry_allowances.move_to_end(fingerprint)
                    if reserved_slot:
                        # Clear the receipt flag while the same lifecycle lock still
                        # owns the slot conversion.  Otherwise a concurrent request
                        # can reserve the just-released counter and the later receipt
                        # cleanup would decrement that unrelated reservation.
                        if reservation is not None:
                            reservation.allowance_slot_reserved = False
                        self._release_retry_allowance_slot_locked()
                    return True
                if reserved_slot:
                    # The receipt-owned slot is the admission guarantee; it is
                    # converted atomically and cannot collide with another grant.
                    if reservation is not None:
                        reservation.allowance_slot_reserved = False
                    self._release_retry_allowance_slot_locked()
                    self._retry_allowances[fingerprint] = None
                    inserted = True
                elif (
                    len(self._retry_allowances) + self._retry_allowance_slots_reserved
                    >= self._retry_allowance_limit
                ):
                    if not self._retry_allowance_overflow_warned:
                        self._retry_allowance_overflow_warned = True
                        warn_overflow = True
                else:
                    self._retry_allowances[fingerprint] = None
                    inserted = True
        if warn_overflow:
            try:
                logger.warning(
                    "Dedup retry allowance capacity is exhausted; new marker admission "
                    "is backpressured instead of evicting failed-work recovery state."
                )
            except BaseException:
                pass
        return inserted

    def _consume_retry_allowance(self, fingerprint: bytes) -> bool:
        """Atomically consume at most one allowance for ``fingerprint``."""
        with self._lifecycle_condition:
            with self._retry_allowance_lock:
                if fingerprint not in self._retry_allowances:
                    return False
                del self._retry_allowances[fingerprint]
                # The matching request now owns the recovery slot until its queue
                # handoff is settled or forget() re-arms the allowance.
                self._retry_allowance_slots_reserved += 1
                return True

    def _clear_retry_allowances(
        self,
        *,
        advance_epoch: bool = True,
        clear_reservations: bool = True,
    ) -> None:
        """Reset retry state and optionally retire the current dedup epoch."""
        with self._lifecycle_condition:
            with self._retry_allowance_lock:
                self._retry_allowances.clear()
                # Reset the one-shot advisory latch alongside the bounded ledger so
                # a close->open cycle can report backpressure again.
                self._retry_allowance_overflow_warned = False
            self._retry_allowance_slots_reserved = 0
            if clear_reservations:
                for table in (self._legacy_reservations, self._legacy_handoffs):
                    for receipts in table.values():
                        for receipt in receipts:
                            request = receipt.request_ref()
                            if request is not None:
                                self._legacy_retired_requests.add(request)
                self._pending_reservations.clear()
                self._active_reservations.clear()
                self._reservations_by_owner.clear()
                self._legacy_reservations.clear()
                self._legacy_handoffs.clear()
                self._legacy_add_intents.clear()
                self._volatile_fingerprints.clear()
                self._volatile_fingerprint_overflow_warned = False
            if advance_epoch:
                self._reservation_epoch += 1
            self._lifecycle_condition.notify_all()

    def _handle_filter_full(
        self,
        fingerprint: str,
        monitor_events: list[_PendingMonitorEvent],
        diagnostics: list[_ContinuationDiagnostic],
    ) -> None:
        """Degrade gracefully when the membership filter reports it is full.

        Warn once per process (module-level ``_filter_full_warned``), emit
        ``monitor.on_filter_full()`` so a wired stats collector increments
        ``dupefilter/filter_full``, and emit a dedup-miss hook so observability
        stays consistent with the not-seen outcome the caller returns.

        Args:
            fingerprint: The request fingerprint that triggered the overflow.
        """
        global _filter_full_warned
        with _diagnostic_state_lock:
            warn = not _filter_full_warned
            if warn:
                _filter_full_warned = True
        if warn:
            diagnostics.append("filter_full")
        # Count the degradation via the monitor contract — ScrapyStatsMonitor
        # increments ``dupefilter/filter_full``; NullMonitor is a no-op. Replaces
        # an earlier reach into ``self._monitor._stats`` (private attribute).
        monitor_events.append(("on_filter_full", ()))
        # Keep the monitor's dedup-miss accounting consistent with the not-seen
        # outcome the caller returns for the overflow item.
        monitor_events.append(("on_dedup_miss", (fingerprint,)))

    def _handle_backend_error(
        self,
        fingerprint: str,
        exc: BaseException,
        monitor_events: list[_PendingMonitorEvent],
        diagnostics: list[_ContinuationDiagnostic],
        *,
        include_miss: bool = True,
    ) -> None:
        """Degrade gracefully when the membership-filter backend is transiently down.

        Risk 4: a transient :class:`BackendConnectionError` or fail-fast
        :class:`CircuitBreakerOpenError` from the SetBackend must not crash the
        crawl. Mirror :meth:`_handle_filter_full`: warn once per process
        (module-level ``_backend_error_warned``), emit one static package error to
        ``monitor.on_error("dedup", safe_error)`` so a wired stats collector
        increments ``errors/dedup``, and emit a dedup-miss hook so observability
        stays consistent with the not-seen outcome the caller returns. The tradeoff
        is possible duplicate fetches until the backend recovers — strictly better
        than crawl death.

        Args:
            fingerprint: The request fingerprint being checked when the error fired.
            exc: The transient backend failure or circuit rejection.
        """
        global _backend_error_warned
        with _diagnostic_state_lock:
            warn = not _backend_error_warned
            if warn:
                _backend_error_warned = True
        if warn:
            diagnostics.append("backend_error")
        monitor_events.append(("on_error", ("dedup", _static_backend_error(exc))))
        if include_miss:
            monitor_events.append(("on_dedup_miss", (fingerprint,)))

    def _handle_forget_backend_error(
        self,
        exc: BaseException,
        monitor_events: list[_PendingMonitorEvent],
        diagnostics: list[_ContinuationDiagnostic],
    ) -> None:
        """Record safe telemetry for failed-push compensation degradation."""
        global _forget_backend_error_warned
        with _diagnostic_state_lock:
            warn = not _forget_backend_error_warned
            if warn:
                _forget_backend_error_warned = True
        if warn:
            diagnostics.append("forget_backend_error")
        monitor_events.append(("on_error", ("dedup", _static_backend_error(exc))))

    def request_fingerprint(self, request: Request) -> str:
        """Generate a fingerprint for a request.

        Uses the configured Scrapy fingerprinter (``crawler.request_fingerprinter``)
        when one was provided via ``from_crawler``; otherwise falls back to
        ``scrapy.utils.request.fingerprint``. The two are byte-identical for the
        default fingerprinter (verified R45), so this only diverges when the
        operator has set a custom ``REQUEST_FINGERPRINTER_CLASS`` — exactly the
        case that should diverge.

        Args:
            request: The request to fingerprint.

        Returns:
            A unique fingerprint string (hex).
        """
        if self._fingerprinter is not None:
            return self._fingerprinter.fingerprint(request).hex()
        return request_fingerprint(request)
