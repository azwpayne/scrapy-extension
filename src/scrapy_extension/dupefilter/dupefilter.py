"""Duplicate filter component for scrapy-extension.

This module provides a Scrapy dupefilter component using backend set interfaces.
"""

from __future__ import annotations

import logging
import sys
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock, RLock, get_ident
from typing import TYPE_CHECKING, Literal, Protocol
from weakref import ReferenceType, WeakSet, ref

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import (
  DEFAULT_MEMORY_MAXSIZE,
  MemoryMembershipFilter,
)
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.exceptions.base import BackendConnectionError, ConfigurationError
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.utils._config import (
  get_bool_setting,
  parse_float_setting,
  parse_int_setting,
)
from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request
  from scrapy.settings import Settings

  from scrapy_extension.backends.connectors import ConnectionManager

  class _Fingerprinter(Protocol):
    """Duck type for Scrapy's request fingerprinter.

    Mirrors ``scrapy.http.request.RequestFingerprinter`` (and any custom
    ``REQUEST_FINGERPRINTER_CLASS``) — the ``fingerprint(request) -> bytes``
    contract. Used so ``BackendDupeFilter`` can honor a configured custom
    fingerprinter instead of always defaulting to the module function.
    """

    def fingerprint(self, request: Request) -> bytes: ...

logger = logging.getLogger(__name__)

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

# Non-removable filters (notably Bloom) cannot compensate a successful add
# after the scheduler's later queue push fails. Keep a bounded, one-shot retry
# allowance per fingerprint instead. 1,024 limits failure-path memory while
# covering a useful transient queue-outage window; overflow evicts FIFO.
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
      frame = frame.f_back
    return False


@dataclass(slots=True, eq=False, repr=False)
class _DedupReservation:
  """Opaque intent to publish a marker after a durable queue push."""

  fingerprint: bytes
  epoch: int
  owner: object
  request: object
  fingerprint_text: str


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


class BackendDupeFilter:
  """Scrapy duplicate filter using a pluggable membership-filter strategy.

  Delegates duplicate detection to a
  :class:`~scrapy_extension.dupefilter.filters.base.MembershipFilter`. The
  default strategy is ``SetMembershipFilter`` (exact, cross-worker,
  byte-identical to the previous hardcoded ``SetBackend`` behavior); other
  strategies (memory, bloom, cuckoo) are selected via ``SCRAPY_DEDUP_STRATEGY``
  (wired in ``from_settings``).

  Attributes:
      connection_manager: The connection manager for backend access.
      key: The key for the fingerprints set / filter scope.
      debug: Whether to log filtered requests.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key: str = "dupefilter",
    *,
    debug: bool = False,
    fingerprinter: _Fingerprinter | None = None,
    membership_filter: MembershipFilter | None = None,
    monitor: Monitor | None = None,
    clear_on_open: bool = False,
    owns_connection_manager: bool = True,
  ) -> None:
    """Initialize the dupefilter.

    Args:
        connection_manager: Connection manager for backend access.
        key: Key for the fingerprints set / filter scope. May contain the
            literal placeholder ``"{spider}"``; when present it is
            substituted with ``spider.name`` at :meth:`open` time so each
            spider gets its own dedup scope (C8 fix).
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
    self.connection_manager = connection_manager
    self.key = key
    self.debug = debug
    self.clear_on_open = clear_on_open
    self._fingerprinter = fingerprinter
    self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
    # Use ``is None`` (not ``or``): a MembershipFilter defines __len__, so an
    # empty filter (len == 0) would be falsy and ``or`` would wrongly discard
    # it. Only fall through when no filter was supplied at all.
    self._filter: MembershipFilter = (
      membership_filter
      if membership_filter is not None
      else SetMembershipFilter(connection_manager, key)
    )
    self._retry_allowances: OrderedDict[bytes, None] = OrderedDict()
    self._retry_allowance_lock = Lock()
    self._retry_allowance_limit = _DEFAULT_RETRY_ALLOWANCE_LIMIT
    self._retry_allowance_overflow_warned = False
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
    self._lifecycle_lock = RLock()
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
      tuple[ReferenceType[object], set[_MonitorFenceToken]],
    ] = {}
    self._opened = False
    self._opened_spider: Spider | None = None
    self._closed = False
    self._closing = False
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

  def _emit_monitor(self, event: _MonitorEvent) -> None:
    """Dispatch one recorded hook outside locks and isolate ordinary errors."""
    hook_name, args, origin_request = event
    event_token = _MonitorFenceToken(get_ident(), "event_token")
    primary: BaseException | None = None
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
        try:
          logger.debug("Dupefilter monitor hook raised; ignored", exc_info=True)
        except Exception:  # noqa: BLE001 - diagnostics are best effort too
          _diagnostic_failed = True
    except BaseException as exc:
      primary = exc
    try:
      with self._lifecycle_lock:
        active = self._active_monitor_requests.get(id(origin_request))
        if active is not None and active[0]() is origin_request:
          active[1].discard(event_token)
          if not active[1]:
            del self._active_monitor_requests[id(origin_request)]
    except BaseException as cleanup_error:
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
        _fallback_cleanup_failed = True
      if primary is None:
        raise cleanup_error
      try:
        logger.debug(
          "Failed to clear monitor observer fence while preserving signal",
          exc_info=(
            type(cleanup_error),
            cleanup_error,
            cleanup_error.__traceback__,
          ),
        )
      except BaseException:
        _diagnostic_failed = True
    if primary is not None:
      raise primary

  def _monitor_origin_ref(self, origin_request: object) -> ReferenceType[object]:
    """Create a weak origin reference that removes an abandoned fence entry."""
    request_id = id(origin_request)
    owner_ref = ref(self)

    def remove_stale_origin(dead_ref: ReferenceType[object]) -> None:
      owner = owner_ref()
      if owner is None:
        return
      try:
        with owner._lifecycle_lock:
          active = owner._active_monitor_requests.get(request_id)
          if active is not None and active[0] is dead_ref:
            del owner._active_monitor_requests[request_id]
      except BaseException:
        _weakref_cleanup_failed = True

    return ref(origin_request, remove_stale_origin)

  def _queue_monitor_events_unlocked(
    self,
    origin_request: object,
    pending_events: list[_PendingMonitorEvent],
  ) -> tuple[_MonitorFenceToken | None, bool]:
    """Append one complete telemetry batch while lifecycle state is locked."""
    monitor_events = [
      (hook_name, args, origin_request)
      for hook_name, args in pending_events
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
    except Exception:  # noqa: BLE001 - diagnostics cannot reject a decision
      return

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendDupeFilter:
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
    from scrapy_extension.dupefilter.filters.factory import (
      DedupeStrategy,
      build_membership_filter,
    )

    raw_strategy = settings.get(
      "SCRAPY_DEDUP_STRATEGY", DedupeStrategy.SET.value
    )
    try:
      strategy = DedupeStrategy(raw_strategy)
    except ValueError as e:
      valid = ", ".join(repr(m.value) for m in DedupeStrategy)
      raise ConfigurationError(
        f"Invalid SCRAPY_DEDUP_STRATEGY {raw_strategy!r}. Valid: {valid}.",
        setting_name="SCRAPY_DEDUP_STRATEGY",
        setting_value=str(raw_strategy),
      ) from e
    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_SET_BACKEND_TYPE",
      settings_key="SCRAPY_SET_BACKEND_SETTINGS",
      required_capabilities={"set"} if strategy is DedupeStrategy.SET else set(),
      component_name="set",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    try:
      key = settings.get("SCRAPY_DUPEFILTER_KEY", "dupefilter")
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
          key.replace("{spider}", "spider"),
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
        membership_filter=membership_filter,
        clear_on_open=clear_on_open,
      )
    except BaseException:
      try:
        manager.close()
      except BaseException:
        try:
          logger.exception(
            "Failed to release ConnectionManager after dupefilter factory failure"
          )
        except BaseException:
          pass
      raise

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
    try:
      dupefilter._fingerprinter = getattr(crawler, "request_fingerprinter", None)
      # Default-on observability: wire a ScrapyStatsMonitor when crawler.stats is
      # available so dedup hit/miss counts show up on the Scrapy stats dump
      # without an explicit ``monitor=`` kwarg. Additive — existing stats untouched.
      stats = getattr(crawler, "stats", None)
      if stats is not None:
        dupefilter._monitor = ScrapyStatsMonitor(stats)
        dupefilter._set_filter_monitor()
      return dupefilter
    except BaseException:
      try:
        dupefilter.close("crawler-factory-failed")
      except BaseException:
        try:
          logger.exception(
            "Failed to release ConnectionManager after dupefilter crawler factory failure"
          )
        except BaseException:
          pass
      raise

  def open(self, spider: Spider | None = None) -> None:
    """Open the dupefilter and its membership filter.

    Called by Scrapy's engine with no args (stock scheduler calls
    ``self.df.open()``); accepts an optional ``spider`` for explicit invocation
    (e.g. from a custom scheduler's ``open(spider)``). When a spider is
    provided, two additive behaviors activate (both default-off / no-op when
    no spider is passed or the relevant setting is unset):

    1. ``{spider}`` key templating (C8): if :attr:`key` contains the literal
       placeholder ``"{spider}"``, it is substituted with ``spider.name`` and
       the resolved key is propagated to the underlying membership filter
       (for backend-backed filters like :class:`SetMembershipFilter`) so each
       spider gets its own dedup scope. Keys without the placeholder are
       passed through unchanged.
    2. ``SCRAPY_DUPEFILTER_CLEAR_ON_OPEN`` (C5): when the dupefilter was
       constructed with ``clear_on_open=True``, any prior fingerprints are
       cleared before the run begins — so a re-run / resume crawl is not
       silently blocked by stale state.

    Args:
        spider: The spider opening the crawl (optional).
    """
    with self._lifecycle_lock:
      if self._closed or self._closing:
        raise RuntimeError("dupefilter is closing or closed")
      if self._opened:
        if spider is self._opened_spider:
          return
        raise RuntimeError("dupefilter is already open for a different spider")
      try:
        if spider is not None:
          _validate_key_name(spider.name, field_name="spider.name")
          self._resolve_spider_key(spider)
        self._clear_retry_allowances()
        self._filter.open()
        if self.clear_on_open:
          self.clear()
      except BaseException:
        try:
          self._close_locked()
        except BaseException:
          try:
            logger.exception("Failed to clean up dupefilter after open failure")
          except BaseException:
            pass
        raise
      self._opened = True
      self._opened_spider = spider

  def _resolve_spider_key(self, spider: Spider) -> None:
    """Substitute ``{spider}`` in :attr:`key` with ``spider.name``, propagating
    to the underlying membership filter.

    No-op when the key does not contain the placeholder. Only backend-backed
    filters (those exposing a mutable ``key`` attribute, e.g.
    :class:`SetMembershipFilter`) receive the propagated update; in-process
    filters (memory/bloom/cuckoo) ignore the key entirely, so the placeholder
    has no effect there — consistent with their per-process scope.
    """
    if "{spider}" not in self.key:
      return
    templated = self.key
    resolved = templated.replace("{spider}", spider.name)
    self.key = resolved
    # Propagate to backend-backed filters that expose a writable ``key``. The
    # filter was built from the same templated key, so only rewrite when its
    # key still equals the templated form — a caller who passed a custom
    # filter with a different key is not silently overwritten.
    if getattr(self._filter, "key", None) == templated:
      self._filter.key = resolved  # type: ignore[attr-defined]

  def close(self, reason: str) -> None:
    """Close the dupefilter and its membership filter.

    Args:
        reason: The reason for closing.
    """
    del reason
    with self._lifecycle_lock:
      self._close_locked()

  def _close_locked(self) -> None:
    """Release one dupefilter lifecycle while ``_lifecycle_lock`` is held."""
    if self._closed:
      return
    self._closing = True
    self._opened = False
    self._opened_spider = None
    # Active receipts have not crossed the queue commit boundary and therefore
    # own no marker. Discard them before closing without backend cleanup.
    for reservation in tuple(self._active_reservations.values()):
      self._discard_reservation(reservation)
    self._clear_retry_allowances()
    primary_error: BaseException | None = None
    if not self._filter_released:
      try:
        self._filter.close()
      except BaseException as exc:
        primary_error = exc
      else:
        self._filter_released = True
    if (
      self._filter_released
      and self._owns_connection_manager
      and not self._manager_released
    ):
      try:
        self.connection_manager.close()
      except BaseException as exc:
        if primary_error is None:
          primary_error = exc
        else:
          try:
            logger.error(
              "ConnectionManager close failed while preserving filter close error",
              exc_info=(type(exc), exc, exc.__traceback__),
            )
          except BaseException:
            pass
      else:
        self._manager_released = True
    if self._filter_released and (
      not self._owns_connection_manager or self._manager_released
    ):
      self._closed = True
      self._closing = False
    if primary_error is not None:
      raise primary_error

  def clear(self) -> None:
    """Clear all tracked fingerprints via the membership filter.

    Used by :meth:`open` when ``clear_on_open=True`` (C5 fix). Delegates to
    the underlying strategy's :meth:`MembershipFilter.clear`, so every
    concrete filter (set/memory/bloom/cuckoo) supports it.
    """
    with self._lifecycle_lock:
      if self._closed or self._closing:
        raise RuntimeError("dupefilter is closing or closed")
      self._filter.clear()
      # Publish the new generation only after the filter clear succeeds. If a
      # remote clear fails, existing receipts must remain able to compensate
      # their still-present markers.
      self._clear_retry_allowances()

  def log(self, request: Request, spider: Spider) -> None:
    """Log a filtered request.

    Args:
        request: The filtered request.
        spider: The spider instance.
    """
    if self.debug:
      logger.debug(
        "Filtered duplicate request: %s",
        request.url,
        extra={"spider": spider},
      )

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
    """Linearize one decision and dispatch its telemetry outside locks."""
    pending_monitor_events: list[_PendingMonitorEvent] = []
    drain_token: _MonitorFenceToken | None = None
    should_warn_overflow = False
    reservation: _DedupReservation | None = None
    published_owner: object | None = None
    compensated_under_lock = False
    try:
      with self._lifecycle_lock:
        try:
          if self._closed or self._closing:
            raise RuntimeError("dupefilter is closing or closed")
          fingerprint = self.request_fingerprint(request)
          encoded_fingerprint = fingerprint.encode()

          request_id = id(request)
          active_monitor = self._active_monitor_requests.get(request_id)
          if active_monitor is not None and active_monitor[0]() is request:
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

          self._pending_reservations.discard(request)
          if transactional:
            assert owner is not None  # nosec B101 - normalized by caller
            existing = self._reservations_by_owner.get(id(owner))
            if existing is not None and existing.owner is owner:
              raise RuntimeError("duplicate-filter owner intent is already active")
            reservation = _DedupReservation(
              encoded_fingerprint,
              self._reservation_epoch,
              owner,
              request,
              fingerprint,
            )
            self._active_reservations[id(reservation)] = reservation
            self._reservations_by_owner[id(owner)] = reservation
            published_owner = owner

          if transactional:
            seen = self._request_seen_for_scheduler_unlocked(
              fingerprint,
              encoded_fingerprint,
              pending_monitor_events,
            )
            if seen:
              assert reservation is not None  # nosec B101 - published above
              self._discard_reservation(reservation)
              reservation = None
          else:
            seen, reservation_state = self._request_seen_unlocked(
              request,
              fingerprint,
              encoded_fingerprint,
              pending_monitor_events,
            )
            if reservation_state is not None:
              self._pending_reservations.add(request)

          drain_token, should_warn_overflow = (
            self._queue_monitor_events_unlocked(
              request,
              pending_monitor_events,
            )
          )
        except BaseException:
          if transactional:
            self._compensate_interrupted_decision(
              reservation,
              published_owner,
            )
            compensated_under_lock = True
          raise
      self._dispatch_queued_monitor_events(
        drain_token,
        should_warn_overflow,
      )
      return DedupDecision(
        seen=seen,
        reservation=reservation,
      )
    except BaseException:
      # The caller never received the opaque receipt. Compensate immediately;
      # putting it back into the legacy WeakSet would be unusable because the
      # next request_seen call clears that side channel before checking state.
      if transactional and not compensated_under_lock:
        self._compensate_interrupted_decision(
          reservation,
          published_owner,
        )
      raise

  def _request_seen_unlocked(
    self,
    request: Request,
    fingerprint: str,
    encoded_fingerprint: bytes,
    monitor_events: list[_PendingMonitorEvent],
  ) -> tuple[bool, Literal["added", "allowance"] | None]:
    """Check if a request has been seen before.

    Args:
        request: The request to check.

    Returns:
        ``(seen, reservation_state)`` for this invocation only.
    """
    del request

    if encoded_fingerprint in self._volatile_fingerprints:
      monitor_events.append(("on_dedup_hit", (fingerprint,)))
      return True, None

    # Non-removable filters retain their original bit/fingerprint after a
    # failed queue push. ``forget`` grants exactly one retry miss; deletion
    # under the shared lock is the linearization point, so concurrent callers
    # cannot consume the same allowance twice. The underlying retained marker
    # makes every other caller a duplicate before and after that one retry.
    if self._consume_retry_allowance(encoded_fingerprint):
      monitor_events.append(("on_dedup_miss", (fingerprint,)))
      return False, "allowance"

    try:
      added = self._filter.add(encoded_fingerprint)
    except NotImplementedError as exc:
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
      self._handle_filter_full(fingerprint, monitor_events)
      return False, None
    except (BackendConnectionError, CircuitBreakerOpenError) as exc:
      # Transient-backend-error graceful degradation (Risk 4).
      #
      # A transient Redis/MongoDB/ES outage raises BackendConnectionError, and
      # an already-tripped circuit rejects the call with
      # CircuitBreakerOpenError. Left uncaught either propagates to the Scrapy
      # engine and crashes the crawl — contradicting the codebase's documented
      # "a dead spider is worse than a duplicate fetch" philosophy. Mirror the
      # FilterFull arm: warn once per process, emit
      # ``monitor.on_error("dedup", exc)`` so a wired collector increments
      # ``errors/dedup``, and degrade to not-seen (allow the request through).
      # The tradeoff is possible duplicate fetches during the outage window —
      # strictly better than crawl death. Distinct from the NotImplementedError
      # arm (unsupported backend, still raises RuntimeError) and the FilterFull
      # arm (filter at capacity).
      self._handle_backend_error(fingerprint, exc, monitor_events)
      return False, None

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
        cap = getattr(self._filter, "capacity", None)
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
    return seen, "added" if added else None

  def _request_seen_for_scheduler_unlocked(
    self,
    fingerprint: str,
    encoded_fingerprint: bytes,
    monitor_events: list[_PendingMonitorEvent],
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
        include_miss=False,
      )
      return False

    if seen:
      monitor_events.append(("on_dedup_hit", (fingerprint,)))
      if not isinstance(self._filter, MemoryMembershipFilter):
        saturation = getattr(self._filter, "saturation", None)
        if saturation is not None:
          capacity = getattr(self._filter, "capacity", None)
          monitor_events.append(
            ("on_filter_saturation", (len(self._filter), capacity))
          )
    return seen

  def commit_reservation(self, reservation: object) -> None:
    """Publish a marker only after the owning queue push is durable."""
    if not isinstance(reservation, _DedupReservation):
      raise TypeError("invalid duplicate-filter reservation receipt")
    pending_monitor_events: list[_PendingMonitorEvent] = []
    drain_token: _MonitorFenceToken | None = None
    should_warn_overflow = False
    with self._lifecycle_lock:
      if (
        reservation.epoch != self._reservation_epoch
        or self._active_reservations.get(id(reservation)) is not reservation
      ):
        self._discard_reservation(reservation)
        return
      # The queue copy is already authoritative. Release bookkeeping before
      # the backend write so an interruption at any later opcode can cause at
      # most replay, never retain the Request/owner until close.
      self._discard_reservation(reservation)
      try:
        added = self._filter.add(reservation.fingerprint)
      except FilterFull:
        self._handle_filter_full(
          reservation.fingerprint_text,
          pending_monitor_events,
        )
      except (BackendConnectionError, CircuitBreakerOpenError) as exc:
        self._handle_backend_error(
          reservation.fingerprint_text,
          exc,
          pending_monitor_events,
        )
      else:
        saturation_event: _PendingMonitorEvent | None = None
        is_memory_filter = isinstance(self._filter, MemoryMembershipFilter)
        if not is_memory_filter or added:
          saturation = getattr(self._filter, "saturation", None)
          if saturation is not None:
            capacity = getattr(self._filter, "capacity", None)
            saturation_event = (
              "on_filter_saturation",
              (len(self._filter), capacity),
            )
        if (
          is_memory_filter
          and saturation_event is not None
        ):
          pending_monitor_events.append(saturation_event)
        pending_monitor_events.append(
          ("on_dedup_miss", (reservation.fingerprint_text,))
        )
        if (
          not is_memory_filter
          and saturation_event is not None
        ):
          pending_monitor_events.append(saturation_event)
      drain_token, should_warn_overflow = self._queue_monitor_events_unlocked(
        reservation.request,
        pending_monitor_events,
      )
    self._dispatch_queued_monitor_events(
      drain_token,
      should_warn_overflow,
    )

  def commit_volatile_reservation(self, reservation: object) -> None:
    """Publish a lifecycle-local marker for a process-local queue push."""
    if not isinstance(reservation, _DedupReservation):
      raise TypeError("invalid duplicate-filter reservation receipt")
    drain_token: _MonitorFenceToken | None = None
    should_warn_overflow = False
    should_warn_marker_overflow = False
    with self._lifecycle_lock:
      if (
        reservation.epoch != self._reservation_epoch
        or self._active_reservations.get(id(reservation)) is not reservation
      ):
        self._discard_reservation(reservation)
        return
      # As for the durable commit path, release the receipt before any
      # interruptible publication. Losing this process-local shadow can admit
      # replay but cannot lose the item already held by the local strategy.
      self._discard_reservation(reservation)
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
      drain_token, should_warn_overflow = self._queue_monitor_events_unlocked(
        reservation.request,
        [("on_dedup_miss", (reservation.fingerprint_text,))],
      )
    self._dispatch_queued_monitor_events(
      drain_token,
      should_warn_overflow,
    )
    if should_warn_marker_overflow:
      logger.warning(
        "Volatile queue dedup shadow reached the %d-entry bound; evicting "
        "the oldest marker may admit safe at-least-once replay",
        self._volatile_fingerprint_limit,
      )

  def rollback_reservation(self, reservation: object) -> None:
    """Discard one uncommitted intent; no membership mutation has occurred."""
    if not isinstance(reservation, _DedupReservation):
      raise TypeError("invalid duplicate-filter reservation receipt")
    drain_token: _MonitorFenceToken | None = None
    should_warn_overflow = False
    with self._lifecycle_lock:
      if (
        reservation.epoch != self._reservation_epoch
        or self._active_reservations.get(id(reservation)) is not reservation
      ):
        self._discard_reservation(reservation)
        return
      self._discard_reservation(reservation)
      drain_token, should_warn_overflow = self._queue_monitor_events_unlocked(
        reservation.request,
        [("on_dedup_miss", (reservation.fingerprint_text,))],
      )
    self._dispatch_queued_monitor_events(
      drain_token,
      should_warn_overflow,
    )

  def rollback_reservation_intent(self, owner: object) -> None:
    """Discard a receipt whose return handoff was interrupted.

    No caller observed a miss decision, so this cleanup intentionally emits no
    monitor event. Keeping it side-effect-free also prevents monitor re-entry
    while an outer lifecycle-lock frame is still active.
    """
    with self._lifecycle_lock:
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
          if owner_reservation is not None and owner_reservation.owner is owner:
            self._discard_reservation(owner_reservation)
    except BaseException:
      try:
        logger.debug(
          "Failed to compensate interrupted duplicate-filter decision",
          exc_info=True,
        )
      except BaseException:
        return

  def _discard_reservation(self, reservation: _DedupReservation) -> None:
    """Forget one receipt without mutating membership state."""
    if self._active_reservations.get(id(reservation)) is reservation:
      del self._active_reservations[id(reservation)]
    owner_reservation = self._reservations_by_owner.get(id(reservation.owner))
    if owner_reservation is reservation:
      del self._reservations_by_owner[id(reservation.owner)]

  def consume_reservation(self, request: Request) -> bool:
    """Consume whether the latest check reserved state for ``request``.

    Scrapy's public dupefilter protocol returns only seen/not-seen. The
    scheduler additionally needs to know whether a not-seen result actually
    wrote a fingerprint before deciding if a failed queue push should call
    :meth:`forget`. Filter-full and transient-outage misses return False but
    create no reservation, so compensating those would delete unrelated state.

    Returns:
        True exactly once for a genuine reservation; False for degraded misses,
        duplicates, unknown requests, and repeated consumption.
    """
    with self._lifecycle_lock:
      if request not in self._pending_reservations:
        return False
      self._pending_reservations.discard(request)
      return True

  def forget(self, request: Request) -> None:
    """Compensate a new fingerprint whose subsequent queue push failed.

    Filters with atomic deletion remove the reservation immediately. Filters
    such as Bloom that raise ``NotImplementedError`` retain their marker and
    receive one bounded retry allowance instead. The next matching
    :meth:`request_seen` atomically consumes that allowance and returns a miss;
    a successful queue push consumes no further state, while another push
    failure calls ``forget`` again and re-arms one allowance.

    Allowances are unique per fingerprint and capped at 1,024 entries. At the
    cap, the oldest allowance is evicted. Insertion and consumption share one
    lock, giving concurrent callers a single linearization order and ensuring
    no allowance can admit two queue pushes.

    Args:
        request: The request whose newly-added fingerprint must be compensated.
    """
    with self._lifecycle_lock:
      if self._closed or self._closing:
        raise RuntimeError("dupefilter is closing or closed")
      self._pending_reservations.discard(request)
      fingerprint = self.request_fingerprint(request).encode()
      try:
        self._filter.remove(fingerprint)
      except NotImplementedError:
        self._grant_retry_allowance(fingerprint)

  def _grant_retry_allowance(self, fingerprint: bytes) -> None:
    """Insert or refresh one bounded retry allowance for ``fingerprint``."""
    warn_overflow = False
    with self._retry_allowance_lock:
      if fingerprint in self._retry_allowances:
        self._retry_allowances.move_to_end(fingerprint)
        return
      if len(self._retry_allowances) >= self._retry_allowance_limit:
        self._retry_allowances.popitem(last=False)
        if not self._retry_allowance_overflow_warned:
          self._retry_allowance_overflow_warned = True
          warn_overflow = True
      self._retry_allowances[fingerprint] = None
    if warn_overflow:
      logger.warning(
        "Dedup retry allowances reached the %d-entry bound; evicting the "
        "oldest failed-push allowance",
        self._retry_allowance_limit,
      )

  def _consume_retry_allowance(self, fingerprint: bytes) -> bool:
    """Atomically consume at most one allowance for ``fingerprint``."""
    with self._retry_allowance_lock:
      if fingerprint not in self._retry_allowances:
        return False
      del self._retry_allowances[fingerprint]
      return True

  def _clear_retry_allowances(self) -> None:
    """Discard transient failed-push allowances at lifecycle boundaries."""
    with self._retry_allowance_lock:
      self._retry_allowances.clear()
    self._pending_reservations.clear()
    self._active_reservations.clear()
    self._reservations_by_owner.clear()
    self._volatile_fingerprints.clear()
    self._volatile_fingerprint_overflow_warned = False
    self._reservation_epoch += 1

  def _handle_filter_full(
    self,
    fingerprint: str,
    monitor_events: list[_PendingMonitorEvent],
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
    if not _filter_full_warned:
      _filter_full_warned = True
      logger.warning(
        "Dedup membership filter is full (filter_full); degrading — overflow "
        "requests will be treated as not-seen and may re-fetch. Increase the "
        "filter capacity or switch to an exact dedup strategy "
        "(SCRAPY_DEDUP_STRATEGY=set). This filter_full warning fires once per "
        "process; subsequent overflows are counted via the "
        "dupefilter/filter_full stat only."
      )
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
    *,
    include_miss: bool = True,
  ) -> None:
    """Degrade gracefully when the membership-filter backend is transiently down.

    Risk 4: a transient :class:`BackendConnectionError` or fail-fast
    :class:`CircuitBreakerOpenError` from the SetBackend must not crash the
    crawl. Mirror :meth:`_handle_filter_full`: warn once per process
    (module-level ``_backend_error_warned``), emit
    ``monitor.on_error("dedup", exc)`` so a wired stats collector increments
    ``errors/dedup``, and emit a dedup-miss hook so observability stays
    consistent with the not-seen outcome the caller returns. The tradeoff is
    possible duplicate fetches until the backend recovers — strictly better
    than crawl death.

    Args:
        fingerprint: The request fingerprint being checked when the error fired.
        exc: The transient backend failure or circuit rejection.
    """
    global _backend_error_warned
    if not _backend_error_warned:
      _backend_error_warned = True
      logger.warning(
        "Dedup backend transiently unavailable (%s); degrading — requests "
        "will be treated as not-seen and may re-fetch until the backend "
        "recovers. This warning fires once per process; subsequent "
        "transient backend errors are counted via the errors/dedup stat only.",
        type(exc).__name__,
      )
    monitor_events.append(("on_error", ("dedup", exc)))
    if include_miss:
      monitor_events.append(("on_dedup_miss", (fingerprint,)))

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
