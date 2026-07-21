"""Circuit-breaker state machine for hot-path backend operations.

Once a backend is connected, every ``push``/``pop``/``add``/``contains``/``store``
hit can raise ``QueueError`` / ``BackendError`` straight to the scheduler, which
logs and returns ``None`` — a degraded backend silently drops requests forever
with no recovery signal and no way to fail-fast.

This module provides a classic three-state circuit breaker
(``CLOSED`` → ``OPEN`` → ``HALF_OPEN``) that wraps hot-path callables so that:

- In ``CLOSED`` state the call is delegated to the backend; a run of
  ``failure_threshold`` consecutive failures trips the breaker to ``OPEN``.
- In ``OPEN`` state the breaker **fails fast**: it raises
  :class:`CircuitBreakerOpenError` **without** calling the backend, so the
  scheduler/pipeline/dupefilter see a typed backend error and the network is
  spared while the backend is known-broken.
- After ``reset_timeout`` seconds elapse, the breaker transitions to
  ``HALF_OPEN`` and allows a single **probe** call through. A successful probe
  resets the breaker to ``CLOSED`` (and clears the failure count); a failed
  probe re-opens it.

The breaker is thread-safe (Scrapy uses threads and the
:class:`~scrapy_extension.backends.connectors.ConnectionManager` is shared). A
monotonic time source is injectable via ``time_fn`` so tests can advance the
clock without ``time.sleep``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import Enum
from typing import Any

from scrapy_extension.backends.base import QueueBackend, SetBackend, StorageBackend
from scrapy_extension.exceptions import BackendError

__all__ = [
  "BreakerState",
  "CircuitBreaker",
  "CircuitBreakerOpenError",
  "wrap_queue_backend",
  "wrap_set_backend",
  "wrap_storage_backend",
]


class BreakerState(str, Enum):
  """Circuit-breaker operating state.

  Attributes:
      CLOSED: Normal operation — calls are delegated to the backend.
      OPEN: Tripped — calls fail-fast without touching the backend.
      HALF_OPEN: Probe mode — a single trial call is allowed through to
          test whether the backend has recovered.
  """

  CLOSED = "closed"
  OPEN = "open"
  HALF_OPEN = "half_open"


class CircuitBreakerOpenError(BackendError):
  """Raised when a call is rejected because the breaker is OPEN.

  Subclasses :class:`~scrapy_extension.exceptions.BackendError` so every
  existing ``except BackendError`` site (scheduler / pipeline / dupefilter)
  continues to handle the rejection uniformly — no new exception plumbing
  required upstream.

  Attributes:
      name: The breaker's human-readable name (e.g. ``"redis-queue"``).
  """

  def __init__(self, name: str) -> None:
    self.name = name
    super().__init__(f"Circuit breaker open for {name!r}")


class CircuitBreaker:
  """Thread-safe three-state circuit breaker.

  The breaker counts **consecutive** failures: a single success resets the
  count to zero. This matches the semantics operators expect — a flapping
  backend that fails intermittently with occasional successes should not trip
  as readily as one that has gone hard-down, and recovery (a single clean
  probe in HALF_OPEN) restores full availability immediately.

  Attributes:
      name: Human-readable identifier used in error messages and logs.
      failure_threshold: Consecutive failures required to trip CLOSED→OPEN.
          Must be >= 1.
      reset_timeout: Seconds the breaker stays OPEN before allowing a
          HALF_OPEN probe. Must be >= 0.
      time_fn: Monotonic clock callable; defaults to :func:`time.monotonic`.
      failure_exceptions: Exception classes that represent backend failures.
          Other exceptions propagate without changing breaker state.
  """

  def __init__(
    self,
    name: str,
    *,
    failure_threshold: int = 5,
    reset_timeout: float = 30.0,
    time_fn: Callable[[], float] | None = None,
    failure_exceptions: tuple[type[BaseException], ...] = (BaseException,),
  ) -> None:
    if failure_threshold < 1:
      msg = f"failure_threshold must be >= 1, got {failure_threshold}"
      raise ValueError(msg)
    if reset_timeout < 0:
      msg = f"reset_timeout must be >= 0, got {reset_timeout}"
      raise ValueError(msg)
    self.name = name
    self.failure_threshold = failure_threshold
    self.reset_timeout = reset_timeout
    self.failure_exceptions = failure_exceptions
    self._time_fn: Callable[[], float] = time_fn or time.monotonic
    self._lock = threading.Lock()
    self._state = BreakerState.CLOSED
    self._failure_count = 0
    self._last_failure_time: float | None = None
    # Guards the HALF_OPEN window so only ONE thread issues the probe call.
    # Set in _allow_call() on the OPEN→HALF_OPEN transition; cleared under the
    # lock once the probe's outcome is recorded. Without this, the lock is
    # released between _allow_call() (which flips OPEN→HALF_OPEN) and func(),
    # so N threads in the reset-timeout window all observe HALF_OPEN and call
    # the backend concurrently — defeating the "single probe" contract.
    self._probe_in_flight: bool = False

  # --- introspection (lock-protected reads for test determinism) ---

  @property
  def state(self) -> BreakerState:
    """Current breaker state (under the lock)."""
    with self._lock:
      return self._state

  @property
  def failure_count(self) -> int:
    """Current consecutive-failure count (under the lock)."""
    with self._lock:
      return self._failure_count

  @property
  def last_failure_time(self) -> float | None:
    """Monotonic timestamp of the last recorded failure, or ``None``."""
    with self._lock:
      return self._last_failure_time

  def reset(self) -> None:
    """Force the breaker back to CLOSED and clear failure bookkeeping.

    Useful for tests and for explicit operator-driven recovery (e.g. after a
    manual reconnect). Does not invoke any backend.
    """
    with self._lock:
      self._state = BreakerState.CLOSED
      self._failure_count = 0
      self._last_failure_time = None
      self._probe_in_flight = False

  def new_generation(self) -> CircuitBreaker:
    """Return a CLOSED breaker with the same immutable configuration.

    Backend reconnect replaces one connection generation with another. A
    retained proxy can still finish an old in-flight call after that point;
    giving the replacement backend a distinct breaker prevents that late
    outcome from mutating the new generation's availability state.

    Returns:
        A fresh breaker with identical name, thresholds, clock, and failure
        exception policy.
    """
    return CircuitBreaker(
      self.name,
      failure_threshold=self.failure_threshold,
      reset_timeout=self.reset_timeout,
      time_fn=self._time_fn,
      failure_exceptions=self.failure_exceptions,
    )

  def _now(self) -> float:
    """Read the current monotonic time via the injected clock."""
    return self._time_fn()

  def _allow_call(self) -> BreakerState:
    """Decide whether a call may proceed and return the effective state.

    Called under ``self._lock``. Returns the state the caller should operate
    under:

    - ``OPEN`` → reject without touching the backend.
    - ``CLOSED`` or ``HALF_OPEN`` → invoke the backend; the caller records
      the outcome via :meth:`_record_success` / :meth:`_record_failure`.

    If the breaker is OPEN but ``reset_timeout`` has elapsed, it transitions
    to ``HALF_OPEN`` here (lazy transition on the next call — no background
    thread required) and claims the single-probe slot via
    ``_probe_in_flight``. While a probe is in flight, any other caller that
    observes HALF_OPEN is rejected as if OPEN, so only ONE thread issues the
    probe call (the documented contract).
    """
    if self._state is BreakerState.OPEN:
      now = self._now()
      opened_at = self._last_failure_time
      if opened_at is not None and (now - opened_at) >= self.reset_timeout:
        # Cool-down elapsed — allow a single probe. Claim the probe slot; no
        # other thread can enter func() in HALF_OPEN until this probe settles.
        self._state = BreakerState.HALF_OPEN
        self._probe_in_flight = True
      else:
        return BreakerState.OPEN
    elif self._state is BreakerState.HALF_OPEN:
      if self._probe_in_flight:
        # A probe is already in flight (another thread claimed the slot in this
        # HALF_OPEN window). Fail fast without issuing a second concurrent probe.
        return BreakerState.OPEN
      # A prior non-counted exception or process signal released the probe
      # slot while deliberately leaving the breaker HALF_OPEN. Re-claim it for
      # this call so concurrent callers still observe the single-probe rule.
      self._probe_in_flight = True
    return self._state

  def _record_success(self, prior_state: BreakerState) -> None:
    """Record a successful call, possibly closing the breaker.

    On any success the consecutive-failure count resets to zero. If the call
    was a HALF_OPEN probe, the breaker closes fully and the probe slot is
    released.

    Stale-prior_state guard: ``call()`` captures ``prior_state`` under the
    lock, releases it for ``func()``, then re-acquires the lock here. If
    another thread tripped the breaker OPEN during func(), the stale success
    must NOT clobber the trip's bookkeeping — clearing ``_last_failure_time``
    here would wedge the breaker OPEN forever (the cool-down check in
    :meth:`_allow_call` gates on ``opened_at is not None``, so a None
    timestamp prevents the OPEN→HALF_OPEN transition; no background timer
    runs, recovery is lazy on the next call). The late success is from a call
    that started BEFORE the trip; it must not undo a trip reflecting the
    backend's state at trip time. See
    ``test_late_success_does_not_wedge_open_breaker``.
    """
    if self._state is BreakerState.OPEN:
      # Another thread tripped the breaker while func() was in flight; the
      # stale success must not clobber the trip timestamp (would wedge OPEN).
      return
    self._failure_count = 0
    self._last_failure_time = None
    if prior_state is BreakerState.HALF_OPEN:
      self._state = BreakerState.CLOSED
      self._probe_in_flight = False

  def _record_failure(self, prior_state: BreakerState) -> None:
    """Record a failed call, possibly tripping / re-opening the breaker.

    A HALF_OPEN probe failure re-opens immediately (one strike and the
    backend is considered still-broken) and the probe slot is released so the
    next cool-down window can issue a fresh probe. A CLOSED failure increments
    the consecutive count and trips when the threshold is reached.
    """
    self._last_failure_time = self._now()
    if prior_state is BreakerState.HALF_OPEN:
      self._state = BreakerState.OPEN
      self._failure_count = 0
      self._probe_in_flight = False
      return
    self._failure_count += 1
    if self._failure_count >= self.failure_threshold:
      self._state = BreakerState.OPEN

  def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Invoke ``func(*args, **kwargs)`` under the breaker's protection.

    Behavior by state:

    - ``CLOSED`` / ``HALF_OPEN``: delegate to ``func``. On success the
      failure count resets (and HALF_OPEN closes); on failure the count
      increments (or the breaker trips / re-opens).
    - ``OPEN``: raise :class:`CircuitBreakerOpenError` **without** calling
      ``func`` — fail-fast so callers and the network are spared while the
      backend is known-broken.

    Args:
        func: The callable to invoke (typically a bound backend method).
        *args: Positional arguments forwarded to ``func``.
        **kwargs: Keyword arguments forwarded to ``func``.

    Returns:
        Whatever ``func`` returns.

    Raises:
        CircuitBreakerOpenError: If the breaker is OPEN.
        Exception: Any exception raised by ``func`` is re-raised unchanged
            after the failure is recorded.

    .. note::
        ``KeyboardInterrupt`` / ``SystemExit`` are **not** treated as backend
        failures — they propagate immediately without touching breaker state,
        matching the connect-path's broad-except discipline elsewhere in the
        package.
    """
    with self._lock:
      prior_state = self._allow_call()
      if prior_state is BreakerState.OPEN:
        raise CircuitBreakerOpenError(self.name)

    try:
      result = func(*args, **kwargs)
    except BaseException as exc:
      # Do not let Ctrl-C / interpreter shutdown perturb breaker bookkeeping.
      if isinstance(exc, (KeyboardInterrupt, SystemExit)):
        # Regression fix: _allow_call() claimed the single HALF_OPEN probe
        # slot (_probe_in_flight=True) before func ran. If the signal arrives
        # mid-probe we must release it — otherwise the breaker wedges
        # HALF_OPEN forever (no timer releases the slot; only _record_success
        # / _record_failure do, and we deliberately skip both for signals).
        if prior_state is BreakerState.HALF_OPEN:
          with self._lock:
            self._probe_in_flight = False
        raise
      if not isinstance(exc, self.failure_exceptions):
        # Caller/input errors are neither a backend success nor a backend
        # failure. A HALF_OPEN call already claimed the sole probe slot, so
        # release it without closing or re-opening the breaker; the next
        # eligible call can perform the real recovery probe.
        if prior_state is BreakerState.HALF_OPEN:
          with self._lock:
            if self._state is BreakerState.HALF_OPEN:
              self._probe_in_flight = False
        raise
      with self._lock:
        self._record_failure(prior_state)
      raise
    else:
      with self._lock:
        self._record_success(prior_state)
      return result


# ---------------------------------------------------------------------------
# Backend proxies
# ---------------------------------------------------------------------------
#
# A proxy wraps the backend's *hot-path* (network-touching) methods under a
# shared breaker while transparently forwarding every other attribute to the
# underlying backend. Non-network methods (``is_connected``, ``backend_type``,
# ``ack``/``nack`` no-ops on atomic backends, ``clear_queue``/``clear_set``/
# ``clear_storage`` which are administrative) deliberately bypass the breaker
# so that an OPEN breaker does not, e.g., block a ``is_connected()`` health
# probe or a shutdown-time ``clear_*``.
#
# The proxies subclass the interface ABCs purely so ``isinstance(proxy,
# QueueBackend)`` etc. continue to hold — existing code (and the
# ConnectionManager's own ``isinstance`` check) relies on the interface type.


class _BackendProxyBase:
  """Common proxy machinery: bind interface methods to wrapped-or-forwarded callables.

  The proxy subclasses the interface ABC purely so ``isinstance(proxy,
  QueueBackend)`` etc. continue to hold. But because the ABCs define every
  interface method on the class, ``__getattr__`` (which fires only when normal
  lookup FAILS) would never forward non-hot-path methods — Python would
  resolve ``proxy.clear_queue`` to the ABC's abstract stub and silently
  no-op instead of delegating to the wrapped backend.

  To avoid that, the constructor binds EVERY interface method as an INSTANCE
  attribute (which shadows the class-level ABC stub):

  - hot-path methods → ``breaker.call``-wrapped bound method
  - every other interface method → the backend's own bound method, unchanged

  Non-method attributes (``backend_type``, ``_backend`` internals) keep
  resolving via ``__getattr__`` → the wrapped backend, since they aren't on
  the class MRO as shadowing descriptors.
  """

  # Subclasses override: names wrapped under the breaker.
  _HOT_PATH: tuple[str, ...] = ()
  # Subclasses override: every other method the interface ABC declares, so we
  # bind them as plain forwarded instance attributes and bypass the ABC stub.
  _FORWARDED: tuple[str, ...] = ()

  def __init__(self, backend: Any, breaker: CircuitBreaker) -> None:
    # Use object.__setattr__ to avoid recursing through our own __setattr__.
    object.__setattr__(self, "_backend", backend)
    object.__setattr__(self, "_breaker", breaker)
    for method_name in self._HOT_PATH:
      if not hasattr(backend, method_name):
        continue
      bound = getattr(backend, method_name)
      object.__setattr__(self, method_name, _wrap_bound(breaker, bound))
    for method_name in self._FORWARDED:
      if not hasattr(backend, method_name):
        continue
      object.__setattr__(self, method_name, getattr(backend, method_name))

  def __getattr__(self, name: str) -> Any:
    # Fires only for attributes NOT on the class MRO and NOT bound in __init__
    # — e.g. ``backend_type`` property, backend-specific attributes. Forwards
    # to the wrapped backend. Non-interface attributes have zero overhead on
    # the hot path because those are bound as instance attributes above.
    return getattr(self._backend, name)


def _wrap_bound(breaker: CircuitBreaker, func: Callable[..., Any]) -> Callable[..., Any]:
  """Return a thin wrapper that funnels ``func`` through ``breaker.call``.

  Using ``breaker.call`` (rather than re-implementing the state machine inline)
  keeps a single source of truth for state transitions and locking. The wrapper
  preserves ``functools.wraps``-style metadata for debuggability.
  """

  def _wrapped(*args: Any, **kwargs: Any) -> Any:
    return breaker.call(func, *args, **kwargs)

  _wrapped.__name__ = getattr(func, "__name__", "wrapped")
  _wrapped.__doc__ = getattr(func, "__doc__", None)
  return _wrapped


class _QueueBackendProxy(_BackendProxyBase, QueueBackend):
  """Wrap a :class:`QueueBackend`'s hot-path ops under a breaker.

  ``queue_len`` is deliberately NOT in the hot path: it is an admin /
  observability probe (stats queries, ``has_pending_requests`` health checks).
  A transient failure in the length query (e.g. a momentary ``CLUSTER DOWN``
  during a stats scrape) must not cascade into a full traffic shutdown by
  tripping the breaker. Traffic-bearing ops (``push``/``pop``/
  ``pop_with_ack``) are wrapped; ``queue_len`` is forwarded unchanged so its
  failures never reach the breaker.

  ``pop_with_ack`` (2026-07-10 fix) must be in the hot path so that (a) a
  broker degradation on the MQ ack-pop path trips the breaker, and (b) the
  proxy dispatches to the backend's ``pop_with_ack`` *override* (the
  per-message token path) rather than the ``QueueBackend`` ABC default. Without
  this the override is shadowed by the ABC default ``pop_with_ack → (self.pop,
  None)`` and MQ tokens silently become ``None`` under
  ``SCRAPY_CIRCUIT_BREAKER_ENABLED``.
  """

  _HOT_PATH = ("push", "pop", "pop_with_ack")
  _FORWARDED = (
    "queue_len",
    "clear_queue",
    "ack",
    "nack",
    "connect",
    "disconnect",
    "is_connected",
    "ping",
  )


class _SetBackendProxy(_BackendProxyBase, SetBackend):
  """Wrap a :class:`SetBackend`'s hot-path ops under a breaker."""

  _HOT_PATH = ("add", "contains", "remove")
  _FORWARDED = ("set_len", "clear_set", "connect", "disconnect", "is_connected", "ping")


class _StorageBackendProxy(_BackendProxyBase, StorageBackend):
  """Wrap a :class:`StorageBackend`'s hot-path ops under a breaker."""

  _HOT_PATH = ("store", "retrieve", "delete")
  _FORWARDED = ("exists", "ttl", "clear_storage", "connect", "disconnect", "is_connected", "ping")


# The proxies are pure forwarders: hot-path methods are installed on each
# instance in ``__init__`` (breaker-wrapped), and every other attribute is
# forwarded to the wrapped backend via ``__getattr__``. ABCMeta, however,
# checks ``__abstractmethods__`` at *class* creation time — before any instance
# exists — and would refuse to instantiate the proxy because the class body
# doesn't define ``push``/``pop``/``add``/``store``/etc. as concrete methods.
#
# Clearing ``__abstractmethods__`` is the standard escape hatch for proxies and
# decorators that forward dynamically (see e.g. ``weakref.proxy`` /
# ``functools.update_wrapper`` patterns). The proxies satisfy the interface
# contract at runtime — ``isinstance(proxy, QueueBackend)`` returns True and
# every abstract method resolves to a concrete callable via the wrapped backend
# — so this does not weaken the type contract, it only satisfies ABCMeta's
# static, class-body-only check.
for _proxy_cls in (_QueueBackendProxy, _SetBackendProxy, _StorageBackendProxy):
  _proxy_cls.__abstractmethods__ = frozenset()


def wrap_queue_backend(backend: QueueBackend, breaker: CircuitBreaker) -> QueueBackend:
  """Wrap ``backend``'s push/pop/pop_with_ack under ``breaker``.

  ``queue_len`` is forwarded unchanged (NOT wrapped): it is an admin /
  observability probe, and a transient stats-query failure must not cascade
  into a full traffic shutdown by tripping the breaker. Other attributes
  (including ``clear_queue``, ``ack``, ``nack``, ``is_connected``) also forward
  unchanged so an OPEN breaker does not block administrative / non-network
  operations. ``pop_with_ack`` IS wrapped (2026-07-10) so MQ ack-pop failures
  trip the breaker and the backend's per-message override is not shadowed by
  the ABC default.
  """
  return _QueueBackendProxy(backend, breaker)  # type: ignore[abstract]


def wrap_set_backend(backend: SetBackend, breaker: CircuitBreaker) -> SetBackend:
  """Wrap ``backend``'s add/contains/remove under ``breaker``."""
  return _SetBackendProxy(backend, breaker)  # type: ignore[abstract]


def wrap_storage_backend(
  backend: StorageBackend, breaker: CircuitBreaker
) -> StorageBackend:
  """Wrap ``backend``'s store/retrieve/delete under ``breaker``."""
  return _StorageBackendProxy(backend, breaker)  # type: ignore[abstract]
