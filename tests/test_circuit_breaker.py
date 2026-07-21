"""Tests for the CircuitBreaker state machine and backend proxies.

Covers the closed→open→half-open state machine, thread-safety, and that
the per-interface proxies wrap ONLY the hot-path methods while forwarding
non-network operations unchanged.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from scrapy_extension.backends.base import QueueBackend, SetBackend, StorageBackend
from scrapy_extension.backends.circuit_breaker import (
  BreakerState,
  CircuitBreaker,
  CircuitBreakerOpenError,
  _BackendProxyBase,
  wrap_queue_backend,
  wrap_set_backend,
  wrap_storage_backend,
)

# ---------------------------------------------------------------------------
# Fake time source — deterministic clock for reset-timeout transitions.
# ---------------------------------------------------------------------------


class FakeClock:
  """Manually-advanced monotonic clock for deterministic breaker timing."""

  def __init__(self, start: float = 0.0) -> None:
    self._t = start

  def __call__(self) -> float:
    return self._t

  def advance(self, seconds: float) -> None:
    self._t += seconds


# ---------------------------------------------------------------------------
# Failing-callable helpers
# ---------------------------------------------------------------------------


def _boom(*_args: Any, **_kwargs: Any) -> Any:
  raise RuntimeError("backend on fire")


def _ok(*_args: Any, **_kwargs: Any) -> Any:
  return "ok"


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


class TestCircuitBreakerConstruction:
  def test_defaults_are_closed(self):
    b = CircuitBreaker("redis-queue")
    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 0
    assert b.last_failure_time is None

  def test_invalid_failure_threshold_raises(self):
    with pytest.raises(ValueError, match="failure_threshold"):
      CircuitBreaker("x", failure_threshold=0)

  def test_invalid_reset_timeout_raises(self):
    with pytest.raises(ValueError, match="reset_timeout"):
      CircuitBreaker("x", reset_timeout=-1.0)


# ---------------------------------------------------------------------------
# CLOSED → OPEN at threshold
# ---------------------------------------------------------------------------


class TestTrippingOpen:
  def test_below_threshold_stays_closed(self):
    b = CircuitBreaker("q", failure_threshold=3)
    for _ in range(2):
      with pytest.raises(RuntimeError):
        b.call(_boom)
    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 2

  def test_at_threshold_trips_open(self):
    b = CircuitBreaker("q", failure_threshold=3)
    for _ in range(3):
      with pytest.raises(RuntimeError):
        b.call(_boom)
    assert b.state is BreakerState.OPEN

  def test_failure_count_increments_per_failure(self):
    b = CircuitBreaker("q", failure_threshold=5)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.failure_count == 1
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.failure_count == 2


# ---------------------------------------------------------------------------
# OPEN rejects without calling the backend
# ---------------------------------------------------------------------------


class TestOpenRejectsFast:
  def test_open_raises_circuit_breaker_open_error_without_calling(self):
    b = CircuitBreaker("redis-q", failure_threshold=1)
    calls = []

    def tracker(*args, **kwargs):
      calls.append(1)
      return None

    # Trip the breaker.
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN

    # A subsequent call must NOT invoke the backend at all.
    with pytest.raises(CircuitBreakerOpenError) as exc_info:
      b.call(tracker)
    assert exc_info.value.name == "redis-q"
    assert calls == []

  def test_open_error_subclasses_backend_error(self):
    from scrapy_extension.exceptions import BackendError

    b = CircuitBreaker("q", failure_threshold=1)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    with pytest.raises(CircuitBreakerOpenError) as exc_info:
      b.call(_ok)
    assert isinstance(exc_info.value, BackendError)


# ---------------------------------------------------------------------------
# HALF_OPEN transition after reset_timeout
# ---------------------------------------------------------------------------


class TestHalfOpenTransition:
  def test_open_transitions_to_half_open_after_timeout(self):
    clock = FakeClock()
    b = CircuitBreaker("q", failure_threshold=1, reset_timeout=30.0, time_fn=clock)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN

    # Still open just before the reset timeout elapses.
    clock.advance(29.9)
    with pytest.raises(CircuitBreakerOpenError):
      b.call(_ok)
    # (state is re-evaluated lazily on each call; stays OPEN until timeout.)

    # After the reset timeout, the next call becomes a probe.
    clock.advance(0.2)  # total elapsed since failure >= 30.0
    assert b.call(_ok) == "ok"
    # Successful probe -> CLOSED, count reset.
    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 0

  def test_probe_failure_reopens(self):
    clock = FakeClock()
    b = CircuitBreaker("q", failure_threshold=2, reset_timeout=5.0, time_fn=clock)
    # Trip: two failures to reach threshold.
    with pytest.raises(RuntimeError):
      b.call(_boom)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN

    clock.advance(5.0)
    # HALF_OPEN probe fails -> back to OPEN immediately.
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN


# ---------------------------------------------------------------------------
# Failures reset on success
# ---------------------------------------------------------------------------


class TestSuccessResets:
  def test_success_in_closed_resets_count(self):
    b = CircuitBreaker("q", failure_threshold=3)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.failure_count == 2
    # A success clears the consecutive-failure tally.
    assert b.call(_ok) == "ok"
    assert b.failure_count == 0
    # Now we need a fresh run of 3 to trip.
    for _ in range(2):
      with pytest.raises(RuntimeError):
        b.call(_boom)
    assert b.state is BreakerState.CLOSED

  def test_half_open_probe_success_closes_and_resets(self):
    clock = FakeClock()
    b = CircuitBreaker("q", failure_threshold=1, reset_timeout=10.0, time_fn=clock)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN
    clock.advance(10.0)
    # Probe succeeds -> CLOSED, and last_failure_time is cleared.
    assert b.call(_ok) == "ok"
    assert b.state is BreakerState.CLOSED
    assert b.last_failure_time is None


class TestStaleSuccessRace:
  """A late success from a slow call (stale ``prior_state``) must not wedge the
  breaker OPEN.

  Race: ``call()`` captures ``prior_state`` under the lock, RELEASES the lock,
  runs ``func()`` (slow, no lock), then re-acquires the lock and calls
  ``_record_success(prior_state)`` with a STALE prior_state. If another thread
  trips the breaker OPEN during func(), the stale ``_record_success(CLOSED)``
  clears ``_last_failure_time = None``. The cool-down check in ``_allow_call``
  gates on ``opened_at is not None`` — a None timestamp prevents the
  OPEN->HALF_OPEN transition, so the breaker can NEVER recover (no background
  timer; recovery is lazy on the next call). Backend permanently unreachable
  until manual ``reset()`` or process restart.
  """

  def test_late_success_does_not_wedge_open_breaker(self):
    clock = FakeClock()
    b = CircuitBreaker("q", failure_threshold=3, reset_timeout=10.0, time_fn=clock)

    # Thread A: capture prior_state=CLOSED under the lock (the slow call's
    # acquire), then "release" — func() is now in flight (not simulated).
    with b._lock:
      prior_state = b._allow_call()
    assert prior_state is BreakerState.CLOSED

    # Thread B: while A's func() is in flight, threshold failures trip OPEN.
    for _ in range(3):
      with b._lock:
        b._record_failure(BreakerState.CLOSED)
    assert b.state is BreakerState.OPEN
    trip_time = b.last_failure_time
    assert trip_time is not None

    # Thread A: slow func() finally SUCCEEDS and records with the STALE
    # prior_state=CLOSED (captured before the trip).
    with b._lock:
      b._record_success(prior_state)

    # The breaker must STILL be OPEN with the trip timestamp INTACT — the wedge
    # would clear _last_failure_time=None, blocking the OPEN->HALF_OPEN check.
    assert b.state is BreakerState.OPEN
    assert b.last_failure_time == trip_time, (
      "late success clobbered _last_failure_time -> breaker wedged OPEN forever "
      "(cool-down check gates on opened_at is not None)"
    )

    # Recovery: after reset_timeout elapses, _allow_call MUST transition to
    # HALF_OPEN. The wedge (cleared timestamp) returns OPEN here — the regression.
    clock.advance(100.0)
    with b._lock:
      effective = b._allow_call()
    assert effective is BreakerState.HALF_OPEN, (
      "breaker could not recover OPEN->HALF_OPEN — _last_failure_time was "
      "cleared by a stale success (permanent backend outage)"
    )


# ---------------------------------------------------------------------------
# Reset helper
# ---------------------------------------------------------------------------


class TestReset:
  def test_reset_returns_to_closed_and_clears_count(self):
    b = CircuitBreaker("q", failure_threshold=1)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN
    b.reset()
    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 0
    assert b.last_failure_time is None


# ---------------------------------------------------------------------------
# Keyboard-interrupt / SystemExit are not treated as backend failures
# ---------------------------------------------------------------------------


class TestSignalPassthrough:
  def test_keyboard_interrupt_does_not_trip(self):
    b = CircuitBreaker("q", failure_threshold=1)

    def raises_ki(*_args, **_kwargs):
      raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
      b.call(raises_ki)
    # Breaker must remain CLOSED — Ctrl-C is not a backend failure.
    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 0

  def test_signal_during_half_open_probe_does_not_wedge(self):
    # Regression: a Ctrl-C / SystemExit arriving during a HALF_OPEN probe used
    # to re-raise without releasing _probe_in_flight. Since the breaker sits in
    # HALF_OPEN (not OPEN), no cool-down timer ever releases the slot — every
    # subsequent call was rejected as OPEN permanently (until process restart).
    clock = FakeClock()
    b = CircuitBreaker("q", failure_threshold=1, reset_timeout=30.0, time_fn=clock)
    with pytest.raises(RuntimeError):
      b.call(_boom)
    assert b.state is BreakerState.OPEN

    clock.advance(30.0)  # cool-down elapses -> next call claims the probe slot
    def raises_ki(*_args, **_kwargs):
      raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
      b.call(raises_ki)
    # Ctrl-C is not a failure -> state stays HALF_OPEN, but the probe slot
    # MUST be released so the next call can retry.
    assert b.state is BreakerState.HALF_OPEN
    # Pre-fix this raised CircuitBreakerOpenError forever. With the fix the
    # next call retries the probe and succeeds -> CLOSED.
    assert b.call(_ok) == "ok"
    assert b.state is BreakerState.CLOSED

  def test_system_exit_does_not_trip(self):
    b = CircuitBreaker("q", failure_threshold=1)

    def raises_se(*_args, **_kwargs):
      raise SystemExit(0)

    with pytest.raises(SystemExit):
      b.call(raises_se)
    assert b.state is BreakerState.CLOSED


class TestCountedFailureContract:
  def test_non_counted_exception_does_not_trip_or_reset_failures(self):
    from scrapy_extension.exceptions import BackendError, QueueError

    b = CircuitBreaker(
      "backend",
      failure_threshold=2,
      failure_exceptions=(BackendError,),
    )
    with pytest.raises(QueueError):
      b.call(lambda: (_ for _ in ()).throw(QueueError("broker down")))
    assert b.failure_count == 1

    with pytest.raises(ValueError):
      b.call(lambda: (_ for _ in ()).throw(ValueError("bad key")))

    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 1

  def test_non_counted_half_open_exception_releases_probe_slot(self):
    from scrapy_extension.exceptions import BackendError, QueueError

    clock = FakeClock()
    b = CircuitBreaker(
      "backend",
      failure_threshold=1,
      reset_timeout=5,
      time_fn=clock,
      failure_exceptions=(BackendError,),
    )
    with pytest.raises(QueueError):
      b.call(lambda: (_ for _ in ()).throw(QueueError("broker down")))
    clock.advance(5)

    with pytest.raises(ValueError):
      b.call(lambda: (_ for _ in ()).throw(ValueError("bad key")))

    assert b.state is BreakerState.HALF_OPEN
    assert b.call(_ok) == "ok"
    assert b.state is BreakerState.CLOSED


# ---------------------------------------------------------------------------
# Thread-safety: concurrent call() under failures must not corrupt state
# ---------------------------------------------------------------------------


class TestThreadSafety:
  def test_concurrent_failures_trip_exactly_once(self):
    """N threads racing to call a failing op must land in OPEN, not crash.

    The breaker lock must serialize the failure-recording critical section so
    the count never goes negative or races past threshold into an inconsistent
    state.
    """
    b = CircuitBreaker("q", failure_threshold=10)
    barrier = threading.Barrier(20)
    errors: list[BaseException] = []

    def worker():
      barrier.wait()
      for _ in range(50):
        try:
          b.call(_boom)
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
          errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert b.state is BreakerState.OPEN
    # No unexpected exception types leaked (only RuntimeError + our open error).
    assert all(
      isinstance(e, (RuntimeError, CircuitBreakerOpenError)) for e in errors
    )

  def test_concurrent_success_path_is_safe(self):
    b = CircuitBreaker("q", failure_threshold=5)
    barrier = threading.Barrier(10)

    def worker():
      barrier.wait()
      for _ in range(100):
        b.call(_ok)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert b.state is BreakerState.CLOSED
    assert b.failure_count == 0


# ---------------------------------------------------------------------------
# Backend proxies: hot-path wrapped, non-network forwarded
# ---------------------------------------------------------------------------


class _FakeQueueBackend(QueueBackend):
  def __init__(self) -> None:
    self.pushed: list[tuple[str, bytes, float]] = []
    self.clear_calls = 0
    self.ack_calls = 0

  def connect(self) -> None: ...
  def disconnect(self) -> None: ...
  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    from scrapy_extension.backends.base import BackendType

    return BackendType.REDIS

  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    self.pushed.append((queue_name, item, priority))

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    return None

  def queue_len(self, queue_name: str) -> int:
    return 0

  def clear_queue(self, queue_name: str) -> None:
    self.clear_calls += 1

  def ack(self, queue_name: str) -> None:
    self.ack_calls += 1


class _FakeMQBackend(_FakeQueueBackend):
  """MQ-style queue backend that overrides ``pop_with_ack`` (per-message token).

  Distinct return values on ``pop()`` vs ``pop_with_ack()`` let tests
  discriminate which dispatch path ``BackendQueue._pop_with_ack`` took.
  """

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    return b"POP-PATH"

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    return (b"ACK-PATH", "REAL-TOKEN")


class _FakeSetBackend(SetBackend):
  def __init__(self) -> None:
    self.added: list[bytes] = []

  def add(self, set_name: str, item: bytes) -> bool:
    self.added.append(item)
    return True

  def remove(self, set_name: str, item: bytes) -> bool:
    return False

  def contains(self, set_name: str, item: bytes) -> bool:
    return False

  def set_len(self, set_name: str) -> int:
    return 0

  def clear_set(self, set_name: str) -> None: ...

  def connect(self) -> None: ...
  def disconnect(self) -> None: ...
  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    from scrapy_extension.backends.base import BackendType

    return BackendType.REDIS


class _FakeStorageBackend(StorageBackend):
  def __init__(self) -> None:
    self.stored: list[tuple[str, bytes]] = []

  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    self.stored.append((key, data))

  def retrieve(self, key: str) -> bytes | None:
    return None

  def delete(self, key: str) -> bool:
    return False

  def exists(self, key: str) -> bool:
    return False

  def ttl(self, key: str) -> int | None:
    return None

  def clear_storage(self, prefix: str | None = None) -> None: ...

  def connect(self) -> None: ...
  def disconnect(self) -> None: ...
  def is_connected(self) -> bool:
    return True

  def ping(self) -> bool:
    return True

  @property
  def backend_type(self):
    from scrapy_extension.backends.base import BackendType

    return BackendType.REDIS


class TestQueueBackendProxy:
  def test_isinstance_preserved(self):
    b = CircuitBreaker("q", failure_threshold=2)
    wrapped = wrap_queue_backend(_FakeQueueBackend(), b)
    assert isinstance(wrapped, QueueBackend)

  def test_hot_path_wrapped_and_open_rejects(self):
    # The proxy snapshots hot-path bound methods at construction; build the
    # proxy around an already-failing backend so the wrapped push trips it.
    backend = _FakeQueueBackend()
    backend.push = _boom  # type: ignore[method-assign]
    breaker = CircuitBreaker("q", failure_threshold=1)
    wrapped = wrap_queue_backend(backend, breaker)

    with pytest.raises(RuntimeError):
      wrapped.push("q", b"x")
    assert breaker.state is BreakerState.OPEN

    # The OPEN breaker rejects push WITHOUT calling the backend.
    call_count = [0]

    def would_call(*_a, **_k):
      call_count[0] += 1

    backend.push = would_call  # type: ignore[method-assign]
    # Note: backend.push reassignment does NOT affect the proxy's captured
    # wrapped method — but in OPEN state the breaker never calls it anyway,
    # so we assert via the breaker's own rejection.
    with pytest.raises(CircuitBreakerOpenError):
      wrapped.push("q", b"x")
    assert call_count[0] == 0

  def test_non_hot_path_methods_forwarded_unchanged(self):
    # Proxy snapshots hot-path at construction; build around a failing pop.
    backend = _FakeQueueBackend()
    backend.pop = _boom  # type: ignore[method-assign]
    breaker = CircuitBreaker("q", failure_threshold=1)
    wrapped = wrap_queue_backend(backend, breaker)

    # Trip the breaker via pop.
    with pytest.raises(RuntimeError):
      wrapped.pop("q")
    assert breaker.state is BreakerState.OPEN

    # Non-network methods must still work — they are NOT blocked by the breaker.
    wrapped.clear_queue("q")
    wrapped.ack("q")
    assert backend.clear_calls == 1
    assert backend.ack_calls == 1
    # is_connected forwards to the wrapped backend.
    assert wrapped.is_connected() is True

  def test_success_path_delegates(self):
    backend = _FakeQueueBackend()
    breaker = CircuitBreaker("q", failure_threshold=3)
    wrapped = wrap_queue_backend(backend, breaker)
    wrapped.push("q1", b"a", 5.0)
    assert backend.pushed == [("q1", b"a", 5.0)]
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0

  def test_pop_with_ack_success_dispatches_to_override_and_preserves_token(self):
    """GREEN-side companion to the failure test: a SUCCESSFUL pop_with_ack
    through the proxy must dispatch to the backend override AND return its
    per-message token (not the ABC default's (pop(), None)). Locks in that
    wrapping does not silently downgrade MQ ack semantics on the happy path.
    """
    backend = _FakeMQBackend()
    breaker = CircuitBreaker("q", failure_threshold=3)
    wrapped = wrap_queue_backend(backend, breaker)
    data, token = wrapped.pop_with_ack("q", 0.0)
    assert data == b"ACK-PATH"
    assert token == "REAL-TOKEN"
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0

  def test_backend_queue_pop_with_ack_token_survives_breaker_proxy(self, mocker):
    """2026-07-11: the PRODUCTION path BackendQueue._pop_with_ack must carry
    the MQ per-message ack token even when the backend is breaker-wrapped.

    The class-level override detection in queue.py (backend.__class__
    .pop_with_ack is not QueueBackend.pop_with_ack) inspects the PROXY class
    when the backend is wrapped — the proxy resolves pop_with_ack to the ABC
    default via MRO, so pre-fix the detection reports no override → backend
    .pop() → token=None (the reviewer-reproduced hazard under
    SCRAPY_CIRCUIT_BREAKER_ENABLED). Post-fix the detection unwraps the proxy
    (._backend) and the token survives end-to-end.
    """
    from scrapy_extension.queue.queue import BackendQueue
    from scrapy_extension.queue.strategies.base import _BoundQueueAckToken
    from scrapy_extension.queue.strategies.passthrough import (
      PassthroughQueueStrategy,
    )

    raw = _FakeMQBackend()
    breaker = CircuitBreaker("q", failure_threshold=5)
    wrapped = wrap_queue_backend(raw, breaker)
    cm = mocker.MagicMock()
    cm.get_queue_backend.return_value = wrapped
    storage_mock = mocker.MagicMock()
    storage_mock.retrieve.return_value = None
    cm.get_storage_backend.return_value = storage_mock

    bq = BackendQueue(
      connection_manager=cm,
      queue_name="q",
      spider=None,
      queue_strategy=PassthroughQueueStrategy(cm),
    )
    data, token = bq._pop_with_ack(0.0)
    assert data == b"ACK-PATH"
    assert isinstance(token, _BoundQueueAckToken)
    assert token.backend is wrapped
    assert token.queue_name == "q"
    assert token.token == "REAL-TOKEN"


  def test_pop_with_ack_is_hot_path_and_dispatches_to_override(self):
    """pop_with_ack is MQ traffic — it must be breaker-wrapped AND dispatch to
    the backend's override (the per-message token path), not the ABC default.

    Pre-fix: ``pop_with_ack`` is in neither ``_HOT_PATH`` nor ``_FORWARDED``,
    so ``wrapped.pop_with_ack`` resolves via normal class lookup to the
    ``QueueBackend`` ABC default ``(self.pop(), None)``. The MQ backend's
    override is shadowed (token silently None under breaker+MQ) AND a
    ``pop_with_ack`` failure never reaches the breaker → RED on both counts.
    Post-fix: ``pop_with_ack`` in ``_HOT_PATH`` → the backend override is
    captured + breaker-wrapped → GREEN.
    """
    backend = _FakeQueueBackend()
    calls: list[tuple] = []

    def _mq_pop_with_ack(queue_name: str, timeout: float = 0.0) -> tuple[bytes | None, Any | None]:
      calls.append((queue_name, timeout))
      msg = "broker down"
      raise RuntimeError(msg)

    backend.pop_with_ack = _mq_pop_with_ack  # type: ignore[method-assign]
    breaker = CircuitBreaker("q", failure_threshold=1)
    wrapped = wrap_queue_backend(backend, breaker)

    with pytest.raises(RuntimeError):
      wrapped.pop_with_ack("q", 0.0)
    # The backend override WAS called (not the ABC default that calls self.pop).
    assert calls == [("q", 0.0)]
    # And the breaker recorded the failure (pop_with_ack is hot-path).
    assert breaker.state is BreakerState.OPEN
    assert breaker.failure_count == 1


class TestSetBackendProxy:
  def test_hot_path_wrapped(self):
    # The proxy snapshots hot-path bound methods at construction; to trip the
    # breaker through the proxy we build it around an already-failing backend.
    backend = _FakeSetBackend()
    backend.add = _boom  # type: ignore[method-assign]
    breaker = CircuitBreaker("s", failure_threshold=1)
    wrapped = wrap_set_backend(backend, breaker)
    assert isinstance(wrapped, SetBackend)

    with pytest.raises(RuntimeError):
      wrapped.add("s", b"x")
    assert breaker.state is BreakerState.OPEN
    # non-hot-path forwarded
    wrapped.clear_set("s")
    assert breaker.state is BreakerState.OPEN  # clear didn't reset


class TestStorageBackendProxy:
  def test_hot_path_wrapped(self):
    # Proxy snapshots hot-path at construction; build around failing store.
    backend = _FakeStorageBackend()
    backend.store = _boom  # type: ignore[method-assign]
    breaker = CircuitBreaker("st", failure_threshold=1)
    wrapped = wrap_storage_backend(backend, breaker)
    assert isinstance(wrapped, StorageBackend)

    with pytest.raises(RuntimeError):
      wrapped.store("k", b"v")
    assert breaker.state is BreakerState.OPEN
    # exists / clear_storage are NOT hot-path -> forwarded through __getattr__
    assert wrapped.exists("k") is False
    wrapped.clear_storage()
    assert breaker.state is BreakerState.OPEN

  def test_store_success_delegates(self):
    backend = _FakeStorageBackend()
    breaker = CircuitBreaker("st", failure_threshold=2)
    wrapped = wrap_storage_backend(backend, breaker)
    wrapped.store("k", b"v", ttl=10)
    assert backend.stored == [("k", b"v")]
    assert breaker.failure_count == 0


# ---------------------------------------------------------------------------
# E3 — queue_len is an admin/observability probe, NOT hot-path traffic.
# A transient failure in the stats query must NOT cascade into a full traffic
# shutdown by tripping the breaker.
# ---------------------------------------------------------------------------


class TestQueueLenNotHotPath:
  def test_queue_len_failures_do_not_trip_the_breaker(self):
    """queue_len is an observability probe — failures must not trip the breaker.

    Pre-fix (queue_len in _HOT_PATH): N consecutive queue_len failures trip the
    breaker → RED. Post-fix (queue_len removed from _HOT_PATH): queue_len is
    forwarded unwrapped, so failures never reach the breaker → GREEN.
    """
    backend = _FakeQueueBackend()
    # queue_len always fails; push/pop succeed.
    backend.queue_len = _boom  # type: ignore[method-assign]
    breaker = CircuitBreaker("q", failure_threshold=3)
    wrapped = wrap_queue_backend(backend, breaker)

    # Hammer queue_len well past the failure threshold.
    for _ in range(10):
      with pytest.raises(RuntimeError):
        wrapped.queue_len("q")

    # Breaker must remain CLOSED — queue_len is an admin probe, not traffic.
    assert breaker.state is BreakerState.CLOSED
    assert breaker.failure_count == 0

    # And traffic ops (push/pop) still work through the breaker.
    wrapped.push("q", b"x", 1.0)
    assert backend.pushed == [("q", b"x", 1.0)]
    assert breaker.state is BreakerState.CLOSED


# ---------------------------------------------------------------------------
# E4 — single half-open probe under concurrency.
# When the breaker is OPEN and the reset_timeout elapses, only ONE thread must
# issue the probe call. The lock is released between _allow_call() (OPEN→
# HALF_OPEN) and func(), so N threads in the reset window can all flip to
# HALF_OPEN and call the backend concurrently.
# ---------------------------------------------------------------------------


class TestHalfOpenSingleProbe:
  def test_concurrent_probes_issue_exactly_one_func_call(self):
    """N threads blocked on an OPEN breaker, clock advances past reset_timeout,
    all released → exactly ONE func() probe call reaches the backend.

    Pre-fix (lock released between OPEN→HALF_OPEN and func): multiple threads
    observe HALF_OPEN and call func concurrently → call_count > 1 → RED.
    Post-fix (probe serialized): exactly one call → GREEN.
    """
    clock = FakeClock()
    breaker = CircuitBreaker(
      "q", failure_threshold=1, reset_timeout=30.0, time_fn=clock
    )
    # Trip the breaker to OPEN.
    with pytest.raises(RuntimeError):
      breaker.call(_boom)
    assert breaker.state is BreakerState.OPEN

    call_count = [0]
    call_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def probe(*_a, **_k):
      # The race window is the gap between _allow_call() flipping to
      # HALF_OPEN and this func body executing. Count callers atomically.
      with call_lock:
        call_count[0] += 1
      # Sleep briefly to widen the race window so other threads can enter
      # if the lock is dropped prematurely.
      time.sleep(0.02)
      return "ok"

    def worker():
      barrier.wait()
      try:
        breaker.call(probe)
      except CircuitBreakerOpenError:
        pass  # allowed — some threads may still see OPEN if not chosen as probe

    n_threads = 8
    threads = [threading.Thread(target=worker) for _ in range(n_threads)]

    # Advance the clock past the reset timeout BEFORE releasing the threads,
    # so every thread's _allow_call() will observe HALF_OPEN on its first call.
    clock.advance(30.0)

    for t in threads:
      t.start()
    for t in threads:
      t.join()

    # The single-probe contract: at most ONE func() call.
    # (We assert <= 1 rather than == 1 because if the first probe SUCCEEDS the
    # breaker closes immediately and subsequent threads legitimately proceed —
    # but those subsequent calls are CLOSED-state traffic calls, not probes,
    # and with the fix only the first thread reaches func() while in HALF_OPEN.
    # The race we are fixing is multiple CONCURRENT probes during the
    # HALF_OPEN window itself. A correct fix makes call_count == 1: the probe
    # runs under the lock, so the first thread closes the breaker before any
    # other thread enters func().)
    assert call_count[0] == 1, (
      f"expected exactly one probe call, got {call_count[0]} — "
      "multiple threads observed HALF_OPEN concurrently"
    )


class TestBackendProxyBaseConstructionSkips:
  """Cover the ``hasattr`` skip branches in ``_BackendProxyBase.__init__``
  (circuit_breaker.py lines 330 + 335) and the ``__getattr__`` forward
  (line 343).

  The ABC-typed proxies (_QueueBackendProxy etc.) instantiate the FULL
  fake backends in every other test, so the skip branches never fire.
  Here we subclass _BackendProxyBase directly (no ABC constraint) and wrap
  a backend missing some declared HOT_PATH / FORWARDED methods.
  """

  def test_skips_hot_path_and_forwarded_methods_backend_lacks(self) -> None:
    """A backend that lacks a declared HOT_PATH or FORWARDED method is
    wrapped without crash — the missing methods are simply not bound as
    instance attributes (the ``continue`` branches at lines 330 + 335)."""

    class _PartialProxy(_BackendProxyBase):
      _HOT_PATH = ("push", "pop")
      _FORWARDED = ("clear_queue", "ack")

    class _MinimalBackend:
      """Has ``push`` + a custom attr, but lacks pop/clear_queue/ack."""

      def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        pass

      custom_attr = "backend-value"

    breaker = CircuitBreaker("q", failure_threshold=1)
    proxy = _PartialProxy(_MinimalBackend(), breaker)

    # push (declared HOT_PATH + present) IS bound as an instance attribute.
    assert "push" in vars(proxy)
    # pop (declared HOT_PATH but ABSENT on the backend) is NOT bound —
    # the ``if not hasattr(backend, method_name): continue`` fired.
    assert "pop" not in vars(proxy)
    # clear_queue + ack (declared FORWARDED but absent) are NOT bound.
    assert "clear_queue" not in vars(proxy)
    assert "ack" not in vars(proxy)

  def test_getattr_forwards_non_method_attribute(self) -> None:
    """``__getattr__`` fires for attributes NOT bound in __init__ and NOT on
    the class MRO — e.g. a backend-specific custom attribute. It forwards to
    the wrapped backend (line 343)."""

    class _PlainProxy(_BackendProxyBase):
      _HOT_PATH = ()
      _FORWARDED = ()

    class _BackendWithAttr:
      custom_attr = "forwarded-value"

      def connect(self) -> None:
        pass

    breaker = CircuitBreaker("q", failure_threshold=1)
    proxy = _PlainProxy(_BackendWithAttr(), breaker)

    # custom_attr is not a method bound in __init__ → __getattr__ forwards it.
    assert proxy.custom_attr == "forwarded-value"
