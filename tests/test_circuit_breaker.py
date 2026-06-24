"""Tests for the CircuitBreaker state machine and backend proxies.

Covers the closed→open→half-open state machine, thread-safety, and that
the per-interface proxies wrap ONLY the hot-path methods while forwarding
non-network operations unchanged.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from scrapy_extension.backends.base import QueueBackend, SetBackend, StorageBackend
from scrapy_extension.backends.circuit_breaker import (
  BreakerState,
  CircuitBreaker,
  CircuitBreakerOpenError,
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

  def test_system_exit_does_not_trip(self):
    b = CircuitBreaker("q", failure_threshold=1)

    def raises_se(*_args, **_kwargs):
      raise SystemExit(0)

    with pytest.raises(SystemExit):
      b.call(raises_se)
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
