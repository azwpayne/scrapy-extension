"""Coverage + regression suite for ConnectionManager (baseline rank #1).

Closes the hottest gaps in src/scrapy_extension/backends/connectors.py
(retry/backoff, registry-cap eviction, breaker-wiring, A2 single-connect)
and guards the registry-lock fix (victim disconnect outside _registry_lock).
See docs/superpowers/specs/2026-07-01-connection-manager-suite-lock-fix-design.md.
"""

from __future__ import annotations

import threading
import time
from types import TracebackType

import pytest

from scrapy_extension.backends.base import (
  Backend,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.connectors import ConnectionManager


class FakeBackend(Backend):
  """In-process Backend with controllable connect/disconnect behavior."""

  def __init__(
    self,
    *,
    connect_failures: int = 0,
    connect_block: threading.Event | None = None,
    disconnect_block: threading.Event | None = None,
    disconnect_entered: threading.Event | None = None,
    backend_type_value: str = "redis",
  ) -> None:
    self.connect_failures = connect_failures
    self.connect_calls = 0
    self.connect_block = connect_block
    self.disconnect_block = disconnect_block
    self.disconnect_entered = disconnect_entered
    self.disconnect_calls = 0
    self._connected = False
    self._backend_type_value = backend_type_value

  @property
  def backend_type(self) -> str:  # type: ignore[override]
    return self._backend_type_value

  def connect(self) -> None:
    self.connect_calls += 1
    if self.connect_block is not None:
      self.connect_block.wait(timeout=5)
    if self.connect_calls <= self.connect_failures:
      raise RuntimeError(f"scheduled failure {self.connect_calls}")
    self._connected = True

  def disconnect(self) -> None:
    self.disconnect_calls += 1
    if self.disconnect_entered is not None:
      self.disconnect_entered.set()
    if self.disconnect_block is not None:
      self.disconnect_block.wait(timeout=5)
    self._connected = False

  def is_connected(self) -> bool:
    return self._connected

  def ping(self) -> bool:
    return self._connected


class FakeFullBackend(FakeBackend, QueueBackend, SetBackend, StorageBackend):
  """FakeBackend that satisfies all three interface isinstance checks.

  Interface methods are stubs returning defaults — the breaker-wiring tests
  only exercise isinstance() gating + wrap_*_backend dispatch, never the ops.
  """

  # --- QueueBackend ---
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    return None

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    return None

  def queue_len(self, queue_name: str) -> int:
    return 0

  def clear_queue(self, queue_name: str) -> None:
    return None

  # --- SetBackend ---
  def add(self, set_name: str, item: bytes) -> bool:
    return False

  def remove(self, set_name: str, item: bytes) -> bool:
    return False

  def contains(self, set_name: str, item: bytes) -> bool:
    return False

  def set_len(self, set_name: str) -> int:
    return 0

  def clear_set(self, set_name: str) -> None:
    return None

  # --- StorageBackend ---
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    return None

  def retrieve(self, key: str) -> bytes | None:
    return None

  def delete(self, key: str) -> bool:
    return False

  def exists(self, key: str) -> bool:
    return False

  def ttl(self, key: str) -> int | None:
    return None

  def clear_storage(self, prefix: str | None = None) -> None:
    return None


@pytest.fixture(autouse=True)
def _clear_registry():
  ConnectionManager.clear_registry()
  yield
  ConnectionManager.clear_registry()


@pytest.fixture
def patch_sleep_random(monkeypatch):
  """Deterministic time.sleep + random.uniform — records uniform(lo,hi) calls."""
  calls: list[tuple[float, float]] = []

  def fake_sleep(_d: float) -> None:
    pass

  def fake_uniform(lo: float, hi: float) -> float:
    calls.append((lo, hi))
    return (lo + hi) / 2  # midpoint — deterministic, strictly in-range

  monkeypatch.setattr("scrapy_extension.backends.connectors.time.sleep", fake_sleep)
  # Risk 6: random.uniform moved from connectors to the extracted _retry module.
  monkeypatch.setattr(
    "scrapy_extension.backends._retry.random.uniform", fake_uniform
  )
  return calls


def test_harness_smoke():
  """Harness sanity: FakeBackend connect/disconnect bookkeeping works."""
  b = FakeBackend(connect_failures=1)
  with pytest.raises(RuntimeError):
    b.connect()
  b.connect()
  assert b.is_connected()
  b.disconnect()
  assert not b.is_connected()
  assert b.connect_calls == 2
  assert b.disconnect_calls == 1


def test_evict_disconnects_victim_OUTSIDE_registry_lock(monkeypatch):
  """T5 REGRESSION: a slow victim disconnect must NOT serialize get_manager().

  Pre-fix: _evict_orphans_under_lock disconnected under _registry_lock, so a
  second get_manager() on a different key blocked on the slow disconnect.
  Post-fix: disconnect happens after the registry lock is released.
  """
  ConnectionManager.clear_registry()
  monkeypatch.setattr(ConnectionManager, "MAX_MANAGERS", 2)

  disconnect_entered = threading.Event()
  disconnect_release = threading.Event()

  # Fill registry to cap (MAX_MANAGERS=2): two live managers.
  m1 = ConnectionManager.get_manager("redis", {"k": 1})
  ConnectionManager.get_manager("redis", {"k": 2})
  assert len(ConnectionManager._managers) == 2

  # Orphan m1 in-registry (simulate the state eviction defends): connect a
  # slow backend, then drop its refcount to 0 WITHOUT popping it.
  slow = FakeBackend(
    disconnect_entered=disconnect_entered,
    disconnect_block=disconnect_release,
  )
  m1._backend = slow
  with ConnectionManager._registry_lock:
    m1._users = 0

  # Peer A: get_manager on a 3rd distinct key triggers eviction of orphan m1.
  peer_a_done = threading.Event()

  def peer_a():
    ConnectionManager.get_manager("redis", {"k": 3})
    peer_a_done.set()

  ta = threading.Thread(target=peer_a)
  ta.start()
  # Eviction reached the slow disconnect:
  assert disconnect_entered.wait(timeout=5), "orphan was not evicted/disconnected"

  # Peer B: a 4th distinct key must NOT block on m1's slow disconnect.
  peer_b_done = threading.Event()

  def peer_b():
    ConnectionManager.get_manager("redis", {"k": 4})
    peer_b_done.set()

  tb = threading.Thread(target=peer_b)
  tb.start()
  assert peer_b_done.wait(timeout=2), (
    "get_manager serialized behind a slow disconnect — registry lock NOT released"
  )

  # Tear down: release the slow disconnect, join threads.
  disconnect_release.set()
  ta.join(timeout=5)
  tb.join(timeout=5)
  assert peer_a_done.is_set()
  # Victim was actually disconnected + evicted — covers the eviction path's
  # correctness, not just its concurrency behavior. [Sourcery review feedback]
  assert slow.disconnect_calls == 1
  assert not slow.is_connected()
  m1_key = ConnectionManager._registry_key("redis", {"k": 1})
  assert m1_key not in ConnectionManager._managers


def _manager_with_backend(fake: FakeBackend) -> ConnectionManager:
  """Build a ConnectionManager whose _create_backend returns ``fake``."""
  m = ConnectionManager("redis", {"retry_attempts": 3, "retry_delay": 1.0})
  m._create_backend = lambda: fake  # type: ignore[method-assign]
  return m


def test_T1_connect_retry_backoff_full_jitter_bounds(patch_sleep_random):
  """T1: full-jitter backoff — random.uniform(0, retry_delay*2**attempt), no real sleep."""
  fake = FakeBackend(connect_failures=2)  # fails twice, succeeds on 3rd
  m = _manager_with_backend(fake)
  m.connect()
  assert fake.connect_calls == 3
  # attempt 0 -> uniform(0, 1*2**0)=(0,1); attempt 1 -> uniform(0, 1*2**1)=(0,2)
  assert patch_sleep_random == [(0.0, 1.0), (0.0, 2.0)]


def test_T2_connect_all_attempts_fail_raises(patch_sleep_random):
  """T2: all retries exhausted -> BackendConnectionError with attempt count."""
  from scrapy_extension.exceptions import BackendConnectionError

  fake = FakeBackend(connect_failures=99)
  m = _manager_with_backend(fake)
  with pytest.raises(
    BackendConnectionError, match="Failed to connect after 3 attempts"
  ):
    m.connect()
  assert fake.connect_calls == 3


def test_T3_connect_emits_on_retry_monitor(patch_sleep_random):
  """T3: on_retry monitor hook fires before each backoff sleep (1-based retry index)."""
  retries: list[tuple[str, int]] = []

  class Recorder:
    def on_connect(self, bt: str) -> None:
      pass

    def on_disconnect(self, bt: str, reason: object) -> None:
      pass

    def on_retry(self, bt: str, attempt: int) -> None:
      retries.append((bt, attempt))

  fake = FakeBackend(connect_failures=2)
  m = _manager_with_backend(fake)
  m.set_monitor(Recorder())  # type: ignore[arg-type]
  m.connect()
  assert retries == [("redis", 1), ("redis", 2)]


def test_T4_attempt_connection_disconnects_half_built_backend_on_failure():
  """T4: connect() that raises after _create_backend must disconnect the half-built backend."""
  fake = FakeBackend(connect_failures=1)
  m = _manager_with_backend(fake)
  with pytest.raises(RuntimeError):
    m._attempt_connection()
  assert fake.disconnect_calls == 1
  assert m._backend is None


def test_T6_evict_warns_once_when_all_entries_live(monkeypatch, caplog):
  """T6: registry at cap with ALL entries _users>0 -> one-shot warning, no force-evict."""
  import logging

  ConnectionManager.clear_registry()
  monkeypatch.setattr(ConnectionManager, "MAX_MANAGERS", 2)
  ConnectionManager.get_manager("redis", {"k": 1})  # live
  ConnectionManager.get_manager("redis", {"k": 2})  # live
  assert len(ConnectionManager._managers) == 2
  with caplog.at_level(
    logging.WARNING, logger="scrapy_extension.backends.connectors"
  ):
    ConnectionManager.get_manager("redis", {"k": 3})  # over cap, all live
  assert ConnectionManager._over_cap_warned is True
  assert any("actively held" in r.message for r in caplog.records)


def test_T7_close_last_holder_disconnects_and_evicts():
  """T7: last holder's close() disconnects the backend + pops the registry entry."""
  fake = FakeBackend()
  m = ConnectionManager.get_manager("redis", {"k": 7})
  m._backend = fake
  key = ConnectionManager._registry_key(m.backend_type, m.settings)
  assert key in ConnectionManager._managers
  m.close()  # _users 1 -> 0 -> last holder
  assert fake.disconnect_calls == 1
  assert key not in ConnectionManager._managers


def test_T8_close_non_last_holder_is_noop_on_backend():
  """T8: non-last holder's close() does NOT disconnect; entry stays for the remaining holder."""
  fake = FakeBackend()
  a = ConnectionManager.get_manager("redis", {"k": 8})  # _users=1
  b = ConnectionManager.get_manager("redis", {"k": 8})  # same key -> _users=2, same mgr
  assert a is b
  a._backend = fake
  a.close()  # _users 2 -> 1, not last
  assert fake.disconnect_calls == 0
  key = ConnectionManager._registry_key(a.backend_type, a.settings)
  assert key in ConnectionManager._managers
  b.close()  # last -> disconnect + evict
  assert fake.disconnect_calls == 1


def test_T9_backend_property_single_connect_owner_among_peers():
  """T9: N threads racing on .backend -> exactly ONE connect(); all see the same backend."""
  connect_block = threading.Event()
  fake = FakeBackend(connect_block=connect_block)
  m = ConnectionManager("redis", {"k": 9})
  m._create_backend = lambda: fake  # type: ignore[method-assign]

  barrier = threading.Barrier(4)
  results: list[object] = []
  results_lock = threading.Lock()

  def worker():
    barrier.wait(timeout=5)
    b = m.backend
    with results_lock:
      results.append(b)

  threads = [threading.Thread(target=worker) for _ in range(4)]
  for t in threads:
    t.start()
  # Wait until the owner entered connect() and is blocked; peers wait on _connected_event.
  deadline = time.monotonic() + 5
  while fake.connect_calls < 1 and time.monotonic() < deadline:
    time.sleep(0.01)
  assert fake.connect_calls == 1, "owner did not take the connect slow path"
  connect_block.set()  # release owner -> all peers unblock
  for t in threads:
    t.join(timeout=5)
  assert len(results) == 4
  assert all(r is fake for r in results)


def test_T10_backend_property_owner_error_propagates_to_all_waiters():
  """T10: one failed attempt is fanned out to every waiting peer."""
  connect_block = threading.Event()
  fake = FakeBackend(connect_failures=99, connect_block=connect_block)
  m = ConnectionManager("redis", {"k": 10, "retry_attempts": 1, "retry_delay": 0.0})
  m._create_backend = lambda: fake  # type: ignore[method-assign]

  barrier = threading.Barrier(3)
  errors: list[BaseException] = []
  errors_lock = threading.Lock()

  def worker():
    barrier.wait(timeout=5)
    try:
      _ = m.backend  # property access triggers connect (raises); assign silences ruff B018
    except BaseException as e:  # noqa: BLE001
      with errors_lock:
        errors.append(e)

  threads = [threading.Thread(target=worker) for _ in range(3)]
  for t in threads:
    t.start()

  # Hold the owner inside connect() long enough for the other two workers to
  # enter the manager's waiter path. Releasing this gate produces one failed
  # connection attempt whose result must be shared by the whole cohort.
  deadline = time.monotonic() + 5
  while fake.connect_calls < 1 and time.monotonic() < deadline:
    time.sleep(0.01)
  assert fake.connect_calls == 1, "owner did not enter connect()"
  time.sleep(0.05)
  connect_block.set()

  for t in threads:
    t.join(timeout=5)
  assert not any(t.is_alive() for t in threads)
  assert len(errors) == 3  # every waiter re-raised
  assert fake.connect_calls == 1  # peers reuse the owner's failed result
  assert m._connecting is False  # owner cleared the flag


def _enable_breaker(monkeypatch):
  """Flip the breaker ON via env (lazy Settings() in _get_breaker reads it)."""
  monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", "true")
  monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "3")
  monkeypatch.setenv("SCRAPY_CIRCUIT_BREAKER_RESET_TIMEOUT", "30")


def test_T11_get_queue_backend_wraps_when_breaker_enabled(monkeypatch):
  """T11: breaker ON -> get_queue_backend returns a wrapped proxy, not the raw backend."""
  _enable_breaker(monkeypatch)
  m = ConnectionManager("redis", {"k": 11})
  m._backend = FakeFullBackend()
  m._breaker_configured = False  # force re-resolution with env on
  wrapped = m.get_queue_backend()
  assert wrapped is not m._backend
  assert m._breaker_configured is True  # wrapping actually fired [Sourcery]


def test_T12_get_set_backend_wraps_when_breaker_enabled(monkeypatch):
  """T12: breaker ON -> get_set_backend returns a wrapped proxy."""
  _enable_breaker(monkeypatch)
  m = ConnectionManager("redis", {"k": 12})
  m._backend = FakeFullBackend()
  m._breaker_configured = False
  wrapped = m.get_set_backend()
  assert wrapped is not m._backend
  assert m._breaker_configured is True  # wrapping actually fired [Sourcery]


def test_T13_get_storage_backend_wraps_when_breaker_enabled(monkeypatch):
  """T13: breaker ON -> get_storage_backend returns a wrapped proxy."""
  _enable_breaker(monkeypatch)
  m = ConnectionManager("redis", {"k": 13})
  m._backend = FakeFullBackend()
  m._breaker_configured = False
  wrapped = m.get_storage_backend()
  assert wrapped is not m._backend
  assert m._breaker_configured is True  # wrapping actually fired [Sourcery]


def test_T14_breaker_disabled_returns_raw_backend_byte_identical(monkeypatch):
  """T14: breaker OFF (default) -> the raw backend is returned unchanged."""
  monkeypatch.delenv("SCRAPY_CIRCUIT_BREAKER_ENABLED", raising=False)
  m = ConnectionManager("redis", {"k": 14})
  m._backend = FakeFullBackend()
  m._breaker_configured = False
  assert m.get_queue_backend() is m._backend
  assert m.get_set_backend() is m._backend
  assert m.get_storage_backend() is m._backend


def test_load_object_invalid_path_raises_value_error():
  """Cover the empty-dotted-path guard in _load_object (connectors.py)."""
  from scrapy_extension.backends.connectors import _load_object

  with pytest.raises(ValueError, match="Invalid dotted path"):
    _load_object("no_separator")


# ---------------------------------------------------------------------------
# Initiative #39 — close the last 5 connectors.py coverage gaps
# (427->436, 558, 586->exit, 707, 854). Each test pins ONE branch the prior
# T1-T14 suite reaches only partially or not at all.
# ---------------------------------------------------------------------------


def test_T6b_over_cap_second_trigger_skips_rewarning(monkeypatch, caplog):
  """427->436: a SECOND over-cap trigger (``_over_cap_warned`` already True)
  takes the ``if not _over_cap_warned`` FALSE branch and skips re-warning.
  T6 covers only the first trigger (the TRUE branch)."""
  import logging

  ConnectionManager.clear_registry()
  monkeypatch.setattr(ConnectionManager, "MAX_MANAGERS", 2)
  ConnectionManager.get_manager("redis", {"k": 1})  # live (_users=1)
  ConnectionManager.get_manager("redis", {"k": 2})  # live (_users=1)

  with caplog.at_level(
    logging.WARNING, logger="scrapy_extension.backends.connectors"
  ):
    ConnectionManager.get_manager("redis", {"k": 3})  # first over-cap -> warns
  assert any("actively held" in r.message for r in caplog.records)

  caplog.clear()
  with caplog.at_level(
    logging.WARNING, logger="scrapy_extension.backends.connectors"
  ):
    ConnectionManager.get_manager("redis", {"k": 4})  # second -> skip re-warn
  assert not any("actively held" in r.message for r in caplog.records)


def test_connect_re_raises_keyboard_interrupt_not_swallowed_by_retry(
  monkeypatch,
):
  """KeyboardInterrupt (and SystemExit) propagate out of connect() instead of
  being treated as a retryable connection error. The mechanism: KI/SystemExit
  inherit from BaseException (not Exception), so ``except Exception`` does not
  catch them — they bypass the retry loop entirely. (The prior ``isinstance``
  re-raise inside the except block was unreachable dead code, removed in #39.)"""
  m = ConnectionManager("redis", {"retry_attempts": 3, "retry_delay": 0.0})

  def _boom() -> None:
    raise KeyboardInterrupt("user interrupt")

  monkeypatch.setattr(m, "_attempt_connection", _boom)
  with pytest.raises(KeyboardInterrupt):
    m.connect()


def test_connect_zero_retry_attempts_is_a_silent_noop(patch_sleep_random):
  """586->exit: ``retry_attempts=0`` -> the for-loop body never runs,
  ``last_exception`` stays None, and connect() falls through the trailing
  ``if last_exception is not None`` guard (FALSE branch) to an implicit
  return. Characterizes the current behavior; the success-path ``else``
  (line 580) is unreachable when there are no attempts."""
  fake = FakeBackend(connect_failures=99)
  m = ConnectionManager("redis", {"retry_attempts": 0, "retry_delay": 1.0})
  m._create_backend = lambda: fake  # type: ignore[method-assign]
  m.connect()  # no attempts -> no raise, silent return
  assert fake.connect_calls == 0


def test_clear_registry_resets_each_configured_breaker(monkeypatch):
  """707: clear_registry resets every manager's CircuitBreaker when one is
  configured (``manager._breaker is not None``). Exercises the reset path the
  breaker-disabled default never reaches."""
  _enable_breaker(monkeypatch)
  fake = FakeBackend()
  m = ConnectionManager.get_manager("redis", {"k": "breaker-reset"})
  m._create_backend = lambda: fake  # type: ignore[method-assign]
  breaker = m._get_breaker()  # configures lazily (enabled via env)
  assert breaker is not None, "breaker should be configured when enabled"
  resets: list[int] = []
  monkeypatch.setattr(breaker, "reset", lambda: resets.append(1))
  ConnectionManager.clear_registry()
  assert len(resets) == 1, "clear_registry did not reset the configured breaker"


def test_get_breaker_dcl_inner_recheck_under_lock(monkeypatch):
  """854: the double-checked-locking inner re-check fires when
  ``_breaker_configured`` flips to True BETWEEN the outer check (no lock) and
  the lock acquisition — i.e. a peer thread configured the breaker while we
  waited. Simulated deterministically by side-effecting on lock ``__enter__``;
  the contract under test is the re-check itself (single-threaded logic once
  the lock is held), not the scheduler timing of a real two-thread race."""
  fake = FakeBackend()
  m = ConnectionManager.get_manager("redis", {"k": "dcl"})
  m._backend = fake
  assert m._breaker_configured is False  # outer check (842) will be False

  original_lock = m._lock
  side_effect_ran = {"v": False}

  class _RacyLock:
    def __enter__(self) -> None:
      original_lock.__enter__()
      # Simulate a peer thread having configured the breaker while we waited.
      m._breaker_configured = True
      m._breaker = None  # valid: the disabled case
      side_effect_ran["v"] = True

    def __exit__(  # type: ignore[override]
      self,
      exc_type: type[BaseException] | None,
      exc_value: BaseException | None,
      traceback: TracebackType | None,
    ) -> None:
      original_lock.__exit__(exc_type, exc_value, traceback)

  monkeypatch.setattr(m, "_lock", _RacyLock())  # type: ignore[assignment]
  result = m._get_breaker()
  assert side_effect_ran["v"] is True
  assert result is None  # inner return at 854 fires; _breaker set to None by "peer"
