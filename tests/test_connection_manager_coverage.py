"""Coverage + regression suite for ConnectionManager (baseline rank #1).

Closes the hottest gaps in src/scrapy_extension/backends/connectors.py
(retry/backoff, registry-cap eviction, breaker-wiring, A2 single-connect)
and guards the registry-lock fix (victim disconnect outside _registry_lock).
See docs/superpowers/specs/2026-07-01-connection-manager-suite-lock-fix-design.md.
"""

from __future__ import annotations

import threading

import pytest

from scrapy_extension.backends.base import Backend
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
  monkeypatch.setattr(
    "scrapy_extension.backends.connectors.random.uniform", fake_uniform
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
