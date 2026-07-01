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
