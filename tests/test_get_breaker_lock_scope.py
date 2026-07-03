"""Tests for ``ConnectionManager._get_breaker`` lock-scope + lazy config (#15).

Pre-#15 the breaker-config read (``Settings()`` — a pydantic env scan) ran
INSIDE ``self._lock``. That lock is shared with ``get_manager()`` /
``close()`` / the A2 slow-path owner gate, so the env scan serialized peer
threads' warm-up even though the scan is process-global idempotent state,
not connection-manager state. #15 hoists the read above the lock (DCL
re-check + construction stay under it).
"""

from __future__ import annotations

from unittest.mock import patch

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager


def _make_manager() -> ConnectionManager:
  """A ConnectionManager that hasn't connected or configured its breaker."""
  return ConnectionManager(BackendType.REDIS)


def test_get_breaker_settings_read_outside_lock():
  """#15: the ``Settings()`` env scan MUST NOT run while ``self._lock`` is
  held. Pre-#15 the scan ran inside ``with self._lock:``, serializing peer
  threads (the lock is shared with ``get_manager``/``close``/A2 slow path).
  The capturing Settings records ``_lock.locked()`` at construction time;
  under #15 it must read False."""
  manager = _make_manager()
  lock_state_during_settings: list[bool] = []

  class CapturingSettings:
    def __init__(self) -> None:
      lock_state_during_settings.append(manager._lock.locked())

    circuit_breaker_enabled = False
    circuit_breaker_failure_threshold = 5
    circuit_breaker_reset_timeout = 30.0

  with patch("scrapy_extension.settings.Settings", CapturingSettings):
    manager._get_breaker()

  assert lock_state_during_settings, "Settings() was never constructed"
  assert lock_state_during_settings[0] is False, (
    "Settings() ran while self._lock was held — peer threads serialize "
    "behind the env scan. Hoist the read above the lock (#15)."
  )


def test_get_breaker_disabled_returns_none_and_caches():
  """Behavioral guard (preserved by #15): when ``circuit_breaker_enabled``
  is False, ``_get_breaker`` returns None and marks the config done so the
  lock-free fast path serves subsequent calls."""
  manager = _make_manager()

  class DisabledSettings:
    circuit_breaker_enabled = False
    circuit_breaker_failure_threshold = 5
    circuit_breaker_reset_timeout = 30.0

  with patch("scrapy_extension.settings.Settings", DisabledSettings):
    assert manager._get_breaker() is None
  assert manager._breaker_configured is True
  assert manager._breaker is None


def test_get_breaker_enabled_threads_settings_into_circuitbreaker():
  """#15 refactor guard: the threshold/reset_timeout read from Settings
  (now hoisted outside the lock as locals) still reach the CircuitBreaker
  constructor with the backend-type-derived name."""
  manager = _make_manager()

  class EnabledSettings:
    circuit_breaker_enabled = True
    circuit_breaker_failure_threshold = 7
    circuit_breaker_reset_timeout = 45.0

  with patch("scrapy_extension.settings.Settings", EnabledSettings), patch(
    "scrapy_extension.backends.connectors.CircuitBreaker"
  ) as mock_cb:
    manager._get_breaker()

  mock_cb.assert_called_once_with(
    name="redis-backend", failure_threshold=7, reset_timeout=45.0
  )


def test_get_breaker_caches_and_does_not_reconstruct():
  """Behavioral guard: the second call hits the lock-free fast path
  (``_breaker_configured``) and does not construct Settings again."""
  manager = _make_manager()
  constructions: list[int] = []

  class CountingSettings:
    def __init__(self) -> None:
      constructions.append(1)

    circuit_breaker_enabled = False
    circuit_breaker_failure_threshold = 5
    circuit_breaker_reset_timeout = 30.0

  with patch("scrapy_extension.settings.Settings", CountingSettings):
    manager._get_breaker()
    manager._get_breaker()
    manager._get_breaker()
  assert len(constructions) == 1
