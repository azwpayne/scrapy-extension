"""Tests for ``ConnectionManager._registry_key`` type-tagging (initiative #14).

Pre-#14 the key used ``json.dumps(..., default=str)``, which collapses
non-JSON values via ``str()`` — so ``datetime(2024,1,1)`` and the string
``"2024-01-01 00:00:00"`` rendered identically and two workers with those
settings silently shared one connection manager (wrong backend conn / wrong
DB index). #14 replaces ``default=str`` with a type-tagging default so
distinct types render distinctly, while keeping the pure-JSON key shape
byte-identical (backward-compatible for the common case).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager


def test_pure_json_settings_key_is_byte_identical_to_legacy():
  """Backward compat: a settings dict of JSON-native scalars MUST produce
  the exact same key as the pre-#14 ``json.dumps(sort_keys=True, separators)``
  form — existing in-flight managers and tests that rely on the common-case
  key shape are unaffected (``default`` is never invoked for JSON-native
  values)."""
  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "localhost", "port": 6379}
  )
  assert key == 'redis:{"host":"localhost","port":6379}'


def test_datetime_vs_string_do_not_collide():
  """Regression (#14): ``datetime(2024,1,1, tzinfo=utc)`` and the string that
  ``str()``-renders to the same representation MUST produce different keys.
  Pre-#14 both collapsed via ``default=str`` → silent wrong-manager sharing
  in multi-backend deployments."""
  key_dt = ConnectionManager._registry_key(
    BackendType.REDIS, {"expire_at": datetime(2024, 1, 1, tzinfo=timezone.utc)}
  )
  key_str = ConnectionManager._registry_key(
    BackendType.REDIS, {"expire_at": str(datetime(2024, 1, 1, tzinfo=timezone.utc))}
  )
  assert key_dt != key_str


def test_path_vs_string_do_not_collide():
  """Regression (#14): ``Path("/a")`` and the string ``"/a"`` MUST produce
  different keys (pre-#14 both rendered as ``/a`` via ``default=str``)."""
  key_path = ConnectionManager._registry_key(BackendType.MONGODB, {"cert": Path("/a")})
  key_str = ConnectionManager._registry_key(BackendType.MONGODB, {"cert": "/a"})
  assert key_path != key_str


def test_backendtype_enum_and_string_produce_same_prefix():
  """``BackendType.REDIS`` (enum) and ``"redis"`` (plain string) MUST map to
  the same ``bt_key`` prefix (pre-existing R5-1 behavior — ``.value``
  extraction). The #14 change must not regress this."""
  key_enum = ConnectionManager._registry_key(BackendType.REDIS, {"k": 1})
  key_str = ConnectionManager._registry_key("redis", {"k": 1})
  assert key_enum == key_str


def test_list_order_is_preserved_not_sorted():
  """Semantics: list/tuple element order is significant (e.g. failover host
  order matters), so ``[a,b]`` and ``[b,a]`` MUST produce different keys.
  Dict KEYS are sorted via ``sort_keys=True``; list ELEMENTS are not."""
  key_ab = ConnectionManager._registry_key(BackendType.REDIS, {"hosts": ["a", "b"]})
  key_ba = ConnectionManager._registry_key(BackendType.REDIS, {"hosts": ["b", "a"]})
  assert key_ab != key_ba


def test_same_settings_produce_same_key():
  """Stability: the same settings dict constructed twice produces the same
  key (deterministic, idempotent)."""
  s = {"host": "h", "port": 6379, "ssl": True}
  assert ConnectionManager._registry_key(BackendType.REDIS, s) == (
    ConnectionManager._registry_key(BackendType.REDIS, s)
  )


def test_mixed_type_keys_exercise_fallback_without_collision():
  """Pathological settings with mixed-type dict keys (str + int) are not
  mutually sortable, so ``json.dumps(sort_keys=True)`` raises ``TypeError``
  and the ``except`` fallback runs.

  Two regression guards in one:
  (1) the OLD fallback ``str(sorted(settings.items()))`` itself raised on
      mixed-type keys (``sorted`` could not compare ``"a" < 2``), so
      ``_registry_key`` crashed — the #14 fallback sorts *type-tagged
      strings* (always totally ordered), so it never raises;
  (2) even in the fallback, distinct values MUST produce distinct keys
      (the lossy ``str()`` collision is gone here too)."""
  # int key + str key -> sort_keys raises TypeError -> except branch.
  # Pathological input is the point of the test — the type ignore is
  # intentional (matches the pattern at test_queue_strategy_snapshot.py:267).
  key_a = ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "y"}  # type: ignore[arg-type]
  )
  key_b = ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "z"}  # type: ignore[arg-type]
  )
  assert key_a != key_b
  # And the fallback key is stable / deterministic:
  assert key_a == ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "y"}  # type: ignore[arg-type]
  )
