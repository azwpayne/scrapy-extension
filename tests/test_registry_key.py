"""Tests for deterministic, non-disclosing connection registry keys.

Registry settings are recursively type-tagged and reduced to one SHA-256
digest. This prevents lossy ``str()`` collisions for complex values and keeps
both Pydantic secrets and ordinary string credentials out of the class-level
registry key.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pydantic import SecretBytes, SecretStr

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import (
  _CONNECTION_MANAGER_SCOPE_KEY,
  ConnectionManager,
)


def test_pure_json_settings_key_has_a_stable_digest():
  """The versioned canonical form produces a fixed cross-process digest."""
  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "localhost", "port": 6379}
  )
  assert key == (
    "redis:72def1606a266419b59791f2ddee2e2233578a833d4d243ae1e8e77d63f85b3f"
  )


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


def test_different_secret_strings_do_not_share_a_registry_key():
  """The masked ``SecretStr.__str__`` value must not drive manager reuse."""
  key_a = ConnectionManager._registry_key(
    BackendType.REDIS, {"password": SecretStr("first-password")}
  )
  key_b = ConnectionManager._registry_key(
    BackendType.REDIS, {"password": SecretStr("second-password")}
  )

  assert key_a != key_b


def test_different_secret_bytes_do_not_share_a_registry_key():
  """``SecretBytes`` must use its underlying bytes, not its masked display."""
  key_a = ConnectionManager._registry_key(
    BackendType.REDIS, {"credential": SecretBytes(b"first-secret")}
  )
  key_b = ConnectionManager._registry_key(
    BackendType.REDIS, {"credential": SecretBytes(b"second-secret")}
  )

  assert key_a != key_b


def test_secret_mapping_keys_use_the_underlying_value():
  """Secret mapping keys must use their underlying value before sorting."""
  key_a = ConnectionManager._registry_key(
    BackendType.REDIS,
    {"credentials": {SecretStr("first-key"): "value"}},
  )
  key_b = ConnectionManager._registry_key(
    BackendType.REDIS,
    {"credentials": {SecretStr("second-key"): "value"}},
  )

  assert key_a != key_b


def test_equivalent_secret_settings_produce_the_same_key():
  """Equal underlying secrets are stable across distinct wrapper instances."""
  settings_a = {
    "password": SecretStr("same-password"),
    "credential": SecretBytes(b"same-bytes"),
  }
  settings_b = {
    "credential": SecretBytes(b"same-bytes"),
    "password": SecretStr("same-password"),
  }

  assert ConnectionManager._registry_key(
    BackendType.REDIS, settings_a
  ) == ConnectionManager._registry_key(BackendType.REDIS, settings_b)


def test_get_manager_reuses_only_equivalent_secret_settings():
  """Manager identity follows the underlying secret rather than its mask."""
  manager_a = ConnectionManager.get_manager(
    BackendType.REDIS, {"password": SecretStr("password-a")}
  )
  manager_b = ConnectionManager.get_manager(
    BackendType.REDIS, {"password": SecretStr("password-b")}
  )
  manager_a_again = ConnectionManager.get_manager(
    BackendType.REDIS, {"password": SecretStr("password-a")}
  )

  assert manager_a is not manager_b
  assert manager_a_again is manager_a


def test_queue_scope_participates_in_manager_identity():
  """Single-consumer backends must not share a manager across queue scopes."""
  shared = {"bootstrap_servers": "broker:9092"}
  manager_a = ConnectionManager.get_manager(
    BackendType.KAFKA,
    {**shared, _CONNECTION_MANAGER_SCOPE_KEY: "queue-a"},
  )
  manager_a_again = ConnectionManager.get_manager(
    BackendType.KAFKA,
    {**shared, _CONNECTION_MANAGER_SCOPE_KEY: "queue-a"},
  )
  manager_b = ConnectionManager.get_manager(
    BackendType.KAFKA,
    {**shared, _CONNECTION_MANAGER_SCOPE_KEY: "queue-b"},
  )

  assert manager_a_again is manager_a
  assert manager_b is not manager_a


def test_registry_key_does_not_contain_plain_string_credentials():
  """Callers may bypass Pydantic; plain credential strings stay out of keys."""
  password = "plain-text-password-that-must-not-leak"

  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "cache.internal", "password": password}
  )

  assert password not in key
  assert "cache.internal" not in key


def test_get_manager_snapshots_nested_settings_before_registry_storage():
  """A caller mutation must not make a registry key point at new settings."""
  source = {"host": "cache.internal", "options": {"database": 0}}
  manager = ConnectionManager.get_manager(BackendType.REDIS, source)

  source["options"]["database"] = 1

  assert manager.settings == {
    "host": "cache.internal",
    "options": {"database": 0},
  }
  assert ConnectionManager.get_manager(
    BackendType.REDIS,
    {"host": "cache.internal", "options": {"database": 0}},
  ) is manager


def test_mixed_type_mapping_keys_are_stable_without_collision():
  """Mixed key types are tagged before sorting, so ordering is total."""
  # Pathological input is intentional: recursive normalization must distinguish
  # int keys from string keys without asking Python to compare the raw values.
  key_a = ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "y"}  # type: ignore[arg-type]
  )
  key_b = ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "z"}  # type: ignore[arg-type]
  )
  assert key_a != key_b
  # Equivalent mappings remain stable regardless of their mixed key types.
  assert key_a == ConnectionManager._registry_key(
    BackendType.REDIS, {"a": "x", 2: "y"}  # type: ignore[arg-type]
  )
  assert ConnectionManager._registry_key(
    BackendType.REDIS, {1: "same"}  # type: ignore[arg-type]
  ) != ConnectionManager._registry_key(BackendType.REDIS, {"1": "same"})


def test_equivalent_slotted_values_produce_the_same_key():
  """Equivalent complex values must not depend on process memory addresses."""

  class SlottedValue:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
      self.name = name

  value_a = SlottedValue("same")
  value_b = SlottedValue("same")
  value_c = SlottedValue("different")
  key_a = ConnectionManager._registry_key(BackendType.REDIS, {"option": value_a})
  key_b = ConnectionManager._registry_key(BackendType.REDIS, {"option": value_b})
  key_c = ConnectionManager._registry_key(BackendType.REDIS, {"option": value_c})

  assert key_a == key_b
  assert key_a != key_c
