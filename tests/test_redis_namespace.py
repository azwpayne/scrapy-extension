"""Redis physical-key isolation and owned-key cleanup contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def _connected_backend(mocker, *, namespace: str = "scrapy-extension"):
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.settings import RedisSettings

  client = mocker.MagicMock()
  client.ping.return_value = True
  client.set.return_value = True
  client.setex.return_value = True
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings(namespace=namespace))
  backend.connect()
  return backend, client


def test_same_logical_name_maps_to_disjoint_physical_domains(mocker) -> None:
  """Queue, set, and storage APIs must never share a Redis key."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  push_script = mocker.MagicMock()
  client.register_script.return_value = push_script
  client.sadd.return_value = 1

  backend.push("shared", b"queued")
  backend.add("shared", b"fingerprint")
  backend.store("shared", b"stored")

  queue_keys = push_script.call_args.kwargs["keys"]
  assert queue_keys == [
    "{crawler-a:queue:shared}:items",
    "{crawler-a:queue:shared}:payload",
    "{crawler-a:queue:shared}:counter",
  ]
  client.sadd.assert_called_once_with("crawler-a:set:shared", b"fingerprint")
  client.set.assert_called_once_with("crawler-a:storage:shared", b"stored")
  assert (
    len({queue_keys[0], client.sadd.call_args.args[0], client.set.call_args.args[0]})
    == 3
  )


def test_all_queue_key_operations_use_queue_domain(mocker) -> None:
  """Depth and cleanup must address the same physical queue as push/pop."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.zcard.return_value = 3

  assert backend.queue_len("jobs") == 3
  backend.clear_queue("jobs")

  client.zcard.assert_called_once_with("{crawler-a:queue:jobs}:items")
  client.delete.assert_called_once_with(
    "{crawler-a:queue:jobs}:items",
    "{crawler-a:queue:jobs}:payload",
    "{crawler-a:queue:jobs}:counter",
  )


def test_blocking_pop_polls_with_atomic_lua_instead_of_destructive_bzpop(
  mocker,
) -> None:
  """A blocking pop must not expose a crash gap after removing the ZSET member."""
  from scrapy_extension.backends import redis as redis_module

  backend, client = _connected_backend(mocker, namespace="crawler-a")
  pop_script = mocker.MagicMock(side_effect=[[0, None], [1, b"payload"]])
  client.register_script.return_value = pop_script
  client.bzpopmin.side_effect = AssertionError("BZPOPMIN is not crash-safe here")
  mocker.patch.object(redis_module, "_BLOCKING_POP_POLL_INTERVAL", 0.0)

  assert backend.pop("jobs", timeout=1.0) == b"payload"

  assert pop_script.call_count == 2
  assert all(
    call.kwargs["keys"]
    == ["{crawler-a:queue:jobs}:items", "{crawler-a:queue:jobs}:payload"]
    for call in pop_script.call_args_list
  )
  client.bzpopmin.assert_not_called()
  client.pipeline.assert_not_called()


def test_all_set_operations_use_set_domain(mocker) -> None:
  """Every SetBackend operation must resolve through the set domain."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.sadd.return_value = 1
  client.srem.return_value = 1
  client.sismember.return_value = 1
  client.scard.return_value = 1

  assert backend.add("seen", b"a") is True
  assert backend.remove("seen", b"a") is True
  assert backend.contains("seen", b"a") is True
  assert backend.set_len("seen") == 1
  backend.clear_set("seen")

  physical_key = "crawler-a:set:seen"
  client.sadd.assert_called_once_with(physical_key, b"a")
  client.srem.assert_called_once_with(physical_key, b"a")
  client.sismember.assert_called_once_with(physical_key, b"a")
  client.scard.assert_called_once_with(physical_key)
  client.delete.assert_called_once_with(physical_key)


def test_all_storage_operations_use_storage_domain(mocker) -> None:
  """Every StorageBackend operation must resolve through the storage domain."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.get.return_value = b"value"
  client.delete.return_value = 1
  client.exists.return_value = 1
  client.ttl.return_value = 30

  backend.store("item", b"value", ttl=30)
  assert backend.retrieve("item") == b"value"
  assert backend.delete("item") is True
  assert backend.exists("item") is True
  assert backend.ttl("item") == 30

  physical_key = "crawler-a:storage:item"
  client.setex.assert_called_once_with(physical_key, 30, b"value")
  client.get.assert_called_once_with(physical_key)
  client.delete.assert_called_once_with(physical_key)
  client.exists.assert_called_once_with(physical_key)
  client.ttl.assert_called_once_with(physical_key)


def test_custom_namespaces_isolate_backend_instances(mocker) -> None:
  """Two applications sharing a DB can select independent key domains."""
  first, first_client = _connected_backend(mocker, namespace="crawler-a")
  second, second_client = _connected_backend(mocker, namespace="crawler-b")

  first.store("item", b"a")
  second.store("item", b"b")

  first_client.set.assert_called_once_with("crawler-a:storage:item", b"a")
  second_client.set.assert_called_once_with("crawler-b:storage:item", b"b")


def test_namespace_loads_from_environment(monkeypatch) -> None:
  """The namespace is configurable through the documented Redis env prefix."""
  from scrapy_extension.settings import RedisSettings

  monkeypatch.setenv("SCRAPY_REDIS_NAMESPACE", "production-crawler")

  assert RedisSettings().namespace == "production-crawler"


def test_namespaced_storage_does_not_fallback_to_legacy_raw_key(mocker) -> None:
  """Legacy raw keys require explicit migration; implicit reads are unsafe."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.get.side_effect = lambda key: b"legacy" if key == "item" else None

  assert backend.retrieve("item") is None
  client.get.assert_called_once_with("crawler-a:storage:item")


def test_clear_storage_without_prefix_deletes_only_owned_storage_keys(mocker) -> None:
  """Storage cleanup must not flush a shared Redis database."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.scan_iter.return_value = iter(
    [b"crawler-a:storage:item-1", b"crawler-a:storage:item-2"]
  )

  backend.clear_storage()

  client.scan_iter.assert_called_once_with(match="crawler-a:storage:*")
  assert client.delete.call_args_list == [
    mocker.call(b"crawler-a:storage:item-1"),
    mocker.call(b"crawler-a:storage:item-2"),
  ]
  client.flushdb.assert_not_called()
  client.flushall.assert_not_called()


def test_clear_storage_prefix_is_scoped_to_storage_domain(mocker) -> None:
  """A logical prefix cannot match queue, set, or foreign application keys."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")
  client.scan_iter.return_value = iter([])

  backend.clear_storage(prefix="items:")

  client.scan_iter.assert_called_once_with(match="crawler-a:storage:items:*")
  client.delete.assert_not_called()


def test_clear_storage_rejects_empty_prefix_instead_of_widening_scope(mocker) -> None:
  """An accidental empty prefix must not broaden into all owned storage."""
  backend, client = _connected_backend(mocker, namespace="crawler-a")

  with pytest.raises(ValueError, match="Invalid prefix"):
    backend.clear_storage(prefix="")

  client.scan_iter.assert_not_called()


@pytest.mark.parametrize(
  "namespace",
  [
    "",
    "contains space",
    "contains*wildcard",
    "contains{hash-tag}",
    "contains:separator",
  ],
)
def test_namespace_rejects_empty_or_redis_pattern_characters(namespace: str) -> None:
  """Namespace must be non-empty and safe to interpolate into SCAN/hash tags."""
  from scrapy_extension.settings import RedisSettings

  with pytest.raises(ValidationError):
    RedisSettings(namespace=namespace)
