"""Redis deployment-mode truth and SDK boundary contracts."""

from __future__ import annotations

import traceback
import warnings
from pathlib import Path
from typing import Any

import pytest
from redis import Redis as SdkRedis
from redis.exceptions import (
  ChildDeadlockedError,
  RedisClusterException,
  SlotNotCoveredError,
)
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.connectors import resolve_backend_config
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
  StorageError,
)
from scrapy_extension.settings import RedisMode, RedisSettings

_SECRET = "redis-mode-contract-secret"


def _client(mocker, name: str = "redis-client"):
  client = mocker.MagicMock(name=name)
  client.ping.return_value = True
  return client


def _assert_secret_free(error: BaseException, marker: str = _SECRET) -> None:
  assert marker not in str(error)
  assert marker not in repr(getattr(error, "__dict__", {}))
  assert marker not in "".join(traceback.format_exception(error))


@pytest.mark.parametrize(
  ("field_name", "kwargs"),
  [
    ("host", {"host": f"user:{_SECRET}@redis.internal"}),
    ("replicas", {"replicas": [f"user:{_SECRET}@replica.internal:6379"]}),
    (
      "sentinels",
      {
        "mode": RedisMode.SENTINEL,
        "sentinels": [f"user:{_SECRET}@sentinel.internal:26379"],
      },
    ),
    (
      "cluster_startup_nodes",
      {
        "mode": RedisMode.CLUSTER,
        "cluster_startup_nodes": [f"redis://user:{_SECRET}@cluster.internal:6379"],
      },
    ),
  ],
)
def test_endpoint_fields_reject_userinfo_without_retention(
  field_name: str, kwargs: dict[str, Any]
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(**kwargs)

  assert exc_info.value.setting_name == field_name
  assert exc_info.value.setting_value is None
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)


@pytest.mark.parametrize(
  "endpoint",
  [
    "redis://host:6379",
    "host/path:6379",
    "host?query:6379",
    "host#fragment:6379",
    "host\\name:6379",
    " host:6379",
    "host:6379 ",
    "ho\nst:6379",
    "host:+6379",
    "host:6_379",
    "host:\uff16\uff13\uff17\uff19",
    "host:0",
    "host:65536",
    "::1:6379",
  ],
)
def test_endpoint_grammar_rejects_ambiguous_or_injection_shapes(
  endpoint: str,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=[endpoint],
    )

  assert exc_info.value.setting_name == "sentinels"
  assert endpoint not in str(exc_info.value)


@pytest.mark.parametrize(
  "port",
  [
    f"6379{_SECRET}",
    " 6379",
    "+6379",
    "6_379",
    "\uff16\uff13\uff17\uff19",
    b"6379",
    6379.0,
    True,
    0,
    65536,
  ],
)
def test_scalar_port_rejects_coercion_without_retaining_input(
  port: object,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(port=port)  # type: ignore[arg-type]

  assert exc_info.value.setting_name == "port"
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)


@pytest.mark.parametrize("host", ["127.1", "2130706433", "0x7f000001"])
def test_scalar_host_rejects_ambiguous_legacy_numeric_ip_forms(
  host: str,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(host=host)

  assert exc_info.value.setting_name == "host"
  assert host not in str(exc_info.value)


def test_hosts_and_endpoint_lists_normalize_bracketed_ipv6() -> None:
  standalone = RedisSettings(host="[2001:0db8::1]")
  sentinel = RedisSettings(
    mode=RedisMode.SENTINEL,
    sentinels=["[2001:0db8::1]:26379"],
  )
  cluster = RedisSettings(
    mode=RedisMode.CLUSTER,
    cluster_startup_nodes=["[2001:0db8::2]:6379"],
  )

  assert standalone.host == "2001:db8::1"
  assert sentinel.sentinels == ["[2001:db8::1]:26379"]
  assert cluster.cluster_startup_nodes == ["[2001:db8::2]:6379"]


def test_cluster_scalar_ipv6_fallback_reaches_sdk_without_brackets(mocker) -> None:
  client = _client(mocker, "cluster-ipv6")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      host="[2001:0db8::1]",
      port=6380,
    )
  )

  backend.connect()

  startup_nodes = constructor.call_args.kwargs["startup_nodes"]
  assert [(node.host, node.port) for node in startup_nodes] == [
    ("2001:db8::1", 6380)
  ]


def test_mutated_endpoint_is_revalidated_before_sdk_construction(mocker) -> None:
  settings = RedisSettings(
    mode=RedisMode.SENTINEL,
    sentinels=["sentinel.internal:26379"],
  )
  settings.sentinels = [f"user:{_SECRET}@sentinel.internal:26379"]
  constructor = mocker.patch("scrapy_extension.backends.redis.Sentinel")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "sentinels"
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)
  constructor.assert_not_called()


def test_environment_endpoint_is_rejected_without_retention(monkeypatch) -> None:
  monkeypatch.setenv("SCRAPY_REDIS_MODE", "cluster")
  monkeypatch.setenv(
    "SCRAPY_REDIS_CLUSTER_STARTUP_NODES",
    f'["user:{_SECRET}@cluster.internal:6379"]',
  )

  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings()

  assert exc_info.value.setting_name == "cluster_startup_nodes"
  _assert_secret_free(exc_info.value)


def test_malformed_environment_endpoint_json_is_not_retained(
  monkeypatch,
) -> None:
  monkeypatch.setenv(
    "SCRAPY_REDIS_CLUSTER_STARTUP_NODES",
    f"user:{_SECRET}@cluster.internal:6379",
  )

  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings()

  assert exc_info.value.setting_name == "cluster_startup_nodes"
  assert exc_info.value.__cause__ is None
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)


@pytest.mark.parametrize(
  "scrapy_values",
  [
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_BACKEND_SETTINGS": {
        "mode": "cluster",
        "cluster_startup_nodes": [f"user:{_SECRET}@cluster.internal:6379"],
      },
    },
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_REDIS_MODE": "cluster",
      "SCRAPY_REDIS_CLUSTER_STARTUP_NODES": [
        f"user:{_SECRET}@cluster.internal:6379"
      ],
    },
  ],
)
def test_scrapy_endpoint_inputs_reach_the_same_safe_validator(
  scrapy_values: dict[str, Any],
) -> None:
  _, raw = resolve_backend_config(
    ScrapySettings(scrapy_values),
    type_key="SCRAPY_QUEUE_BACKEND_TYPE",
    settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
  )

  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(**raw)

  assert exc_info.value.setting_name == "cluster_startup_nodes"
  _assert_secret_free(exc_info.value)


def test_sentinel_required_field_error_never_echoes_endpoints() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel.internal:26379"],
      sentinel_master_name="",
    )

  assert exc_info.value.setting_name == "sentinel_master_name"
  assert "sentinel.internal" not in str(exc_info.value)


@pytest.mark.parametrize("field_name", ["masters", "Masters", "MASTERS"])
def test_ghost_masters_field_is_rejected_without_retaining_endpoint(
  field_name: str,
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(
      **{field_name: [f"user:{_SECRET}@master.internal:6379"]}
    )

  assert exc_info.value.setting_name == "masters"
  _assert_secret_free(exc_info.value)


def test_ghost_masters_environment_field_fails_fast(monkeypatch) -> None:
  monkeypatch.setenv(
    "SCRAPY_REDIS_MASTERS",
    f"user:{_SECRET}@master.internal:6379",
  )

  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings()

  assert exc_info.value.setting_name == "masters"
  assert exc_info.value.__cause__ is None
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)
  assert "masters" not in RedisSettings.model_json_schema()["properties"]


def test_mutated_ghost_masters_field_fails_before_sdk_construction(
  mocker,
) -> None:
  settings = RedisSettings()
  settings.masters = [f"user:{_SECRET}@master.internal:6379"]
  constructor = mocker.patch("scrapy_extension.backends.redis.Redis")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "masters"
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)
  constructor.assert_not_called()


@pytest.mark.parametrize(
  ("kwargs", "setting_name"),
  [
    ({"sentinels": ["sentinel.internal:26379"]}, "sentinels"),
    (
      {
        "mode": RedisMode.SENTINEL,
        "sentinels": ["sentinel.internal:26379"],
        "cluster_startup_nodes": ["cluster.internal:6379"],
      },
      "cluster_startup_nodes",
    ),
    (
      {
        "mode": RedisMode.CLUSTER,
        "sentinels": ["sentinel.internal:26379"],
      },
      "sentinels",
    ),
    ({"cluster_max_redirects": 2}, "cluster_max_redirects"),
    ({"sentinel_retry_on_timeout": False}, "sentinel_retry_on_timeout"),
  ],
)
def test_nonselected_topology_intent_is_rejected(
  kwargs: dict[str, Any], setting_name: str
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(**kwargs)

  assert exc_info.value.setting_name == setting_name


def test_cluster_accepts_db_zero_but_never_forwards_it(mocker) -> None:
  client = _client(mocker, "cluster-db-zero")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["cluster.internal:6379"],
      db=0,
    )
  )

  backend.connect()

  assert "db" not in constructor.call_args.kwargs


def test_cluster_rejects_nonzero_db_at_configuration_time() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(mode=RedisMode.CLUSTER, db=7)

  assert exc_info.value.setting_name == "db"


def test_mutated_cluster_db_is_rejected_before_sdk_construction(mocker) -> None:
  settings = RedisSettings(mode=RedisMode.CLUSTER, db=0)
  settings.db = 7
  constructor = mocker.patch("scrapy_extension.backends.redis.RedisCluster")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "db"
  assert exc_info.value.__context__ is None
  constructor.assert_not_called()


class _ClusterRedirectClient:
  RedisClusterRequestTTL = 16

  def __init__(self) -> None:
    self.closed = False

  def ping(self) -> bool:
    return True

  def close(self) -> None:
    self.closed = True


def test_cluster_redirect_budget_is_instance_local_and_plus_one(mocker) -> None:
  zero = _ClusterRedirectClient()
  five = _ClusterRedirectClient()
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster",
    side_effect=[zero, five],
  )
  first = RedisBackend(
    RedisSettings(mode=RedisMode.CLUSTER, cluster_max_redirects=0)
  )
  second = RedisBackend(
    RedisSettings(mode=RedisMode.CLUSTER, cluster_max_redirects=5)
  )

  first.connect()
  second.connect()

  assert zero.RedisClusterRequestTTL == 1
  assert five.RedisClusterRequestTTL == 6
  assert _ClusterRedirectClient.RedisClusterRequestTTL == 16
  for call in constructor.call_args_list:
    kwargs = call.kwargs
    assert "cluster_error_retry_attempts" not in kwargs
    assert kwargs["retry"].get_retries() == 0


def test_master_slave_is_a_primary_only_deprecated_alias(mocker) -> None:
  client = _client(mocker, "primary")
  client.sadd.return_value = 1
  client.get.return_value = b"stored"
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  settings = RedisSettings(mode=RedisMode.MASTER_SLAVE)
  backend = RedisBackend(settings)

  with pytest.warns(FutureWarning, match="primary-only"):
    backend.add("seen", b"fingerprint")
  assert backend.retrieve("item") == b"stored"

  constructor.assert_called_once()
  client.sadd.assert_called_once()
  client.get.assert_called_once()
  assert settings.read_from_replicas is False


@pytest.mark.parametrize(
  ("kwargs", "setting_name"),
  [
    ({"replicas": ["replica.internal:6379"]}, "replicas"),
    ({"read_from_replicas": True}, "read_from_replicas"),
  ],
)
def test_unsupported_replica_read_intent_fails_fast(
  kwargs: dict[str, Any], setting_name: str
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(mode=RedisMode.MASTER_SLAVE, **kwargs)

  assert exc_info.value.setting_name == setting_name


def test_master_slave_compatibility_fields_are_schema_deprecated() -> None:
  schema = RedisSettings.model_json_schema()["properties"]

  assert schema["replicas"]["deprecated"] is True
  assert schema["read_from_replicas"]["deprecated"] is True


def test_master_slave_warning_has_static_safe_attribution(mocker) -> None:
  client = _client(mocker, "warning-attribution")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  with warnings.catch_warnings(record=True) as records:
    warnings.simplefilter("always", FutureWarning)
    backend = RedisBackend(
      RedisSettings(
        mode=RedisMode.MASTER_SLAVE,
        password=_SECRET,
      )
    )
    backend.connect()

  assert len(records) == 1
  warning = records[0]
  assert Path(warning.filename).resolve() != Path(__file__).resolve()
  assert warning.filename.endswith("/scrapy_extension/backends/redis.py")
  rendered = warnings.formatwarning(
    warning.message,
    warning.category,
    warning.filename,
    warning.lineno,
    warning.line,
  )
  assert _SECRET not in rendered


def test_master_slave_warning_follows_the_revalidated_reconnect_mode(
  mocker,
) -> None:
  client = _client(mocker, "mutated-master-slave")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  settings = RedisSettings()
  backend = RedisBackend(settings)
  settings.mode = RedisMode.MASTER_SLAVE

  with pytest.warns(FutureWarning, match="primary-only"):
    backend.connect()

  backend.disconnect()
  settings.mode = RedisMode.STANDALONE
  with warnings.catch_warnings():
    warnings.simplefilter("error", FutureWarning)
    backend.connect()


@pytest.mark.parametrize(
  "tls_kwargs",
  [
    {"ssl_cafile": f"/{_SECRET}/ca.pem"},
    {
      "ssl_certfile": f"/{_SECRET}/client.pem",
      "ssl_keyfile": f"/{_SECRET}/client.key",
    },
  ],
)
def test_tls_material_requires_explicit_tls_without_path_retention(
  tls_kwargs: dict[str, str],
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(ssl_enabled=False, **tls_kwargs)

  assert exc_info.value.setting_name == "ssl_enabled"
  _assert_secret_free(exc_info.value)


def test_mutated_tls_intent_is_revalidated_before_sdk_construction(mocker) -> None:
  settings = RedisSettings()
  settings.ssl_cafile = f"/{_SECRET}/ca.pem"
  constructor = mocker.patch("scrapy_extension.backends.redis.Redis")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "ssl_enabled"
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)
  constructor.assert_not_called()


def test_decode_responses_uses_lossless_binary_round_trip(mocker) -> None:
  client = _client(mocker, "decode-responses")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  backend = RedisBackend(RedisSettings(decode_responses=True))
  backend.connect()
  encoded = b"\x00\xff\x80valid"
  decoded = encoded.decode("utf-8", errors="surrogateescape")
  script = mocker.Mock(return_value=[1, decoded])

  assert backend._atomic_pop_once("jobs", "items", "payload", script) == encoded
  client.get.return_value = decoded
  assert backend.retrieve("binary") == encoded
  kwargs = constructor.call_args.kwargs
  assert kwargs["decode_responses"] is True
  assert kwargs["encoding_errors"] == "surrogateescape"

  sdk_client = SdkRedis(**kwargs)
  try:
    sdk_decoded = sdk_client.connection_pool.get_encoder().decode(encoded)
    assert sdk_decoded == decoded
    assert sdk_decoded.encode("utf-8", errors="surrogateescape") == encoded
  finally:
    sdk_client.close()


def test_sanitized_sdk_startup_failure_drops_raw_context(mocker) -> None:
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis",
    side_effect=RuntimeError(f"driver {_SECRET}"),
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    RedisBackend(RedisSettings()).connect()

  assert exc_info.value.__cause__ is None
  assert exc_info.value.__context__ is None
  _assert_secret_free(exc_info.value)
  constructor.assert_called_once()


def _invoke_operation(
  backend: RedisBackend,
  client: Any,
  mocker,
  operation: str,
  failure: BaseException,
) -> None:
  if operation == "push":
    client.register_script.return_value = mocker.Mock(side_effect=failure)
    backend.push("jobs", b"payload")
  elif operation == "pop-register":
    client.register_script.side_effect = failure
    backend.pop("jobs")
  elif operation == "pop":
    client.register_script.return_value = mocker.Mock(side_effect=failure)
    backend.pop("jobs")
  elif operation == "queue_len":
    client.zcard.side_effect = failure
    backend.queue_len("jobs")
  elif operation == "clear_queue":
    client.delete.side_effect = failure
    backend.clear_queue("jobs")
  elif operation == "add":
    client.sadd.side_effect = failure
    backend.add("seen", b"payload")
  elif operation == "remove":
    client.srem.side_effect = failure
    backend.remove("seen", b"payload")
  elif operation == "contains":
    client.sismember.side_effect = failure
    backend.contains("seen", b"payload")
  elif operation == "set_len":
    client.scard.side_effect = failure
    backend.set_len("seen")
  elif operation == "clear_set":
    client.delete.side_effect = failure
    backend.clear_set("seen")
  elif operation == "store":
    client.set.side_effect = failure
    backend.store("item", b"payload")
  elif operation == "store-ttl":
    client.setex.side_effect = failure
    backend.store("item", b"payload", ttl=30)
  elif operation == "retrieve":
    client.get.side_effect = failure
    backend.retrieve("item")
  elif operation == "delete":
    client.delete.side_effect = failure
    backend.delete("item")
  elif operation == "exists":
    client.exists.side_effect = failure
    backend.exists("item")
  elif operation == "ttl":
    client.ttl.side_effect = failure
    backend.ttl("item")
  elif operation == "clear_storage":
    client.scan_iter.side_effect = failure
    backend.clear_storage()
  else:  # pragma: no cover - parameter table owns this branch
    raise AssertionError(operation)


_OPERATION_CONTRACTS: tuple[
  tuple[str, type[BaseException], str, str], ...
] = (
  ("push", QueueError, "operation", "push"),
  ("pop-register", QueueError, "operation", "pop"),
  ("pop", QueueError, "operation", "pop"),
  ("queue_len", QueueError, "operation", "queue_len"),
  ("clear_queue", QueueError, "operation", "clear_queue"),
  ("add", BackendConnectionError, "backend_type", "redis"),
  ("remove", BackendConnectionError, "backend_type", "redis"),
  ("contains", BackendConnectionError, "backend_type", "redis"),
  ("set_len", BackendConnectionError, "backend_type", "redis"),
  ("clear_set", BackendConnectionError, "backend_type", "redis"),
  ("store", StorageError, "operation", "store"),
  ("store-ttl", StorageError, "operation", "store"),
  ("retrieve", StorageError, "operation", "retrieve"),
  ("delete", StorageError, "operation", "delete"),
  ("exists", StorageError, "operation", "exists"),
  ("ttl", StorageError, "operation", "ttl"),
  ("clear_storage", StorageError, "operation", "clear_storage"),
)


@pytest.mark.parametrize(
  ("operation", "error_type", "field_name", "field_value"),
  _OPERATION_CONTRACTS,
)
def test_cluster_sdk_exception_obeys_every_operation_contract(
  mocker,
  operation: str,
  error_type: type[BaseException],
  field_name: str,
  field_value: str,
) -> None:
  client = _client(mocker, operation)
  mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(RedisSettings(mode=RedisMode.CLUSTER))
  backend.connect()
  failure = SlotNotCoveredError(f"driver detail {_SECRET}")

  with pytest.raises(error_type) as exc_info:
    _invoke_operation(backend, client, mocker, operation, failure)

  assert exc_info.value.__cause__ is failure
  assert getattr(exc_info.value, field_name) == field_value
  assert _SECRET not in str(exc_info.value)


def test_cluster_root_exception_is_also_typed_and_sanitized(mocker) -> None:
  client = _client(mocker, "cluster-root")
  mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(RedisSettings(mode=RedisMode.CLUSTER))
  backend.connect()
  failure = RedisClusterException(f"command payload {_SECRET}")
  client.get.side_effect = failure

  with pytest.raises(StorageError) as exc_info:
    backend.retrieve("item")

  assert exc_info.value.__cause__ is failure
  assert _SECRET not in str(exc_info.value)


def test_cluster_sdk_health_exception_returns_false(mocker) -> None:
  client = _client(mocker, "cluster-health")
  mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(RedisSettings(mode=RedisMode.CLUSTER))
  backend.connect()
  client.ping.side_effect = SlotNotCoveredError("slot unavailable")

  assert backend.ping() is False
  assert backend.is_connected() is False


@pytest.mark.parametrize(
  ("operation", "error_type", "field_name", "field_value"),
  _OPERATION_CONTRACTS,
)
def test_pool_child_deadlock_obeys_every_operation_contract(
  mocker,
  operation: str,
  error_type: type[BaseException],
  field_name: str,
  field_value: str,
) -> None:
  client = _client(mocker, operation)
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  failure = ChildDeadlockedError()

  with pytest.raises(error_type) as exc_info:
    _invoke_operation(backend, client, mocker, operation, failure)

  assert exc_info.value.__cause__ is failure
  assert getattr(exc_info.value, field_name) == field_value


def test_pool_child_deadlock_health_failure_returns_false(mocker) -> None:
  client = _client(mocker, "child-deadlock-health")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()
  client.ping.side_effect = ChildDeadlockedError()

  assert backend.ping() is False
  assert backend.is_connected() is False


def test_non_sdk_failure_messages_do_not_copy_logical_names(mocker) -> None:
  client = _client(mocker, "static-failure-messages")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)
  backend = RedisBackend(RedisSettings())
  backend.connect()

  pop_script = mocker.Mock(return_value=[0, None])
  client.register_script.return_value = pop_script
  assert backend._generation is not None
  backend._generation.retired.set()
  with pytest.raises(QueueError) as pop_error:
    backend.pop(_SECRET, timeout=0.1)

  client.set.return_value = False
  with pytest.raises(StorageError) as store_error:
    backend.store(_SECRET, b"payload")

  assert _SECRET not in str(pop_error.value)
  assert _SECRET not in str(store_error.value)


@pytest.mark.parametrize(
  "result",
  [
    b"redis-mode-contract-secret",
    [99, b"redis-mode-contract-secret"],
    [3, "redis-mode-contract-secret"],
  ],
)
def test_corrupt_pop_response_never_copies_payload_into_error(
  mocker, result: Any
) -> None:
  backend = RedisBackend(RedisSettings())
  script = mocker.Mock(return_value=result)

  with pytest.raises(QueueError) as exc_info:
    backend._atomic_pop_once("jobs", "items", "payload", script)

  assert _SECRET not in str(exc_info.value)
