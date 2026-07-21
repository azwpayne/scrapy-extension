"""Redis SDK retry policy contracts for outcome-ambiguous commands."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, ClassVar

import pytest
from redis import Redis as SdkRedis
from redis.cluster import (
  ClusterNode,
  CommandsParser,
  NodesManager,
)
from redis.cluster import (
  RedisCluster as SdkRedisCluster,
)
from redis.connection import Connection
from redis.exceptions import AuthenticationError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry
from redis.sentinel import (
  Sentinel as SdkSentinel,
)
from redis.sentinel import (
  SentinelConnectionPool,
  SentinelManagedSSLConnection,
)
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends.connectors import resolve_backend_config
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import QueueError
from scrapy_extension.settings import RedisMode, RedisSettings


def _client(mocker, name: str):
  client = mocker.MagicMock(name=name)
  client.ping.return_value = True
  return client


def _assert_no_ambiguous_replay(retry: Retry) -> None:
  """A committed command with a lost response is attempted only once."""
  attempts = 0
  failures: list[BaseException] = []
  timeout = RedisTimeoutError("response lost after server commit")

  def committed_then_timed_out() -> None:
    nonlocal attempts
    attempts += 1
    raise timeout

  with pytest.raises(RedisTimeoutError) as exc_info:
    retry.call_with_retry(committed_then_timed_out, failures.append)

  assert exc_info.value is timeout
  assert retry.get_retries() == 0
  assert attempts == 1
  assert failures == [timeout]


def _assert_one_control_plane_retry(retry: Retry) -> None:
  """One Sentinel control request may retry one timeout."""
  attempts = 0
  failures: list[BaseException] = []
  timeout = RedisTimeoutError("sentinel response timeout")

  def discover() -> str:
    nonlocal attempts
    attempts += 1
    if attempts == 1:
      raise timeout
    return "master-found"

  assert retry.call_with_retry(discover, failures.append) == "master-found"
  assert retry.get_retries() == 1
  assert attempts == 2
  assert failures == [timeout]


class _CommittedThenResponseLostConnection(Connection):
  """Real redis-py connection seam with a committed send and lost response."""

  commands: ClassVar[list[tuple[Any, ...]]] = []
  retry_counts: ClassVar[list[int]] = []
  disconnect_calls: ClassVar[int] = 0

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    type(self).retry_counts.append(self.retry.get_retries())

  @classmethod
  def reset_observations(cls) -> None:
    cls.commands = []
    cls.retry_counts = []
    cls.disconnect_calls = 0

  def connect(self) -> None:
    return None

  def can_read(self, timeout: float = 0) -> bool:
    return False

  def send_command(self, *args, **kwargs) -> None:
    type(self).commands.append(args)

  def read_response(self, *args, **kwargs):
    raise RedisTimeoutError("response lost after server commit")

  def disconnect(self, *args, **kwargs) -> None:
    type(self).disconnect_calls += 1

  def should_reconnect(self) -> bool:
    return False


class _CommittedThenResponseLostSentinelConnection(
  SentinelManagedSSLConnection
):
  """Real Sentinel-managed TLS connection with a lost response."""

  commands: ClassVar[list[tuple[Any, ...]]] = []
  retry_counts: ClassVar[list[int]] = []
  disconnect_calls: ClassVar[int] = 0

  def __init__(self, *args, **kwargs) -> None:
    super().__init__(*args, **kwargs)
    type(self).retry_counts.append(self.retry.get_retries())

  @classmethod
  def reset_observations(cls) -> None:
    cls.commands = []
    cls.retry_counts = []
    cls.disconnect_calls = 0

  def connect(self) -> None:
    return None

  def can_read(self, timeout: float = 0) -> bool:
    return False

  def send_command(self, *args, **kwargs) -> None:
    type(self).commands.append(args)

  def read_response(self, *args, **kwargs):
    raise RedisTimeoutError("response lost after sentinel-master commit")

  def disconnect(self, *args, **kwargs) -> None:
    type(self).disconnect_calls += 1

  def should_reconnect(self) -> bool:
    return False


@pytest.mark.parametrize(
  "mode", [RedisMode.STANDALONE, RedisMode.MASTER_SLAVE]
)
def test_direct_data_clients_receive_explicit_no_replay_policy(
  mocker, mode: RedisMode
) -> None:
  client = _client(mocker, mode.value)
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  backend = RedisBackend(RedisSettings(mode=mode))

  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert "retry_on_timeout" not in kwargs
  retry = kwargs["retry"]
  assert isinstance(retry, Retry)
  _assert_no_ambiguous_replay(retry)


def test_each_data_client_candidate_owns_a_fresh_retry_policy(mocker) -> None:
  first = _client(mocker, "first")
  second = _client(mocker, "second")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=[first, second]
  )
  backend = RedisBackend(RedisSettings())

  backend.connect()
  first_retry = constructor.call_args.kwargs["retry"]
  backend.disconnect()
  backend.connect()
  second_retry = constructor.call_args.kwargs["retry"]

  assert first_retry is not second_retry
  _assert_no_ambiguous_replay(first_retry)
  _assert_no_ambiguous_replay(second_retry)


@pytest.mark.parametrize("legacy_value", [False, True])
def test_legacy_data_retry_setting_is_accepted_but_warns_and_cannot_replay(
  mocker, legacy_value: bool
) -> None:
  client = _client(mocker, f"legacy-{legacy_value}")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  settings = RedisSettings(retry_on_timeout=legacy_value)

  with pytest.warns(FutureWarning, match="retry_on_timeout.*deprecated"):
    backend = RedisBackend(settings)
  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert "retry_on_timeout" not in kwargs
  _assert_no_ambiguous_replay(kwargs["retry"])


def test_legacy_data_retry_default_and_schema_remain_compatible() -> None:
  settings = RedisSettings()
  schema = RedisSettings.model_json_schema()

  assert settings.retry_on_timeout is True
  assert schema["properties"]["retry_on_timeout"]["deprecated"] is True


def test_legacy_environment_setting_warns_and_cannot_enable_replay(
  monkeypatch: pytest.MonkeyPatch, mocker
) -> None:
  monkeypatch.setenv("SCRAPY_REDIS_RETRY_ON_TIMEOUT", "false")
  client = _client(mocker, "legacy-environment")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  settings = RedisSettings()

  assert settings.retry_on_timeout is False
  with pytest.warns(FutureWarning, match="retry_on_timeout.*deprecated"):
    backend = RedisBackend(settings)
  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert "retry_on_timeout" not in kwargs
  _assert_no_ambiguous_replay(kwargs["retry"])


def test_default_setting_does_not_emit_legacy_warning(mocker) -> None:
  client = _client(mocker, "default-no-warning")
  mocker.patch("scrapy_extension.backends.redis.Redis", return_value=client)

  with warnings.catch_warnings():
    warnings.simplefilter("error", FutureWarning)
    backend = RedisBackend(RedisSettings())
    backend.connect()


def test_legacy_warning_does_not_render_configuration_source() -> None:
  secret_marker = "warning-source-secret-marker"
  with warnings.catch_warnings(record=True) as records:
    warnings.simplefilter("always", FutureWarning)
    RedisBackend(
      RedisSettings(password=secret_marker, retry_on_timeout=True)
    )

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
  assert secret_marker not in rendered


def test_flat_legacy_setting_reaches_backend_but_cannot_enable_replay(
  mocker,
) -> None:
  scrapy_settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_REDIS_RETRY_ON_TIMEOUT": True,
    }
  )
  _, raw_settings = resolve_backend_config(
    scrapy_settings,
    type_key="SCRAPY_QUEUE_BACKEND_TYPE",
    settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
  )
  client = _client(mocker, "legacy-flat")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )

  with pytest.warns(FutureWarning, match="SCRAPY_REDIS_RETRY_ON_TIMEOUT"):
    backend = RedisBackend(RedisSettings(**raw_settings))
  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert "retry_on_timeout" not in kwargs
  _assert_no_ambiguous_replay(kwargs["retry"])


@pytest.mark.parametrize("control_retry_enabled", [False, True])
def test_sentinel_control_and_data_retry_policies_are_isolated(
  mocker, control_retry_enabled: bool
) -> None:
  master = _client(mocker, "sentinel-master")
  sentinel = mocker.MagicMock(name="sentinel")
  sentinel.master_for.return_value = master
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Sentinel", return_value=sentinel
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379"],
      sentinel_retry_on_timeout=control_retry_enabled,
    )
  )

  backend.connect()

  sentinel_kwargs: dict[str, Any] = constructor.call_args.kwargs
  control_kwargs: dict[str, Any] = sentinel_kwargs["sentinel_kwargs"]
  master_kwargs: dict[str, Any] = sentinel.master_for.call_args.kwargs
  assert "retry_on_timeout" not in sentinel_kwargs
  assert "retry_on_timeout" not in control_kwargs
  assert "retry_on_timeout" not in master_kwargs
  sentinel_default_retry = sentinel_kwargs["retry"]
  control_retry = control_kwargs["retry"]
  master_retry = master_kwargs["retry"]
  assert sentinel_default_retry is not control_retry
  assert control_retry is not master_retry
  assert master_retry is not sentinel_default_retry
  _assert_no_ambiguous_replay(sentinel_default_retry)
  _assert_no_ambiguous_replay(master_retry)
  if control_retry_enabled:
    _assert_one_control_plane_retry(control_retry)
  else:
    _assert_no_ambiguous_replay(control_retry)


def test_sentinel_control_policy_does_not_retry_authentication_failure(
  mocker,
) -> None:
  master = _client(mocker, "sentinel-master")
  sentinel = mocker.MagicMock(name="sentinel")
  sentinel.master_for.return_value = master
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Sentinel", return_value=sentinel
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379"],
      sentinel_retry_on_timeout=True,
    )
  )
  backend.connect()
  control_retry = constructor.call_args.kwargs["sentinel_kwargs"]["retry"]
  attempts = 0
  failure_callbacks: list[BaseException] = []

  def authenticate() -> None:
    nonlocal attempts
    attempts += 1
    raise AuthenticationError("invalid sentinel credentials")

  with pytest.raises(AuthenticationError):
    control_retry.call_with_retry(authenticate, failure_callbacks.append)

  assert attempts == 1
  assert failure_callbacks == []


def test_cluster_outer_client_receives_explicit_no_replay_policy(mocker) -> None:
  client = _client(mocker, "cluster")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["cluster-a:6379"],
    )
  )

  backend.connect()

  kwargs = constructor.call_args.kwargs
  assert "retry_on_timeout" not in kwargs
  assert "cluster_error_retry_attempts" not in kwargs
  _assert_no_ambiguous_replay(kwargs["retry"])


def test_real_sdk_honors_captured_policy_without_deprecated_argument_warning(
  mocker,
) -> None:
  client = _client(mocker, "captured")
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  RedisBackend(RedisSettings()).connect()
  kwargs = constructor.call_args.kwargs

  with warnings.catch_warnings():
    warnings.simplefilter("error", DeprecationWarning)
    sdk_client = SdkRedis(**kwargs)
  try:
    pool_retry = sdk_client.connection_pool.connection_kwargs["retry"]
    _assert_no_ambiguous_replay(pool_retry)
  finally:
    sdk_client.close()


@pytest.mark.parametrize("operation", ["push", "pop"])
def test_real_sdk_script_transport_does_not_replay_after_lost_response(
  mocker, operation: str
) -> None:
  """Exercise Backend -> Script -> Redis.execute_command -> Connection.retry."""
  _CommittedThenResponseLostConnection.reset_observations()
  sdk_clients: list[SdkRedis] = []

  def real_client_factory(**kwargs):
    client = SdkRedis(**kwargs)
    client.connection_pool.connection_class = _CommittedThenResponseLostConnection
    client.ping = lambda: True  # type: ignore[method-assign]
    sdk_clients.append(client)
    return client

  mocker.patch(
    "scrapy_extension.backends.redis.Redis", side_effect=real_client_factory
  )
  backend = RedisBackend(RedisSettings())
  try:
    backend.connect()

    if operation == "push":
      call = lambda: backend.push("queue", b"payload")
    else:
      call = lambda: backend.pop("queue")

    with pytest.raises(QueueError) as exc_info:
      call()

    assert isinstance(exc_info.value.__cause__, RedisTimeoutError)
    assert _CommittedThenResponseLostConnection.retry_counts == [0]
    assert len(_CommittedThenResponseLostConnection.commands) == 1
    command = _CommittedThenResponseLostConnection.commands[0][0]
    assert command in {"EVALSHA", "EVAL"}
    assert _CommittedThenResponseLostConnection.disconnect_calls >= 1
  finally:
    backend.disconnect()
  assert len(sdk_clients) == 1


def test_real_cluster_script_transport_does_not_replay_after_lost_response(
  mocker,
) -> None:
  """Exercise the real RedisCluster outer loop and node connection policy."""
  _CommittedThenResponseLostConnection.reset_observations()
  mocker.patch.object(NodesManager, "initialize", autospec=True)
  mocker.patch.object(CommandsParser, "initialize", autospec=True)
  sdk_clients: list[SdkRedisCluster] = []

  def real_cluster_factory(**kwargs):
    client = SdkRedisCluster(**kwargs)
    node = ClusterNode("cluster-a", 6379, server_type="primary")
    node.redis_connection = client.nodes_manager.create_redis_node(
      node.host, node.port
    )
    node.redis_connection.connection_pool.connection_class = (
      _CommittedThenResponseLostConnection
    )
    slot = client.keyslot(b"{scrapy-extension:queue:queue}:items")
    client.nodes_manager.nodes_cache = {node.name: node}
    client.nodes_manager.slots_cache = {slot: [node]}
    client.nodes_manager.default_node = node
    client.ping = lambda: True  # type: ignore[method-assign]
    sdk_clients.append(client)
    return client

  mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster",
    side_effect=real_cluster_factory,
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["cluster-a:6379"],
    )
  )
  try:
    backend.connect()
    cluster = sdk_clients[0]

    with pytest.raises(QueueError) as exc_info:
      backend.push("queue", b"payload")

    assert isinstance(exc_info.value.__cause__, RedisTimeoutError)
    assert cluster.retry.get_retries() == 0
    assert _CommittedThenResponseLostConnection.retry_counts == [0]
    assert len(_CommittedThenResponseLostConnection.commands) == 1
    command = _CommittedThenResponseLostConnection.commands[0][0]
    assert command in {"EVALSHA", "EVAL"}
    assert _CommittedThenResponseLostConnection.disconnect_calls >= 1
  finally:
    backend.disconnect()

  assert len(sdk_clients) == 1


def test_real_sentinel_master_transport_does_not_replay_lost_response(
  mocker,
) -> None:
  """Exercise the real Sentinel master pool and managed TLS connection."""
  _CommittedThenResponseLostSentinelConnection.reset_observations()
  sdk_clients: list[SdkRedis] = []

  def real_sentinel_factory(*args, **kwargs):
    sentinel = SdkSentinel(*args, **kwargs)
    real_master_for = sentinel.master_for

    def master_for(*master_args, **master_kwargs):
      client = real_master_for(*master_args, **master_kwargs)
      pool = client.connection_pool
      assert isinstance(pool, SentinelConnectionPool)
      pool.connection_class = _CommittedThenResponseLostSentinelConnection
      client.ping = lambda: True  # type: ignore[method-assign]
      sdk_clients.append(client)
      return client

    sentinel.master_for = master_for  # type: ignore[method-assign]
    return sentinel

  mocker.patch(
    "scrapy_extension.backends.redis.Sentinel",
    side_effect=real_sentinel_factory,
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379"],
      ssl_enabled=True,
      ssl_cafile="/tls/ca.pem",
    )
  )
  try:
    backend.connect()

    with pytest.raises(QueueError) as exc_info:
      backend.pop("queue")

    assert isinstance(exc_info.value.__cause__, RedisTimeoutError)
    assert _CommittedThenResponseLostSentinelConnection.retry_counts == [0]
    assert len(_CommittedThenResponseLostSentinelConnection.commands) == 1
    command = _CommittedThenResponseLostSentinelConnection.commands[0][0]
    assert command in {"EVALSHA", "EVAL"}
    assert _CommittedThenResponseLostSentinelConnection.disconnect_calls >= 1
  finally:
    backend.disconnect()

  assert len(sdk_clients) == 1
