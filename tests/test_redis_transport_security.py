"""Security contracts for authenticated Redis transports."""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import RedisMode, RedisSettings


@pytest.mark.parametrize(
  "settings_kwargs",
  [
    {"host": "redis.internal", "password": "remote-redis-secret"},
    {"host": "redis.internal", "username": "remote-redis-secret"},
    {
      "mode": RedisMode.SENTINEL,
      "sentinels": ["sentinel.internal:26379"],
      "sentinel_password": "remote-redis-secret",
    },
    {
      "mode": RedisMode.SENTINEL,
      "sentinels": ["sentinel.internal:26379"],
      "password": "remote-redis-secret",
    },
    {
      "mode": RedisMode.SENTINEL,
      "sentinels": ["sentinel.internal:26379"],
      "sentinel_username": "remote-redis-secret",
    },
    {
      "mode": RedisMode.CLUSTER,
      "cluster_startup_nodes": ["cluster.internal:6379"],
      "password": "remote-redis-secret",
    },
    {
      "mode": RedisMode.CLUSTER,
      "host": "localhost",
      "password": "remote-redis-secret",
    },
    {
      "mode": RedisMode.MASTER_SLAVE,
      "host": "localhost",
      "password": "remote-redis-secret",
    },
  ],
)
def test_authenticated_non_direct_redis_connections_require_verified_tls(
  settings_kwargs,
) -> None:
  """Discovery and compatibility modes cannot infer a local auth boundary."""
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(**settings_kwargs)

  assert exc_info.value.setting_name == "ssl_enabled"
  assert "remote-redis-secret" not in str(exc_info.value)
  assert "remote-redis-secret" not in repr(exc_info.value)


@pytest.mark.parametrize("host", ["localhost", "localhost.", "127.0.0.1", "::1"])
def test_authenticated_direct_literal_loopback_redis_remains_valid(host: str) -> None:
  """The explicit standalone local-development path remains available."""
  settings = RedisSettings(host=host, password="local-redis-secret")

  assert settings.ssl_enabled is False


def test_lookalike_localhost_is_remote_for_authenticated_redis() -> None:
  """A DNS-controlled suffix must not receive the local plaintext exception."""
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(host="attacker.localhost", password="lookalike-redis-secret")

  assert exc_info.value.setting_name == "ssl_enabled"
  assert "lookalike-redis-secret" not in str(exc_info.value)


def test_authenticated_remote_redis_requires_hostname_verification() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(
      host="redis.internal",
      password="hostname-redis-secret",
      ssl_enabled=True,
      ssl_cafile="/tls/ca.pem",
      ssl_check_hostname=False,
    )

  assert exc_info.value.setting_name == "ssl_check_hostname"
  assert "hostname-redis-secret" not in str(exc_info.value)


@pytest.mark.parametrize(
  ("mode", "constructor_path"),
  [
    (RedisMode.STANDALONE, "scrapy_extension.backends.redis.Redis"),
    (RedisMode.SENTINEL, "scrapy_extension.backends.redis.Sentinel"),
    (RedisMode.CLUSTER, "scrapy_extension.backends.redis.RedisCluster"),
  ],
)
def test_connect_rejects_mutated_authenticated_tls_hostname_policy_before_sdk_io(
  mocker, mode: RedisMode, constructor_path: str
) -> None:
  """A post-construction hostname downgrade cannot reach any Redis SDK path."""
  settings_kwargs: dict[str, object] = {
    "mode": mode,
    "password": "runtime-hostname-redis-secret",
    "ssl_enabled": True,
    "ssl_cafile": "/tls/ca.pem",
  }
  if mode is RedisMode.SENTINEL:
    settings_kwargs["sentinels"] = ["sentinel.internal:26379"]
  elif mode is RedisMode.CLUSTER:
    settings_kwargs["cluster_startup_nodes"] = ["cluster.internal:6379"]
  else:
    settings_kwargs["host"] = "redis.internal"
  settings = RedisSettings(**settings_kwargs)
  settings.ssl_check_hostname = False
  constructor = mocker.patch(constructor_path)

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "ssl_check_hostname"
  assert "runtime-hostname-redis-secret" not in str(exc_info.value)
  assert "runtime-hostname-redis-secret" not in repr(exc_info.value)
  constructor.assert_not_called()


@pytest.mark.parametrize(
  ("mode", "mutate", "constructor_path"),
  [
    (
      RedisMode.STANDALONE,
      lambda settings: (
        setattr(settings, "host", "redis.internal"),
        setattr(settings, "password", SecretStr("runtime-redis-secret")),
      ),
      "scrapy_extension.backends.redis.Redis",
    ),
    (
      RedisMode.STANDALONE,
      lambda settings: (
        setattr(settings, "host", "redis.internal"),
        setattr(settings, "username", "runtime-redis-secret"),
      ),
      "scrapy_extension.backends.redis.Redis",
    ),
    (
      RedisMode.SENTINEL,
      lambda settings: setattr(
        settings, "sentinel_password", SecretStr("runtime-redis-secret")
      ),
      "scrapy_extension.backends.redis.Sentinel",
    ),
    (
      RedisMode.SENTINEL,
      lambda settings: setattr(
        settings, "sentinel_username", "runtime-redis-secret"
      ),
      "scrapy_extension.backends.redis.Sentinel",
    ),
    (
      RedisMode.CLUSTER,
      lambda settings: (
        setattr(settings, "cluster_startup_nodes", ["cluster.internal:6379"]),
        setattr(settings, "password", SecretStr("runtime-redis-secret")),
      ),
      "scrapy_extension.backends.redis.RedisCluster",
    ),
    (
      RedisMode.MASTER_SLAVE,
      lambda settings: setattr(
        settings, "password", SecretStr("runtime-redis-secret")
      ),
      "scrapy_extension.backends.redis.Redis",
    ),
  ],
)
def test_connect_rejects_mutated_authenticated_redis_before_sdk_io(
  mocker, mode: RedisMode, mutate, constructor_path: str
) -> None:
  """Mutable settings reapply the authenticated transport policy at connect."""
  settings_kwargs: dict[str, object] = {"mode": mode}
  if mode is RedisMode.SENTINEL:
    settings_kwargs["sentinels"] = ["localhost:26379"]
  elif mode is RedisMode.CLUSTER:
    settings_kwargs["cluster_startup_nodes"] = ["localhost:6379"]
  settings = RedisSettings(**settings_kwargs)
  mutate(settings)
  constructor = mocker.patch(constructor_path)

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "ssl_enabled"
  assert "runtime-redis-secret" not in str(exc_info.value)
  assert "runtime-redis-secret" not in repr(exc_info.value)
  constructor.assert_not_called()


def test_authenticated_remote_redis_tls_reaches_sdk_with_hostname_verification(
  mocker,
) -> None:
  client = mocker.MagicMock()
  client.ping.return_value = True
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.Redis", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      host="redis.internal",
      password="verified-redis-secret",
      ssl_enabled=True,
      ssl_cafile="/tls/ca.pem",
    )
  )

  backend.connect()

  assert constructor.call_args.kwargs["ssl"] is True
  assert constructor.call_args.kwargs["ssl_cert_reqs"] == "required"
  assert constructor.call_args.kwargs["ssl_check_hostname"] is True


def test_authenticated_remote_cluster_tls_reaches_sdk_with_verified_transport(
  mocker,
) -> None:
  client = mocker.MagicMock()
  client.ping.return_value = True
  constructor = mocker.patch(
    "scrapy_extension.backends.redis.RedisCluster", return_value=client
  )
  backend = RedisBackend(
    RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["cluster.internal:6379"],
      password="verified-cluster-redis-secret",
      ssl_enabled=True,
      ssl_cafile="/tls/ca.pem",
    )
  )

  backend.connect()

  assert constructor.call_args.kwargs["ssl"] is True
  assert constructor.call_args.kwargs["ssl_ca_certs"] == "/tls/ca.pem"
  assert constructor.call_args.kwargs["ssl_cert_reqs"] == "required"
  assert constructor.call_args.kwargs["ssl_check_hostname"] is True
