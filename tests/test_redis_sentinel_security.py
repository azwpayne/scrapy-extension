"""Security contract tests for Redis Sentinel control-plane connections."""

from __future__ import annotations

import traceback

import pytest
from pydantic import SecretStr

from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RedisMode, RedisSettings


def _settings(**overrides) -> RedisSettings:
  values = {
    "mode": RedisMode.SENTINEL,
    "sentinels": ["sentinel-a:26379", "sentinel-b:26380"],
    "sentinel_master_name": "crawler-primary",
    "sentinel_username": "sentinel-user",
    "sentinel_password": "sentinel-secret",
    "username": "redis-user",
    "password": "redis-secret",
    "socket_timeout": 17.0,
    "socket_connect_timeout": 3.0,
    "retry_on_timeout": False,
    "sentinel_retry_on_timeout": False,
    "ssl_enabled": True,
    "ssl_cafile": "/tls/ca.pem",
    "ssl_certfile": "/tls/client.pem",
    "ssl_keyfile": "/tls/client.key",
    "ssl_check_hostname": True,
  }
  values.update(overrides)
  return RedisSettings(**values)


def _connect_with_captured_sentinel(mocker, settings: RedisSettings):
  captured: dict[str, object] = {}
  master = mocker.Mock()
  master.ping.return_value = True

  def sentinel_factory(sentinels, **kwargs):
    captured["sentinels"] = sentinels
    captured["sentinel_kwargs"] = kwargs
    instance = mocker.Mock()
    instance.master_for.return_value = master
    captured["instance"] = instance
    return instance

  mocker.patch(
    "scrapy_extension.backends.redis.Sentinel", side_effect=sentinel_factory
  )
  backend = RedisBackend(settings)
  backend.connect()
  return backend, master, captured


def test_sentinel_control_plane_inherits_tls_and_socket_policy(mocker) -> None:
  settings = _settings()

  _backend, _master, captured = _connect_with_captured_sentinel(mocker, settings)

  constructor_kwargs = captured["sentinel_kwargs"]
  control = constructor_kwargs["sentinel_kwargs"]
  assert control["socket_timeout"] == 17.0
  assert control["socket_connect_timeout"] == 3.0
  assert control["retry_on_timeout"] is False
  assert control["ssl"] is True
  assert control["ssl_ca_certs"] == "/tls/ca.pem"
  assert control["ssl_certfile"] == "/tls/client.pem"
  assert control["ssl_keyfile"] == "/tls/client.key"
  assert control["ssl_check_hostname"] is True


def test_locked_redis_sdk_maps_sentinel_ssl_flag_to_ssl_connection() -> None:
  """Protect the redis-py 7.3-8.x sentinel_kwargs integration boundary."""
  from redis.connection import SSLConnection
  from redis.sentinel import Sentinel

  sentinel = Sentinel(
    [("localhost", 26379)],
    sentinel_kwargs={
      "ssl": True,
      "ssl_ca_certs": "/tls/ca.pem",
      "ssl_check_hostname": True,
    },
  )

  pool = sentinel.sentinels[0].connection_pool
  assert pool.connection_class is SSLConnection
  assert pool.connection_kwargs["ssl_ca_certs"] == "/tls/ca.pem"
  assert pool.connection_kwargs["ssl_check_hostname"] is True


def test_sentinel_and_master_credentials_are_repr_redacted(mocker) -> None:
  settings = _settings()

  _backend, _master, captured = _connect_with_captured_sentinel(mocker, settings)

  constructor_kwargs = captured["sentinel_kwargs"]
  control = constructor_kwargs["sentinel_kwargs"]
  sentinel = captured["instance"]
  master_kwargs = sentinel.master_for.call_args.kwargs
  assert control["password"] == "sentinel-secret"
  assert master_kwargs["password"] == "redis-secret"
  assert "sentinel-secret" not in repr(control)
  assert "redis-secret" not in repr(master_kwargs)


def test_sentinel_connection_uses_one_preconstruction_snapshot(mocker) -> None:
  settings = _settings()
  master = mocker.Mock()
  master.ping.return_value = True
  sentinel = mocker.Mock()
  sentinel.master_for.return_value = master

  def mutate_after_construction(*_args, **_kwargs):
    settings.sentinel_master_name = "attacker-master"
    settings.sentinel_password = SecretStr("attacker-sentinel-secret")
    settings.password = SecretStr("attacker-redis-secret")
    settings.ssl_cafile = "/attacker/ca.pem"
    return sentinel

  mocker.patch(
    "scrapy_extension.backends.redis.Sentinel",
    side_effect=mutate_after_construction,
  )

  RedisBackend(settings).connect()

  assert sentinel.master_for.call_args.args[0] == "crawler-primary"
  master_kwargs = sentinel.master_for.call_args.kwargs
  assert master_kwargs["password"] == "redis-secret"
  assert master_kwargs["ssl_ca_certs"] == "/tls/ca.pem"


def test_connect_revalidates_mutated_tls_snapshot_before_sdk_io(mocker) -> None:
  settings = _settings()
  settings.ssl_cafile = None
  sentinel = mocker.patch("scrapy_extension.backends.redis.Sentinel")

  with pytest.raises(ConfigurationError) as exc_info:
    RedisBackend(settings).connect()

  assert exc_info.value.setting_name == "ssl_cafile"
  sentinel.assert_not_called()


def test_sentinel_startup_error_does_not_echo_credentials(mocker) -> None:
  settings = _settings()
  mocker.patch(
    "scrapy_extension.backends.redis.Sentinel",
    side_effect=RuntimeError(
      "failed with sentinel-secret and redis-secret in local config"
    ),
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    RedisBackend(settings).connect()

  public_message = str(exc_info.value)
  assert "sentinel-secret" not in public_message
  assert "redis-secret" not in public_message
  rendered_traceback = "".join(traceback.format_exception(exc_info.value))
  assert "sentinel-secret" not in rendered_traceback
  assert "redis-secret" not in rendered_traceback
  assert exc_info.value.__cause__ is None


@pytest.mark.parametrize(
  ("certfile", "keyfile", "missing_name"),
  [
    ("/tls/client.pem", None, "ssl_keyfile"),
    (None, "/tls/client.key", "ssl_certfile"),
  ],
)
def test_redis_tls_client_certificate_must_be_a_pair(
  certfile: str | None, keyfile: str | None, missing_name: str
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(
      ssl_enabled=True,
      ssl_cafile="/tls/ca.pem",
      ssl_certfile=certfile,
      ssl_keyfile=keyfile,
    )

  assert exc_info.value.setting_name == missing_name


def test_redis_tls_rejects_blank_ca_path() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RedisSettings(ssl_enabled=True, ssl_cafile="   ")

  assert exc_info.value.setting_name == "ssl_cafile"
