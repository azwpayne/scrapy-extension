"""Regression tests for direct backend startup redaction boundaries."""

from __future__ import annotations

import builtins
import traceback
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import SecretStr

from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError

_MARKER = "direct-backend-config-redaction-marker"


def _assert_value_is_redacted(value: object, marker: str) -> None:
  """Inspect values that deliberately hide their contents in ``repr``."""
  if isinstance(value, SecretStr):
    assert marker not in value.get_secret_value()
  elif isinstance(value, str):
    assert marker not in value
  elif type(value) is tuple:
    for item in value:
      _assert_value_is_redacted(item, marker)
  elif type(value) is dict:
    for key, item in value.items():
      _assert_value_is_redacted(key, marker)
      _assert_value_is_redacted(item, marker)


def _assert_package_traceback_is_redacted(error: BaseException, marker: str) -> None:
  """Inspect frames directly so repr-redacting values cannot mask retention."""
  trace = error.__traceback__
  while trace is not None:
    frame = trace.tb_frame
    if "/src/scrapy_extension/" in frame.f_code.co_filename:
      locals_snapshot = frame.f_locals
      assert marker not in repr(locals_snapshot)
      for local in locals_snapshot.values():
        _assert_value_is_redacted(local, marker)
        try:
          config = vars(local).get("config")
        except TypeError:
          config = None
        if config is not None:
          config_values = vars(config)
          assert marker not in repr(config_values)
          _assert_value_is_redacted(config_values, marker)
        if type(local) is tuple:
          for argument in local:
            _assert_value_is_redacted(argument, marker)
            try:
              config = vars(argument).get("config")
            except TypeError:
              config = None
            if config is not None:
              config_values = vars(config)
              assert marker not in repr(config_values)
              _assert_value_is_redacted(config_values, marker)
    trace = trace.tb_next


def _kafka_backend() -> Any:
  from scrapy_extension.backends.kafka import KafkaBackend
  from scrapy_extension.settings import KafkaSettings

  return KafkaBackend(KafkaSettings())


def _rabbitmq_backend() -> Any:
  from scrapy_extension.backends.rabbitmq import RabbitMQBackend
  from scrapy_extension.settings import RabbitMQSettings

  return RabbitMQBackend(RabbitMQSettings())


def _pulsar_backend() -> Any:
  from scrapy_extension.backends.pulsar import PulsarBackend
  from scrapy_extension.settings import PulsarSettings

  return PulsarBackend(PulsarSettings())


def _memcached_backend() -> Any:
  from scrapy_extension.backends.memcached import MemcachedBackend
  from scrapy_extension.settings import MemcachedSettings

  return MemcachedBackend(MemcachedSettings())


def _dynamodb_backend() -> Any:
  from scrapy_extension.backends.dynamodb import DynamoDBBackend
  from scrapy_extension.settings import DynamoDBSettings

  return DynamoDBBackend(DynamoDBSettings())


def _sqs_backend() -> Any:
  from scrapy_extension.backends.sqs import SqsBackend
  from scrapy_extension.settings import SqsSettings

  return SqsBackend(SqsSettings())


def _rocketmq_backend() -> Any:
  from scrapy_extension.backends.rocketmq import RocketMQBackend
  from scrapy_extension.settings import RocketMQSettings

  return RocketMQBackend(RocketMQSettings())


def _redis_backend() -> Any:
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.settings import RedisSettings

  return RedisBackend(RedisSettings())


_BACKEND_FACTORIES: tuple[tuple[str, Callable[[], Any]], ...] = (
  ("kafka", _kafka_backend),
  ("rabbitmq", _rabbitmq_backend),
  ("pulsar", _pulsar_backend),
  ("memcached", _memcached_backend),
  ("dynamodb", _dynamodb_backend),
  ("sqs", _sqs_backend),
  ("rocketmq", _rocketmq_backend),
  ("redis", _redis_backend),
)


def _snapshot_hook_name(backend_name: str) -> str:
  if backend_name == "redis":
    return "_capture_connection_plan"
  if backend_name == "rocketmq":
    return "_connect_unlocked"
  return "_capture_connection_snapshot"


@pytest.mark.parametrize(("_backend_name", "factory"), _BACKEND_FACTORIES)
def test_direct_connect_rebuilds_mutated_config_without_traceback_state(
  _backend_name: str,
  factory: Callable[[], Any],
) -> None:
  """Every direct startup API drops a mutated settings object before raising."""
  backend = factory()
  backend.config.mode = _MARKER

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()

  error = exc_info.value
  assert _MARKER not in str(error)
  assert _MARKER not in repr(error.__dict__)
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.setting_value is None
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


@pytest.mark.parametrize(("backend_name", "factory"), _BACKEND_FACTORIES)
def test_direct_snapshot_rebuilds_mutated_config_without_traceback_state(
  backend_name: str,
  factory: Callable[[], Any],
) -> None:
  """Private snapshot seams cannot bypass the same terminal redaction rule."""
  backend = factory()
  backend.config.mode = _MARKER

  with pytest.raises(ConfigurationError) as exc_info:
    getattr(backend, _snapshot_hook_name(backend_name))()

  error = exc_info.value
  assert _MARKER not in str(error)
  assert _MARKER not in repr(error.__dict__)
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.setting_value is None
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


@pytest.mark.parametrize(("backend_name", "factory"), _BACKEND_FACTORIES)
def test_direct_connect_rebuilds_connection_errors_without_config_frames(
  monkeypatch: pytest.MonkeyPatch,
  backend_name: str,
  factory: Callable[[], Any],
) -> None:
  """Operational startup errors preserve their type but not direct config."""
  backend = factory()
  backend.config.__dict__["round41b_marker"] = _MARKER

  def _raise_connection_error(*args: object, **kwargs: object) -> None:
    del args, kwargs
    raise BackendConnectionError(
      f"driver diagnostic included {_MARKER}", backend_type="untrusted"
    )

  hook_name = (
    "_connect_for_epoch"
    if backend_name == "redis"
    else _snapshot_hook_name(backend_name)
  )
  monkeypatch.setattr(backend, hook_name, _raise_connection_error)

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()

  error = exc_info.value
  assert _MARKER not in str(error)
  assert _MARKER not in repr(error.__dict__)
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.backend_type == backend_name
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


@pytest.mark.parametrize(
  ("backend_name", "factory"),
  (("dynamodb", _dynamodb_backend), ("sqs", _sqs_backend)),
)
@pytest.mark.parametrize("entrypoint", ("connect", "snapshot"))
def test_aws_credential_validation_preserves_safe_text_without_secret_traceback(
  backend_name: str,
  factory: Callable[[], Any],
  entrypoint: str,
) -> None:
  """Trusted AWS policy text remains actionable after terminal redaction."""
  backend = factory()
  backend.config.aws_access_key_id = SecretStr(_MARKER)
  operation = (
    backend.connect
    if entrypoint == "connect"
    else getattr(backend, "_capture_connection_snapshot")
  )

  with pytest.raises(ConfigurationError) as exc_info:
    operation()

  error = exc_info.value
  assert str(error) == (
    "aws_secret_access_key is required when aws_access_key_id is set; "
    "set both or leave both unset to use the ambient credential chain."
  )
  assert error.setting_name == "aws_secret_access_key"
  assert error.setting_value == "***REDACTED***"
  assert _MARKER not in repr(error.setting_value)
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


def test_rocketmq_internal_import_error_drops_config_traceback(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  """A non-missing optional dependency failure keeps its identity, not config."""
  backend = _rocketmq_backend()
  backend.config.__dict__["round41b_marker"] = _MARKER
  failure = ImportError("rocketmq internal ABI/import failure")
  original_import = builtins.__import__

  def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
    if name == "rocketmq" or name.startswith("rocketmq."):
      raise failure
    return original_import(name, *args, **kwargs)

  monkeypatch.setattr(builtins, "__import__", guarded_import)

  with pytest.raises(ImportError) as exc_info:
    backend.connect()

  error = exc_info.value
  assert error is failure
  assert error.__cause__ is None
  assert error.__context__ is None
  assert _MARKER not in "".join(traceback.format_exception(error))
  _assert_package_traceback_is_redacted(error, _MARKER)


def test_redis_reentrant_connect_preserves_safe_message_without_config_frames() -> None:
  """The static re-entrancy contract survives the terminal startup boundary."""
  backend = _redis_backend()
  backend.config.__dict__["round41b_marker"] = _MARKER
  backend._connect_local.depth = 1
  try:
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
  finally:
    del backend._connect_local.depth

  error = exc_info.value
  assert str(error) == "Cannot connect to Redis re-entrantly while building a candidate."
  assert error.backend_type == "redis"
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


def test_kafka_constructor_error_drops_config_from_traceback() -> None:
  """The early auto-commit contract cannot retain unrelated settings."""
  from scrapy_extension.backends.kafka import KafkaBackend
  from scrapy_extension.settings import KafkaSettings

  config = KafkaSettings(enable_auto_commit=True)
  config.__dict__["round41b_marker"] = _MARKER

  with pytest.raises(ConfigurationError) as exc_info:
    KafkaBackend(config)

  error = exc_info.value
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)


@pytest.mark.parametrize(
  "guard_name",
  ("RocketMQSetBackend", "RocketMQStorageBackend"),
)
def test_rocketmq_guard_constructor_drops_config_from_traceback(
  guard_name: str,
) -> None:
  """Unsupported capability guards are public constructors too."""
  from scrapy_extension.backends import rocketmq
  from scrapy_extension.settings import RocketMQSettings

  config = RocketMQSettings()
  config.__dict__["round41b_marker"] = _MARKER
  guard = getattr(rocketmq, guard_name)

  with pytest.raises(ConfigurationError) as exc_info:
    guard(config)

  error = exc_info.value
  assert _MARKER not in "".join(traceback.format_exception(error))
  assert error.__cause__ is None
  assert error.__context__ is None
  _assert_package_traceback_is_redacted(error, _MARKER)
