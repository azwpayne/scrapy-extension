"""Shared safe rendering boundary for Pydantic settings validation errors."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any

from pydantic import ValidationError
from pydantic_core import InitErrorDetails
from pydantic_settings import BaseSettings, SettingsError

from scrapy_extension.exceptions._redaction import sanitize_configuration_error
from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._aws import _AWS_SAFE_CONFIGURATION_MESSAGES
from scrapy_extension.settings._broker_endpoints import (
  KAFKA_BROKER_ENDPOINTS_ERROR,
  ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
)

_SAFE_SETTINGS_ERROR_NAMES: frozenset[str] = frozenset(
  {
    "SCRAPY_BACKEND_TYPE",
    "collection_names",
  }
)
_SAFE_SETTINGS_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
  {
    "api_key and basic-auth (username/password) are mutually exclusive; remove one authentication method.",
    "api_key must not be blank when supplied.",
    "Authenticated Pulsar connections require 'pulsar+ssl://' transport.",
    "Kafka CONFLUENT mode requires 'confluent_api_key' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
    "Kafka CONFLUENT mode requires 'confluent_api_key and confluent_api_secret' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
    "Kafka CONFLUENT mode requires 'confluent_api_secret' to be set. Without them the client could fall back to an unauthenticated SDK transport.",
    "Kafka TLS connections require ssl_check_hostname=True.",
    KAFKA_BROKER_ENDPOINTS_ERROR,
    "min_pool_size must be <= max_pool_size — an inverted pair makes the connection pool unable to satisfy any checkout (deadlock under load).",
    "MongoDB ATLAS mode requires an explicit 'mongodb+srv://' uri. atlas_cluster_name cannot replace uri because the backend uses uri verbatim and a complete Atlas SRV hostname cannot be derived from a cluster display name.",
    "MongoDB REPLICA_SET mode requires 'replica_set_name' to be set, or a uri that already carries a '?replicaSet=...' query.",
    "Pulsar cluster service_url must use a single scheme followed by a comma-separated endpoint list.",
    "Redis SENTINEL mode requires 'sentinel_master_name' to be set. No endpoint or credential values are included in this error.",
    "Redis SENTINEL mode requires 'sentinels' to be set. No endpoint or credential values are included in this error.",
    "Redis SENTINEL mode requires 'sentinels and sentinel_master_name' to be set. No endpoint or credential values are included in this error.",
    ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
    "SASL credentials (sasl_username / sasl_password / sasl_mechanism) require a 'SASL_'-prefixed security_protocol ('SASL_SSL'); kafka-python silently ignores the SASL fields otherwise (auth never attempted).",
    "ssl_enabled=True requires 'ssl_cafile' to be set (path to a CA certificate bundle).",
    "username must not be blank when supplied.",
  }
  | _AWS_SAFE_CONFIGURATION_MESSAGES
)
_TRUSTED_SETTINGS_CLASSES: frozenset[tuple[str, str]] = frozenset(
  {
    ("scrapy_extension.settings.base", "Settings"),
    ("scrapy_extension.settings.dynamodb", "DynamoDBSettings"),
    ("scrapy_extension.settings.elasticsearch", "ElasticSearchSettings"),
    ("scrapy_extension.settings.kafka", "KafkaSettings"),
    ("scrapy_extension.settings.memcached", "MemcachedSettings"),
    ("scrapy_extension.settings.mongodb", "MongoDBSettings"),
    ("scrapy_extension.settings.pulsar", "PulsarSettings"),
    ("scrapy_extension.settings.rabbitmq", "RabbitMQSettings"),
    ("scrapy_extension.settings.redis", "RedisSettings"),
    ("scrapy_extension.settings.rocketmq", "RocketMQSettings"),
    ("scrapy_extension.settings.sqs", "SqsSettings"),
  }
)


def _safe_location(value: object, field_names: frozenset[str]) -> tuple[str | int, ...]:
  """Keep a verified top-level field location without retaining raw keys."""
  if not isinstance(value, (list, tuple)) or not value:
    return ("configuration",)
  first = value[0]
  if not isinstance(first, str) or first not in field_names:
    return ("configuration",)
  location: list[str | int] = [first]
  for part in value[1:]:
    location.append(part if isinstance(part, int) else "value")
  return tuple(location)


def _redacted_validation_error(
  error: ValidationError, field_names: frozenset[str]
) -> ValidationError:
  """Rebuild a typed Pydantic error without input or validator diagnostics."""
  safe_lines: list[InitErrorDetails] = []
  for detail in error.errors():
    location = detail.get("loc") if isinstance(detail, Mapping) else None
    safe_lines.append(
      {
        "type": "value_error",
        "loc": _safe_location(location, field_names),
        "input": None,
        "ctx": {"error": ValueError("Invalid configuration value.")},
      }
    )
  if not safe_lines:
    safe_lines.append(
      {
        "type": "value_error",
        "loc": ("configuration",),
        "input": None,
        "ctx": {"error": ValueError("Invalid configuration value.")},
      }
    )
  return ValidationError.from_exception_data("Settings", safe_lines)


def _trusted_settings_field_names(instance: BaseSettings) -> frozenset[str]:
  """Return field names only for the extension's exact bundled models."""
  try:
    settings_type = type(instance)
    module_name = settings_type.__module__
    qualified_name = settings_type.__qualname__
    if (
      type(module_name) is not str
      or type(qualified_name) is not str
      or (module_name, qualified_name) not in _TRUSTED_SETTINGS_CLASSES
    ):
      return frozenset()
    module = import_module(module_name)
    if getattr(module, qualified_name, None) is not settings_type:
      return frozenset()
    fields = settings_type.model_fields
  except Exception:  # noqa: BLE001 - third-party metadata is not trusted
    return frozenset()
  if not isinstance(fields, Mapping):
    return frozenset()
  return frozenset(name for name in fields if type(name) is str)


class RedactedBaseSettings(BaseSettings):
  """Base settings that retain typed errors without retaining raw input.

  ``hide_input_in_errors`` only affects Pydantic's human-readable rendering;
  its ``errors()`` and ``json()`` APIs otherwise retain the original input.
  This boundary rebuilds any Pydantic validation failure with verified field
  locations and ``input=None`` after the original handler exits.
  """

  def __init__(self, **values: Any) -> None:
    validation_error: ValidationError | None = None
    source_error: SettingsError | None = None
    configuration_error: ConfigurationError | None = None
    unexpected_failure = False
    try:
      super().__init__(**values)
    except ValidationError as error:
      validation_error = error
    except SettingsError as error:
      source_error = error
    except ConfigurationError as error:
      configuration_error = error
    except Exception:  # noqa: BLE001 - settings inputs and sources are untrusted
      unexpected_failure = True
    if source_error is not None:
      # The exception traceback retains this frame. Drop raw input and the
      # source error before raising so frame-local introspection cannot reach
      # Pydantic's JSON decoder diagnostics.
      del values
      del source_error
      del self
      raise ConfigurationError(
        "Settings source contains an invalid configuration value.",
        setting_name="settings",
      )
    if validation_error is not None:
      field_names = _trusted_settings_field_names(self)
      redacted_error = _redacted_validation_error(validation_error, field_names)
      # As above, the new exception carries this frame in its traceback. Its
      # temporary source data must not remain available through ``f_locals``.
      del values
      del validation_error
      del self
      raise redacted_error
    if configuration_error is not None:
      if type(configuration_error) is ConfigurationError:
        field_names = _trusted_settings_field_names(self)
        original_message = (
          configuration_error.args[0] if configuration_error.args else None
        )
        sanitized_error = sanitize_configuration_error(
          configuration_error,
          field_names | _SAFE_SETTINGS_ERROR_NAMES,
          message=(
            original_message
            if (
              field_names
              and type(original_message) is str
              and original_message in _SAFE_SETTINGS_CONFIGURATION_MESSAGES
            )
            else "Settings contain an invalid configuration value."
          ),
          fallback_setting_name="settings",
        )
        del original_message
      else:
        sanitized_error = ConfigurationError(
          "Settings contain an invalid configuration value.",
          setting_name="settings",
        )
      # Validator-raised ConfigurationError instances carry their original
      # input frames.  Rebuild a static error outside the handler and remove
      # the original input/model references from this new traceback frame.
      del values
      del configuration_error
      del self
      raise sanitized_error
    if unexpected_failure:
      del values
      del self
      raise ConfigurationError(
        "Settings contain an invalid configuration value.",
        setting_name="settings",
      )
