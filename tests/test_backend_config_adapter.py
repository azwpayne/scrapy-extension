"""Tests for adapting Scrapy settings into backend configuration."""

from __future__ import annotations

import pytest
from scrapy.settings import Settings as ScrapySettings

from scrapy_extension.backends import connectors
from scrapy_extension.backends.connectors import resolve_backend_config
from scrapy_extension.backends.registry import BackendDescriptor

pytestmark = pytest.mark.unit


def _resolve_queue(settings: ScrapySettings) -> tuple[str, dict[str, object]]:
  return resolve_backend_config(
    settings,
    type_key="SCRAPY_QUEUE_BACKEND_TYPE",
    settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
  )


def test_redis_flat_scrapy_setting_is_forwarded_to_backend() -> None:
  settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_REDIS_HOST": "redis.internal",
      "SCRAPY_REDIS_DB": 0,
      "SCRAPY_REDIS_DECODE_RESPONSES": False,
      "SCRAPY_REDIS_RETRY_ON_TIMEOUT": False,
      "SCRAPY_MONGO_URI": "mongodb://must-not-leak:27017",
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "redis"
  assert backend_settings == {
    "host": "redis.internal",
    "db": 0,
    "decode_responses": False,
    "retry_on_timeout": False,
  }


def test_mongodb_flat_scrapy_settings_use_model_env_prefix() -> None:
  settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "mongodb",
      "SCRAPY_MONGO_URI": "mongodb://mongo.internal:27017",
      "SCRAPY_MONGO_DATABASE": "crawl_items",
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "mongodb"
  assert backend_settings == {
    "uri": "mongodb://mongo.internal:27017",
    "database": "crawl_items",
  }


def test_nested_backend_settings_override_flat_scrapy_settings() -> None:
  settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_REDIS_HOST": "flat-redis.internal",
      "SCRAPY_REDIS_PORT": 6379,
      "SCRAPY_BACKEND_SETTINGS": {
        "host": "nested-redis.internal",
        "db": 4,
      },
    }
  )

  _, backend_settings = _resolve_queue(settings)

  assert backend_settings == {
    "host": "nested-redis.internal",
    "port": 6379,
    "db": 4,
  }


def test_backend_type_falls_back_to_same_named_component_environment(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  monkeypatch.setenv("SCRAPY_QUEUE_BACKEND_TYPE", "mongodb")
  settings = ScrapySettings(
    {
      "SCRAPY_MONGO_URI": "mongodb://queue.internal:27017",
      "SCRAPY_MONGO_DATABASE": "flat_items",
      "SCRAPY_QUEUE_BACKEND_SETTINGS": {"database": "queue_items"},
      "SCRAPY_BACKEND_SETTINGS": {"database": "global_items"},
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "mongodb"
  assert backend_settings == {
    "uri": "mongodb://queue.internal:27017",
    "database": "queue_items",
  }


def test_plugin_backend_skips_bundled_flat_setting_extraction(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  descriptor = BackendDescriptor(
    backend_type="third_party",
    backend_cls_path="tests.test_registry._StubBackend",
    settings_cls_path="scrapy_extension.settings.RedisSettings",
    capabilities=frozenset({"queue"}),
  )
  monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
  settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "third_party",
      "SCRAPY_REDIS_HOST": "must-not-leak",
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "third_party"
  assert backend_settings == {}


def test_non_pydantic_settings_class_safely_skips_flat_extraction(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  descriptor = BackendDescriptor(
    backend_type="redis",
    backend_cls_path="tests.test_registry._StubBackend",
    settings_cls_path="builtins.dict",
    capabilities=frozenset({"queue", "set", "storage"}),
  )
  monkeypatch.setattr(connectors, "get_descriptor", lambda _: descriptor)
  settings = ScrapySettings(
    {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_REDIS_HOST": "must-not-be-read",
      "SCRAPY_BACKEND_SETTINGS": {"db": 2},
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "redis"
  assert backend_settings == {"db": 2}


def test_global_environment_type_uses_global_nested_settings(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  monkeypatch.setenv("SCRAPY_BACKEND_TYPE", "mongodb")
  settings = ScrapySettings(
    {
      "SCRAPY_MONGO_URI": "mongodb://global.internal:27017",
      "SCRAPY_BACKEND_SETTINGS": {"database": "global_items"},
      "SCRAPY_QUEUE_BACKEND_SETTINGS": {"database": "component_items"},
    }
  )

  backend_type, backend_settings = _resolve_queue(settings)

  assert backend_type == "mongodb"
  assert backend_settings == {
    "uri": "mongodb://global.internal:27017",
    "database": "global_items",
  }


@pytest.mark.parametrize(
  ("scrapy_values", "environment_values", "expected"),
  [
    (
      {
        "SCRAPY_QUEUE_BACKEND_TYPE": "mongodb",
        "SCRAPY_BACKEND_TYPE": "redis",
      },
      {
        "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
        "SCRAPY_BACKEND_TYPE": "rabbitmq",
      },
      "mongodb",
    ),
    (
      {"SCRAPY_BACKEND_TYPE": "redis"},
      {"SCRAPY_QUEUE_BACKEND_TYPE": "mongodb"},
      "redis",
    ),
    (
      {},
      {
        "SCRAPY_QUEUE_BACKEND_TYPE": "mongodb",
        "SCRAPY_BACKEND_TYPE": "redis",
      },
      "mongodb",
    ),
    ({}, {"SCRAPY_BACKEND_TYPE": "mongodb"}, "mongodb"),
    ({}, {"SCRAPY_BACKEND_TYPE": ""}, "redis"),
    ({}, {}, "redis"),
  ],
)
def test_backend_type_priority(
  monkeypatch: pytest.MonkeyPatch,
  scrapy_values: dict[str, str],
  environment_values: dict[str, str],
  expected: str,
) -> None:
  """Scrapy component > Scrapy global > env component > env global > redis."""
  for key in ("SCRAPY_QUEUE_BACKEND_TYPE", "SCRAPY_BACKEND_TYPE"):
    monkeypatch.delenv(key, raising=False)
  for key, value in environment_values.items():
    monkeypatch.setenv(key, value)
  settings = ScrapySettings(scrapy_values)

  backend_type, _ = _resolve_queue(settings)

  assert backend_type == expected
