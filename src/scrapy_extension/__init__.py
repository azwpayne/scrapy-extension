"""Scrapy extension for distributed crawling with multiple backend support.

This package provides distributed crawling capabilities for Scrapy with support
for multiple backends: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, and RocketMQ.
"""

from __future__ import annotations

import importlib
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    JSONSerializer,
    QueueBackend,
    Serializer,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    QueueError,
    SerializationError,
)
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings.base import Settings
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

try:
    __version__ = _pkg_version("scrapy-extension")
except PackageNotFoundError:
    __version__ = "0.0.0"


# Optional imports: (module_path, attribute_name)
# These are lazily loaded via __getattr__ so that backend-specific dependencies
# (e.g. redis, pymongo, kafka-python, pika, elasticsearch, rocketmq) are only
# required when the corresponding class is actually used.
_OPTIONAL_IMPORTS: dict[str, tuple[str, str]] = {
    # Backend classes
    "ElasticSearchBackend": ("scrapy_extension.backends.elasticsearch", "ElasticSearchBackend"),
    "KafkaBackend": ("scrapy_extension.backends.kafka", "KafkaBackend"),
    "MongoDBBackend": ("scrapy_extension.backends.mongodb", "MongoDBBackend"),
    "RabbitMQBackend": ("scrapy_extension.backends.rabbitmq", "RabbitMQBackend"),
    "RedisBackend": ("scrapy_extension.backends.redis", "RedisBackend"),
    "RocketMQBackend": ("scrapy_extension.backends.rocketmq", "RocketMQBackend"),
    # Settings classes
    "ElasticSearchMode": ("scrapy_extension.settings.elasticsearch", "ElasticSearchMode"),
    "ElasticSearchSettings": ("scrapy_extension.settings.elasticsearch", "ElasticSearchSettings"),
    "KafkaMode": ("scrapy_extension.settings.kafka", "KafkaMode"),
    "KafkaSettings": ("scrapy_extension.settings.kafka", "KafkaSettings"),
    "MongoDBMode": ("scrapy_extension.settings.mongodb", "MongoDBMode"),
    "MongoDBSettings": ("scrapy_extension.settings.mongodb", "MongoDBSettings"),
    "RabbitMQMode": ("scrapy_extension.settings.rabbitmq", "RabbitMQMode"),
    "RabbitMQSettings": ("scrapy_extension.settings.rabbitmq", "RabbitMQSettings"),
    "RedisMode": ("scrapy_extension.settings.redis", "RedisMode"),
    "RedisSettings": ("scrapy_extension.settings.redis", "RedisSettings"),
    "RocketMQMode": ("scrapy_extension.settings.rocketmq", "RocketMQMode"),
    "RocketMQSettings": ("scrapy_extension.settings.rocketmq", "RocketMQSettings"),
}

# Extra name -> pip extras mapping for helpful error messages
_BACKEND_EXTRAS: dict[str, str] = {
    "ElasticSearchBackend": "elasticsearch",
    "ElasticSearchMode": "elasticsearch",
    "ElasticSearchSettings": "elasticsearch",
    "KafkaBackend": "kafka",
    "KafkaMode": "kafka",
    "KafkaSettings": "kafka",
    "MongoDBBackend": "mongodb",
    "MongoDBMode": "mongodb",
    "MongoDBSettings": "mongodb",
    "RabbitMQBackend": "rabbitmq",
    "RabbitMQMode": "rabbitmq",
    "RabbitMQSettings": "rabbitmq",
    "RedisBackend": "redis",
    "RedisMode": "redis",
    "RedisSettings": "redis",
    "RocketMQBackend": "rocketmq",
    "RocketMQMode": "rocketmq",
    "RocketMQSettings": "rocketmq",
}


def __getattr__(name: str) -> object:
    """Lazily import optional backend classes and settings (PEP 562).

    Core classes (Backend, ConnectionManager, Settings, etc.) are always
    available. Backend-specific classes require their respective optional
    dependencies and are imported on first access.
    """
    if name in _OPTIONAL_IMPORTS:
        module_path, attr_name = _OPTIONAL_IMPORTS[name]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, attr_name)
        except ImportError as e:
            extra = _BACKEND_EXTRAS.get(name, name)
            raise ImportError(
                f"{name} requires additional dependencies. "
                f"Install with: pip install scrapy-extension[{extra}]"
            ) from e
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Backends
    "Backend",
    "BackendConnectionError",
    "BackendDupeFilter",
    # Exceptions
    "BackendError",
    "BackendPipeline",
    # Components
    "BackendQueue",
    "BackendScheduler",
    # Spider
    "BackendSpiderMixin",
    "BackendType",
    "ConfigurationError",
    # Connection
    "ConnectionManager",
    "ElasticSearchBackend",
    "ElasticSearchMode",
    "ElasticSearchSettings",
    "JSONSerializer",
    "KafkaBackend",
    "KafkaMode",
    "KafkaSettings",
    "MongoDBBackend",
    "MongoDBMode",
    "MongoDBSettings",
    "QueueBackend",
    "QueueError",
    "RabbitMQBackend",
    "RabbitMQMode",
    "RabbitMQSettings",
    "RedisBackend",
    "RedisMode",
    "RedisSettings",
    "RocketMQBackend",
    "RocketMQMode",
    "RocketMQSettings",
    "SerializationError",
    # Serialization
    "Serializer",
    "SetBackend",
    # Configuration
    "Settings",
    "StorageBackend",
]
