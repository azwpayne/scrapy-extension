"""Backend implementations for scrapy-extension.

This module provides abstract base classes and concrete implementations
for different backend types (Redis, MongoDB, Kafka, RabbitMQ).

Concrete backend classes are lazily loaded via PEP 562 __getattr__ to avoid
importing optional dependencies at module level.
"""

import importlib

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

_BACKEND_MODULES = {
    "RedisBackend": ("scrapy_extension.backends.redis", "RedisBackend"),
    "MongoDBBackend": ("scrapy_extension.backends.mongodb", "MongoDBBackend"),
    "KafkaBackend": ("scrapy_extension.backends.kafka", "KafkaBackend"),
    "RabbitMQBackend": ("scrapy_extension.backends.rabbitmq", "RabbitMQBackend"),
    "ElasticSearchBackend": (
        "scrapy_extension.backends.elasticsearch",
        "ElasticSearchBackend",
    ),
    "RocketMQBackend": ("scrapy_extension.backends.rocketmq", "RocketMQBackend"),
    "PulsarBackend": ("scrapy_extension.backends.pulsar", "PulsarBackend"),
    "MemcachedBackend": ("scrapy_extension.backends.memcached", "MemcachedBackend"),
}


def __getattr__(name: str) -> object:
    if name in _BACKEND_MODULES:
        module_path, attr_name = _BACKEND_MODULES[name]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, attr_name)
        except ImportError as e:
            backend = name.replace("Backend", "").lower()
            raise ImportError(
                f"{name} requires the '{backend}' extra. "
                f"Install with: pip install scrapy-extension[{backend}]"
            ) from e
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Backend",
    "BackendType",
    "ConnectionManager",
    "ElasticSearchBackend",
    "JSONSerializer",
    "KafkaBackend",
    "MemcachedBackend",
    "MongoDBBackend",
    "PulsarBackend",
    "QueueBackend",
    "RabbitMQBackend",
    "RedisBackend",
    "RocketMQBackend",
    "Serializer",
    "SetBackend",
    "StorageBackend",
]
