"""Backend implementations for scrapy-extension.

This module provides abstract base classes and concrete implementations
for different backend types (Redis, MongoDB, Kafka, RabbitMQ).
"""

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  JSONSerializer,
  QueueBackend,
  Serializer,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.backends.rabbitmq_backend import RabbitMQBackend
from scrapy_extension.backends.redis_backend import RedisBackend

__all__ = [
  "Backend",
  "BackendType",
  "JSONSerializer",
  "QueueBackend",
  "RabbitMQBackend",
  "RedisBackend",
  "Serializer",
  "SetBackend",
  "StorageBackend",
]
