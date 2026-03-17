"""Backend implementations for scrapy-extension.

This module provides abstract base classes and concrete implementations
for different backend types (Redis, MongoDB, Kafka, RabbitMQ).
"""

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
  Serializer,
  JSONSerializer,
)
from scrapy_extension.backends.redis_backend import RedisBackend

__all__ = [
  "Backend",
  "BackendType",
  "QueueBackend",
  "SetBackend",
  "StorageBackend",
  "Serializer",
  "JSONSerializer",
  "RedisBackend",
]
