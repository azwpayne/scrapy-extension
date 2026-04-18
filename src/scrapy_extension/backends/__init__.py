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
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.backends.rabbitmq import RabbitMQBackend
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend

__all__ = [
  "Backend",
  "BackendType",
  "ConnectionManager",
  "JSONSerializer",
  "QueueBackend",
  "RabbitMQBackend",
  "RedisBackend",
  "RocketMQBackend",
  "Serializer",
  "SetBackend",
  "StorageBackend",
]
