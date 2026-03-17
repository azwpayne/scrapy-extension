"""Scrapy extension for distributed crawling with multiple backend support.

This package provides distributed crawling capabilities for Scrapy with support
for multiple backends: Redis, MongoDB, Kafka, and RabbitMQ.
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
from scrapy_extension.backends.redis_backend import RedisBackend
from scrapy_extension.components.dupefilter import BackendDupeFilter
from scrapy_extension.components.pipeline import BackendPipeline
from scrapy_extension.components.queue import BackendQueue
from scrapy_extension.components.scheduler import BackendScheduler
from scrapy_extension.config.settings import RedisSettings, Settings
from scrapy_extension.connection.manager import ConnectionManager
from scrapy_extension.exceptions import (
  BackendConnectionError,
  BackendError,
  ConfigurationError,
  QueueError,
  SerializationError,
)
from scrapy_extension.spider_mixin import BackendSpiderMixin

__version__ = "0.1.0"
__all__ = [
  # Backends
  "Backend",
  "BackendType",
  "QueueBackend",
  "SetBackend",
  "StorageBackend",
  "RedisBackend",
  # Serialization
  "Serializer",
  "JSONSerializer",
  # Configuration
  "Settings",
  "RedisSettings",
  # Connection
  "ConnectionManager",
  # Components
  "BackendQueue",
  "BackendScheduler",
  "BackendDupeFilter",
  "BackendPipeline",
  # Spider
  "BackendSpiderMixin",
  # Exceptions
  "BackendError",
    "BackendConnectionError",
  "QueueError",
  "SerializationError",
  "ConfigurationError",
]


def hello() -> str:
  """Return a greeting message."""
  return "Hello from scrapy-extension!"
