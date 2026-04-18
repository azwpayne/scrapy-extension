"""Scrapy extension for distributed crawling with multiple backend support.

This package provides distributed crawling capabilities for Scrapy with support
for multiple backends: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, and RocketMQ.
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
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.backends.kafka import KafkaBackend
from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.backends.rabbitmq import RabbitMQBackend
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend
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
from scrapy_extension.settings import (
  ElasticSearchMode,
  ElasticSearchSettings,
  KafkaMode,
  KafkaSettings,
  MongoDBMode,
  MongoDBSettings,
  RabbitMQMode,
  RabbitMQSettings,
  RedisMode,
  RedisSettings,
  RocketMQMode,
  RocketMQSettings,
  Settings,
)
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

__version__ = "0.1.0"

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
