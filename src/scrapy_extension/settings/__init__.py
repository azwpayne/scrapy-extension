"""Configuration module for scrapy-extension.

This module provides pydantic-settings based configuration classes
for all backend types.
"""

from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.dynamodb import DynamoDBMode, DynamoDBSettings
from scrapy_extension.settings.elasticsearch import (
  ElasticSearchMode,
  ElasticSearchSettings,
)
from scrapy_extension.settings.kafka import KafkaMode, KafkaSettings
from scrapy_extension.settings.memcached import MemcachedMode, MemcachedSettings
from scrapy_extension.settings.mongodb import MongoDBMode, MongoDBSettings
from scrapy_extension.settings.pulsar import PulsarMode, PulsarSettings
from scrapy_extension.settings.rabbitmq import RabbitMQMode, RabbitMQSettings
from scrapy_extension.settings.redis import RedisMode, RedisSettings
from scrapy_extension.settings.rocketmq import RocketMQMode, RocketMQSettings
from scrapy_extension.settings.sqs import SqsMode, SqsSettings

__all__ = [
  "DynamoDBMode",
  "DynamoDBSettings",
  "ElasticSearchMode",
  "ElasticSearchSettings",
  "KafkaMode",
  "KafkaSettings",
  "MemcachedMode",
  "MemcachedSettings",
  "MongoDBMode",
  "MongoDBSettings",
  "PulsarMode",
  "PulsarSettings",
  "RabbitMQMode",
  "RabbitMQSettings",
  "RedisMode",
  "RedisSettings",
  "RocketMQMode",
  "RocketMQSettings",
  "Settings",
  "SqsMode",
  "SqsSettings",
]
