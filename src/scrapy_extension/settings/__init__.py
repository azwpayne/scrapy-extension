"""Configuration module for scrapy-extension.

This module provides pydantic-settings based configuration classes
for all backend types.
"""

from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.kafka import KafkaMode, KafkaSettings
from scrapy_extension.settings.mongodb import MongoDBMode, MongoDBSettings
from scrapy_extension.settings.rabbitmq import RabbitMQMode, RabbitMQSettings
from scrapy_extension.settings.redis import RedisMode, RedisSettings

__all__ = [
  "KafkaMode",
  "KafkaSettings",
  "MongoDBMode",
  "MongoDBSettings",
  "RabbitMQMode",
  "RabbitMQSettings",
  "RedisMode",
  "RedisSettings",
  "Settings",
]
