"""Configuration module for scrapy-extension.

This module provides pydantic-settings based configuration classes
for all backend types.
"""

from scrapy_extension.config.settings import (
    BackendType,
    RedisSettings,
    Settings,
)

__all__ = [
    "Settings",
    "RedisSettings",
    "BackendType",
]
