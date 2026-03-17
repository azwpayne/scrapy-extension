"""Settings classes for scrapy-extension.

This module provides pydantic-settings based configuration for all
backend types with environment variable support.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scrapy_extension.backends.base import BackendType


class Settings(BaseSettings):
    """Base settings for scrapy-extension.

    These settings apply to all backend types and can be configured
    via environment variables with the SCRAPY_ prefix.

    Attributes:
        backend_type: The type of backend to use.
        serializer: The serializer to use for data encoding.
        retry_attempts: Number of connection retry attempts.
        retry_delay: Delay between retry attempts in seconds.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_",
        case_sensitive=False,
        extra="ignore",
    )

    backend_type: BackendType = Field(
        default=BackendType.REDIS,
        description="Backend type for distributed crawling",
    )
    serializer: Literal["json"] = Field(
        default="json",
        description="Serializer to use for data encoding",
    )
    retry_attempts: int = Field(
        default=3,
        ge=0,
        description="Number of connection retry attempts",
    )
    retry_delay: float = Field(
        default=1.0,
        ge=0,
        description="Delay between retry attempts in seconds",
    )


class RedisSettings(BaseSettings):
    """Redis-specific settings.

    These settings configure the Redis connection and can be set
    via environment variables with the SCRAPY_REDIS_ prefix.

    Attributes:
        host: Redis server hostname.
        port: Redis server port.
        db: Redis database number.
        password: Redis authentication password.
        socket_timeout: Socket timeout in seconds.
        socket_connect_timeout: Socket connection timeout in seconds.
        retry_on_timeout: Whether to retry on timeout.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_REDIS_",
        case_sensitive=False,
        extra="ignore",
    )

    host: str = Field(
        default="localhost",
        description="Redis server hostname",
    )
    port: int = Field(
        default=6379,
        ge=1,
        le=65535,
        description="Redis server port",
    )
    db: int = Field(
        default=0,
        ge=0,
        description="Redis database number",
    )
    password: str | None = Field(
        default=None,
        description="Redis authentication password",
    )
    socket_timeout: float | None = Field(
        default=30.0,
        ge=0,
        description="Socket timeout in seconds",
    )
    socket_connect_timeout: float | None = Field(
        default=5.0,
        ge=0,
        description="Socket connection timeout in seconds",
    )
    retry_on_timeout: bool = Field(
        default=True,
        description="Whether to retry on timeout",
    )
