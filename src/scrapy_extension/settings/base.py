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
