# @author  : azwpayne(https://github.com/azwpayne)
# @name    : memcached.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Memcached settings (subsystem ③ — new NoSQL backend)
from __future__ import annotations

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MemcachedMode(str, Enum):
  """Memcached deployment modes.

  Attributes:
      STANDALONE: Single memcached instance (default).
  """

  STANDALONE = "standalone"


class MemcachedSettings(BaseSettings):
  """Memcached-specific settings (key-value cache / NoSQL).

  Configurable via environment variables with the SCRAPY_MEMCACHED_ prefix.
  Memcached is used only for StorageBackend (KV with TTL); it has no native
  ordered queue or set.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_MEMCACHED_", case_sensitive=False, extra="ignore"
  )

  mode: MemcachedMode = Field(
    default=MemcachedMode.STANDALONE,
    description="Memcached deployment mode (standalone)",
  )
  host: str = Field(default="localhost", description="Memcached host")
  port: int = Field(default=11211, description="Memcached port")
