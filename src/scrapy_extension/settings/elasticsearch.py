"""ElasticSearch settings for scrapy-extension.

This module provides pydantic-settings based configuration for
ElasticSearch backend connections.
"""

from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ElasticSearchMode(str, Enum):
  """ElasticSearch deployment modes.

  Attributes:
      STANDALONE: Single node or cluster via hosts list (default).
      CLOUD: Elastic Cloud with cloud_id + api_key.
  """

  STANDALONE = "standalone"
  CLOUD = "cloud"


class ElasticSearchSettings(BaseSettings):
  """ElasticSearch-specific settings.

  Supports two deployment modes:
  - standalone: Connect via hosts list (default)
  - cloud: Connect via Elastic Cloud cloud_id

  Attributes:
      mode: Deployment mode.
      hosts: List of ES host URLs (standalone).
      cloud_id: Elastic Cloud identifier (cloud).
      api_key: API key for authentication.
      username: Basic auth username.
      password: Basic auth password.
      verify_certs: Whether to verify SSL certificates.
      ca_certs: Path to CA certificate file.
      request_timeout: Request timeout in seconds.
      max_retries: Maximum retry attempts.
      retry_on_timeout: Whether to retry on timeout.
      queue_index: Index name for queue operations.
      set_index: Index name for set operations.
      storage_index: Index name for storage operations.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_ELASTICSEARCH_",
    case_sensitive=False,
    extra="ignore",
  )

  # === Mode Selection ===
  mode: ElasticSearchMode = Field(
    default=ElasticSearchMode.STANDALONE,
    description="Deployment mode (standalone, cloud)",
  )

  # === Standalone Settings ===
  hosts: list[str] = Field(
    default_factory=lambda: ["http://localhost:9200"],
    description="List of ElasticSearch host URLs",
  )

  # === Cloud Settings ===
  cloud_id: str | None = Field(
    default=None,
    description="Elastic Cloud identifier",
  )

  # === Authentication ===
  api_key: SecretStr | None = Field(
    default=None,
    description="API key for authentication",
  )
  username: str | None = Field(
    default=None,
    description="Basic auth username",
  )
  password: SecretStr | None = Field(
    default=None,
    description="Basic auth password",
  )

  # === SSL Settings ===
  verify_certs: bool = Field(
    default=True,
    description="Verify SSL certificates",
  )
  ca_certs: str | None = Field(
    default=None,
    description="Path to CA certificate file",
  )

  # === Connection Settings ===
  request_timeout: float = Field(
    default=30.0,
    ge=0,
    description="Request timeout in seconds",
  )
  max_retries: int = Field(
    default=3,
    ge=0,
    description="Maximum retry attempts",
  )
  retry_on_timeout: bool = Field(
    default=True,
    description="Retry on timeout",
  )

  # === Index Names ===
  queue_index: str = Field(
    default="scrapy_queue",
    description="Index name for queue operations",
  )
  set_index: str = Field(
    default="scrapy_set",
    description="Index name for set operations",
  )
  storage_index: str = Field(
    default="scrapy_storage",
    description="Index name for storage operations",
  )

  @model_validator(mode="after")
  def validate_mode_requirements(self) -> ElasticSearchSettings:
    """Fail-fast: CLOUD mode requires ``cloud_id``.

    Mirrors the Redis SENTINEL validator (R8). Without this, the error
    surfaced at ``connect()`` time (BackendConnectionError) rather than at
    construction — far from the misconfiguration. Verified against
    ``connect()`` (which already rejects CLOUD-without-cloud_id), so this
    only moves the failure earlier; no valid configuration is newly
    rejected. ``api_key`` is intentionally NOT required — CLOUD can
    authenticate via basic_auth too (per ``_build_kwargs``).

    Raises:
        ValueError: If CLOUD mode is selected without ``cloud_id``.
    """
    if self.mode == ElasticSearchMode.CLOUD and not self.cloud_id:
      msg = (
        "ElasticSearch CLOUD mode requires 'cloud_id' to be set. "
        f"Got cloud_id={self.cloud_id!r}."
      )
      raise ValueError(msg)
    return self
