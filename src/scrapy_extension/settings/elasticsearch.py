"""ElasticSearch settings for scrapy-extension.

This module provides pydantic-settings based configuration for
ElasticSearch backend connections.
"""

from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError

_VALID_ES_SCHEMES: tuple[str, ...] = ("http://", "https://")


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
  def _validate_hosts_scheme(self) -> Self:
    """SV4: every ``hosts`` entry must start with ``http://`` or ``https://``.

    SEC-3 (round 6) guards ``http://`` + credentials (cleartext leak); this
    validator guards the scheme itself for the no-creds case. A bare
    ``localhost:9200`` or ``es-cluster`` otherwise surfaces as an opaque
    transport error inside the elasticsearch-py client (it does not infer a
    default scheme). Empty strings are rejected.

    Raises:
        ConfigurationError: if any host entry lacks a valid scheme.
    """
    bad = [
      host
      for host in self.hosts
      if not host or not host.lower().startswith(_VALID_ES_SCHEMES)
    ]
    if bad:
      raise ConfigurationError(
        (
          "each hosts entry must start with 'http://' or 'https://'. "
          f"Got invalid entries={bad!r} (full hosts={self.hosts!r})."
        ),
        setting_name="hosts",
        setting_value=self.hosts,
      )
    return self

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

  @model_validator(mode="after")
  def _validate_no_cleartext_credentials(self) -> Self:
    """SEC-3: forbid credentials over ``http://`` (cleartext).

    Sending ``api_key`` or ``password`` over a plaintext ``http://`` host
    leaks them on the wire. Reject at config time (fail-fast) rather than
    silently shipping an insecure transport. ``https://`` + creds is fine;
    ``http://`` with no creds is fine (e.g. a no-auth local dev node).

    Mirrors the RabbitMQ guest-guard pattern (raise, not warn).

    Raises:
        ConfigurationError: if any host URL scheme is ``http://`` and either
            ``api_key`` or ``password`` is set.
    """
    has_credential = self.api_key is not None or self.password is not None
    if not has_credential:
      return self
    has_http_host = any(
      host.lower().startswith("http://") for host in self.hosts
    )
    if has_http_host:
      raise ConfigurationError(
        (
          "Credentials over http:// (cleartext) are not permitted; use "
          "https:// for any authenticated host or remove the credentials. "
          f"Got hosts={self.hosts!r} with api_key/password set."
        ),
        setting_name="hosts",
      )
    return self

  @model_validator(mode="after")
  def _validate_auth_method_exclusivity(self) -> Self:
    """SV3-5 (L-M): ``api_key`` and (``username``, ``password``) are mutually exclusive.

    ``_build_kwargs`` prefers ``api_key`` when set and silently drops
    ``basic_auth``. An operator who configures both believes basic_auth is
    enforced while it never reaches the cluster — a silent auth-bypass
    footgun. Fail-fast at config time; require the operator to pick one
    method.

    Verified safe: no existing repo fixture sets both (all ``api_key``
    fixtures omit ``username``; all ``basic_auth`` fixtures omit ``api_key``).

    Raises:
        ConfigurationError: if ``api_key`` is set and either ``username`` or
            ``password`` is also set.
    """
    if self.api_key is None:
      return self
    if self.username is not None or self.password is not None:
      raise ConfigurationError(
        (
          "api_key and basic-auth (username/password) are mutually "
          "exclusive — when both are set, api_key is used and basic_auth "
          "is silently dropped (auth-method ambiguity). Remove one "
          "authentication method. "
          f"Got api_key={'<set>' if self.api_key is not None else None}, "
          f"username={self.username!r}, password="
          f"{'<set>' if self.password is not None else None}."
        ),
        setting_name="api_key",
        setting_value=self.api_key,
      )
    return self
