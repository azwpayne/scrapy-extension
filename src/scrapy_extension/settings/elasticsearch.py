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
    extra="forbid",
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
    le=86400,
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
    # R28-B: STANDALONE targets the cluster via ``hosts``; an empty list
    # (e.g. ``SCRAPY_ELASTICSEARCH_HOSTS=`` set to an empty value) otherwise
    # surfaces as an opaque elasticsearch-py client error at connect().
    # CLOUD mode is unaffected — it uses ``cloud_id``, not hosts.
    if self.mode == ElasticSearchMode.STANDALONE and not self.hosts:
      raise ConfigurationError(
        (
          "STANDALONE mode requires at least one 'hosts' entry "
          "(e.g. http://host:9200 or https://host:9200). Got hosts=[]. "
          "CLOUD mode uses 'cloud_id' and does not require hosts."
        ),
        setting_name="hosts",
        setting_value=self.hosts,
      )
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
    """Fail-fast: CLOUD mode requires ``cloud_id`` AND an auth method.

    Mirrors the Redis SENTINEL validator (R8). Without this, the error
    surfaced at ``connect()`` time (BackendConnectionError) rather than at
    construction — far from the misconfiguration. Verified against
    ``connect()`` (which already rejects CLOUD-without-cloud_id), so this
    only moves the failure earlier; no valid configuration is newly
    rejected.

    R26-F: Elastic Cloud always 401s an anonymous client, so CLOUD mode also
    requires at least one auth method — ``api_key`` OR basic auth
    (``username`` + ``password``). Pre-R26-F a no-auth CLOUD config surfaced
    as an opaque ``BackendConnectionError('health check returned false')``
    (ping returns false on 401); now it fails fast at construction.

    Raises:
        ValueError: If CLOUD mode is selected without ``cloud_id`` or without
            any auth method.
    """
    if self.mode == ElasticSearchMode.CLOUD:
      if not self.cloud_id:
        msg = (
          "ElasticSearch CLOUD mode requires 'cloud_id' to be set. "
          f"Got cloud_id={self.cloud_id!r}."
        )
        raise ValueError(msg)
      # R27-A: truthiness (not ``is not None``) so an empty-string secret —
      # e.g. ``SCRAPY_ELASTICSEARCH_API_KEY=""`` (env var set but unpopulated)
      # — is treated as absent, matching ``_build_kwargs`` (which uses
      # ``if self.config.api_key:``). Pre-R27-A an empty secret passed this
      # check but was dropped at build time → anonymous client → 401.
      has_api_key = bool(self.api_key)
      has_basic_auth = bool(self.username) and bool(self.password)
      if not (has_api_key or has_basic_auth):
        msg = (
          "ElasticSearch CLOUD mode requires an auth method: set 'api_key' "
          "or both 'username' and 'password'. Elastic Cloud always rejects "
          "an anonymous client (401), so a no-auth config would surface as "
          "an opaque health-check failure at connect() rather than here."
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
    # Cloud connections use ``cloud_id`` and never pass ``hosts`` to the
    # Elasticsearch client. The standalone localhost default is therefore not
    # a transport target in CLOUD mode and must not trigger this guard.
    if self.mode == ElasticSearchMode.CLOUD:
      return self

    # R27-A: truthiness mirrors the CLOUD auth check above so an empty
    # ``api_key``/``password`` is not treated as a credential present (which
    # would false-positive the cleartext guard on a permitted no-auth http dev
    # node — the validator's own docstring allows ``http://`` with no creds).
    has_credential = bool(self.api_key) or bool(self.password)
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
    # R28-A: truthiness (not ``is not None``) mirrors R27-A's two other ES
    # validators so an empty-string secret (env var set but unpopulated) is
    # treated as absent. Pre-R28-A an empty ``api_key`` + basic_auth was
    # falsely rejected as "mutually exclusive" even though ``_build_kwargs``
    # drops the empty key and uses basic_auth. Completes R27-A's stated
    # "all three sites" intent (this is the third validator).
    if not self.api_key:
      return self
    if bool(self.username) or bool(self.password):
      raise ConfigurationError(
        (
          "api_key and basic-auth (username/password) are mutually "
          "exclusive — when both are set, api_key is used and basic_auth "
          "is silently dropped (auth-method ambiguity). Remove one "
          "authentication method. "
          f"Got api_key={'<set>' if self.api_key else None}, "
          f"username={self.username!r}, password="
          f"{'<set>' if self.password else None}."
        ),
        setting_name="api_key",
        setting_value=self.api_key,
      )
    return self
