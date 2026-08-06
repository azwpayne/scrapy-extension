"""ElasticSearch settings for scrapy-extension.

This module provides pydantic-settings based configuration for
ElasticSearch backend connections.
"""

from __future__ import annotations

from enum import Enum
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings._transport_security import (
    is_loopback_host,
    validate_allow_remote_plaintext,
    warn_remote_unauthenticated_plaintext,
)

_VALID_ES_SCHEMES: frozenset[str] = frozenset({"http", "https"})


class ElasticSearchMode(str, Enum):
    """ElasticSearch deployment modes.

    Attributes:
        STANDALONE: Single node or cluster via hosts list (default).
        CLOUD: Elastic Cloud with cloud_id + api_key.
    """

    STANDALONE = "standalone"
    CLOUD = "cloud"


class ElasticSearchSettings(RedactedBaseSettings):
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
        hide_input_in_errors=True,
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
    allow_remote_plaintext: bool = Field(
        default=False,
        description=(
            "Acknowledge an unauthenticated http:// connection to a non-loopback "
            "host on a trusted private network"
        ),
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
        """Validate structurally safe standalone Elasticsearch endpoints.

        A bare ``localhost:9200`` otherwise surfaces as an opaque transport error
        inside elasticsearch-py. URL userinfo, query strings, and fragments can
        carry secrets that would then reach driver errors or logs, so settings must
        use the dedicated authentication fields instead.

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
            )
        for host in self.hosts:
            if not isinstance(host, str) or not host:
                raise ConfigurationError(
                    "each hosts entry must be a non-empty http:// or https:// endpoint.",
                    setting_name="hosts",
                )
            # Check raw input before urlsplit(), which deliberately normalizes some
            # controls such as newlines. Do not include the host in an error because
            # URL userinfo/query strings may contain credentials.
            if any(char.isspace() or ord(char) < 32 for char in host):
                raise ConfigurationError(
                    "hosts entries must not contain whitespace or control characters.",
                    setting_name="hosts",
                )
            invalid_authority = False
            try:
                parsed = urlsplit(host)
                # Accessing .port validates the numeric port, including out-of-range
                # values, without imposing a port on deployments that use a proxy.
                _ = parsed.port
            except ValueError:
                invalid_authority = True
            if invalid_authority:
                raise ConfigurationError(
                    "each hosts entry must contain a valid network authority.",
                    setting_name="hosts",
                )
            if (
                parsed.scheme.lower() not in _VALID_ES_SCHEMES
                or not parsed.netloc
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ConfigurationError(
                    "each hosts entry must be an http:// or https:// endpoint without "
                    "userinfo, query, or fragment.",
                    setting_name="hosts",
                )
        return self

    @model_validator(mode="after")
    def _validate_auth_completeness(self) -> Self:
        """Reject blank, partial, or ambiguous authentication before client setup."""
        api_key = self.api_key.get_secret_value() if self.api_key is not None else None
        password = (
            self.password.get_secret_value() if self.password is not None else None
        )
        if api_key is not None and not api_key.strip():
            raise ConfigurationError(
                "api_key must not be blank when supplied.", setting_name="api_key"
            )
        if self.username is not None and not self.username.strip():
            raise ConfigurationError(
                "username must not be blank when supplied.", setting_name="username"
            )
        if password is not None and not password.strip():
            raise ConfigurationError(
                "password must not be blank when supplied.", setting_name="password"
            )

        has_api_key = api_key is not None
        has_username = self.username is not None
        has_password = password is not None
        if has_api_key and (has_username or has_password):
            raise ConfigurationError(
                "api_key and basic-auth (username/password) are mutually exclusive; "
                "remove one authentication method.",
                setting_name="api_key",
            )
        if has_username != has_password:
            missing = "password" if has_username else "username"
            raise ConfigurationError(
                f"basic authentication requires both username and password; set '{missing}'.",
                setting_name=missing,
            )
        return self

    @model_validator(mode="after")
    def _validate_capability_indices(self) -> Self:
        """Keep destructive queue, set, and storage operations physically isolated."""
        indices = {
            "queue_index": self.queue_index,
            "set_index": self.set_index,
            "storage_index": self.storage_index,
        }
        for name, value in indices.items():
            if not value.strip():
                raise ConfigurationError(
                    f"{name} must not be blank.", setting_name=name
                )
        if len(set(indices.values())) != len(indices):
            raise ConfigurationError(
                "queue_index, set_index, and storage_index must be pairwise distinct "
                "so a capability clear cannot delete another capability's data.",
                setting_name="queue_index",
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
            has_api_key = self.api_key is not None
            has_basic_auth = self.username is not None and self.password is not None
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

        has_credential = self.api_key is not None or self.password is not None
        if not has_credential:
            return self
        has_http_host = any(host.lower().startswith("http://") for host in self.hosts)
        if has_http_host:
            raise ConfigurationError(
                (
                    "Credentials over http:// (cleartext) are not permitted; use "
                    "https:// for any authenticated host or remove the credentials."
                ),
                setting_name="hosts",
            )
        return self

    @model_validator(mode="after")
    def _validate_authenticated_tls(self) -> Self:
        """Require certificate verification whenever standalone auth is enabled."""
        if (
            self.mode == ElasticSearchMode.STANDALONE
            and not self.verify_certs
            and (self.api_key is not None or self.username is not None)
        ):
            raise ConfigurationError(
                "verify_certs must be enabled for authenticated standalone connections.",
                setting_name="verify_certs",
            )
        return self

    @model_validator(mode="after")
    def _warn_remote_unauthenticated_plaintext(self) -> Self:
        """Keep remote unauthenticated HTTP compatible while making it explicit."""
        allow_remote_plaintext = validate_allow_remote_plaintext(
            self.allow_remote_plaintext
        )
        has_credential = any(
            value is not None for value in (self.api_key, self.username, self.password)
        )
        has_remote_plaintext_host = any(
            urlsplit(host).scheme.lower() == "http"
            and not is_loopback_host(urlsplit(host).hostname)
            for host in self.hosts
        )
        if (
            self.mode is ElasticSearchMode.STANDALONE
            and not has_credential
            and has_remote_plaintext_host
        ):
            warn_remote_unauthenticated_plaintext(
                "Elasticsearch", allow_remote_plaintext
            )
        return self
