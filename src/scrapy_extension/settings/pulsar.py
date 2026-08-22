# @author  : azwpayne(https://github.com/azwpayne)
# @name    : pulsar.py
# @time    : 2026/6/19
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Pulsar settings (subsystem ③ — new backend)
from __future__ import annotations

import re
from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._endpoint_validation import parse_host_port_authority
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings._transport_security import (
    is_loopback_host,
    normalize_allow_remote_plaintext,
    require_remote_plaintext_opt_in,
    validate_allow_remote_plaintext,
)

_VALID_PULSAR_SCHEMES: tuple[str, ...] = ("pulsar://", "pulsar+ssl://")
_PULSAR_MAX_SUBSCRIPTION_NAME_CHARS = 255
_PULSAR_SUBSCRIPTION_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_=:.\-]+$")


def validate_pulsar_subscription_name(value: object) -> str:
    """Validate a Pulsar subscription name before consumer creation."""
    if (
        type(value) is not str
        or not value
        or len(value) > _PULSAR_MAX_SUBSCRIPTION_NAME_CHARS
        or _PULSAR_SUBSCRIPTION_NAME_PATTERN.fullmatch(value) is None
    ):
        raise ConfigurationError(
            "Pulsar subscription_name must use 1-255 ASCII name characters.",
            setting_name="subscription_name",
        )
    return value


def _auth_token_value(value: object) -> str | None:
    """Extract a non-empty Pulsar token without retaining invalid input."""
    if value is None:
        return None
    if type(value) is SecretStr:
        token = value.get_secret_value()
    elif type(value) is str:
        token = value
    else:
        raise ConfigurationError(
            "auth_token must be a string when explicitly configured.",
            setting_name="auth_token",
        )
    if not token.strip():
        raise ConfigurationError(
            "auth_token must be non-empty when explicitly configured.",
            setting_name="auth_token",
        )
    return token


def validate_pulsar_connection(
    service_url: object,
    auth_token: object,
    tls_trust_certs_file: object,
    allow_insecure_connection: object,
    tls_validate_hostname: object,
    allow_remote_plaintext: object = False,
) -> tuple[str, str | None, str | None, bool, bool]:
    """Validate and normalize one coherent Pulsar connection value set.

    Endpoint authorities are parsed without DNS resolution. The returned token
    is raw for SDK use; callers that retain it must wrap it in the backend's
    repr-redacting string type.
    """
    if type(service_url) is not str:
        raise ConfigurationError(
            "service_url must be a string.", setting_name="service_url"
        )
    lowered = service_url.lower()
    scheme = next(
        (
            candidate
            for candidate in _VALID_PULSAR_SCHEMES
            if lowered.startswith(candidate)
        ),
        None,
    )
    if scheme is None:
        raise ConfigurationError(
            "service_url must start with 'pulsar://' or 'pulsar+ssl://'.",
            setting_name="service_url",
        )
    endpoint_text = service_url[len(scheme) :]
    if not endpoint_text or "://" in endpoint_text:
        raise ConfigurationError(
            "Pulsar cluster service_url must use a single scheme followed by a "
            "comma-separated endpoint list.",
            setting_name="service_url",
        )
    endpoints = tuple(endpoint_text.split(","))
    if not endpoints or any(not endpoint for endpoint in endpoints):
        raise ConfigurationError(
            "service_url must contain one or more non-empty Pulsar endpoints.",
            setting_name="service_url",
        )
    endpoint_hosts: list[str] = []
    normalized_endpoints: list[str] = []
    for endpoint in endpoints:
        parsed_endpoint = parse_host_port_authority(endpoint)
        if parsed_endpoint is None:
            raise ConfigurationError(
                "Each Pulsar endpoint must be a host with an optional valid port.",
                setting_name="service_url",
            )
        host, port = parsed_endpoint
        endpoint_hosts.append(host)
        endpoint_text_value = f"[{host}]" if ":" in host else host
        if port is not None:
            endpoint_text_value = f"{endpoint_text_value}:{port}"
        normalized_endpoints.append(endpoint_text_value)

    if tls_trust_certs_file is not None and (
        type(tls_trust_certs_file) is not str or not tls_trust_certs_file.strip()
    ):
        raise ConfigurationError(
            "tls_trust_certs_file must be a non-empty path when configured.",
            setting_name="tls_trust_certs_file",
        )
    if not isinstance(allow_insecure_connection, bool):
        raise ConfigurationError(
            "allow_insecure_connection must be a boolean.",
            setting_name="allow_insecure_connection",
        )
    if not isinstance(tls_validate_hostname, bool):
        raise ConfigurationError(
            "tls_validate_hostname must be a boolean.",
            setting_name="tls_validate_hostname",
        )
    normalized_allow_remote_plaintext = validate_allow_remote_plaintext(
        allow_remote_plaintext
    )

    token = _auth_token_value(auth_token)
    normalized_url = f"{scheme}{','.join(normalized_endpoints)}"
    endpoints_are_loopback = all(is_loopback_host(host) for host in endpoint_hosts)
    if scheme == "pulsar://":
        if tls_trust_certs_file is not None:
            raise ConfigurationError(
                "tls_trust_certs_file requires a pulsar+ssl:// service_url.",
                setting_name="tls_trust_certs_file",
            )
        if allow_insecure_connection:
            raise ConfigurationError(
                "allow_insecure_connection applies only to pulsar+ssl://.",
                setting_name="allow_insecure_connection",
            )
        if not tls_validate_hostname:
            raise ConfigurationError(
                "tls_validate_hostname applies only to pulsar+ssl:// and must remain true for plaintext.",
                setting_name="tls_validate_hostname",
            )
    elif not endpoints_are_loopback:
        if allow_insecure_connection:
            raise ConfigurationError(
                "Remote Pulsar TLS connections require certificate verification.",
                setting_name="allow_insecure_connection",
            )
        if not tls_validate_hostname:
            raise ConfigurationError(
                "Remote Pulsar TLS connections require hostname verification.",
                setting_name="tls_validate_hostname",
            )
    if token is not None:
        if scheme != "pulsar+ssl://":
            raise ConfigurationError(
                "Authenticated Pulsar connections require 'pulsar+ssl://' transport.",
                setting_name="service_url",
            )
        if allow_insecure_connection:
            raise ConfigurationError(
                "Authenticated Pulsar connections require certificate verification.",
                setting_name="allow_insecure_connection",
            )
        if not tls_validate_hostname:
            raise ConfigurationError(
                "Authenticated Pulsar connections require hostname verification.",
                setting_name="tls_validate_hostname",
            )
    elif scheme == "pulsar://" and not endpoints_are_loopback:
        require_remote_plaintext_opt_in("Pulsar", normalized_allow_remote_plaintext)
    return (
        normalized_url,
        token,
        tls_trust_certs_file,
        allow_insecure_connection,
        tls_validate_hostname,
    )


class PulsarMode(str, Enum):
    """Pulsar deployment modes.

    Pulsar encodes topology in ``service_url`` (single host vs comma-separated
    for a cluster), so these modes are informational; the connect path is shared.

    Attributes:
        STANDALONE: Single Pulsar broker (default).
        CLUSTER: Multi-broker Pulsar cluster (comma-separated service_url).
    """

    STANDALONE = "standalone"
    CLUSTER = "cluster"


class PulsarSettings(RedactedBaseSettings):
    """Pulsar-specific settings.

    Configurable via environment variables with the SCRAPY_PULSAR_ prefix.
    Pulsar has no native priority queue — ``priority`` on push is ignored.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_PULSAR_",
        case_sensitive=False,
        extra="forbid",
        hide_input_in_errors=True,
    )

    mode: PulsarMode = Field(
        default=PulsarMode.STANDALONE,
        description="Pulsar deployment mode (standalone, cluster)",
    )
    service_url: str = Field(
        default="pulsar://localhost:6650",
        description="Pulsar service URL (comma-separated for cluster)",
    )
    allow_remote_plaintext: bool = Field(
        default=False,
        description=(
            "Acknowledge an unauthenticated pulsar:// connection to a non-loopback "
            "broker on a trusted private network"
        ),
    )

    # === Consumer / work-queue settings ===
    subscription_name: str = Field(
        default="scrapy-extension",
        description="Shared subscription name (competing-consumers work queue)",
    )
    consumer_type: Literal["Shared", "Failover", "Exclusive", "Key_Shared"] = Field(
        default="Shared",
        description="Subscription type: Shared (work queue), Failover, Exclusive, Key_Shared",
    )
    initial_position: Literal["Earliest", "Latest"] = Field(
        default="Earliest",
        description="Subscription initial position: Earliest or Latest",
    )
    negative_ack_redelivery_delay_ms: int = Field(
        default=60000,
        ge=0,
        description="Redelivery delay after a negative ack (ms)",
    )

    # === Auth (optional) ===
    auth_token: SecretStr | None = Field(
        default=None,
        description="Authentication token (Pulsar AuthenticationToken)",
    )
    tls_trust_certs_file: str | None = Field(
        default=None,
        description="Path to TLS trust certs file (for pulsar+ssl://)",
    )
    allow_insecure_connection: bool = Field(
        default=False,
        description=(
            "Allow insecure TLS connections for unauthenticated development only"
        ),
    )
    tls_validate_hostname: bool = Field(
        default=True,
        description=(
            "Validate the broker hostname against its TLS certificate; disable only "
            "for unauthenticated local compatibility"
        ),
    )

    @field_validator("service_url", mode="before")
    @classmethod
    def _reject_service_url_subclasses(cls, value: object) -> str:
        """Reject URL subclasses before pydantic can erase their identity."""
        if type(value) is not str:
            raise ConfigurationError(
                "service_url must be a string.", setting_name="service_url"
            )
        return value

    @field_validator("allow_remote_plaintext", mode="before")
    @classmethod
    def _normalize_remote_plaintext_opt_in(cls, value: object) -> bool:
        """Accept canonical environment booleans but reject truthy lookalikes."""
        return normalize_allow_remote_plaintext(value)

    @field_validator("subscription_name", mode="before")
    @classmethod
    def _validate_subscription_name(cls, value: object) -> str:
        """Keep construction and connection-time subscription checks identical."""
        return validate_pulsar_subscription_name(value)

    @model_validator(mode="after")
    def _validate_connection(self) -> Self:
        """Validate URL grammar, credentials, and authenticated TLS policy.

        A bare ``host:port`` or ``http://`` value otherwise surfaces as an
        opaque ``ValueError`` inside the pulsar client at connect. The SDK treats
        the scheme as case-sensitive and expects a cluster as one scheme followed
        by comma-separated endpoints (``pulsar://one:6650,two:6650``). Normalize
        scheme case but reject whitespace and controls so every accepted value is
        directly consumable by the client.

        Raises:
            ConfigurationError: if ``service_url`` does not start with a valid
                Pulsar scheme.
        """
        (
            self.service_url,
            _token,
            _trust_file,
            _allow_insecure,
            _validate_hostname,
        ) = validate_pulsar_connection(
            self.service_url,
            self.auth_token,
            self.tls_trust_certs_file,
            self.allow_insecure_connection,
            self.tls_validate_hostname,
            self.allow_remote_plaintext,
        )
        return self
