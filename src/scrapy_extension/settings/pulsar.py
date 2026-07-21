# @author  : azwpayne(https://github.com/azwpayne)
# @name    : pulsar.py
# @time    : 2026/6/19
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Pulsar settings (subsystem ③ — new backend)
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError

_VALID_PULSAR_SCHEMES: tuple[str, ...] = ("pulsar://", "pulsar+ssl://")


def _auth_token_value(value: object) -> str | None:
  """Extract a non-empty Pulsar token without retaining invalid input."""
  if value is None:
    return None
  if isinstance(value, SecretStr):
    token = value.get_secret_value()
  elif isinstance(value, str):
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
) -> tuple[str, str | None, str | None, bool, bool]:
  """Validate and normalize one coherent Pulsar connection value set.

  The returned token is raw for SDK use; callers that retain it must wrap it
  in the backend's repr-redacting string type. Invalid URLs and credentials are
  never attached to the raised exception.
  """
  if not isinstance(service_url, str):
    raise ConfigurationError(
      "service_url must be a string.", setting_name="service_url"
    )
  url = service_url.strip()
  lowered = url.lower()
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
  endpoint_text = url[len(scheme) :]
  if "://" in endpoint_text:
    raise ConfigurationError(
      "Pulsar cluster service_url must use a single scheme followed by a "
      "comma-separated endpoint list.",
      setting_name="service_url",
    )
  endpoints = tuple(endpoint.strip() for endpoint in endpoint_text.split(","))
  if not endpoints or any(not endpoint for endpoint in endpoints):
    raise ConfigurationError(
      "service_url must contain one or more non-empty Pulsar endpoints.",
      setting_name="service_url",
    )
  if any("@" in endpoint for endpoint in endpoints):
    raise ConfigurationError(
      "Pulsar service_url must not contain URL userinfo; configure "
      "auth_token separately.",
      setting_name="service_url",
    )

  if tls_trust_certs_file is not None and (
    not isinstance(tls_trust_certs_file, str)
    or not tls_trust_certs_file.strip()
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

  token = _auth_token_value(auth_token)
  normalized_url = f"{scheme}{','.join(endpoints)}"
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


class PulsarSettings(BaseSettings):
  """Pulsar-specific settings.

  Configurable via environment variables with the SCRAPY_PULSAR_ prefix.
  Pulsar has no native priority queue — ``priority`` on push is ignored.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_PULSAR_", case_sensitive=False, extra="forbid"
  )

  mode: PulsarMode = Field(
    default=PulsarMode.STANDALONE,
    description="Pulsar deployment mode (standalone, cluster)",
  )
  service_url: str = Field(
    default="pulsar://localhost:6650",
    description="Pulsar service URL (comma-separated for cluster)",
  )

  # === Consumer / work-queue settings ===
  subscription_name: str = Field(
    default="scrapy-extension",
    description="Shared subscription name (competing-consumers work queue)",
  )
  consumer_type: Literal["Shared", "Failover", "Exclusive", "Key_Shared"] = (
    Field(
      default="Shared",
      description="Subscription type: Shared (work queue), Failover, Exclusive, Key_Shared",
    )
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

  @model_validator(mode="after")
  def _validate_connection(self) -> Self:
    """Validate URL grammar, credentials, and authenticated TLS policy.

    A bare ``host:port`` or ``http://`` value otherwise surfaces as an
    opaque ``ValueError`` inside the pulsar client at connect. The SDK treats
    the scheme as case-sensitive and expects a cluster as one scheme followed
    by comma-separated endpoints (``pulsar://one:6650,two:6650``). Normalize
    scheme case and surrounding endpoint whitespace here so every accepted
    value is directly consumable by the client.

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
    )
    return self
