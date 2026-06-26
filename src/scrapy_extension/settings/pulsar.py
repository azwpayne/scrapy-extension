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
    env_prefix="SCRAPY_PULSAR_", case_sensitive=False, extra="ignore"
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
    description="Allow insecure TLS connections (dev only)",
  )

  @model_validator(mode="after")
  def _validate_service_url_scheme(self) -> Self:
    """SV4: ``service_url`` must start with ``pulsar://`` or ``pulsar+ssl://``.

    A bare ``host:port`` or ``http://`` value otherwise surfaces as an
    opaque ``ValueError`` inside the pulsar client at connect. For a cluster,
    each comma-separated entry must carry a valid scheme (checked on the
    first segment for the common single-URL case; cluster URLs follow the
    same ``pulsar://`` scheme).

    Raises:
        ConfigurationError: if ``service_url`` does not start with a valid
            Pulsar scheme.
    """
    url = self.service_url.strip()
    if not url or not url.lower().startswith(_VALID_PULSAR_SCHEMES):
      raise ConfigurationError(
        (
          "service_url must start with 'pulsar://' or 'pulsar+ssl://'. "
          f"Got service_url={self.service_url!r}."
        ),
        setting_name="service_url",
        setting_value=self.service_url,
      )
    return self

  @model_validator(mode="after")
  def _validate_auth_token_requires_ssl(self) -> Self:
    """SV3-2 (H): ``auth_token`` set → ``service_url`` must be ``pulsar+ssl://``.

    Pulsar's ``AuthenticationToken`` is transmitted on every broker
    connection. Without TLS (``pulsar://`` rather than ``pulsar+ssl://``),
    the token traverses the wire in cleartext — a credential-leak footgun.
    This mirrors the Redis ``ssl_enabled``→``ssl_cafile`` and Kafka
    SASL→``security_protocol`` raise-on-incoherence pattern.

    Raises:
        ConfigurationError: if ``auth_token`` is set and ``service_url``
            does not start with ``pulsar+ssl://``.
    """
    if self.auth_token is None:
      return self
    if not self.service_url.lower().startswith("pulsar+ssl://"):
      raise ConfigurationError(
        (
          "auth_token is set but service_url is not 'pulsar+ssl://' "
          f"(got service_url={self.service_url!r}). The token would be "
          "sent in cleartext over a non-TLS connection. Use "
          "'pulsar+ssl://' to protect the token on the wire."
        ),
        setting_name="service_url",
        setting_value=self.service_url,
      )
    return self
