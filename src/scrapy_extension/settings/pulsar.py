# @author  : azwpayne(https://github.com/azwpayne)
# @name    : pulsar.py
# @time    : 2026/6/19
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Pulsar settings (subsystem ③ — new backend)
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
  consumer_type: str = Field(
    default="Shared",
    description="Subscription type: Shared (work queue), Failover, Exclusive, Key_Shared",
  )
  initial_position: str = Field(
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
