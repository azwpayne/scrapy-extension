# @author  : azwpayne(https://github.com/azwpayne)
# @name    : memcached.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Memcached settings (subsystem ③ — new NoSQL backend)
from __future__ import annotations

from enum import Enum
from ipaddress import ip_address

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError


class MemcachedMode(str, Enum):
  """Memcached deployment modes.

  Attributes:
      STANDALONE: Single memcached instance (default).
  """

  STANDALONE = "standalone"


def normalize_memcached_host(host: object) -> str:
  """Return a bare Memcached host without retaining malformed input."""
  if not isinstance(host, str):
    raise ConfigurationError(
      "Memcached host must be a non-empty hostname or IP address.",
      setting_name="host",
    )
  normalized = host.strip()
  if normalized.startswith("[") and normalized.endswith("]"):
    normalized = normalized[1:-1]
  if not normalized or any(char in normalized for char in "/@?#"):
    raise ConfigurationError(
      "Memcached host must be a bare hostname or IP address.",
      setting_name="host",
    )
  try:
    ip_address(normalized)
  except ValueError:
    if ":" in normalized:
      raise ConfigurationError(
        "Memcached host must not include a port or URL scheme.",
        setting_name="host",
      ) from None
  return normalized


def is_memcached_loopback(host: str) -> bool:
  """Return whether ``host`` is confined to the local machine."""
  normalized = normalize_memcached_host(host).lower().rstrip(".")
  if normalized == "localhost" or normalized.endswith(".localhost"):
    return True
  try:
    return ip_address(normalized).is_loopback
  except ValueError:
    return False


def validate_memcached_connection(
  mode: object,
  host: object,
  port: object,
  allow_remote_plaintext: object,
) -> tuple[MemcachedMode, str, int, bool]:
  """Validate one complete Memcached connection snapshot."""
  if mode is not MemcachedMode.STANDALONE:
    try:
      mode_text = str(mode)
    except (TypeError, ValueError):
      mode_text = getattr(mode, "value", repr(mode))
    raise ConfigurationError(
      f"Unsupported Memcached mode: {mode_text}",
      setting_name="mode",
      setting_value=mode,
    )
  normalized_host = normalize_memcached_host(host)
  if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
    raise ConfigurationError(
      "Memcached port must be between 1 and 65535.", setting_name="port"
    )
  if not isinstance(allow_remote_plaintext, bool):
    raise ConfigurationError(
      "allow_remote_plaintext must be a boolean.",
      setting_name="allow_remote_plaintext",
    )
  if not is_memcached_loopback(normalized_host) and not allow_remote_plaintext:
    raise ConfigurationError(
      "Remote Memcached uses an unauthenticated plaintext protocol. Set "
      "allow_remote_plaintext=True only for an explicitly trusted private "
      "network.",
      setting_name="allow_remote_plaintext",
    )
  return mode, normalized_host, port, allow_remote_plaintext


def validate_memcached_flush_policy(allow_flush_all: object) -> bool:
  """Require an explicit boolean for the destructive server-wide permission."""
  if not isinstance(allow_flush_all, bool):
    raise ConfigurationError(
      "allow_flush_all must be a boolean.",
      setting_name="allow_flush_all",
    )
  return allow_flush_all


def normalize_memcached_flush_setting(allow_flush_all: object) -> bool:
  """Parse canonical environment booleans without accepting broad coercions."""
  if isinstance(allow_flush_all, str):
    normalized = allow_flush_all.strip().lower()
    if normalized == "true":
      return True
    if normalized == "false":
      return False
  return validate_memcached_flush_policy(allow_flush_all)


class MemcachedSettings(BaseSettings):
  """Memcached-specific settings (key-value cache / NoSQL).

  Configurable via environment variables with the SCRAPY_MEMCACHED_ prefix.
  Memcached is used only for StorageBackend (KV with TTL); it has no native
  ordered queue or set.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_MEMCACHED_", case_sensitive=False, extra="forbid"
  )

  mode: MemcachedMode = Field(
    default=MemcachedMode.STANDALONE,
    description="Memcached deployment mode (standalone)",
  )
  host: str = Field(
    default="localhost",
    min_length=1,
    description="Memcached host",
  )
  port: int = Field(
    default=11211,
    ge=1,
    le=65535,
    description="Memcached port",
  )
  allow_remote_plaintext: bool = Field(
    default=False,
    description=(
      "Allow an unauthenticated plaintext connection to a non-loopback host "
      "only when the network boundary is explicitly trusted"
    ),
  )
  allow_flush_all: bool = Field(
    default=False,
    description=(
      "Permit clear_storage(None) to issue server-wide flush_all. Disabled by "
      "default because Memcached cannot scope deletion to this application."
    ),
  )

  @field_validator("allow_flush_all", mode="before")
  @classmethod
  def _validate_flush_permission_type(cls, value: object) -> bool:
    """Prevent permissive bool coercion for a destructive capability."""
    return normalize_memcached_flush_setting(value)

  @model_validator(mode="after")
  def _validate_connection(self) -> Self:
    """Normalize the host and enforce the trusted-network boundary."""
    _mode, self.host, _port, _allow_remote = validate_memcached_connection(
      self.mode,
      self.host,
      self.port,
      self.allow_remote_plaintext,
    )
    validate_memcached_flush_policy(self.allow_flush_all)
    return self
