# @author  : azwpayne(https://github.com/azwpayne)
# @name    : redis.py
# @time    : 2026/3/18 20:38 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from enum import Enum
from ipaddress import IPv6Address, ip_address
from typing import Annotated, Any

from pydantic import Field, SecretStr, ValidationInfo, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from scrapy_extension.exceptions.base import ConfigurationError


class RedisMode(str, Enum):
  """Redis deployment modes.

  Attributes:
      STANDALONE: Single Redis instance (default).
      MASTER_SLAVE: Deprecated primary-only compatibility alias.
      SENTINEL: High availability with automatic failover.
      CLUSTER: Redis Cluster with automatic sharding.
  """

  STANDALONE = "standalone"
  MASTER_SLAVE = "master_slave"
  SENTINEL = "sentinel"
  CLUSTER = "cluster"


_DNS_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
_ASCII_PORT = re.compile(r"^[0-9]{1,5}$")
_REDIS_ENDPOINT_FORBIDDEN = frozenset("@/?#\\%")


def _redis_address_error(setting_name: str, *, index: int | None = None) -> ConfigurationError:
  """Build one value-free Redis address error."""
  suffix = "" if index is None else f" at index {index}"
  return ConfigurationError(
    f"Redis setting '{setting_name}' contains an invalid address{suffix}.",
    setting_name=setting_name,
  )


def _parsed_ip(value: str) -> Any | None:
  """Parse an IP address without retaining its ValueError as exception context."""
  try:
    return ip_address(value)
  except ValueError:
    return None


def normalize_redis_host(host: object, *, setting_name: str = "host") -> str:
  """Return a canonical bare DNS/IP host without retaining malformed input."""
  if not isinstance(host, str) or not host or not host.isascii():
    raise _redis_address_error(setting_name)
  if (
    any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in host)
    or any(char in _REDIS_ENDPOINT_FORBIDDEN for char in host)
  ):
    raise _redis_address_error(setting_name)

  bracketed = host.startswith("[") or host.endswith("]")
  candidate = host
  if bracketed:
    if not (host.startswith("[") and host.endswith("]")):
      raise _redis_address_error(setting_name)
    candidate = host[1:-1]

  parsed = _parsed_ip(candidate)
  if parsed is not None:
    if bracketed and not isinstance(parsed, IPv6Address):
      raise _redis_address_error(setting_name)
    return str(parsed)
  if bracketed or ":" in candidate:
    raise _redis_address_error(setting_name)

  dns_name = candidate[:-1] if candidate.endswith(".") else candidate
  labels = dns_name.split(".")
  legacy_numeric_labels = all(
    label.isdigit() or re.fullmatch(r"0[xX][0-9A-Fa-f]+", label)
    for label in labels
  )
  if (
    not dns_name
    or len(dns_name) > 253
    or legacy_numeric_labels
    or any(not _DNS_LABEL.fullmatch(label) for label in labels)
  ):
    raise _redis_address_error(setting_name)
  return candidate


def normalize_redis_port(
  port: object,
  *,
  setting_name: str = "port",
  index: int | None = None,
) -> int:
  """Return a strict Redis TCP port without retaining malformed input."""
  if isinstance(port, bool):
    raise _redis_address_error(setting_name, index=index)
  if isinstance(port, int):
    normalized = port
  elif isinstance(port, str) and _ASCII_PORT.fullmatch(port):
    normalized = int(port)
  else:
    raise _redis_address_error(setting_name, index=index)
  if not 1 <= normalized <= 65535:
    raise _redis_address_error(setting_name, index=index)
  return normalized


def parse_redis_endpoint(
  endpoint: object,
  *,
  setting_name: str,
  index: int | None = None,
) -> tuple[str, int]:
  """Parse strict ``host:port`` or ``[IPv6]:port`` endpoint syntax."""
  if not isinstance(endpoint, str) or not endpoint or not endpoint.isascii():
    raise _redis_address_error(setting_name, index=index)
  if (
    any(
      char.isspace() or ord(char) < 32 or ord(char) == 127
      for char in endpoint
    )
    or any(char in _REDIS_ENDPOINT_FORBIDDEN for char in endpoint)
  ):
    raise _redis_address_error(setting_name, index=index)

  if endpoint.startswith("["):
    closing = endpoint.find("]")
    if closing <= 1 or endpoint[closing + 1 : closing + 2] != ":":
      raise _redis_address_error(setting_name, index=index)
    if "]" in endpoint[closing + 1 :]:
      raise _redis_address_error(setting_name, index=index)
    host_text = endpoint[1:closing]
    port_text = endpoint[closing + 2 :]
    parsed = _parsed_ip(host_text)
    if not isinstance(parsed, IPv6Address):
      raise _redis_address_error(setting_name, index=index)
    host = str(parsed)
  else:
    if endpoint.count(":") != 1:
      raise _redis_address_error(setting_name, index=index)
    host_text, port_text = endpoint.split(":", 1)
    host = normalize_redis_host(host_text, setting_name=setting_name)

  port = normalize_redis_port(
    port_text,
    setting_name=setting_name,
    index=index,
  )
  return host, port


def format_redis_endpoint(host: str, port: int) -> str:
  """Format one already-normalized Redis endpoint."""
  rendered_host = f"[{host}]" if ":" in host else host
  return f"{rendered_host}:{port}"


def validate_redis_tls(
  ssl_enabled: bool,
  ssl_cafile: str | None,
  ssl_certfile: str | None,
  ssl_keyfile: str | None,
) -> None:
  """Validate Redis TLS trust and optional client-certificate material."""
  tls_paths = {
    "ssl_cafile": ssl_cafile,
    "ssl_certfile": ssl_certfile,
    "ssl_keyfile": ssl_keyfile,
  }
  for setting_name, value in tls_paths.items():
    if value is not None and not value.strip():
      raise ConfigurationError(
        f"Redis TLS setting '{setting_name}' cannot be blank.",
        setting_name=setting_name,
      )
  if not ssl_enabled and any(value is not None for value in tls_paths.values()):
    raise ConfigurationError(
      "Redis TLS certificate settings require ssl_enabled=True.",
      setting_name="ssl_enabled",
    )
  if ssl_enabled and ssl_cafile is None:
    raise ConfigurationError(
      (
        "ssl_enabled=True requires 'ssl_cafile' to be set (path to a CA "
        "certificate bundle)."
      ),
      setting_name="ssl_cafile",
      setting_value=ssl_cafile,
    )
  if (ssl_certfile is None) != (ssl_keyfile is None):
    missing_name = "ssl_keyfile" if ssl_certfile is not None else "ssl_certfile"
    raise ConfigurationError(
      "Redis TLS client authentication requires both certificate and key files.",
      setting_name=missing_name,
    )


class RedisSettings(BaseSettings):
  """Redis-specific settings for all deployment modes.

  These settings configure the Redis connection and can be set
  via environment variables with the SCRAPY_REDIS_ prefix.

  Redis keys created by older releases were not namespaced. The safe layout
  intentionally does not fall back to those raw keys: an automatic fallback
  could read or delete an unrelated application's key in a shared database.
  Operators upgrading a persistent deployment must drain or explicitly
  migrate legacy keys into the configured namespace before switching versions.
  Independent applications sharing one Redis database must configure distinct
  namespaces; the default separates backend domains, not separate deployments.

  Supports three deployment topologies plus one compatibility alias:
  - standalone: Single Redis instance (default)
  - master_slave: Deprecated primary-only alias; no replica routing
  - sentinel: High availability with automatic failover
  - cluster: Redis Cluster with automatic sharding

  Attributes:
      mode: Redis deployment mode.
      host: Redis server hostname (standalone/master_slave).
      port: Redis server port (standalone/master_slave).
      db: Redis database number (standalone/master_slave/sentinel).
      namespace: Physical Redis key namespace for application isolation.
      password: Redis authentication password.
      socket_timeout: Socket timeout in seconds.
      socket_connect_timeout: Socket connection timeout in seconds.
      retry_on_timeout: Deprecated compatibility input; data commands are not
        automatically replayed after timeout for either value.
      replicas: Deprecated unsupported replica-read compatibility input.
      sentinels: List of sentinel nodes for sentinel mode.
      sentinel_master_name: Master name for sentinel mode.
      sentinel_password: Separate password for sentinel authentication.
      cluster_startup_nodes: List of startup nodes for cluster mode.
      cluster_skip_full_coverage_check: Skip full coverage check for cluster.
      decode_responses: Whether redis-py decodes internally before the backend
        losslessly restores byte-oriented public results.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_REDIS_",
    case_sensitive=False,
    extra="forbid",
  )

  # === Mode Selection ===
  mode: RedisMode = Field(
    default=RedisMode.STANDALONE,
    description=(
      "Redis deployment mode: standalone, sentinel, cluster, or the "
      "deprecated primary-only master_slave alias"
    ),
  )

  # === Standalone / Master-Slave Settings ===
  host: str = Field(
    default="localhost",
    min_length=1,
    description="Bare Redis DNS name, IPv4 address, or IPv6 address",
  )
  port: int = Field(
    default=6379,
    ge=1,
    le=65535,
    description="Redis server port (standalone/master_slave)",
  )
  db: int = Field(
    default=0,
    ge=0,
    description="Redis database number (standalone/master_slave/sentinel)",
  )

  # === Physical Key Isolation ===
  namespace: str = Field(
    default="scrapy-extension",
    min_length=1,
    max_length=128,
    pattern=r"^[a-zA-Z0-9._-]+$",
    description=(
      "Namespace prepended to every physical Redis key. Queue, set, and "
      "storage keys also receive fixed domain prefixes so the same logical "
      "name can be used safely by all three backend interfaces."
    ),
  )

  # === Authentication ===
  password: SecretStr | None = Field(
    default=None,
    description="Redis authentication password",
  )
  username: str | None = Field(
    default=None,
    description="Redis ACL username (Redis 6+)",
  )

  # === Connection Settings ===
  socket_timeout: float | None = Field(
    default=30.0,
    ge=0,
    description="Socket timeout in seconds",
  )
  socket_connect_timeout: float | None = Field(
    default=5.0,
    ge=0,
    description="Socket connection timeout in seconds",
  )
  retry_on_timeout: bool = Field(
    default=True,
    description=(
      "Deprecated compatibility input. Redis data-plane commands never use "
      "automatic SDK timeout retries because their outcome may be ambiguous."
    ),
    json_schema_extra={"deprecated": True},
  )
  max_connections: int | None = Field(
    default=None,
    ge=1,
    description="Maximum connections per pool (None = effectively unbounded)",
  )
  decode_responses: bool = Field(
    default=False,
    description=(
      "Ask redis-py to decode responses internally. Bundled backend methods "
      "still return their byte-oriented public contract and use lossless "
      "surrogateescape conversion for arbitrary binary values."
    ),
  )

  # === Master-Slave Mode Settings ===
  replicas: Annotated[list[str], NoDecode] = Field(
    default_factory=list,
    description=(
      "Deprecated unsupported replica-read input; must remain empty. "
      "Use Sentinel for discovery/failover."
    ),
    json_schema_extra={"deprecated": True},
  )
  read_from_replicas: bool = Field(
    default=False,
    description=(
      "Deprecated unsupported replica-read input; must remain false."
    ),
    json_schema_extra={"deprecated": True},
  )

  # === Sentinel Mode Settings ===
  sentinels: Annotated[list[str], NoDecode] = Field(
    default_factory=list,
    description="List of sentinel host:port (e.g., ['sentinel1:26379', 'sentinel2:26379'])",
  )
  sentinel_master_name: str = Field(
    default="mymaster",
    description="Master name configured in sentinel",
  )
  sentinel_password: SecretStr | None = Field(
    default=None,
    description=(
      "Password used only for Sentinel control-plane authentication; there "
      "is no fallback to the Redis data-plane password"
    ),
  )
  sentinel_username: str | None = Field(
    default=None,
    description="Username for sentinel authentication (Redis 6+)",
  )
  min_other_sentinels: int = Field(
    default=0,
    ge=0,
    description="Minimum other sentinels for quorum verification",
  )
  sentinel_retry_on_timeout: bool = Field(
    default=True,
    description=(
      "Retry one timeout per read-only Sentinel control SDK request"
    ),
  )

  # === Cluster Mode Settings ===
  cluster_startup_nodes: Annotated[list[str], NoDecode] = Field(
    default_factory=list,
    description="List of startup host:port for cluster mode",
  )
  cluster_skip_full_coverage_check: bool = Field(
    default=False,
    description="Skip check that all slots are covered by cluster",
  )
  cluster_max_redirects: int = Field(
    default=5,
    ge=0,
    le=100,
    description=(
      "Maximum Cluster MOVED/ASK/TRYAGAIN protocol continuations after the "
      "initial command attempt"
    ),
  )

  # === SSL/TLS Settings ===
  ssl_enabled: bool = Field(
    default=False,
    description="Enable SSL/TLS connection",
  )
  ssl_cafile: str | None = Field(
    default=None,
    description="Path to CA certificate file for SSL",
  )
  ssl_certfile: str | None = Field(
    default=None,
    description="Path to client certificate file for SSL",
  )
  ssl_keyfile: str | None = Field(
    default=None,
    description="Path to client private key file for SSL",
  )
  ssl_check_hostname: bool = Field(
    default=True,
    description=(
      "Verify TLS certificate hostname matches the connected host. "
      "Defaults to True to prevent MITM via misconfigured ssl_enabled. "
      "Operators using IP-only service discovery may set this to False "
      "(not recommended; prefer DNS/SNI)."
    ),
  )

  # Rejected compatibility tombstone. Keeping it as a non-decoded field makes
  # direct BaseSettings environment loading surface SCRAPY_REDIS_MASTERS
  # safely instead of silently ignoring it or retaining malformed JSON.
  masters: Annotated[SkipJsonSchema[list[str] | None], NoDecode] = Field(
    default=None,
    exclude=True,
    repr=False,
    description=(
      "Rejected historical input; use cluster_startup_nodes for Cluster seeds"
    ),
    json_schema_extra={"deprecated": True},
  )

  @model_validator(mode="before")
  @classmethod
  def _reject_ghost_masters_field(cls, values: Any) -> Any:
    """Reject the historical doc-only field without retaining its value."""
    if isinstance(values, Mapping):
      for key in values:
        if isinstance(key, str) and key.casefold() == "masters":
          raise ConfigurationError(
            (
              "Redis setting 'masters' is unsupported; use "
              "cluster_startup_nodes."
            ),
            setting_name="masters",
          )
    return values

  @field_validator("host", mode="before")
  @classmethod
  def _normalize_host(cls, value: object) -> str:
    """Validate the scalar host before Pydantic can retain malformed input."""
    return normalize_redis_host(value)

  @field_validator("port", mode="before")
  @classmethod
  def _normalize_port(cls, value: object) -> int:
    """Validate the scalar port before Pydantic can retain malformed input."""
    return normalize_redis_port(value)

  @field_validator(
    "replicas", "sentinels", "cluster_startup_nodes", mode="before"
  )
  @classmethod
  def _normalize_endpoint_list(
    cls, value: object, info: ValidationInfo
  ) -> list[str]:
    """Validate endpoint lists before malformed values enter model state."""
    setting_name = info.field_name or "redis"
    decoded_value: object | None = None
    decode_failed = False
    if isinstance(value, str):
      try:
        decoded_value = json.loads(value)
      except (TypeError, ValueError):
        decode_failed = True
      if decode_failed:
        raise ConfigurationError(
          f"Redis setting '{setting_name}' must be a JSON endpoint list.",
          setting_name=setting_name,
        )
      value = decoded_value
    if not isinstance(value, (list, tuple)):
      raise ConfigurationError(
        f"Redis setting '{setting_name}' must be a list of endpoints.",
        setting_name=setting_name,
      )
    normalized: list[str] = []
    for index, endpoint in enumerate(value):
      host, port = parse_redis_endpoint(
        endpoint,
        setting_name=setting_name,
        index=index,
      )
      normalized.append(format_redis_endpoint(host, port))
    return normalized

  @model_validator(mode="after")
  def validate_mode_requirements(self) -> RedisSettings:
    """Validate that mode-specific settings are present for the chosen mode.

    SENTINEL mode requires ``sentinels`` and ``sentinel_master_name`` — both
    are unambiguous intent signals; without them the sentinel client cannot
    form a quorum and the error surfaces deep inside ``redis-py`` at connect.

    Cluster may fall back to ``host:port`` when no explicit startup seed is
    supplied, but Redis Cluster supports only database zero. The historical
    replica-read inputs never affected routing and now fail explicitly instead
    of silently promising eventual-consistency reads that do not exist.

    Raises:
        ConfigurationError: If a SENTINEL-mode required field is missing.
    """
    if self.mode == RedisMode.SENTINEL:
      missing = []
      if not self.sentinels:
        missing.append("sentinels")
      if not self.sentinel_master_name:
        missing.append("sentinel_master_name")
      if missing:
        fields = " and ".join(missing)
        raise ConfigurationError(
          (
            f"Redis SENTINEL mode requires '{fields}' to be set. "
            "No endpoint or credential values are included in this error."
          ),
          setting_name=missing[0],
        )
    if self.mode == RedisMode.CLUSTER and self.db != 0:
      raise ConfigurationError(
        "Redis Cluster supports only database 0; use namespace for isolation.",
        setting_name="db",
      )
    if self.mode != RedisMode.SENTINEL:
      if self.sentinels:
        raise ConfigurationError(
          "Redis sentinels require mode='sentinel'.",
          setting_name="sentinels",
        )
      sentinel_intent = (
        ("sentinel_master_name", self.sentinel_master_name != "mymaster"),
        ("sentinel_password", self.sentinel_password is not None),
        ("sentinel_username", self.sentinel_username is not None),
        ("min_other_sentinels", self.min_other_sentinels != 0),
        (
          "sentinel_retry_on_timeout",
          self.sentinel_retry_on_timeout is not True,
        ),
      )
      for setting_name, configured in sentinel_intent:
        if configured:
          raise ConfigurationError(
            f"Redis setting '{setting_name}' requires mode='sentinel'.",
            setting_name=setting_name,
          )
    if self.mode != RedisMode.CLUSTER:
      if self.cluster_startup_nodes:
        raise ConfigurationError(
          "Redis cluster_startup_nodes require mode='cluster'.",
          setting_name="cluster_startup_nodes",
        )
      cluster_intent = (
        (
          "cluster_skip_full_coverage_check",
          self.cluster_skip_full_coverage_check is not False,
        ),
        ("cluster_max_redirects", self.cluster_max_redirects != 5),
      )
      for setting_name, configured in cluster_intent:
        if configured:
          raise ConfigurationError(
            f"Redis setting '{setting_name}' requires mode='cluster'.",
            setting_name=setting_name,
          )
    if self.replicas:
      raise ConfigurationError(
        "Redis replica routing is unsupported; replicas must remain empty.",
        setting_name="replicas",
      )
    if self.read_from_replicas:
      raise ConfigurationError(
        "Redis replica routing is unsupported; read_from_replicas must be false.",
        setting_name="read_from_replicas",
      )
    return self

  @model_validator(mode="after")
  def _validate_ssl_enabled_requires_cafile(self) -> RedisSettings:
    """SV3-3 (M): ``ssl_enabled=True`` → require ``ssl_cafile``.

    Without an explicit CA bundle, ``redis-py`` falls back to OpenSSL's
    default verification path — which on minimal containers may be empty or
    missing system roots, causing either an opaque ``SSL`` error at connect
    or (worse, when ``ssl_check_hostname=False`` is also set) a silent MITM
    risk. Fail-fast at config time: operators with self-signed certs must
    provide their own CA file; there is no implicit opt-out.

    Verified safe to raise: no existing repo fixture constructs
    ``RedisSettings(ssl_enabled=True)`` without ``ssl_cafile`` in a way that
    is intended to be valid (the lone ``ssl_enabled=True`` fixture in
    ``tests/test_backend_modes.py`` sets both).

    Raises:
        ConfigurationError: if ``ssl_enabled`` is True and ``ssl_cafile`` is
            unset/empty.
    """
    validate_redis_tls(
      self.ssl_enabled,
      self.ssl_cafile,
      self.ssl_certfile,
      self.ssl_keyfile,
    )
    return self
