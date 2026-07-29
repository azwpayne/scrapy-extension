# @author  : azwpayne(https://github.com/azwpayne)
# @name    : mongodb.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :

from __future__ import annotations

from enum import Enum
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Literal, NoReturn, cast
from urllib.parse import parse_qsl, urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions._redaction import configuration_error_boundary
from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._redacted import RedactedBaseSettings

_VALID_MONGO_SCHEMES: tuple[str, ...] = ("mongodb://", "mongodb+srv://")
_MONGODB_CONFIGURATION_FIELD_NAMES: frozenset[str] = frozenset(
  {
    "auth_mechanism",
    "auth_source",
    "collection_names",
    "database",
    "mode",
    "mongos_routers",
    "password",
    "replica_set_members",
    "replica_set_name",
    "tls_allow_invalid_certificates",
    "tls_enabled",
    "uri",
    "username",
    "w",
    "w_timeout_ms",
  }
)
_MONGODB_CONFIGURATION_ERROR = "MongoDB configuration is invalid."


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_collection_domains(
  queue_collection: object,
  set_collection: object,
  storage_collection: object,
) -> tuple[str, str, str]:
  """Require one physical collection per public capability domain.

  A storage-wide clear deletes every non-marker document. Sharing its
  collection with queue or set documents would therefore delete data owned by
  another capability. The same boundary also prevents incompatible unique
  indexes from being installed on one mixed-schema collection.
  """
  collection_names = (
    queue_collection,
    set_collection,
    storage_collection,
  )
  if not all(type(name) is str for name in collection_names):
    raise ConfigurationError(
      "MongoDB capability collection names must be built-in strings.",
      setting_name="collection_names",
    )
  validated_names = cast(tuple[str, str, str], collection_names)
  # R29-A: reject empty/whitespace collection names — otherwise pymongo raises
  # InvalidName deep in _initialize_collections/_create_indexes at connect,
  # wrapped as a network-flavored BackendConnectionError (the opaque-at-connect
  # footgun this validator layer exists to prevent). ``('', 'sets', 'storage')``
  # is 3 distinct values, so the distinctness check below does not catch it.
  if not all(name and name.strip() for name in validated_names):
    raise ConfigurationError(
      "MongoDB capability collection names must be non-empty.",
      setting_name="collection_names",
    )
  if len(set(validated_names)) != len(validated_names):
    raise ConfigurationError(
      (
        "MongoDB queue, set, and storage capability domains must use "
        "distinct physical collection names."
      ),
      setting_name="collection_names",
    )
  return validated_names


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_write_concern(
  w: object, w_timeout_ms: object
) -> tuple[int | str, int | None]:
  """Return a write concern whose completion confirms server acknowledgement."""
  normalized_w: int | str
  normalized_timeout: int | None
  if isinstance(w, bool):
    raise ConfigurationError(
      "MongoDB w must be a positive integer or 'majority', not a boolean.",
      setting_name="w",
    )
  if isinstance(w, int):
    if w < 1:
      raise ConfigurationError(
        "MongoDB mutations require an acknowledged write concern (w >= 1).",
        setting_name="w",
      )
    normalized_w = w
  elif isinstance(w, str):
    candidate = w.strip()
    if candidate == "majority":
      normalized_w = candidate
    else:
      numeric_w: int | None = None
      invalid_w = False
      try:
        numeric_w = int(candidate, 10)
      except ValueError:
        invalid_w = True
      if invalid_w:
        raise ConfigurationError(
          "MongoDB w must be a positive integer or 'majority'.",
          setting_name="w",
        )
      assert numeric_w is not None
      if numeric_w < 1:
        raise ConfigurationError(
          "MongoDB mutations require an acknowledged write concern (w >= 1).",
          setting_name="w",
        )
      normalized_w = numeric_w
  else:
    raise ConfigurationError(
      "MongoDB w must be a positive integer or 'majority'.",
      setting_name="w",
    )

  if w_timeout_ms is None:
    normalized_timeout = None
  elif isinstance(w_timeout_ms, bool):
    raise ConfigurationError(
      "MongoDB w_timeout_ms must be a non-negative integer or None.",
      setting_name="w_timeout_ms",
    )
  elif isinstance(w_timeout_ms, int):
    if w_timeout_ms < 0:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      )
    normalized_timeout = w_timeout_ms
  elif isinstance(w_timeout_ms, str):
    normalized_timeout = None
    invalid_timeout = False
    try:
      normalized_timeout = int(w_timeout_ms.strip(), 10)
    except ValueError:
      invalid_timeout = True
    if invalid_timeout:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      )
    assert normalized_timeout is not None
    if normalized_timeout < 0:
      raise ConfigurationError(
        "MongoDB w_timeout_ms must be a non-negative integer or None.",
        setting_name="w_timeout_ms",
      )
  else:
    raise ConfigurationError(
      "MongoDB w_timeout_ms must be a non-negative integer or None.",
      setting_name="w_timeout_ms",
    )
  return normalized_w, normalized_timeout


class MongoDBMode(str, Enum):
  """MongoDB deployment modes.

  Attributes:
      STANDALONE: Single MongoDB instance (default).
      REPLICA_SET: Replica set for high availability.
      SHARDED_CLUSTER: Sharded cluster for horizontal scaling.
      ATLAS: MongoDB Atlas cloud service.
  """

  STANDALONE = "standalone"
  REPLICA_SET = "replica_set"
  SHARDED_CLUSTER = "sharded_cluster"
  ATLAS = "atlas"


_INSECURE_TLS_URI_OPTIONS: frozenset[str] = frozenset(
  {
    "tlsallowinvalidcertificates",
    "tlsallowinvalidhostnames",
    "tlsdisableocspendpointcheck",
    "tlsinsecure",
  }
)
_MONGODB_PROXY_URI_OPTIONS: frozenset[str] = frozenset(
  {
    "proxyhost",
    "proxyport",
    "proxyusername",
    "proxypassword",
  }
)
_EXTERNAL_AUTH_MECHANISMS: frozenset[str] = frozenset(
  {"GSSAPI", "MONGODB-AWS", "MONGODB-X509"}
)
_PASSWORD_AUTH_MECHANISMS: frozenset[str] = frozenset(
  {"SCRAM-SHA-1", "SCRAM-SHA-256", "MONGODB-CR", "PLAIN"}
)
_SUPPORTED_AUTH_MECHANISMS: frozenset[str] = frozenset(
  {
    *_PASSWORD_AUTH_MECHANISMS,
    *_EXTERNAL_AUTH_MECHANISMS,
  }
)
_PRODUCTION_MONGO_MODES: frozenset[MongoDBMode] = frozenset(
  {MongoDBMode.ATLAS, MongoDBMode.SHARDED_CLUSTER, MongoDBMode.REPLICA_SET}
)
_LOCAL_PLAINTEXT_BLOCKED_TOPOLOGY_OPTIONS: frozenset[str] = frozenset(
  {
    "replicaset",
    "loadbalanced",
    "srvmaxhosts",
    "srvservicename",
  }
)


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_uri(value: object) -> str:
  """Require a safe MongoDB URI before it reaches the driver.

  MongoDB connection strings are later given verbatim to PyMongo.  Validate
  their authority and policy-bearing options here rather than allowing a
  malformed or security-downgrading value to surface during client creation.
  """
  if type(value) is not str or not value or not value.lower().startswith(
    _VALID_MONGO_SCHEMES
  ):
    raise ConfigurationError(
      "uri must start with 'mongodb://' or 'mongodb+srv://'.",
      setting_name="uri",
    )
  # MongoDB URIs do not support fragments.  ``urlsplit`` discards one before
  # the query policy is inspected, while PyMongo parses the full connection
  # string and can still consume options after it.
  if "#" in value:
    raise ConfigurationError(
      "MongoDB URI must not contain fragments.", setting_name="uri"
    )
  parsed = None
  malformed_uri = False
  try:
    parsed = urlsplit(value)
  except ValueError:
    malformed_uri = True
  if malformed_uri:
    raise ConfigurationError(
      "MongoDB URI is malformed.", setting_name="uri"
    )
  assert parsed is not None
  if not parsed.netloc:
    raise ConfigurationError(
      "MongoDB URI must include at least one server endpoint.",
      setting_name="uri",
    )
  if parsed.username is not None or parsed.password is not None:
    raise ConfigurationError(
      "MongoDB URI must not contain userinfo; configure username/password settings instead.",
      setting_name="uri",
    )
  _validate_mongodb_uri_authority(parsed.scheme, parsed.netloc)
  # PyMongo accepts both '&' and ';' separators. Keeping blank values avoids
  # a policy bypass such as ``?tlsInsecure=``.
  option_names = {
    name for name, _value in _mongodb_uri_option_pairs(parsed.query)
  }
  if option_names & _MONGODB_PROXY_URI_OPTIONS:
    raise ConfigurationError(
      "MongoDB URI must not contain proxy query options.",
      setting_name="uri",
    )
  if option_names & _INSECURE_TLS_URI_OPTIONS:
    raise ConfigurationError(
      "MongoDB URI must not disable TLS certificate or hostname verification.",
      setting_name="uri",
    )
  if any(name.startswith(("auth", "tls", "ssl")) for name in option_names):
    raise ConfigurationError(
      (
        "MongoDB URI must not contain authentication, credential, or TLS "
        "query options; configure dedicated settings instead."
      ),
      setting_name="uri",
    )
  return value


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_database(value: object) -> str:
  """Require a concrete database name before indexing a client with it."""
  if type(value) is not str or not value.strip():
    raise ConfigurationError(
      "MongoDB 'database' name must be a non-empty string.",
      setting_name="database",
    )
  return value


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_auth_source(value: object) -> str:
  """Require a concrete authentication source before calling PyMongo."""
  if type(value) is not str or not value.strip():
    raise ConfigurationError(
      "MongoDB 'auth_source' must be a non-empty string.",
      setting_name="auth_source",
    )
  return value


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_replica_set_name(value: object) -> str | None:
  """Require a concrete optional replica-set name for the driver kwarg."""
  if value is None:
    return None
  if type(value) is not str or not value.strip():
    raise ConfigurationError(
      "MongoDB 'replica_set_name' must be a non-empty string when set.",
      setting_name="replica_set_name",
    )
  return value


def _invalid_mongodb_seed_endpoint(setting_name: str) -> NoReturn:
  """Raise a static error without echoing possibly credential-bearing input."""
  raise ConfigurationError(
    "MongoDB seed endpoints must be host, host:port, or '[IPv6]:port' values.",
    setting_name=setting_name,
  )


def _normalize_mongodb_seed_host(host: str, setting_name: str) -> str:
  """Return a canonical IP or conservative DNS hostname for a seed endpoint."""
  if (
    not host
    or not host.isascii()
    or any(char.isspace() or char in "/@?#\\%" for char in host)
  ):
    _invalid_mongodb_seed_endpoint(setting_name)
  try:
    return str(ip_address(host))
  except ValueError:
    pass

  hostname = host[:-1] if host.endswith(".") else host
  labels = hostname.split(".")
  if (
    not hostname
    or len(hostname) > 253
    or any(
      not label
      or len(label) > 63
      or label[0] == "-"
      or label[-1] == "-"
      or any(not (char.isascii() and (char.isalnum() or char == "-")) for char in label)
      for label in labels
    )
  ):
    _invalid_mongodb_seed_endpoint(setting_name)
  return host.lower()


def _parse_mongodb_seed_port(port_text: str, setting_name: str) -> int:
  """Parse the optional seed port without accepting URI syntax."""
  if not port_text or not port_text.isascii() or not port_text.isdecimal():
    _invalid_mongodb_seed_endpoint(setting_name)
  port = int(port_text)
  if not 1 <= port <= 65535:
    _invalid_mongodb_seed_endpoint(setting_name)
  return port


def _parse_mongodb_seed_endpoint(
  value: object, setting_name: str
) -> tuple[str, str]:
  """Parse one safe MongoDB seed and return ``(uri_seed, host)``.

  Seed lists are interpolated into a generated MongoDB URI. They therefore
  accept only host syntax, never URL authority/path/query syntax.
  """
  if type(value) is not str or not value or value != value.strip():
    _invalid_mongodb_seed_endpoint(setting_name)
  text = value
  if "," in text or any(char.isspace() or char in "/@?#\\%" for char in text):
    _invalid_mongodb_seed_endpoint(setting_name)

  port: int | None = None
  if text.startswith("["):
    closing = text.find("]")
    if closing <= 1:
      _invalid_mongodb_seed_endpoint(setting_name)
    host = text[1:closing]
    remainder = text[closing + 1 :]
    if remainder:
      if not remainder.startswith(":"):
        _invalid_mongodb_seed_endpoint(setting_name)
      port = _parse_mongodb_seed_port(remainder[1:], setting_name)
    address: IPv4Address | IPv6Address | None = None
    try:
      address = ip_address(host)
    except ValueError:
      pass
    if address is None:
      _invalid_mongodb_seed_endpoint(setting_name)
    if address.version != 6:
      _invalid_mongodb_seed_endpoint(setting_name)
    normalized_host = str(address)
    uri_seed = f"[{normalized_host}]"
  else:
    address = None
    try:
      address = ip_address(text)
    except ValueError:
      pass
    if address is None:
      if text.count(":") > 1:
        _invalid_mongodb_seed_endpoint(setting_name)
      if ":" in text:
        host, port_text = text.rsplit(":", 1)
        port = _parse_mongodb_seed_port(port_text, setting_name)
      else:
        host = text
      normalized_host = _normalize_mongodb_seed_host(host, setting_name)
      uri_seed = normalized_host
    else:
      normalized_host = str(address)
      uri_seed = (
        f"[{normalized_host}]" if address.version == 6 else normalized_host
      )
  if port is not None:
    uri_seed = f"{uri_seed}:{port}"
  return uri_seed, normalized_host


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_seed_endpoints(
  value: object, setting_name: str
) -> tuple[str, ...]:
  """Validate and normalize a list of URI-safe replica/mongos seeds."""
  if not isinstance(value, (list, tuple)):
    raise ConfigurationError(
      "MongoDB seed endpoints must be a list or tuple of endpoint strings.",
      setting_name=setting_name,
    )
  return tuple(
    _parse_mongodb_seed_endpoint(endpoint, setting_name)[0] for endpoint in value
  )


def _invalid_mongodb_uri_authority() -> NoReturn:
  """Raise a static URI error without reflecting an untrusted authority."""
  raise ConfigurationError(
    "MongoDB URI must contain valid server endpoint authorities.",
    setting_name="uri",
  )


def _validate_mongodb_uri_authority(scheme: str, authority: str) -> None:
  """Validate URI hosts with the strict generated-seed authority grammar."""
  if scheme.lower() == "mongodb":
    endpoints = authority.split(",")
    if not endpoints:
      _invalid_mongodb_uri_authority()
    for endpoint in endpoints:
      valid_endpoint = True
      try:
        _parse_mongodb_seed_endpoint(endpoint, "uri")
      except ConfigurationError:
        valid_endpoint = False
      if not valid_endpoint:
        _invalid_mongodb_uri_authority()
    return

  # ``mongodb+srv`` delegates host discovery to one DNS hostname.  It has no
  # seed list, port, bracketed IP literal, or userinfo form.
  if (
    not authority
    or "," in authority
    or any(char in authority for char in ":[]")
  ):
    _invalid_mongodb_uri_authority()
  normalized_host: str | None = None
  invalid_host = False
  try:
    normalized_host = _normalize_mongodb_seed_host(authority, "uri")
  except ConfigurationError:
    invalid_host = True
  if invalid_host:
    _invalid_mongodb_uri_authority()
  assert normalized_host is not None
  is_ip_address = True
  try:
    ip_address(normalized_host)
  except ValueError:
    is_ip_address = False
  if not is_ip_address:
    return
  _invalid_mongodb_uri_authority()


def _mongodb_uri_option_pairs(query: str) -> tuple[tuple[str, str], ...]:
  """Return lower-cased Mongo URI option names using both PyMongo separators."""
  return tuple(
    (name.lower(), option_value)
    for name, option_value in parse_qsl(
      query.replace(";", "&"), keep_blank_values=True
    )
  )


def _mongodb_password_text(value: object) -> str | None:
  if value is None:
    return None
  if isinstance(value, SecretStr):
    return value.get_secret_value()
  if type(value) is str:
    return value
  raise ConfigurationError(
    "MongoDB 'password' must be a string or SecretStr.",
    setting_name="password",
  )


def uses_mongodb_external_auth(mechanism: object) -> bool:
  """Return whether a mechanism uses an external/ambient identity."""
  return type(mechanism) is str and mechanism in _EXTERNAL_AUTH_MECHANISMS


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_authentication(
  username: object,
  password: object,
  auth_mechanism: object,
  auth_source: object = "admin",
) -> bool:
  """Validate mechanism-specific authentication before driver construction."""
  if username is not None and (
    type(username) is not str or not username.strip()
  ):
    raise ConfigurationError(
      "MongoDB 'username' must be non-empty.",
      setting_name="username",
    )
  password_text = _mongodb_password_text(password)
  if password_text is not None and not password_text.strip():
    raise ConfigurationError(
      "MongoDB 'password' must be non-empty.",
      setting_name="password",
    )
  if auth_mechanism is not None and (
    type(auth_mechanism) is not str
    or auth_mechanism not in _SUPPORTED_AUTH_MECHANISMS
  ):
    raise ConfigurationError(
      "MongoDB auth_mechanism is unsupported.",
      setting_name="auth_mechanism",
    )
  normalized_auth_source = validate_mongodb_auth_source(auth_source)
  username_set = username is not None
  password_set = password_text is not None

  if auth_mechanism == "GSSAPI":
    if not username_set:
      raise ConfigurationError(
        "MongoDB GSSAPI authentication requires a username.",
        setting_name="username",
      )
    if normalized_auth_source not in {"admin", "$external"}:
      raise ConfigurationError(
        "MongoDB external authentication requires auth_source='$external'.",
        setting_name="auth_source",
      )
    return True

  if auth_mechanism == "MONGODB-X509":
    if password_set:
      raise ConfigurationError(
        "MongoDB MONGODB-X509 authentication does not support a password.",
        setting_name="password",
      )
    if normalized_auth_source not in {"admin", "$external"}:
      raise ConfigurationError(
        "MongoDB external authentication requires auth_source='$external'.",
        setting_name="auth_source",
      )
    return True

  if auth_mechanism == "MONGODB-AWS":
    if username_set != password_set:
      raise ConfigurationError(
        "MongoDB MONGODB-AWS username and password must be configured together.",
        setting_name="username" if not username_set else "password",
      )
    if normalized_auth_source not in {"admin", "$external"}:
      raise ConfigurationError(
        "MongoDB external authentication requires auth_source='$external'.",
        setting_name="auth_source",
      )
    return True

  if username_set != password_set:
    raise ConfigurationError(
      "MongoDB username and password must be configured together.",
      setting_name="username" if not username_set else "password",
    )
  if auth_mechanism in _PASSWORD_AUTH_MECHANISMS and not username_set:
    raise ConfigurationError(
      "MongoDB password authentication requires username and password.",
      setting_name="username",
    )
  return username_set


def _mongodb_endpoint_is_loopback(endpoint: object) -> bool:
  """Classify a single host[:port] conservatively; unknown means remote."""
  try:
    _uri_seed, host = _parse_mongodb_seed_endpoint(
      endpoint, "replica_set_members"
    )
  except ConfigurationError:
    return False
  normalized = host.lower().rstrip(".")
  if normalized == "localhost":
    return True
  try:
    return ip_address(normalized).is_loopback
  except ValueError:
    return False


def is_mongodb_direct_loopback_uri(uri: str) -> bool:
  """Return whether a URI can safely use the local plaintext compatibility path.

  A single loopback seed alone is not sufficient: PyMongo can discover a
  replica topology from that seed and subsequently authenticate to non-local
  members.  The backend pins direct mode for this narrow path, so reject
  topology-bearing URI options that would conflict with it.
  """
  if "#" in uri:
    return False
  try:
    parsed = urlsplit(uri)
  except ValueError:
    return False
  if parsed.scheme.lower() != "mongodb":
    return False
  endpoints = parsed.netloc.split(",")
  if len(endpoints) != 1 or not _mongodb_endpoint_is_loopback(endpoints[0]):
    return False
  for name, option_value in _mongodb_uri_option_pairs(parsed.query):
    if name in _LOCAL_PLAINTEXT_BLOCKED_TOPOLOGY_OPTIONS:
      return False
    if name == "directconnection" and option_value.strip().lower() != "true":
      return False
  return True


def _mongodb_endpoints_are_loopback(
  mode: MongoDBMode,
  uri: str,
  replica_set_members: object,
  mongos_routers: object,
) -> bool:
  """Return true only when every effective endpoint is known loopback."""
  if mode is MongoDBMode.REPLICA_SET and replica_set_members:
    endpoints = replica_set_members
  elif mode is MongoDBMode.SHARDED_CLUSTER and mongos_routers:
    endpoints = mongos_routers
  else:
    parsed = urlsplit(uri)
    if parsed.scheme.lower() == "mongodb+srv":
      return False
    endpoints = parsed.netloc.split(",")
  return isinstance(endpoints, (list, tuple)) and bool(endpoints) and all(
    _mongodb_endpoint_is_loopback(endpoint) for endpoint in endpoints
  )


@configuration_error_boundary(
  _MONGODB_CONFIGURATION_ERROR,
  _MONGODB_CONFIGURATION_FIELD_NAMES,
)
def validate_mongodb_transport_security(
  *,
  mode: MongoDBMode,
  uri: str,
  replica_set_members: object,
  mongos_routers: object,
  tls_enabled: object,
  tls_allow_invalid_certificates: object,
  username: object,
  password: object,
  auth_mechanism: object,
  auth_source: object = "admin",
) -> None:
  """Require verified TLS for remote authenticated MongoDB connections."""
  if not isinstance(mode, MongoDBMode):
    raise ConfigurationError(
      "MongoDB mode is unsupported.", setting_name="mode"
    )
  normalized_uri = validate_mongodb_uri(uri)
  normalized_replica_set_members = validate_mongodb_seed_endpoints(
    replica_set_members, "replica_set_members"
  )
  normalized_mongos_routers = validate_mongodb_seed_endpoints(
    mongos_routers, "mongos_routers"
  )
  if not isinstance(tls_enabled, bool):
    raise ConfigurationError(
      "MongoDB tls_enabled must be a boolean.", setting_name="tls_enabled"
    )
  if not isinstance(tls_allow_invalid_certificates, bool):
    raise ConfigurationError(
      "MongoDB tls_allow_invalid_certificates must be a boolean.",
      setting_name="tls_allow_invalid_certificates",
    )
  loopback_only = _mongodb_endpoints_are_loopback(
    mode,
    normalized_uri,
    normalized_replica_set_members,
    normalized_mongos_routers,
  )
  if tls_allow_invalid_certificates and (
    mode in _PRODUCTION_MONGO_MODES or not loopback_only
  ):
    raise ConfigurationError(
      "tls_allow_invalid_certificates=True is not permitted for remote or production-tier MongoDB connections.",
      setting_name="tls_allow_invalid_certificates",
      setting_value=True,
    )
  derived_seed_uri = (
    mode is MongoDBMode.REPLICA_SET and bool(normalized_replica_set_members)
  ) or (mode is MongoDBMode.SHARDED_CLUSTER and bool(normalized_mongos_routers))
  uri_scheme = urlsplit(normalized_uri).scheme.lower()
  effective_tls = (
    tls_enabled
    or mode is MongoDBMode.ATLAS
    or (not derived_seed_uri and uri_scheme == "mongodb+srv")
  )
  local_standalone_plaintext = (
    mode is MongoDBMode.STANDALONE
    and is_mongodb_direct_loopback_uri(normalized_uri)
  )
  if (
    validate_mongodb_authentication(
      username, password, auth_mechanism, auth_source
    )
    and not effective_tls
    and not local_standalone_plaintext
  ):
    raise ConfigurationError(
      (
        "Authenticated MongoDB connections require verified TLS unless they "
        "are direct standalone loopback development connections."
      ),
      setting_name="tls_enabled",
    )


class MongoDBSettings(RedactedBaseSettings):
  """MongoDB-specific settings for all deployment modes.

  These settings configure the MongoDB connection and can be set
  via environment variables with the SCRAPY_MONGO_ prefix.

  Supports four deployment modes:
  - standalone: Single MongoDB instance (default)
  - replica_set: Replica set for high availability
  - sharded_cluster: Sharded cluster for horizontal scaling
  - atlas: MongoDB Atlas cloud service
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_MONGO_",
    case_sensitive=False,
    extra="forbid",
    hide_input_in_errors=True,
  )

  # === Mode Selection ===
  mode: MongoDBMode = Field(
    default=MongoDBMode.STANDALONE,
    description="MongoDB deployment mode (standalone, replica_set, sharded_cluster, atlas)",
  )

  # === Connection Settings ===
  uri: str = Field(
    default="mongodb://localhost:27017",
    description="MongoDB connection URI (used for all modes)",
  )
  database: str = Field(
    default="scrapy_extension",
    description="MongoDB database name",
  )

  # === Collection Names ===
  queue_collection: str = Field(
    default="queues",
    description="Collection name for queue storage",
  )
  set_collection: str = Field(
    default="sets",
    description="Collection name for set storage",
  )
  storage_collection: str = Field(
    default="storage",
    description="Collection name for key-value storage",
  )

  # === Replica Set Settings ===
  replica_set_name: str | None = Field(
    default=None,
    description="Replica set name (for replica_set mode)",
  )
  replica_set_members: list[str] = Field(
    default_factory=list,
    description="List of replica set member host:port",
  )
  read_preference: Literal[
    "primary",
    "primaryPreferred",
    "secondary",
    "secondaryPreferred",
    "nearest",
  ] = Field(
    default="primary",
    description="Read preference (primary, secondary, nearest, primaryPreferred, secondaryPreferred)",
  )

  # === Sharded Cluster Settings ===
  mongos_routers: list[str] = Field(
    default_factory=list,
    description="List of mongos router host:port (for sharded_cluster mode)",
  )

  # === Atlas Settings ===
  atlas_cluster_name: str | None = Field(
    default=None,
    description=(
      "Optional Atlas cluster label. It cannot replace uri because an Atlas "
      "SRV hostname cannot be derived from the display name alone."
    ),
  )

  # === Connection Pool Settings ===
  min_pool_size: int = Field(
    default=1,
    ge=0,
    description="Minimum connection pool size",
  )
  max_pool_size: int = Field(
    default=10,
    ge=1,
    description="Maximum connection pool size",
  )
  max_idle_time_ms: int = Field(
    default=60000,
    ge=0,
    description="Maximum connection idle time in milliseconds",
  )
  wait_queue_timeout_ms: int = Field(
    default=5000,
    ge=0,
    description="Maximum wait time for connection from pool",
  )

  # === Authentication Settings ===
  username: str | None = Field(
    default=None,
    description="MongoDB username",
  )
  password: SecretStr | None = Field(
    default=None,
    description="MongoDB password",
  )
  auth_source: str = Field(
    default="admin",
    description="Authentication database",
  )
  auth_mechanism: (
    Literal[
      "SCRAM-SHA-1",
      "SCRAM-SHA-256",
      "MONGODB-CR",
      "PLAIN",
      "GSSAPI",
      "MONGODB-X509",
      "MONGODB-AWS",
    ]
    | None
  ) = Field(
    default=None,
    description="Authentication mechanism (SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-CR, PLAIN, GSSAPI, MONGODB-X509, MONGODB-AWS)",
  )

  # === TLS/SSL Settings ===
  tls_enabled: bool = Field(
    default=False,
    description="Enable TLS/SSL connection",
  )
  tls_ca_file: str | None = Field(
    default=None,
    description="Path to CA certificate file",
  )
  tls_cert_file: str | None = Field(
    default=None,
    description="Path to client certificate file",
  )
  tls_key_file: str | None = Field(
    default=None,
    description="Path to client private key file",
  )
  tls_allow_invalid_certificates: bool = Field(
    default=False,
    description="Allow invalid certificates (not recommended for production)",
  )

  # === Write Concern ===
  w: int | str = Field(
    default=1,
    description="Acknowledged write concern (positive integer or 'majority')",
  )
  journal: bool = Field(
    default=True,
    description="Wait for journal commit",
  )
  w_timeout_ms: int | None = Field(
    default=None,
    description="Non-negative write concern timeout in milliseconds",
  )

  # === Server Selection ===
  server_selection_timeout_ms: int = Field(
    default=30000,
    ge=0,
    description="Server selection timeout in milliseconds",
  )
  heartbeat_frequency_ms: int = Field(
    default=10000,
    ge=0,
    description="Heartbeat frequency in milliseconds",
  )

  @field_validator("w", mode="before")
  @classmethod
  def _normalize_write_concern(cls, value: object) -> int | str:
    """Normalize numeric environment text and reject bool before coercion."""
    normalized, _timeout = validate_mongodb_write_concern(value, None)
    return normalized

  @field_validator("w_timeout_ms", mode="before")
  @classmethod
  def _reject_invalid_write_timeout(cls, value: object) -> int | None:
    """Reject booleans and negatives before pydantic can coerce them."""
    _w, normalized = validate_mongodb_write_concern(1, value)
    return normalized

  @model_validator(mode="after")
  def _validate_write_concern(self) -> Self:
    """Require a server-acknowledged public mutation boundary."""
    validate_mongodb_write_concern(self.w, self.w_timeout_ms)
    return self

  @field_validator("database", mode="after")
  @classmethod
  def _reject_blank_database(cls, value: str) -> str:
    """Keep construction and connection-time database checks identical."""
    return validate_mongodb_database(value)

  @field_validator("auth_source", mode="after")
  @classmethod
  def _reject_blank_auth_source(cls, value: str) -> str:
    """Keep construction and connection-time auth-source checks identical."""
    return validate_mongodb_auth_source(value)

  @field_validator("username", mode="after")
  @classmethod
  def _reject_blank_username(cls, value: str | None) -> str | None:
    """R32-A: reject empty/whitespace ``username`` — the backend's ``_auth_kwargs``
    gates on bare truthiness (``if not (username and password)``), so a whitespace
    value is truthy and passed verbatim to MongoClient → opaque auth failure.
    R31-A's sweep covered auth_source but missed the username/password siblings.
    None is allowed (credential unset).
    """
    if value is not None and not value.strip():
      raise ConfigurationError(
        "MongoDB 'username' must be non-empty.",
        setting_name="username",
        setting_value=value,
      )
    return value

  @field_validator("password", mode="after")
  @classmethod
  def _reject_blank_password(cls, value: SecretStr | None) -> SecretStr | None:
    """R32-A: reject empty/whitespace ``password`` — ``SecretStr('   ')`` is truthy
    so it bypasses the backend's both-or-neither gate and reaches MongoClient
    verbatim → opaque auth failure. Same rationale as ``username``. None is allowed.
    ``setting_value`` is intentionally omitted (secret — never surface in errors).
    """
    if value is not None and not value.get_secret_value().strip():
      raise ConfigurationError(
        "MongoDB 'password' must be non-empty.",
        setting_name="password",
      )
    return value

  @field_validator("replica_set_members", mode="after")
  @classmethod
  def _validate_replica_set_members(cls, value: list[str]) -> list[str]:
    """Allow only URI-safe replica-set seed endpoints."""
    return list(validate_mongodb_seed_endpoints(value, "replica_set_members"))

  @field_validator("mongos_routers", mode="after")
  @classmethod
  def _validate_mongos_routers(cls, value: list[str]) -> list[str]:
    """Allow only URI-safe mongos seed endpoints."""
    return list(validate_mongodb_seed_endpoints(value, "mongos_routers"))

  @field_validator("replica_set_name", mode="after")
  @classmethod
  def _validate_replica_set_name(cls, value: str | None) -> str | None:
    """Validate the optional driver ``replicaSet`` kwarg at construction."""
    return validate_mongodb_replica_set_name(value)

  @model_validator(mode="after")
  def _validate_collection_domains(self) -> Self:
    """Keep queue, set, and storage documents in isolated collections."""
    validate_mongodb_collection_domains(
      self.queue_collection,
      self.set_collection,
      self.storage_collection,
    )
    return self

  @model_validator(mode="after")
  def _validate_authentication_and_transport_security(self) -> Self:
    """Require coherent authentication and verified remote transport."""
    validate_mongodb_transport_security(
      mode=self.mode,
      uri=self.uri,
      replica_set_members=self.replica_set_members,
      mongos_routers=self.mongos_routers,
      tls_enabled=self.tls_enabled,
      tls_allow_invalid_certificates=self.tls_allow_invalid_certificates,
      username=self.username,
      password=self.password,
      auth_mechanism=self.auth_mechanism,
      auth_source=self.auth_source,
    )
    return self

  @field_validator("uri")
  @classmethod
  def _validate_uri_scheme(cls, v: str) -> str:
    """SV4: ``uri`` must start with ``mongodb://`` or ``mongodb+srv://``.

    A bare ``host:port`` or empty string otherwise surfaces as an opaque
    ``InvalidURI`` / ``ConfigurationError`` at ``MongoClient`` construction.
    ``min_length=1`` rejects the empty string at the Field level; this
    validator guards the scheme. ``mongodb+srv://`` is required for Atlas
    (DNS SRV records).

    Raises:
        ConfigurationError: if ``uri`` does not start with a valid scheme.
    """
    return validate_mongodb_uri(v)

  @model_validator(mode="after")
  def _validate_mode_requirements(self) -> Self:
    """SV2: mode-specific required fields for REPLICA_SET and ATLAS.

    - REPLICA_SET: requires ``replica_set_name`` OR a ``uri`` that already
      carries a ``?replicaSet=`` query (the driver-recognized way to declare
      the RS in the URI). This preserves the documented URI-verbatim fallback
      (mongodb.py:_connect_replica_set) while catching the genuine footgun
      (REPLICA_SET mode with neither name nor URI hint → opaque driver error).
    - ATLAS: requires an explicit ``mongodb+srv://`` URI. The backend connects
      with ``uri`` verbatim; ``atlas_cluster_name`` is only a label and lacks
      the deployment-specific DNS suffix needed to construct a connection URI.

    Mirrors the Redis SENTINEL validator (raise, not warn). STANDALONE and
    SHARDED_CLUSTER are unaffected (SHARDED_CLUSTER uses mongos routers in
    the URI; no extra hint needed).

    Raises:
        ConfigurationError: if a mode-specific required field is missing.
    """
    if self.mode == MongoDBMode.REPLICA_SET:
      uri_has_rs = "replicaSet=" in self.uri
      # R29-D: strip-aware — a whitespace ``replica_set_name`` (``not "  "`` is
      # False) otherwise bypasses this truthiness check and surfaces at connect
      # as an opaque discovery error with ``replicaSet='  '``.
      name = self.replica_set_name
      name_set = name is not None and bool(name.strip())
      if not name_set and not uri_has_rs:
        raise ConfigurationError(
          (
            "MongoDB REPLICA_SET mode requires 'replica_set_name' to be set, "
            "or a uri that already carries a '?replicaSet=...' query."
          ),
          setting_name="replica_set_name",
          setting_value=self.replica_set_name,
        )
    elif self.mode == MongoDBMode.ATLAS:
      uri_is_srv = self.uri.lower().startswith("mongodb+srv://")
      if not uri_is_srv:
        raise ConfigurationError(
          (
            "MongoDB ATLAS mode requires an explicit 'mongodb+srv://' uri. "
            "atlas_cluster_name cannot replace uri because the backend uses "
            "uri verbatim and a complete Atlas SRV hostname cannot be derived "
            "from a cluster display name."
          ),
          setting_name="uri",
        )
    return self

  @model_validator(mode="after")
  def _validate_pool_size_ordering(self) -> Self:
    """SV3-4 (M): ``min_pool_size <= max_pool_size``.

    An inverted pair (min > max) makes pymongo's connection pool unable to
    ever satisfy a checkout → opaque ``ConnectionFailure`` / deadlock under
    load. Catch at config time. Both bounds are individually constrained by
    Field-level ``ge`` (min ≥ 0, max ≥ 1); this validator guards their
    relative ordering.

    Raises:
        ConfigurationError: if ``min_pool_size > max_pool_size``.
    """
    if self.min_pool_size > self.max_pool_size:
      raise ConfigurationError(
        (
          "min_pool_size must be <= max_pool_size — an inverted pair makes "
          "the connection pool unable to satisfy any checkout (deadlock "
          "under load)."
        ),
        setting_name="min_pool_size",
      )
    return self
