# @author  : azwpayne(https://github.com/azwpayne)
# @name    : kafka.py
# @time    : 2026/3/18 20:39 Wed
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    :
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._broker_endpoints import (
    normalize_kafka_broker_endpoints,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings._transport_security import (
    is_loopback_host,
    normalize_allow_remote_plaintext,
    require_remote_plaintext_opt_in,
    validate_allow_remote_plaintext,
)


class KafkaMode(str, Enum):
    """Kafka deployment modes.

    Attributes:
        STANDALONE: Single Kafka broker (default).
        CLUSTER: Multi-broker Kafka cluster.
        CONFLUENT: Confluent Cloud configuration.
    """

    STANDALONE = "standalone"
    CLUSTER = "cluster"
    CONFLUENT = "confluent"


class KafkaTopicNameGeneration(str, Enum):
    """Physical topic mapping generations.

    ``AUTO`` preserves the historical ``scrapy-<queue>`` topic when it is a
    valid Kafka name and hashes logical names that are not. ``LEGACY_V1`` is
    retained only for draining existing safe topics; ``V2`` is deterministic
    and collision-resistant for every supported logical queue identity.
    """

    AUTO = "auto"
    LEGACY_V1 = "legacy"
    V2 = "v2"


_KAFKA_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_SSL"})
_PASSWORD_SASL_MECHANISMS = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
# R141-F6: the CONFLUENT branch of ``_build_client_security_config`` always
# hands the SDK a fixed SASL_SSL/PLAIN client. ``_KAFKA_DEFAULT_SECURITY_PROTOCOL``
# is the STANDALONE field default: an unset and an explicitly-set-to-default
# value are indistinguishable after the backend's capture-and-revalidate
# rebuild (``model_validate`` of a full ``__dict__`` marks every field as set),
# and the fixed client overrides both identically — so both stay valid and only
# values that differ from BOTH the default and the fixed protocol are refused.
_KAFKA_DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT"
_KAFKA_CONFLUENT_FIXED_SECURITY_PROTOCOL = "SASL_SSL"


def _kafka_credential_value(value: object, field_name: str) -> str | None:
    """Return a non-empty credential without retaining it in failures."""
    if value is None:
        return None
    if type(value) is SecretStr:
        raw_value = value.get_secret_value()
    elif type(value) is str:
        raw_value = value
    else:
        raise ConfigurationError(
            f"{field_name} must be a string when explicitly configured.",
            setting_name=field_name,
        )
    if not raw_value.strip():
        raise ConfigurationError(
            f"{field_name} must be non-empty when explicitly configured.",
            setting_name=field_name,
        )
    return raw_value


def validate_kafka_authentication(
    mode: object,
    security_protocol: object,
    sasl_mechanism: object,
    sasl_username: object,
    sasl_password: object,
    confluent_api_key: object,
    confluent_api_secret: object,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Validate one mechanism-aware Kafka authentication value set.

    The raw values returned here are intended only for immediate SDK config
    construction. Callers retaining them must use a repr-redacting wrapper.
    Invalid credentials are never attached to the raised exception.
    """
    if type(mode) not in (KafkaMode, str) or (
        type(mode) is str and mode not in {member.value for member in KafkaMode}
    ):
        raise ConfigurationError("Kafka mode is unsupported.", setting_name="mode")
    if type(security_protocol) is not str or security_protocol not in {
        "PLAINTEXT",
        "SSL",
        "SASL_PLAINTEXT",
        "SASL_SSL",
    }:
        raise ConfigurationError(
            "security_protocol must be a supported Kafka protocol.",
            setting_name="security_protocol",
        )
    protocol = security_protocol
    if protocol == "SASL_PLAINTEXT":
        raise ConfigurationError(
            "SASL credentials require SASL_SSL; SASL_PLAINTEXT transmits them without TLS.",
            setting_name="security_protocol",
        )
    sasl_fields_set = (
        sasl_username is not None
        or sasl_password is not None
        or sasl_mechanism is not None
    )
    sasl_enabled = protocol.startswith("SASL_")
    if sasl_fields_set and not sasl_enabled:
        raise ConfigurationError(
            (
                "SASL credentials (sasl_username / sasl_password / sasl_mechanism) "
                "require a 'SASL_'-prefixed security_protocol "
                "('SASL_SSL'); kafka-python silently ignores "
                "the SASL fields otherwise (auth never attempted)."
            ),
            setting_name="security_protocol",
        )

    mechanism: str | None = None
    username: str | None = None
    password: str | None = None
    if sasl_enabled:
        if type(sasl_mechanism) is not str or not sasl_mechanism:
            raise ConfigurationError(
                "A SASL security_protocol requires an explicit sasl_mechanism.",
                setting_name="sasl_mechanism",
            )
        mechanism = sasl_mechanism
        if mechanism in _PASSWORD_SASL_MECHANISMS:
            username = _kafka_credential_value(sasl_username, "sasl_username")
            if username is None:
                raise ConfigurationError(
                    f"{mechanism} authentication requires sasl_username.",
                    setting_name="sasl_username",
                )
            password = _kafka_credential_value(sasl_password, "sasl_password")
            if password is None:
                raise ConfigurationError(
                    f"{mechanism} authentication requires sasl_password.",
                    setting_name="sasl_password",
                )
        elif mechanism == "GSSAPI":
            if sasl_username is not None or sasl_password is not None:
                raise ConfigurationError(
                    "GSSAPI uses ambient Kerberos credentials; sasl_username and "
                    "sasl_password would be ignored.",
                    setting_name=(
                        "sasl_username"
                        if sasl_username is not None
                        else "sasl_password"
                    ),
                )
        elif mechanism == "OAUTHBEARER":
            raise ConfigurationError(
                "OAUTHBEARER is unsupported because this backend does not expose the "
                "token-provider object required by kafka-python.",
                setting_name="sasl_mechanism",
            )
        else:
            raise ConfigurationError(
                "sasl_mechanism must be supported by this Kafka backend.",
                setting_name="sasl_mechanism",
            )

    key: str | None = None
    secret: str | None = None
    confluent_fields_set = (
        confluent_api_key is not None or confluent_api_secret is not None
    )
    if mode == KafkaMode.CONFLUENT:
        missing = []
        if confluent_api_key is None:
            missing.append("confluent_api_key")
        if confluent_api_secret is None:
            missing.append("confluent_api_secret")
        if missing:
            fields = " and ".join(missing)
            raise ConfigurationError(
                (
                    f"Kafka CONFLUENT mode requires '{fields}' to be set. "
                    "Without them the client could fall back to an unauthenticated "
                    "SDK transport."
                ),
                setting_name=missing[0],
            )
        key = _kafka_credential_value(confluent_api_key, "confluent_api_key")
        secret = _kafka_credential_value(confluent_api_secret, "confluent_api_secret")
    elif confluent_fields_set:
        raise ConfigurationError(
            "Confluent API credentials require mode='confluent'; other modes ignore them.",
            setting_name="mode",
        )

    return mechanism, username, password, key, secret


def validate_kafka_transport_security(
    mode: object, security_protocol: object, ssl_check_hostname: object
) -> None:
    """Reject TLS configurations that disable broker hostname verification."""
    if type(mode) not in (KafkaMode, str) or (
        type(mode) is str and mode not in {member.value for member in KafkaMode}
    ):
        raise ConfigurationError("Kafka mode is unsupported.", setting_name="mode")
    if type(security_protocol) is not str or security_protocol not in {
        "PLAINTEXT",
        "SSL",
        "SASL_PLAINTEXT",
        "SASL_SSL",
    }:
        raise ConfigurationError(
            "security_protocol must be a supported Kafka protocol.",
            setting_name="security_protocol",
        )
    uses_tls = security_protocol in {"SSL", "SASL_SSL"} or mode == KafkaMode.CONFLUENT
    if uses_tls and ssl_check_hostname is not True:
        raise ConfigurationError(
            "Kafka TLS connections require ssl_check_hostname=True.",
            setting_name="ssl_check_hostname",
        )


def validate_kafka_confluent_client_contract(
    mode: object,
    security_protocol: object,
    ssl_cafile: object,
    ssl_certfile: object,
    ssl_keyfile: object,
) -> None:
    """R141-F6: CONFLUENT mode must fail fast on client config it cannot honor.

    The backend's CONFLUENT branch builds a fixed SASL_SSL/PLAIN client
    (``_build_client_security_config``): TLS material never reaches the
    consumer/admin clients and any ``security_protocol`` value is silently
    overridden. Settings-layer rejection replaces the silent drop, so a pinned
    private CA cannot "succeed" without ever reaching the SDK. See the
    ES CLOUD ``ca_certs`` fail-fast precedent for the same pattern.
    """
    if type(mode) not in (KafkaMode, str) or (
        type(mode) is str and mode not in {member.value for member in KafkaMode}
    ):
        raise ConfigurationError("Kafka mode is unsupported.", setting_name="mode")
    if mode not in (KafkaMode.CONFLUENT, KafkaMode.CONFLUENT.value):
        return
    for setting_name, tls_material in (
        ("ssl_cafile", ssl_cafile),
        ("ssl_certfile", ssl_certfile),
        ("ssl_keyfile", ssl_keyfile),
    ):
        if tls_material is not None:
            raise ConfigurationError(
                (
                    "Kafka CONFLUENT mode does not accept TLS material "
                    "(ssl_cafile / ssl_certfile / ssl_keyfile) because the backend "
                    "builds a fixed SASL_SSL client that drops it; a private-CA pin "
                    "would silently never reach the SDK."
                ),
                setting_name=setting_name,
            )
    if security_protocol not in (
        _KAFKA_DEFAULT_SECURITY_PROTOCOL,
        _KAFKA_CONFLUENT_FIXED_SECURITY_PROTOCOL,
    ):
        raise ConfigurationError(
            (
                "Kafka CONFLUENT mode does not accept an explicit security_protocol "
                "because the backend builds a fixed SASL_SSL client that overrides "
                "it; leave security_protocol unset (recommended) — an explicit "
                "'SASL_SSL' additionally requires full SASL material "
                "(sasl_mechanism and sasl_username/sasl_password) that the fixed "
                "client overrides anyway, and is not recommended."
            ),
            setting_name="security_protocol",
        )


def _kafka_broker_endpoints_are_loopback(endpoints: str) -> bool:
    """Return whether every normalized Kafka broker endpoint is local."""
    normalized_endpoints = normalize_kafka_broker_endpoints(
        endpoints, "bootstrap_servers"
    )
    hosts: list[str] = []
    for endpoint in normalized_endpoints.split(","):
        if endpoint.startswith("["):
            hosts.append(endpoint[1 : endpoint.index("]")])
        else:
            hosts.append(endpoint.split(":", 1)[0])
    return bool(hosts) and all(is_loopback_host(host) for host in hosts)


def _kafka_policy_int(value: object, field_name: str, minimum: int) -> int:
    """Return a bounded policy integer, rejecting bools after model mutation."""
    if type(value) is not int or value < minimum:
        raise ConfigurationError(
            f"{field_name} must be an integer greater than or equal to {minimum}.",
            setting_name=field_name,
        )
    return value


def validate_kafka_delivery_policy(
    acks: object,
    max_priority_partitions: object,
    num_partitions: object,
    replication_factor: object,
    retention_ms: object,
    min_insync_replicas: object,
) -> tuple[int | str, int, int, int, int]:
    """Validate the broker-confirmed enqueue and new-topic durability policy."""
    if type(acks) not in (int, str) or acks not in (1, "all"):
        raise ConfigurationError(
            "Kafka QueueBackend requires acks=1 or acks='all'; acks=0 cannot "
            "confirm broker acceptance.",
            setting_name="acks",
        )
    normalized_acks: int | str = acks
    priority_partitions = _kafka_policy_int(
        max_priority_partitions, "max_priority_partitions", 1
    )
    configured_partitions = _kafka_policy_int(num_partitions, "num_partitions", 1)
    if configured_partitions != priority_partitions:
        raise ConfigurationError(
            "num_partitions and max_priority_partitions must match because Kafka "
            "priority values map directly to physical partitions.",
            setting_name="num_partitions",
        )
    replicas = _kafka_policy_int(replication_factor, "replication_factor", 1)
    retention = _kafka_policy_int(retention_ms, "retention_ms", 0)
    min_isr = _kafka_policy_int(min_insync_replicas, "min_insync_replicas", 1)
    if min_isr > replicas:
        raise ConfigurationError(
            "min_insync_replicas cannot exceed replication_factor.",
            setting_name="min_insync_replicas",
        )
    return normalized_acks, priority_partitions, replicas, retention, min_isr


class KafkaSettings(RedactedBaseSettings):
    """Kafka-specific settings for all deployment modes.

    These settings configure the Kafka connection and can be set
    via environment variables with the SCRAPY_KAFKA_ prefix.

    Supports three deployment modes:
    - standalone: Single Kafka broker (default)
    - cluster: Multi-broker Kafka cluster
    - confluent: Confluent Cloud configuration
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_KAFKA_",
        case_sensitive=False,
        extra="forbid",
        hide_input_in_errors=True,
    )

    # === Mode Selection ===
    mode: KafkaMode = Field(
        default=KafkaMode.STANDALONE,
        description="Kafka deployment mode (standalone, cluster, confluent)",
    )

    # === Connection Settings ===
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (comma-separated for cluster)",
    )

    # === Cluster Settings ===
    cluster_brokers: list[str] = Field(
        default_factory=list,
        description="List of broker host:port for cluster mode",
    )

    # === SASL/SSL Authentication ===
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = (
        Field(
            default="PLAINTEXT",
            description=(
                "Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL); "
                "SASL_PLAINTEXT is permanently unavailable — the validator always "
                "rejects it"
            ),
        )
    )
    allow_remote_plaintext: bool = Field(
        default=False,
        description=(
            "Acknowledge an unauthenticated PLAINTEXT connection to non-loopback "
            "Kafka brokers on a trusted private network"
        ),
    )
    sasl_mechanism: (
        Literal[
            "PLAIN",
            "SCRAM-SHA-256",
            "SCRAM-SHA-512",
            "GSSAPI",
            "OAUTHBEARER",
        ]
        | None
    ) = Field(
        default=None,
        description=(
            "SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, "
            "OAUTHBEARER); OAUTHBEARER is permanently unavailable — the validator "
            "always rejects it"
        ),
    )
    sasl_username: str | None = Field(
        default=None,
        description="SASL username",
    )
    sasl_password: SecretStr | None = Field(
        default=None,
        description="SASL password",
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
        description="Verify broker hostname matches certificate",
    )

    # === Confluent Cloud Settings ===
    confluent_api_key: SecretStr | None = Field(
        default=None,
        description="Confluent Cloud API key",
    )
    confluent_api_secret: SecretStr | None = Field(
        default=None,
        description="Confluent Cloud API secret",
    )
    confluent_bootstrap_servers: str | None = Field(
        default=None,
        description="Confluent Cloud bootstrap servers (e.g., pkc-xxx.us-east-1.aws.confluent.cloud:9092)",
    )

    # === Priority Queue Settings ===
    max_priority_partitions: int = Field(
        default=10,
        ge=1,
        le=255,
        description="Number of partitions for priority support",
    )

    # === Producer Settings ===
    acks: str | int = Field(
        default="all",
        description="Producer acks (0, 1, or 'all')",
    )
    retries: int = Field(
        default=3,
        ge=0,
        description="Number of send retries",
    )
    batch_size: int = Field(
        default=16384,
        ge=0,
        description="Batch size in bytes",
    )
    linger_ms: int = Field(
        default=5,
        ge=0,
        description="Time to wait for batching",
    )
    compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = Field(
        default=None,
        description="Compression type (gzip, snappy, lz4, zstd)",
    )
    max_in_flight_requests_per_connection: int = Field(
        default=5,
        ge=1,
        description="Max in-flight requests per connection",
    )

    # === Consumer Settings ===
    group_id: str = Field(
        default="scrapy-extension",
        description="Consumer group ID",
    )
    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        default="earliest",
        description="Auto offset reset (earliest, latest, none)",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description=(
            "Must remain False; the queue ack contract requires manual commit. "
            "Enabling auto-commit can ack a request before Scrapy processes it."
        ),
    )
    auto_commit_interval_ms: int = Field(
        default=5000,
        ge=0,
        description="Auto commit interval in ms",
    )
    max_poll_records: int = Field(
        default=500,
        ge=1,
        description="Max records per poll",
    )
    session_timeout_ms: int = Field(
        default=10000,
        ge=0,
        description="Session timeout in ms",
    )
    request_timeout_ms: int = Field(
        default=40000,
        gt=0,
        description="Request timeout in ms",
    )

    # === Topic Settings ===
    topic_name_generation: KafkaTopicNameGeneration = Field(
        default=KafkaTopicNameGeneration.AUTO,
        description=(
            "Physical topic mapping. auto preserves safe legacy topics and hashes "
            "logical names requiring v2; legacy drains existing topics only."
        ),
        json_schema_extra={"deprecated_values": ["legacy"]},
    )
    replication_factor: int = Field(
        default=1,
        ge=1,
        description="Topic replication factor",
    )
    num_partitions: int = Field(
        default=10,
        ge=1,
        description="Number of topic partitions",
    )
    retention_ms: int = Field(
        default=604800000,
        ge=0,
        description="Retention time in ms (7 days)",
    )
    min_insync_replicas: int = Field(
        default=1,
        ge=1,
        description="Minimum in-sync replicas for producer acks",
    )

    @field_validator("allow_remote_plaintext", mode="before")
    @classmethod
    def _normalize_remote_plaintext_opt_in(cls, value: object) -> bool:
        """Accept canonical environment booleans but reject truthy lookalikes."""
        return normalize_allow_remote_plaintext(value)

    @field_validator("bootstrap_servers", mode="after")
    @classmethod
    def _normalize_bootstrap_servers(cls, value: str) -> str:
        """Keep Kafka bootstrap syntax valid before the client reaches DNS."""
        return normalize_kafka_broker_endpoints(value, "bootstrap_servers")

    @field_validator("cluster_brokers", mode="after")
    @classmethod
    def _normalize_cluster_brokers(cls, value: list[str]) -> list[str]:
        """Validate each optional cluster member with the Kafka endpoint grammar."""
        return [
            normalize_kafka_broker_endpoints(endpoint, "cluster_brokers")
            for endpoint in value
        ]

    @field_validator("confluent_bootstrap_servers", mode="after")
    @classmethod
    def _normalize_confluent_bootstrap_servers(cls, value: str | None) -> str | None:
        """Preserve an empty optional override as the documented fallback signal."""
        if value is None:
            return None
        if not value.strip(" "):
            return ""
        return normalize_kafka_broker_endpoints(value, "confluent_bootstrap_servers")

    @field_validator("group_id", mode="before")
    @classmethod
    def _reject_blank_group_id(cls, value: object) -> object:
        """R141-F17: reject a blank consumer identity at settings time.

        Sibling backends reject blank identity fields (RocketMQ
        ``consumer_group``, Pulsar ``subscription_name``); a blank Kafka
        ``group_id`` reaches KafkaConsumer verbatim and surfaces as an opaque
        broker error at first poll rather than here.
        """
        if type(value) is not str or not value.strip():
            raise ConfigurationError(
                "Kafka group_id must be a non-empty string.",
                setting_name="group_id",
            )
        return value

    @field_validator("acks", mode="before")
    @classmethod
    def _reject_boolean_acks(cls, value: object) -> object:
        """Normalize env text while preventing bool-to-int coercion."""
        if isinstance(value, bool):
            raise ConfigurationError(
                "Kafka acks must be 1 or 'all', not a boolean.", setting_name="acks"
            )
        if value == "1":
            return 1
        return value

    @model_validator(mode="after")
    def _require_manual_commit(self) -> KafkaSettings:
        """R141-F18: enforce the queue ack contract at the settings layer.

        ``KafkaBackend.__init__`` re-checks this for duck-typed configs; the
        message is kept identical so both layers name the same contract.
        """
        if self.enable_auto_commit is not False:
            raise ConfigurationError(
                (
                    "KafkaBackend requires enable_auto_commit=False because queue "
                    "delivery completion is controlled by QueueBackend.ack(); "
                    "enabling Kafka auto-commit can commit a request before Scrapy "
                    "processes it."
                ),
                setting_name="enable_auto_commit",
                setting_value=True,
            )
        return self

    @model_validator(mode="after")
    def _validate_confluent_client_contract(self) -> KafkaSettings:
        """R141-F6: CONFLUENT TLS material / explicit protocols fail fast.

        Delegates to :func:`validate_kafka_confluent_client_contract`; see its
        docstring for the fixed-client rationale and the default-protocol
        exemption (explicitness is erased by capture-and-revalidate). Runs
        before ``_validate_authentication`` so the categorical CONFLUENT
        rejection wins over the (moot) TLS cert/key pairing rule.
        """
        validate_kafka_confluent_client_contract(
            self.mode,
            self.security_protocol,
            self.ssl_cafile,
            self.ssl_certfile,
            self.ssl_keyfile,
        )
        return self

    @model_validator(mode="after")
    def _validate_authentication(self) -> KafkaSettings:
        """Fail fast on incomplete or mechanism-inconsistent authentication."""
        validate_kafka_authentication(
            self.mode,
            self.security_protocol,
            self.sasl_mechanism,
            self.sasl_username,
            self.sasl_password,
            self.confluent_api_key,
            self.confluent_api_secret,
        )
        validate_kafka_transport_security(
            self.mode, self.security_protocol, self.ssl_check_hostname
        )
        uses_tls = (
            self.security_protocol in {"SSL", "SASL_SSL"}
            or self.mode == KafkaMode.CONFLUENT
        )
        if uses_tls and (self.ssl_certfile is None) != (self.ssl_keyfile is None):
            missing_name = (
                "ssl_keyfile" if self.ssl_certfile is not None else "ssl_certfile"
            )
            raise ConfigurationError(
                "Kafka TLS client authentication requires both certificate and "
                "key files.",
                setting_name=missing_name,
            )
        return self

    @model_validator(mode="after")
    def _require_remote_unauthenticated_plaintext_opt_in(self) -> KafkaSettings:
        """Require explicit acceptance before using remote anonymous PLAINTEXT."""
        allow_remote_plaintext = validate_allow_remote_plaintext(
            self.allow_remote_plaintext
        )
        has_authentication = any(
            value is not None
            for value in (
                self.sasl_mechanism,
                self.sasl_username,
                self.sasl_password,
                self.confluent_api_key,
                self.confluent_api_secret,
            )
        )
        if self.mode is KafkaMode.CLUSTER and self.cluster_brokers:
            endpoints = ",".join(self.cluster_brokers)
        elif self.mode is KafkaMode.CONFLUENT:
            endpoints = self.confluent_bootstrap_servers or self.bootstrap_servers
        else:
            endpoints = self.bootstrap_servers
        if (
            self.security_protocol == "PLAINTEXT"
            and not has_authentication
            and not _kafka_broker_endpoints_are_loopback(endpoints)
        ):
            require_remote_plaintext_opt_in("Kafka", allow_remote_plaintext)
        return self

    @model_validator(mode="after")
    def _validate_confluent_endpoint(self) -> KafkaSettings:
        """R26-E: CONFLUENT mode must not silently point at the localhost default.

        ``_bootstrap_servers`` resolves CONFLUENT via
        ``confluent_bootstrap_servers or bootstrap_servers``. If neither is set,
        CONFLUENT inherits the STANDALONE ``localhost:9092`` default and surfaces
        at connect() as an opaque SASL_SSL/PLAIN handshake error against
        localhost. R9b closed the PLAINTEXT dimension; R26-E closes the
        localhost-default-endpoint dimension. A real Confluent endpoint in
        EITHER field is accepted (the documented "reuse ``bootstrap_servers``"
        pattern is preserved); only the unchanged localhost default is rejected.
        """
        # R28-C: tighten to ``.strip()`` so an empty OR whitespace endpoint on
        # either field is treated as "no real endpoint". Pre-R28-C only the
        # literal ``localhost:9092`` default was rejected (R26-E), so empty /
        # whitespace ``bootstrap_servers`` / ``confluent_bootstrap_servers``
        # slipped through and surfaced at connect() as an opaque kafka-python
        # error.
        if (
            self.mode == KafkaMode.CONFLUENT
            and not (self.confluent_bootstrap_servers or "").strip()
        ):
            if (self.bootstrap_servers or "").strip() in ("", "localhost:9092"):
                raise ConfigurationError(
                    "Kafka CONFLUENT mode requires a real Confluent Cloud endpoint: set "
                    "'confluent_bootstrap_servers' (e.g. "
                    "pkc-xxx.us-east-1.aws.confluent.cloud:9092) or override "
                    "'bootstrap_servers' with a real endpoint. An empty, whitespace, or "
                    "localhost:9092 (the STANDALONE default) value cannot reach "
                    "Confluent Cloud.",
                    setting_name="bootstrap_servers",
                )
        return self

    @model_validator(mode="after")
    def _validate_delivery_policy(self) -> KafkaSettings:
        """Require confirmed sends and a coherent new-topic durability policy."""
        validate_kafka_delivery_policy(
            self.acks,
            self.max_priority_partitions,
            self.num_partitions,
            self.replication_factor,
            self.retention_ms,
            self.min_insync_replicas,
        )
        return self
