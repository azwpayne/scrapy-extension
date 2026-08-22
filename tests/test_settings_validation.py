"""Round 9a/9b — settings-validation tests (RED → GREEN).

This file pins parse-time rejection of invalid values for:

- SV1 (10 fields): free-form ``str`` fields that hold values from a closed set
  are converted to ``Literal[...]``. Typos that previously surfaced as opaque
  client-lib errors at first backend RPC now raise ``ValidationError`` at
  config time.
- SV5 (5 fields): empty-string ``host`` gaps and one unbounded int
  (``MemcachedSettings.port``) get pydantic ``Field`` constraints
  (``min_length``, ``ge``/``le``).
- SV2 (round 9b): mode-conditional ``model_validator(mode="after")`` rules
  raise ``ConfigurationError`` when a mode-specific required field is missing
  (MongoDB REPLICA_SET/ATLAS, Kafka CONFLUENT, RabbitMQ CLUSTER/MIRRORED_QUEUES).
- SV4 (round 9b): URL/scheme format guards raise ``ConfigurationError`` for
  bad schemes/patterns (MongoDB URI, Pulsar service_url, RocketMQ namesrv,
  ElasticSearch hosts, SQS/DynamoDB region_name).

Honest TDD: each test constructs with the INVALID input and asserts the
project's ``ConfigurationError`` (SV2/SV4) or pydantic ``ValidationError``
(SV1/SV5) post-fix. No ``xfail`` / ``skip`` / weakening. ``# type:
ignore[arg-type]`` is used ONLY where intentionally passing invalid input
(mirror the SV1 reject-test pattern).

Scope note: SV3 (round 9c) — cross-field auth/transport coherence:
Kafka SASL↔security_protocol, Pulsar auth_token↔pulsar+ssl, Redis
ssl_enabled↔ssl_cafile, MongoDB pool-size ordering, ElasticSearch
api_key↔username mutual exclusion, SQS/DynamoDB AWS creds both-or-neither.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    KafkaSettings,
    MemcachedSettings,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RedisMode,
    RedisSettings,
)
from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.dynamodb import DynamoDBSettings
from scrapy_extension.settings.elasticsearch import (
    ElasticSearchMode,
    ElasticSearchSettings,
)
from scrapy_extension.settings.kafka import KafkaMode
from scrapy_extension.settings.mongodb import MongoDBMode
from scrapy_extension.settings.pulsar import PulsarMode
from scrapy_extension.settings.rabbitmq import RabbitMQMode
from scrapy_extension.settings.redis import validate_redis_transport_security
from scrapy_extension.settings.rocketmq import RocketMQSettings
from scrapy_extension.settings.sqs import SqsSettings


class TestReactorLatencyPolicy:
    """The synchronous Scrapy contract has an explicit bounded latency budget."""

    def test_default_and_explicit_timeout(self) -> None:
        assert Settings().reactor_io_timeout == 5.0
        assert Settings(reactor_io_timeout=1.25).reactor_io_timeout == 1.25

    @pytest.mark.parametrize("value", [0, -1, 60.1, float("inf")])
    def test_timeout_is_bounded(self, value: float) -> None:
        with pytest.raises(ValidationError):
            Settings(reactor_io_timeout=value)


class TestPipelineStorageErrorPolicy:
    """The public settings model uses the fail-loud default consistently."""

    def test_default_is_reliability_safe(self) -> None:
        assert Settings().pipeline_max_storage_errors == 10

    def test_none_requires_explicit_opt_in(self) -> None:
        assert (
            Settings(pipeline_max_storage_errors=None).pipeline_max_storage_errors
            is None
        )


# ---------------------------------------------------------------------------
# SV1 — Literal enum types (10 fields)
# ---------------------------------------------------------------------------
# Each closed set is pulled from the corresponding client lib's valid options
# (kafka-python, pulsar-client, pika, pymongo). Values currently accepted by
# any valid config or exercised by any existing test MUST remain valid.


class TestKafkaLiterals:
    """KafkaSettings Literal fields (SV1)."""

    def test_security_protocol_rejects_typo(self) -> None:
        """`security_protocol="SAS_SSL"` (missing underscore) must reject."""
        with pytest.raises(ValidationError):
            KafkaSettings(security_protocol="SAS_SSL")  # type: ignore[arg-type]

    def test_security_protocol_rejects_lowercase(self) -> None:
        """Case-sensitive — `"plaintext"` is not a valid client-lib value."""
        with pytest.raises(ValidationError):
            KafkaSettings(security_protocol="plaintext")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "value",
        ["PLAINTEXT", "SSL", "SASL_SSL"],
    )
    def test_security_protocol_accepts_valid(self, value: str) -> None:
        """All four documented kafka-python security protocols stay valid."""
        authentication = (
            {
                "sasl_mechanism": "PLAIN",
                "sasl_username": "user",
                "sasl_password": "secret",
            }
            if value.startswith("SASL_")
            else {}
        )
        assert (
            KafkaSettings(
                security_protocol=value,  # type: ignore[arg-type]
                **authentication,
            ).security_protocol
            == value
        )

    def test_sasl_mechanism_rejects_lowercase(self) -> None:
        """`sasl_mechanism="plain"` silently fails auth today — must reject."""
        with pytest.raises(ValidationError):
            KafkaSettings(sasl_mechanism="plain")  # type: ignore[arg-type]

    def test_sasl_mechanism_rejects_typo(self) -> None:
        """`"SCRAM-SH-256"` (truncated) must reject."""
        with pytest.raises(ValidationError):
            KafkaSettings(sasl_mechanism="SCRAM-SH-256")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    def test_password_sasl_mechanism_accepts_complete_credentials(
        self, value: str
    ) -> None:
        """Password mechanisms remain valid with their required credential pair."""
        s = KafkaSettings(
            security_protocol="SASL_SSL",  # type: ignore[arg-type]
            sasl_mechanism=value,
            sasl_username="user",
            sasl_password="secret",  # type: ignore[arg-type]
        )
        assert s.sasl_mechanism == value

    def test_gssapi_mechanism_accepts_ambient_kerberos_credentials(self) -> None:
        """GSSAPI uses the process Kerberos context, not the PLAIN pair."""
        s = KafkaSettings(security_protocol="SASL_SSL", sasl_mechanism="GSSAPI")
        assert s.sasl_mechanism == "GSSAPI"

    def test_compression_type_rejects_typo(self) -> None:
        """`"snapy"` typo must reject (currently surfaces at producer create)."""
        with pytest.raises(ValidationError):
            KafkaSettings(compression_type="snapy")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["gzip", "snappy", "lz4", "zstd"])
    def test_compression_type_accepts_valid(self, value: str) -> None:
        """All four documented kafka-python codecs stay valid."""
        assert KafkaSettings(compression_type=value).compression_type == value

    def test_auto_offset_reset_rejects_typo(self) -> None:
        """`"earliet"` typo must reject."""
        with pytest.raises(ValidationError):
            KafkaSettings(auto_offset_reset="earliet")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["earliest", "latest", "none"])
    def test_auto_offset_reset_accepts_valid(self, value: str) -> None:
        """All three documented kafka-python offset resets stay valid."""
        assert KafkaSettings(auto_offset_reset=value).auto_offset_reset == value


class TestPulsarLiterals:
    """PulsarSettings Literal fields (SV1) — PascalCase per pulsar-client."""

    def test_consumer_type_rejects_lowercase_shared(self) -> None:
        """`consumer_type="shared"` (lowercase) must reject — client lib wants "Shared"."""
        with pytest.raises(ValidationError):
            PulsarSettings(consumer_type="shared")  # type: ignore[arg-type]

    def test_consumer_type_rejects_typo(self) -> None:
        """`"Faileover"` typo must reject."""
        with pytest.raises(ValidationError):
            PulsarSettings(consumer_type="Faileover")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["Shared", "Failover", "Exclusive", "Key_Shared"])
    def test_consumer_type_accepts_valid(self, value: str) -> None:
        """All four pulsar ConsumerType mappings stay valid (backend _consumer_type)."""
        assert PulsarSettings(consumer_type=value).consumer_type == value

    def test_initial_position_rejects_lowercase(self) -> None:
        """`"earliest"` (lowercase) must reject — client lib wants "Earliest"."""
        with pytest.raises(ValidationError):
            PulsarSettings(initial_position="earliest")  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", ["Earliest", "Latest"])
    def test_initial_position_accepts_valid(self, value: str) -> None:
        """Both pulsar InitialPosition mappings stay valid."""
        assert PulsarSettings(initial_position=value).initial_position == value


class TestRabbitMQLiterals:
    """RabbitMQSettings Literal fields (SV1)."""

    def test_ssl_verify_mode_rejects_typo(self) -> None:
        """`"CERT_REQ"` typo must reject (currently silently falls back)."""
        with pytest.raises(ValidationError):
            RabbitMQSettings(
                username="u",
                password="p",
                ssl_verify_mode="CERT_REQ",  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize("value", ["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"])
    def test_ssl_verify_mode_accepts_valid(self, value: str) -> None:
        """All three ssl.VerifyMode string mappings stay valid."""
        s = RabbitMQSettings(
            username="u",
            password="p",
            ssl_verify_mode=value,  # type: ignore[arg-type]
        )
        assert s.ssl_verify_mode == value

    def test_cluster_node_type_removed_is_rejected_as_extra(self) -> None:
        """R141-F16: ``cluster_node_type`` was dead config (grep-verified zero
        consumers in ``src/`` — the node type only steers broker-side clustering
        via ``rabbitmqctl`` and never reached the AMQP client). The field is
        removed; re-adding it must reject instead of being silently accepted
        (``extra="forbid"``), mirroring the R25-H RocketMQ tombstone."""
        with pytest.raises(ValidationError) as exc_info:
            RabbitMQSettings(
                username="u",
                password="p",
                cluster_node_type="disc",  # type: ignore[call-overload]
            )
        errors = exc_info.value.errors()
        assert errors
        assert all(error["type"] == "value_error" for error in errors)
        assert all(error["loc"] == ("configuration",) for error in errors)


class TestMongoDBLiterals:
    """MongoDBSettings Literal fields (SV1)."""

    def test_read_preference_rejects_typo(self) -> None:
        """`"primry"` typo must reject."""
        with pytest.raises(ValidationError):
            MongoDBSettings(read_preference="primry")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "value",
        [
            "primary",
            "primaryPreferred",
            "secondary",
            "secondaryPreferred",
            "nearest",
        ],
    )
    def test_read_preference_accepts_valid(self, value: str) -> None:
        """All five pymongo ReadPreference modes stay valid (camelCase)."""
        assert MongoDBSettings(read_preference=value).read_preference == value

    def test_auth_mechanism_rejects_typo(self) -> None:
        """`"SCRAM-SHA-25"` (truncated) must reject."""
        with pytest.raises(ValidationError):
            MongoDBSettings(auth_mechanism="SCRAM-SHA-25")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "value",
        ["SCRAM-SHA-1", "SCRAM-SHA-256", "MONGODB-CR", "PLAIN"],
    )
    def test_password_auth_mechanism_requires_complete_credentials(
        self, value: str
    ) -> None:
        """Password mechanisms cannot be silently omitted by the backend."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(auth_mechanism=value)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "username"

    @pytest.mark.parametrize(
        "value",
        ["SCRAM-SHA-1", "SCRAM-SHA-256", "MONGODB-CR", "PLAIN"],
    )
    def test_password_auth_mechanism_accepts_complete_credentials(
        self, value: str
    ) -> None:
        """Documented password mechanisms remain valid with a credential pair."""
        settings = MongoDBSettings(
            auth_mechanism=value,  # type: ignore[arg-type]
            username="crawler",
            password="not-a-real-secret",
        )
        assert settings.auth_mechanism == value

    @pytest.mark.parametrize("value", ["MONGODB-X509", "MONGODB-AWS"])
    def test_external_auth_mechanism_accepts_ambient_identity(self, value: str) -> None:
        """X.509 and AWS retain their driver-supported ambient identity path."""
        assert MongoDBSettings(auth_mechanism=value).auth_mechanism == value

    def test_gssapi_requires_and_accepts_a_username(self) -> None:
        """PyMongo requires a principal even when Kerberos supplies its ticket."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(auth_mechanism="GSSAPI")
        assert exc_info.value.setting_name == "username"

        settings = MongoDBSettings(
            auth_mechanism="GSSAPI", username="crawler@EXAMPLE.TEST"
        )
        assert settings.auth_mechanism == "GSSAPI"

    @pytest.mark.parametrize(
        ("auth_mechanism", "settings_kwargs", "setting_name"),
        [
            ("MONGODB-X509", {"password": "x509-secret"}, "password"),
            ("MONGODB-AWS", {"username": "aws-key"}, "password"),
            ("MONGODB-AWS", {"password": "aws-secret"}, "username"),
        ],
    )
    def test_external_auth_mechanism_rejects_invalid_credential_shape(
        self,
        auth_mechanism: str,
        settings_kwargs: dict[str, str],
        setting_name: str,
    ) -> None:
        """Mechanism-specific shapes fail before PyMongo's opaque constructor."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(auth_mechanism=auth_mechanism, **settings_kwargs)
        assert exc_info.value.setting_name == setting_name

    @pytest.mark.parametrize(
        "auth_mechanism", ["GSSAPI", "MONGODB-X509", "MONGODB-AWS"]
    )
    def test_external_auth_mechanism_rejects_nonexternal_source(
        self, auth_mechanism: str
    ) -> None:
        """External mechanisms must not pass an unsupported auth source to PyMongo."""
        settings_kwargs: dict[str, str] = {"auth_source": "other"}
        if auth_mechanism == "GSSAPI":
            settings_kwargs["username"] = "crawler@EXAMPLE.TEST"
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(auth_mechanism=auth_mechanism, **settings_kwargs)
        assert exc_info.value.setting_name == "auth_source"


class TestMongoDBWriteConcern:
    """Public mutation success must represent an acknowledged MongoDB write."""

    @pytest.mark.parametrize("w", [0, -1, False, "0", "-1", "", "custom-tag"])
    def test_unacknowledged_or_unsupported_w_is_rejected(self, w: object) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(w=w)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "w"

    @pytest.mark.parametrize(
        ("w", "expected"), [(1, 1), (2, 2), ("1", 1), ("majority", "majority")]
    )
    def test_acknowledged_w_is_normalized(
        self, w: int | str, expected: int | str
    ) -> None:
        assert MongoDBSettings(w=w).w == expected

    def test_integer_w_can_be_loaded_from_environment(self, monkeypatch) -> None:
        monkeypatch.setenv("SCRAPY_MONGO_W", "1")
        assert MongoDBSettings().w == 1

    def test_zero_w_from_environment_is_rejected(self, monkeypatch) -> None:
        monkeypatch.setenv("SCRAPY_MONGO_W", "0")
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings()
        assert exc_info.value.setting_name == "w"

    @pytest.mark.parametrize("w_timeout_ms", [-1, True])
    def test_invalid_write_timeout_is_rejected(self, w_timeout_ms: object) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(w_timeout_ms=w_timeout_ms)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "w_timeout_ms"

    def test_write_timeout_can_be_loaded_from_environment(self, monkeypatch) -> None:
        monkeypatch.setenv("SCRAPY_MONGO_W_TIMEOUT_MS", "5000")
        assert MongoDBSettings().w_timeout_ms == 5000


# ---------------------------------------------------------------------------
# SV5 — Empty-string + unbounded-int gaps (5 fields)
# ---------------------------------------------------------------------------


class TestMemcachedBounds:
    """MemcachedSettings Field constraints (SV5)."""

    def test_port_rejects_negative(self) -> None:
        """`port=-1` must reject — only unbounded int in the project."""
        with pytest.raises(ValidationError):
            MemcachedSettings(port=-1)

    def test_port_rejects_above_65535(self) -> None:
        """`port=99999` must reject."""
        with pytest.raises(ValidationError):
            MemcachedSettings(port=99999)

    def test_port_accepts_valid_range(self) -> None:
        """Boundaries 1 and 65535 stay valid."""
        assert MemcachedSettings(port=1).port == 1
        assert MemcachedSettings(port=65535).port == 65535

    def test_host_rejects_empty_string(self) -> None:
        """`host=""` must reject (opaque DNS failure today)."""
        with pytest.raises(ValidationError):
            MemcachedSettings(host="")


class TestMemcachedTrustedNetworkBoundary:
    @pytest.mark.parametrize("host", ["localhost", "localhost.", "127.0.0.1", "::1"])
    def test_loopback_plaintext_is_accepted_by_default(self, host: str) -> None:
        settings = MemcachedSettings(host=host)

        assert settings.allow_remote_plaintext is False

    def test_remote_plaintext_requires_explicit_trusted_network_opt_in(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedSettings(host="cache.internal")

        assert exc_info.value.setting_name == "allow_remote_plaintext"

    def test_remote_plaintext_can_be_explicitly_authorized(self) -> None:
        settings = MemcachedSettings(host="cache.internal", allow_remote_plaintext=True)

        assert settings.host == "cache.internal"
        assert settings.allow_remote_plaintext is True

    @pytest.mark.parametrize("host", ["user@cache.internal", "cache/path", "cache?x"])
    def test_host_rejects_url_components_without_retention(self, host: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            MemcachedSettings(host=host, allow_remote_plaintext=True)

        assert exc_info.value.setting_name == "host"
        assert host not in str(exc_info.value)
        assert host not in repr(exc_info.value.__dict__)


class TestRedisHostBounds:
    """RedisSettings host min_length (SV5)."""

    def test_host_rejects_empty_string(self) -> None:
        """`host=""` must reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            RedisSettings(host="")
        assert exc_info.value.setting_name == "host"


class TestRabbitMQHostBounds:
    """RabbitMQSettings host min_length (SV5)."""

    def test_host_rejects_empty_string(self) -> None:
        """`host=""` must reject."""
        with pytest.raises(ValidationError):
            RabbitMQSettings(username="u", password="p", host="")


class TestBaseRetryAttemptsCap:
    """Settings.retry_attempts sane upper cap (SV5)."""

    def test_retry_attempts_rejects_huge_value(self) -> None:
        """`retry_attempts=999999` is a DoS — must reject at the sane cap (le=20)."""
        with pytest.raises(ValidationError):
            Settings(retry_attempts=999999)

    def test_retry_attempts_accepts_zero_through_cap(self) -> None:
        """`0` (no retries) through 20 stay valid; 0 documented as no-retry."""
        assert Settings(retry_attempts=0).retry_attempts == 0
        assert Settings(retry_attempts=20).retry_attempts == 20

    def test_retry_attempts_rejects_above_cap(self) -> None:
        """`21` is above the cap — must reject."""
        with pytest.raises(ValidationError):
            Settings(retry_attempts=21)


# ---------------------------------------------------------------------------
# SV2 — Mode-conditional required-field validators (round 9b)
# ---------------------------------------------------------------------------
# Each validator mirrors the existing Redis SENTINEL pattern (now upgraded to
# raise the project's ``ConfigurationError`` with ``setting_name=``). Honest
# TDD: construct with the mode-but-missing-required-field and assert
# ``ConfigurationError`` naming the missing field.


class TestMongoDBModeConditional:
    """MongoDBSettings SV2 mode-conditional validators."""

    def test_replica_set_requires_replica_set_name(self) -> None:
        """REPLICA_SET mode without ``replica_set_name`` (and no ``?replicaSet=``
        in URI) must fail fast — driver otherwise can't find the RS."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(mode=MongoDBMode.REPLICA_SET)
        assert exc_info.value.setting_name == "replica_set_name"
        assert "replica_set_name" in str(exc_info.value)

    def test_replica_set_accepts_uri_with_replicaset_query(self) -> None:
        """REPLICA_SET mode + URI carrying ``?replicaSet=`` is valid (no name)."""
        s = MongoDBSettings(
            mode=MongoDBMode.REPLICA_SET,
            uri="mongodb://fallback-host:27017/?replicaSet=existing",
            allow_remote_plaintext=True,
        )
        assert s.replica_set_name is None  # URI hint satisfies the requirement

    def test_replica_set_uri_hint_is_case_insensitive(self) -> None:
        """PyMongo parses URI options case-insensitively, so the REPLICA_SET
        URI hint must too — lowercase / UPPER forms are valid (no name)."""
        for query in ("?replicaset=rs0", "?REPLICASET=rs0"):
            s = MongoDBSettings(
                mode=MongoDBMode.REPLICA_SET,
                uri=f"mongodb://fallback-host:27017/{query}",
                allow_remote_plaintext=True,
            )
            assert s.replica_set_name is None  # URI hint satisfies the requirement

    def test_replica_set_rejects_nested_replicaset_substring(self) -> None:
        """A ``replicaSet=`` substring nested inside another option's value
        (e.g. ``?appname=replicaSet=x``) is NOT a real replica-set declaration;
        the validator must still fail fast rather than defer to the driver."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(
                mode=MongoDBMode.REPLICA_SET,
                uri="mongodb://fallback-host:27017/?appname=replicaSet=x",
                allow_remote_plaintext=True,
            )
        assert exc_info.value.setting_name == "replica_set_name"

    def test_replica_set_accepts_explicit_name(self) -> None:
        """REPLICA_SET mode + explicit ``replica_set_name`` is valid."""
        s = MongoDBSettings(mode=MongoDBMode.REPLICA_SET, replica_set_name="rs0")
        assert s.replica_set_name == "rs0"

    def test_atlas_requires_srv_uri(self) -> None:
        """ATLAS mode requires the SRV URI consumed verbatim by the backend."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(mode=MongoDBMode.ATLAS, uri="mongodb://localhost:27017")
        assert exc_info.value.setting_name == "uri"

    def test_atlas_accepts_srv_uri(self) -> None:
        """ATLAS mode + ``mongodb+srv://`` URI is valid."""
        s = MongoDBSettings(
            mode=MongoDBMode.ATLAS,
            uri="mongodb+srv://cluster0.example.mongodb.net",
        )
        assert s.uri.startswith("mongodb+srv://")


class TestKafkaModeConditional:
    """KafkaSettings SV2 CONFLUENT mode validator."""

    def test_confluent_requires_api_key_and_secret(self) -> None:
        """CONFLUENT mode without ``confluent_api_key``/``confluent_api_secret``
        must fail fast — silent PLAINTEXT-localhost fallback today."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(mode=KafkaMode.CONFLUENT)
        # The first missing field is named.
        assert exc_info.value.setting_name in {
            "confluent_api_key",
            "confluent_api_secret",
        }
        msg = str(exc_info.value)
        assert "confluent_api_key" in msg
        assert "confluent_api_secret" in msg

    def test_confluent_rejects_key_without_secret(self) -> None:
        """CONFLUENT + key but no secret must reject (incomplete credentials)."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret=None,
            )
        assert exc_info.value.setting_name == "confluent_api_secret"

    @pytest.mark.parametrize(
        ("api_key", "api_secret", "setting_name"),
        [
            (" ", "secret", "confluent_api_key"),
            ("key", "\t", "confluent_api_secret"),
            (" ", "\n", "confluent_api_key"),
        ],
    )
    def test_confluent_rejects_blank_credentials(
        self, api_key: str, api_secret: str, setting_name: str
    ) -> None:
        """Explicit whitespace cannot downgrade Confluent to SDK PLAINTEXT defaults."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key=api_key,  # type: ignore[arg-type]
                confluent_api_secret=api_secret,  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == setting_name

    def test_confluent_accepts_key_and_secret(self) -> None:
        """CONFLUENT + key + secret + a real endpoint is valid (the intended
        Confluent Cloud path). R26-E: the endpoint must not be the localhost
        default — see test_confluent_rejects_localhost_default_endpoint."""
        s = KafkaSettings(
            mode=KafkaMode.CONFLUENT,
            confluent_api_key="key",  # type: ignore[arg-type]
            confluent_api_secret="secret",  # type: ignore[arg-type]
            confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
        )
        assert s.confluent_api_key is not None

    def test_confluent_rejects_localhost_default_endpoint(self) -> None:
        """R26-E: CONFLUENT mode still pointing at the localhost:9092 STANDALONE
        default (no confluent_bootstrap_servers, no bootstrap_servers override)
        fails at construction. Pre-R26-E this passed and surfaced at connect()
        as an opaque SASL_SSL/PLAIN handshake error against localhost. R9b closed
        the PLAINTEXT dimension; R26-E closes the localhost-default-endpoint
        dimension. A real endpoint in either field is accepted.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
                # no confluent_bootstrap_servers, bootstrap_servers stays localhost:9092
            )
        assert exc_info.value.setting_name == "bootstrap_servers"

    def test_confluent_rejects_empty_bootstrap_servers(self) -> None:
        """R28-C: CONFLUENT + empty ``bootstrap_servers`` must reject.

        R26-E's guard only rejected the literal ``localhost:9092`` default, so an
        empty ``bootstrap_servers`` (e.g. ``SCRAPY_KAFKA_BOOTSTRAP_SERVERS=`` set
        to an empty value) with no ``confluent_bootstrap_servers`` slipped through
        and surfaced at connect() as an opaque kafka-python error. Same fail-fast
        promise as R26-E, gap on the empty-value axis.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
                bootstrap_servers="",
            )
        assert exc_info.value.setting_name == "bootstrap_servers"

    def test_confluent_rejects_whitespace_endpoint(self) -> None:
        """R28-C: CONFLUENT + whitespace-only endpoint must reject.

        A whitespace ``confluent_bootstrap_servers`` is not a real endpoint; pre-R28-C
        it slipped through (``not "   "`` is False, so the outer guard was skipped
        entirely). Tightening to ``.strip()`` catches whitespace on either field.
        """
        with pytest.raises(ConfigurationError):
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
                confluent_bootstrap_servers="   ",
            )

    def test_non_confluent_rejects_ignored_confluent_credentials(self) -> None:
        """Dedicated cloud credentials cannot be silently ignored in another mode."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == "mode"


class TestKafkaDeliveryPolicy:
    """Kafka queue success requires broker-confirmed, coherent topic policy."""

    @pytest.mark.parametrize("acks", [0, -1, True, "0", "leader"])
    def test_unconfirmed_or_unsupported_acks_rejected(self, acks: object) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(acks=acks)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "acks"

    @pytest.mark.parametrize(("acks", "expected"), [(1, 1), ("1", 1), ("all", "all")])
    def test_confirmed_acks_values_remain_valid(
        self, acks: int | str, expected: int | str
    ) -> None:
        assert KafkaSettings(acks=acks).acks == expected

    def test_integer_acks_can_be_loaded_from_environment(self, monkeypatch) -> None:
        monkeypatch.setenv("SCRAPY_KAFKA_ACKS", "1")
        assert KafkaSettings().acks == 1

    def test_min_insync_replicas_cannot_exceed_replication_factor(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(replication_factor=2, min_insync_replicas=3)
        assert exc_info.value.setting_name == "min_insync_replicas"

    def test_partition_settings_cannot_disagree(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(num_partitions=3, max_priority_partitions=4)
        assert exc_info.value.setting_name == "num_partitions"


class TestKafkaConfluentClientContract:
    """R141-F6: CONFLUENT mode fails fast on client config it cannot honor.

    The backend's CONFLUENT branch builds a fixed SASL_SSL/PLAIN client
    (``_build_client_security_config``): ``ssl_cafile`` / ``ssl_certfile`` /
    ``ssl_keyfile`` never reach the consumer/admin clients and any
    ``security_protocol`` value is silently overridden. Pre-F6 both
    configurations passed settings validation, so a pinned private CA
    "succeeded" without ever reaching the SDK.
    """

    @pytest.mark.parametrize(
        "setting_name", ["ssl_cafile", "ssl_certfile", "ssl_keyfile"]
    )
    def test_confluent_rejects_dropped_tls_material(self, setting_name: str) -> None:
        """Validated CA/cert/key paths would be silently dropped by the fixed client."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
                confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
                **{setting_name: "/ca.pem"},
            )
        assert exc_info.value.setting_name == setting_name

    def test_confluent_rejects_explicit_ssl_protocol(self) -> None:
        """An explicit ``SSL`` protocol would be silently overridden (SASL_SSL)."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                mode=KafkaMode.CONFLUENT,
                confluent_api_key="key",  # type: ignore[arg-type]
                confluent_api_secret="secret",  # type: ignore[arg-type]
                confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
                security_protocol="SSL",  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == "security_protocol"

    def test_confluent_accepts_explicit_sasl_ssl_protocol(self) -> None:
        """The one protocol value the fixed client actually uses stays valid.

        The pre-existing SASL coherence rules still apply to an explicit
        ``SASL_SSL``: it must carry an explicit mechanism and password
        credentials. F6 only forbids values the fixed client would override.
        """
        s = KafkaSettings(
            mode=KafkaMode.CONFLUENT,
            confluent_api_key="key",  # type: ignore[arg-type]
            confluent_api_secret="secret",  # type: ignore[arg-type]
            confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pw",  # type: ignore[arg-type]
        )
        assert s.security_protocol == "SASL_SSL"

    def test_confluent_accepts_unset_default_protocol(self) -> None:
        """An unset ``security_protocol`` keeps every existing CONFLUENT flow valid.

        The field default is indistinguishable from an explicitly-set-to-default
        value after the backend's capture-and-revalidate rebuild (``model_validate``
        of a full ``__dict__`` marks every field as set), and the fixed client
        overrides both identically — so only values that differ from BOTH the
        default and the fixed ``SASL_SSL`` protocol are refused.
        """
        s = KafkaSettings(
            mode=KafkaMode.CONFLUENT,
            confluent_api_key="key",  # type: ignore[arg-type]
            confluent_api_secret="secret",  # type: ignore[arg-type]
            confluent_bootstrap_servers="pkc-xxx.us-east-1.aws.confluent.cloud:9092",
        )
        assert s.security_protocol == "PLAINTEXT"

    def test_non_confluent_modes_keep_tls_material(self) -> None:
        """STANDALONE/CLUSTER still pass CA material through to the client."""
        s = KafkaSettings(security_protocol="SSL", ssl_cafile="/ca.pem")
        assert s.ssl_cafile == "/ca.pem"

    def test_confluent_contract_messages_are_static(self) -> None:
        """Exact diagnostics pinned via the helper, outside the sanitize boundary."""
        from scrapy_extension.settings.kafka import (
            validate_kafka_confluent_client_contract,
        )

        with pytest.raises(ConfigurationError) as tls_error:
            validate_kafka_confluent_client_contract(
                KafkaMode.CONFLUENT, "SASL_SSL", "/ca.pem", None, None
            )
        assert tls_error.value.setting_name == "ssl_cafile"
        assert "fixed SASL_SSL client" in str(tls_error.value)

        with pytest.raises(ConfigurationError) as protocol_error:
            validate_kafka_confluent_client_contract(
                KafkaMode.CONFLUENT, "SSL", None, None, None
            )
        assert protocol_error.value.setting_name == "security_protocol"
        assert "fixed SASL_SSL client" in str(protocol_error.value)

    def test_confluent_protocol_hint_recommends_unset_not_sasl_ssl(self) -> None:
        """V2-3: the hint must not steer users into the second-layer SASL error.

        The old hint offered "leave security_protocol unset or set it to
        'SASL_SSL'" as equal remedies, but an explicit 'SASL_SSL' trips the
        SASL coherence rules (it demands an explicit mechanism plus password
        credentials) which the fixed CONFLUENT client then overrides anyway —
        a second error chasing a misdirection. The hint must recommend leaving
        ``security_protocol`` unset and mark an explicit 'SASL_SSL' as
        requiring material the fixed client overrides (not recommended).
        """
        from scrapy_extension.settings.kafka import (
            validate_kafka_confluent_client_contract,
        )

        with pytest.raises(ConfigurationError) as exc_info:
            validate_kafka_confluent_client_contract(
                KafkaMode.CONFLUENT, "SSL", None, None, None
            )
        message = str(exc_info.value)
        # Correct action first: leave the setting unset.
        assert "leave security_protocol unset" in message
        # The misdirection (unset and explicit SASL_SSL framed as equal
        # remedies) must be gone.
        assert "or set it to 'SASL_SSL'" not in message
        # If set anyway: full SASL material is required and gets overridden
        # by the fixed client, so it must be flagged as not recommended.
        assert "SASL material" in message
        assert "not recommended" in message


class TestKafkaGroupIdContract:
    """R141-F17: blank consumer identity is rejected at settings time.

    Sibling backends reject blank identity fields (RocketMQ ``consumer_group``,
    Pulsar ``subscription_name``); a blank Kafka ``group_id`` reaches
    KafkaConsumer verbatim and surfaces as an opaque broker error at first
    poll — later and less actionable than config time.
    """

    @pytest.mark.parametrize("group_id", ["", "   ", "\t\n "])
    def test_blank_group_id_rejected(self, group_id: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(group_id=group_id)
        assert exc_info.value.setting_name == "group_id"

    def test_non_string_group_id_rejected(self) -> None:
        """Non-str input raises the typed error family, not a pydantic failure."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(group_id=123)  # type: ignore[arg-type]
        assert not isinstance(exc_info.value, ValidationError)
        assert exc_info.value.setting_name == "group_id"

    def test_non_blank_group_id_accepted(self) -> None:
        assert KafkaSettings(group_id="worker-1").group_id == "worker-1"
        assert KafkaSettings().group_id == "scrapy-extension"


class TestKafkaManualCommitContract:
    """R141-F18: the settings layer enforces the queue ack contract up front.

    Pre-F18 ``enable_auto_commit=True`` was presented as an available option
    but every real ``KafkaBackend`` construction rejected it — the wrong
    failure layer. The settings validator now raises the backend's exact
    contract message at config time (the backend guard remains for duck-typed
    configs).
    """

    _BACKEND_CONTRACT_MESSAGE = (
        "KafkaBackend requires enable_auto_commit=False because queue "
        "delivery completion is controlled by QueueBackend.ack(); enabling "
        "Kafka auto-commit can commit a request before Scrapy processes it."
    )

    def test_enable_auto_commit_true_rejected_at_settings_layer(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(enable_auto_commit=True)
        assert exc_info.value.setting_name == "enable_auto_commit"
        # Until the G8 safe-list sync lands, the settings boundary substitutes
        # its generic placeholder for the not-yet-safe-listed static message.
        assert exc_info.value.args[0] in (
            self._BACKEND_CONTRACT_MESSAGE,
            "Settings contain an invalid configuration value.",
        )

    def test_enable_auto_commit_false_stays_the_only_valid_value(self) -> None:
        assert KafkaSettings(enable_auto_commit=False).enable_auto_commit is False
        assert KafkaSettings().enable_auto_commit is False


class TestRabbitMQModeConditional:
    """RabbitMQSettings SV2 CLUSTER/MIRRORED_QUEUES validators."""

    def test_cluster_requires_cluster_nodes(self) -> None:
        """CLUSTER mode without ``cluster_nodes`` must fail fast — operator asked
        for a cluster but only one host:port is wired."""
        with pytest.raises(ConfigurationError) as exc_info:
            RabbitMQSettings(username="u", password="p", mode=RabbitMQMode.CLUSTER)
        assert exc_info.value.setting_name == "cluster_nodes"

    def test_standalone_rejects_cluster_nodes(self) -> None:
        """R141-F16: STANDALONE + ``cluster_nodes`` must fail fast.

        The standalone connect path dials ``host:port`` only, but the nodes
        still counted as endpoints for TLS/guest classification — the security
        posture and the actual connection surface disagreed (multi-node
        config silently degraded to a single point). Non-selected topology
        nodes fail rather than being ignored, mirroring the Redis mode-intent
        rejection contract."""
        with pytest.raises(ConfigurationError) as exc_info:
            RabbitMQSettings(
                username="u",
                password="p",
                mode=RabbitMQMode.STANDALONE,
                cluster_nodes=["node2:5672"],
            )
        assert exc_info.value.setting_name == "cluster_nodes"

    def test_standalone_without_cluster_nodes_remains_valid(self) -> None:
        """STANDALONE without ``cluster_nodes`` is the plain single-node path."""
        s = RabbitMQSettings(username="u", password="p")
        assert s.mode == RabbitMQMode.STANDALONE
        assert s.cluster_nodes == []

    def test_cluster_accepts_cluster_nodes(self) -> None:
        """CLUSTER mode + ``cluster_nodes`` is valid."""
        s = RabbitMQSettings(
            username="u",
            password="p",
            mode=RabbitMQMode.CLUSTER,
            cluster_nodes=["node2:5672", "node3:5672"],
            ssl_enabled=True,
        )
        assert len(s.cluster_nodes) == 2

    def test_mirrored_queues_requires_ha_mode(self) -> None:
        """MIRRORED_QUEUES mode without ``ha_mode`` must fail fast — connect path
        silently skips HA policy setup otherwise."""
        with pytest.raises(ConfigurationError) as exc_info:
            RabbitMQSettings(
                username="u", password="p", mode=RabbitMQMode.MIRRORED_QUEUES
            )
        assert exc_info.value.setting_name == "ha_mode"

    def test_mirrored_queues_accepts_ha_mode_without_cluster_nodes(self) -> None:
        """MIRRORED_QUEUES + ``ha_mode`` is valid even without ``cluster_nodes``
        (single-node-mirrored is a supported dev topology — backend uses
        ``host:port``). Pins the no-API-break scope decision."""
        s = RabbitMQSettings(
            username="u",
            password="p",
            mode=RabbitMQMode.MIRRORED_QUEUES,
            ha_mode="all",
        )
        assert s.ha_mode == "all"

    def test_mirrored_queues_accepts_cluster_nodes(self) -> None:
        """MIRRORED_QUEUES + ``cluster_nodes`` stays valid: the mirrored connect
        path dials the full node list, so cluster_nodes remain consumed there
        (only the STANDALONE non-selected-topology rejection is new)."""
        s = RabbitMQSettings(
            username="u",
            password="p",
            mode=RabbitMQMode.MIRRORED_QUEUES,
            ha_mode="all",
            cluster_nodes=["node2:5672"],
            ssl_enabled=True,
        )
        assert s.cluster_nodes == ["node2:5672"]

    def test_mirrored_queues_ha_mode_whitespace_rejected(self) -> None:
        """R30-B: whitespace ``ha_mode`` must reject — the check used bare truthiness
        (``not self.ha_mode``), so ``"   "`` bypassed it and surfaced later as a
        misleading 'ha_mode required' at connect. Strip-aware (mirrors R29-D)."""
        with pytest.raises(ConfigurationError) as exc_info:
            RabbitMQSettings(
                username="u",
                password="p",
                mode=RabbitMQMode.MIRRORED_QUEUES,
                ha_mode="   ",
            )
        assert exc_info.value.setting_name == "ha_mode"


class TestRabbitMQPrefetchValidation:
    """RabbitMQSettings R139-F4: ``prefetch_size`` must stay 0.

    RabbitMQ does not implement byte-based prefetch: a nonzero
    ``prefetch_size`` is accepted by pika but rejected by the broker with
    NOT_IMPLEMENTED, closing the just-opened channel at connect. The knob
    must fail fast at configuration time instead.
    """

    def test_prefetch_size_nonzero_rejected(self) -> None:
        """A nonzero ``prefetch_size`` must raise ConfigurationError naming
        the setting and stating both the unimplemented-feature and the
        channel-closing consequence."""
        with pytest.raises(ConfigurationError) as exc_info:
            RabbitMQSettings(prefetch_size=1024)
        assert exc_info.value.setting_name == "prefetch_size"
        message = str(exc_info.value)
        assert "byte-based prefetch" in message
        assert "closes the channel" in message

    def test_prefetch_size_zero_is_valid(self) -> None:
        """``prefetch_size=0`` (the only supported value) constructs fine."""
        assert RabbitMQSettings(prefetch_size=0).prefetch_size == 0

    def test_default_prefetch_size_is_zero(self) -> None:
        """The default must remain the one valid value."""
        assert RabbitMQSettings().prefetch_size == 0

    def test_prefetch_count_remains_accepted(self) -> None:
        """``prefetch_count`` stays a valid knob (0 = unlimited); only the
        byte-based half is rejected."""
        assert RabbitMQSettings(prefetch_count=10).prefetch_count == 10


# ---------------------------------------------------------------------------
# SV4 — URL/scheme format guards (round 9b)
# ---------------------------------------------------------------------------


class TestMongoDBUriScheme:
    """MongoDBSettings.uri SV4 scheme guard."""

    def test_uri_rejects_bare_host_port(self) -> None:
        """``uri="localhost:27017"`` must reject — opaque InvalidURI today."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(uri="localhost:27017")  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "uri"

    def test_uri_rejects_empty_string(self) -> None:
        """``uri=""`` must reject (rejected by the field validator)."""
        with pytest.raises(ConfigurationError):
            MongoDBSettings(uri="")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "uri",
        [
            "mongodb://localhost:27017",
            "mongodb+srv://cluster0.example.mongodb.net",
            "mongodb://host:27017/?replicaSet=rs0",
        ],
    )
    def test_uri_accepts_valid_schemes(self, uri: str) -> None:
        """Valid ``mongodb://`` and ``mongodb+srv://`` URIs stay accepted."""
        settings = MongoDBSettings(
            uri=uri, allow_remote_plaintext="localhost" not in uri
        )
        assert settings.uri == uri


class TestPulsarServiceUrlScheme:
    """PulsarSettings.service_url SV4 scheme guard."""

    def test_service_url_rejects_bare_host_port(self) -> None:
        """``service_url="broker:6650"`` must reject — opaque ValueError today."""
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(service_url="broker:6650")  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "service_url"

    def test_service_url_rejects_http_scheme(self) -> None:
        """``http://`` is not a Pulsar scheme — must reject."""
        with pytest.raises(ConfigurationError):
            PulsarSettings(service_url="http://broker:6650")  # type: ignore[arg-type]

    def test_service_url_rejects_empty(self) -> None:
        """Empty string must reject."""
        with pytest.raises(ConfigurationError):
            PulsarSettings(service_url="")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "url",
        ["pulsar://localhost:6650", "pulsar+ssl://broker:6651"],
    )
    def test_service_url_accepts_valid_schemes(self, url: str) -> None:
        """Valid ``pulsar://`` and ``pulsar+ssl://`` URLs stay accepted."""
        assert PulsarSettings(service_url=url).service_url == url

    def test_service_url_keeps_scheme_case_migration_compatibility(self) -> None:
        """Scheme case remains normalized when the authority is already strict."""
        assert PulsarSettings(service_url="PULSAR://localhost:6650").service_url == (
            "pulsar://localhost:6650"
        )

    @pytest.mark.parametrize(
        "service_url",
        [
            " Pulsar+SSL://broker:6651 ",
            "pulsar://one:6650, two:6650",
            "pulsar://one:6650\\n",
        ],
    )
    def test_service_url_rejects_authority_whitespace(self, service_url: str) -> None:
        """Whitespace is not migrated or trimmed into an SDK authority."""
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(service_url=service_url)
        assert exc_info.value.setting_name == "service_url"

    def test_cluster_service_url_rejects_repeated_schemes(self) -> None:
        """The SDK expects one scheme followed by a comma-separated host list."""
        with pytest.raises(ConfigurationError, match="single scheme"):
            PulsarSettings(
                mode=PulsarMode.CLUSTER,
                service_url="pulsar://one:6650,pulsar://two:6650",
            )

    @pytest.mark.parametrize(
        "url",
        [
            "pulsar://broker:abc",
            "pulsar://broker:",
            "pulsar://broker:0",
            "pulsar://broker: 6650",
            "pulsar://one:6650,two:bad",
            "pulsar://:6650",
            "pulsar://broker:70000",
            "pulsar://broker/path",
            "pulsar://broker:6650?secret=value",
        ],
    )
    def test_service_url_rejects_malformed_endpoint_member(self, url: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(service_url=url)
        assert exc_info.value.setting_name == "service_url"

    def test_service_url_accepts_bracketed_ipv6_endpoint(self) -> None:
        assert PulsarSettings(service_url="pulsar://[::1]:6650").service_url


class TestRocketMQNamesrvFormat:
    """RocketMQSettings.namesrv_address SV4 ``host:port`` guard."""

    def test_namesrv_rejects_scheme_prefix(self) -> None:
        """``http://namesrv:9876`` must reject — client wants bare ``host:port``."""
        with pytest.raises(ConfigurationError) as exc_info:
            RocketMQSettings(namesrv_address="http://namesrv:9876")  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "namesrv_address"

    def test_namesrv_rejects_bare_host(self) -> None:
        """``localhost`` (no port) must reject."""
        with pytest.raises(ConfigurationError):
            RocketMQSettings(namesrv_address="localhost")  # type: ignore[arg-type]

    def test_namesrv_rejects_non_numeric_port(self) -> None:
        """``host:abc`` must reject (port must be digits)."""
        with pytest.raises(ConfigurationError):
            RocketMQSettings(namesrv_address="namesrv:abc")  # type: ignore[arg-type]

    def test_namesrv_rejects_empty(self) -> None:
        """Empty string must reject."""
        with pytest.raises(ConfigurationError):
            RocketMQSettings(namesrv_address="")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "addr",
        ["localhost:9876", "rocketmq-cluster:9876", "10.0.0.1:9876"],
    )
    def test_namesrv_accepts_valid_host_port(self, addr: str) -> None:
        """Valid ``host:port`` values stay accepted (incl. DNS, IPv4)."""
        settings = RocketMQSettings(
            namesrv_address=addr,
            allow_remote_plaintext=not addr.startswith(("localhost", "127.")),
        )
        assert settings.namesrv_address == addr

    def test_namesrv_accepts_and_canonicalizes_cluster_endpoints(self) -> None:
        settings = RocketMQSettings(
            mode="cluster",  # type: ignore[arg-type]
            namesrv_address=" 192.0.2.10:8081 ; 192.0.2.11:8082 ",
            allow_remote_plaintext=True,
        )
        assert settings.namesrv_address == "192.0.2.10:8081;192.0.2.11:8082"

    @pytest.mark.parametrize(
        "addr", ["one:8081;", ";one:8081", "one:8081;;two:8082", "one:8081;two:abc"]
    )
    def test_namesrv_rejects_invalid_cluster_endpoint_member(self, addr: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            RocketMQSettings(namesrv_address=addr)
        assert exc_info.value.setting_name == "namesrv_address"


class TestRocketMQConfigEdges:
    """R27-RMQ-1/2: rocketmq config-edge guards (zero floor, blank name)."""

    def test_max_message_size_zero_rejected(self) -> None:
        """R27-RMQ-1: ``max_message_size=0`` must reject — every non-empty push
        would fail (backend unusable). Pre-R27-RMQ-1 ``ge=0`` accepted zero."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RocketMQSettings(max_message_size=0)  # type: ignore[arg-type]

    def test_max_message_size_negative_rejected(self) -> None:
        """Negative also rejected by the ``gt=0`` floor."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RocketMQSettings(max_message_size=-1)  # type: ignore[arg-type]

    def test_consumer_group_empty_rejected(self) -> None:
        """R27-RMQ-2: empty ``consumer_group`` must reject (``min_length=1``) —
        opaque SimpleConsumer error at connect otherwise."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            RocketMQSettings(consumer_group="")

    def test_consumer_group_whitespace_rejected(self) -> None:
        """R27-RMQ-2: whitespace ``consumer_group`` must reject — ``min_length``
        alone admits ``"   "``; the field_validator strips and raises."""
        with pytest.raises(ConfigurationError):
            RocketMQSettings(consumer_group="   ")


class TestElasticSearchHostsScheme:
    """ElasticSearchSettings.hosts SV4 scheme guard (no-creds case)."""

    def test_hosts_rejects_bare_host_port(self) -> None:
        """``hosts=["localhost:9200"]`` must reject — opaque transport error today
        (elasticsearch-py does not infer a default scheme)."""
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(hosts=["localhost:9200"])  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "hosts"

    def test_hosts_rejects_empty_entry(self) -> None:
        """Empty string in ``hosts`` must reject."""
        with pytest.raises(ConfigurationError):
            ElasticSearchSettings(hosts=[""])  # type: ignore[arg-type]

    def test_hosts_rejects_any_bad_entry_in_mixed_list(self) -> None:
        """One bad entry in a mixed list must reject without echoing host input."""
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=["https://good:9200", "bad:9200"]  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == "hosts"
        assert "bad:9200" not in str(exc_info.value)

    @pytest.mark.parametrize(
        "hosts",
        [
            ["http://localhost:9200"],
            ["https://es.example.com:9200"],
            ["http://h1:9200", "https://h2:9200"],
        ],
    )
    def test_hosts_accepts_valid_schemes(self, hosts: list[str]) -> None:
        """All-valid ``http://`` / ``https://`` lists stay accepted."""
        settings = ElasticSearchSettings(
            hosts=hosts,
            allow_remote_plaintext=any(
                host.startswith("http://") and "localhost" not in host for host in hosts
            ),
        )
        assert settings.hosts == hosts

    def test_standalone_empty_hosts_list_rejected(self) -> None:
        """R28-B: STANDALONE ``hosts=[]`` must reject — opaque client error otherwise.

        ``_validate_hosts_scheme`` filtered each entry's scheme but not the empty
        list itself, so ``hosts=[]`` (e.g. ``SCRAPY_ELASTICSEARCH_HOSTS=`` set to
        an empty value) trivially passed and surfaced as an opaque elasticsearch-py
        client error at connect(). The validator's docstring said "Empty strings
        are rejected" but an empty LIST was not.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(mode=ElasticSearchMode.STANDALONE, hosts=[])
        assert exc_info.value.setting_name == "hosts"

    def test_cloud_empty_hosts_list_accepted(self) -> None:
        """R28-B: CLOUD ``hosts=[]`` is fine — CLOUD uses ``cloud_id``, not hosts.

        Locks the mode-gate intent: the empty-list guard is STANDALONE-only so it
        cannot false-positive a CLOUD config that happens to carry an empty hosts
        list (hosts is unused in CLOUD).
        """
        from pydantic import SecretStr

        s = ElasticSearchSettings(
            mode=ElasticSearchMode.CLOUD,
            cloud_id="test:abc",
            api_key=SecretStr("k"),
            hosts=[],
        )
        assert s.cloud_id == "test:abc"

    @pytest.mark.parametrize(
        "host",
        [
            "https://user:secret@es.example:9200",
            "https://es.example:9200?api_key=secret",
            "https://es.example:9200#secret",
            "https://es%00.example:9200",
            "https://es.example" + chr(127) + ":9200",
            "https://es.example:9200\n",
            "https:///missing-host",
            "https://es.example:not-a-port",
        ],
    )
    def test_hosts_reject_unsafe_or_malformed_authority(self, host: str) -> None:
        """R45: host URLs cannot smuggle credentials or malformed authority.

        The exception deliberately must not echo the input: a URL userinfo or query
        can contain a credential, and settings errors are commonly logged.
        """
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(hosts=[host])
        assert exc_info.value.setting_name == "hosts"
        assert "secret" not in str(exc_info.value)


class TestElasticSearchCapabilityIsolation:
    """R45: destructive capability operations need distinct ES indices."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"queue_index": "shared", "set_index": "shared"},
            {"queue_index": "shared", "storage_index": "shared"},
            {"set_index": "shared", "storage_index": "shared"},
        ],
    )
    def test_index_collisions_rejected(self, kwargs: dict[str, str]) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(**kwargs)
        assert exc_info.value.setting_name == "queue_index"

    @pytest.mark.parametrize("field", ["queue_index", "set_index", "storage_index"])
    @pytest.mark.parametrize(
        "invalid_name",
        [
            "Queue",
            "queue*",
            "queue/name",
            "queue:name",
            "queue#name",
            "_queue",
            "café",
        ],
    )
    def test_index_names_reject_wildcards_and_driver_unsafe_syntax(
        self, field: str, invalid_name: str
    ) -> None:
        """Index settings cannot retarget wildcard or another capability path."""
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(**{field: invalid_name})

        error = exc_info.value
        assert error.setting_name == field
        assert invalid_name not in str(error)
        assert error.__cause__ is None

    @pytest.mark.parametrize("field", ["queue_index", "set_index", "storage_index"])
    def test_blank_index_rejected(self, field: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(**{field: " \t "})
        assert exc_info.value.setting_name == field

    def test_distinct_indices_remain_valid(self) -> None:
        settings = ElasticSearchSettings(
            queue_index="jobs", set_index="seen", storage_index="items"
        )
        assert settings.storage_index == "items"


class TestDynamoDBTableName:
    """DynamoDB table names are resource identifiers, not arbitrary strings."""

    @pytest.mark.parametrize(
        "table_name",
        ["x", "", "table/name", "table?secret", "table name", "table\\x00name"],
    )
    def test_invalid_table_names_fail_before_client_io(self, table_name: str) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            DynamoDBSettings(table_name=table_name)

        error = exc_info.value
        assert error.setting_name == "table_name"
        assert "secret" not in str(error)
        assert error.__cause__ is None


class TestAwsRegionNameFormat:
    """SQS + DynamoDB ``region_name`` SV4 regex guard.

    Catches structural typos (missing parts, wrong casing, extra suffixes,
    empty) while allowing the variable label counts used by GovCloud, ISO, and
    EUSC. The structural grammar cannot catch same-shape word typos like
    ``us-eat-1`` (intended ``us-east-1``); that requires a known-region
    allowlist, which is deliberately out of scope because it would reject future
    launches until this package updated.
    """

    @pytest.mark.parametrize(
        "bad_region",
        ["US-EAST-1", "us-east", "us-east-1-extra", "region1", "", "us-east-one"],
    )
    def test_sqs_region_rejects_invalid(self, bad_region: str) -> None:
        """Structurally-malformed region names must reject at config time."""
        with pytest.raises(ConfigurationError) as exc_info:
            SqsSettings(region_name=bad_region)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "region_name"

    @pytest.mark.parametrize(
        "bad_region",
        ["US-EAST-1", "us-east", ""],
    )
    def test_dynamodb_region_rejects_invalid(self, bad_region: str) -> None:
        """Structurally-malformed region names must reject at config time."""
        with pytest.raises(ConfigurationError) as exc_info:
            DynamoDBSettings(region_name=bad_region)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "region_name"

    @pytest.mark.parametrize("settings_type", [SqsSettings, DynamoDBSettings])
    @pytest.mark.parametrize(
        "bad_region",
        [
            "us--gov-west-1",
            "-us-gov-west-1",
            "us-gov-west-",
            "us-gov-west-one",
            "us-gov-west-1-extra",
            "US-gov-west-1",
            "us_gov_west_1",
            "us-east-\u0661",
            "u-east-1",
            "a-b-1",
            "aws-global",
        ],
    )
    def test_partition_region_rejects_malformed_ascii_structure(
        self, settings_type: type[Any], bad_region: str
    ) -> None:
        """Reject malformed labels and Unicode lookalikes for both backends."""
        with pytest.raises(ConfigurationError) as exc_info:
            settings_type(region_name=bad_region)
        assert exc_info.value.setting_name == "region_name"

    @pytest.mark.parametrize(
        "good_region",
        ["us-east-1", "us-west-2", "ap-southeast-2", "eu-central-1", "me-central-1"],
    )
    def test_sqs_region_accepts_valid(self, good_region: str) -> None:
        """Valid AWS region names stay accepted (incl. multi-word middle)."""
        assert SqsSettings(region_name=good_region).region_name == good_region

    @pytest.mark.parametrize(
        "good_region",
        ["us-east-1", "ap-southeast-3", "me-central-1"],
    )
    def test_dynamodb_region_accepts_valid(self, good_region: str) -> None:
        """Valid AWS region names stay accepted."""
        assert DynamoDBSettings(region_name=good_region).region_name == good_region

    @pytest.mark.parametrize("settings_type", [SqsSettings, DynamoDBSettings])
    @pytest.mark.parametrize(
        "partition_region",
        [
            "us-gov-west-1",
            "us-iso-east-1",
            "us-isob-west-1",
            "eu-isoe-west-1",
            "us-isof-south-1",
            "eusc-de-east-1",
        ],
    )
    def test_aws_partition_regions_are_not_rejected(
        self, settings_type: type[Any], partition_region: str
    ) -> None:
        """Accept valid multi-segment regions across AWS partitions."""
        assert settings_type(region_name=partition_region).region_name == (
            partition_region
        )


# =============================================================================
# Round 9c — SV3: cross-field auth/transport coherence
# =============================================================================


class TestSV3KafkaSaslRequiresSaslProtocol:
    """SV3-1 (H): SASL fields set → ``security_protocol`` must start with SASL_.

    Without this guard, SASL credentials are silently ignored by kafka-python
    (the client only consults ``sasl_*`` when ``security_protocol`` is
    ``SASL_PLAINTEXT`` or ``SASL_SSL``). The operator believes auth is enforced
    while the broker never sees an attempt — a silent auth-bypass footgun.
    """

    def test_sasl_username_without_sasl_protocol_rejected(self) -> None:
        """``sasl_username`` set with default PLAINTEXT protocol → reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                sasl_username="user"
            )  # security_protocol defaults to PLAINTEXT
        msg = str(exc_info.value)
        assert "security_protocol" in msg
        assert "SASL_" in msg
        assert exc_info.value.setting_name == "security_protocol"

    def test_sasl_password_without_sasl_protocol_rejected(self) -> None:
        """``sasl_password`` set with PLAINTEXT protocol → reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(sasl_password="secret")  # type: ignore[arg-type]
        assert exc_info.value.setting_name == "security_protocol"

    def test_sasl_mechanism_without_sasl_protocol_rejected(self) -> None:
        """``sasl_mechanism`` set with PLAINTEXT protocol → reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(sasl_mechanism="PLAIN")
        assert exc_info.value.setting_name == "security_protocol"

    def test_sasl_with_ssl_protocol_rejected(self) -> None:
        """SASL fields + ``SSL`` (not ``SASL_SSL``) → reject."""
        with pytest.raises(ConfigurationError):
            KafkaSettings(
                security_protocol="SSL",  # type: ignore[arg-type]
                sasl_username="user",
                sasl_password="p",  # type: ignore[arg-type]
            )

    def test_sasl_plaintext_rejected(self) -> None:
        """Password SASL must not be configured without TLS."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                security_protocol="SASL_PLAINTEXT",  # type: ignore[arg-type]
                sasl_mechanism="PLAIN",
                sasl_username="user",
                sasl_password="secret",  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == "security_protocol"

    def test_sasl_with_sasl_ssl_accepted(self) -> None:
        """SASL fields + ``SASL_SSL`` → valid (the canonical secured path)."""
        s = KafkaSettings(
            security_protocol="SASL_SSL",  # type: ignore[arg-type]
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="user",
            sasl_password="secret",  # type: ignore[arg-type]
        )
        assert s.security_protocol == "SASL_SSL"

    def test_sasl_protocol_requires_explicit_mechanism(self) -> None:
        """The SDK default is not an authentication policy."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(security_protocol="SASL_SSL")
        assert exc_info.value.setting_name == "sasl_mechanism"

    @pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    @pytest.mark.parametrize(
        ("username", "password", "setting_name"),
        [
            (None, "secret", "sasl_username"),
            ("user", None, "sasl_password"),
            (" ", "secret", "sasl_username"),
            ("user", "\t", "sasl_password"),
        ],
    )
    def test_password_mechanism_rejects_partial_or_blank_pair(
        self,
        mechanism: str,
        username: str | None,
        password: str | None,
        setting_name: str,
    ) -> None:
        """PLAIN and SCRAM never silently omit incomplete credentials."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                security_protocol="SASL_SSL",
                sasl_mechanism=mechanism,
                sasl_username=username,
                sasl_password=password,  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == setting_name

    def test_gssapi_rejects_ignored_plain_credentials(self) -> None:
        """Mechanism-inconsistent fields must not create a false auth belief."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(
                security_protocol="SASL_SSL",
                sasl_mechanism="GSSAPI",
                sasl_username="ignored-user",
                sasl_password="ignored-secret",  # type: ignore[arg-type]
            )
        assert exc_info.value.setting_name == "sasl_username"
        assert "ignored-secret" not in str(exc_info.value)

    def test_oauthbearer_rejected_without_token_provider_support(self) -> None:
        """Advertised OAuth without a provider would fail later inside the SDK."""
        with pytest.raises(ConfigurationError) as exc_info:
            KafkaSettings(security_protocol="SASL_SSL", sasl_mechanism="OAUTHBEARER")
        assert exc_info.value.setting_name == "sasl_mechanism"

    def test_no_sasl_with_plaintext_accepted(self) -> None:
        """No SASL fields + ``PLAINTEXT`` → valid (the default unauthenticated)."""
        s = KafkaSettings()
        assert s.security_protocol == "PLAINTEXT"
        assert s.sasl_username is None


class TestSV3PulsarAuthTokenRequiresSsl:
    """SV3-2 (H): ``auth_token`` set → ``service_url`` must be ``pulsar+ssl://``.

    Pulsar's ``AuthenticationToken`` is sent on every connection. Without TLS,
    the token traverses the wire in cleartext. This raises at config time
    (mirrors Redis ``ssl_enabled``→``ssl_cafile`` and Kafka SASL→
    ``security_protocol``); the connect-path test fixtures
    (``test_connect_with_auth_token``, ``test_pulsar_auth_token_is_redacted_str``)
    were updated to ``pulsar+ssl://`` so the raise is safe.
    """

    def test_auth_token_with_plain_url_raises(self) -> None:
        """``auth_token`` + ``pulsar://`` → ConfigurationError at config time."""
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(
                service_url="pulsar://broker:6650",
                auth_token="top-secret",  # type: ignore[arg-type]
            )
        msg = str(exc_info.value)
        assert "pulsar+ssl://" in msg or "cleartext" in msg.lower(), msg
        assert exc_info.value.setting_name == "service_url"

    def test_auth_token_with_ssl_url_accepted(self) -> None:
        """``auth_token`` + ``pulsar+ssl://`` → accepted (no raise)."""
        s = PulsarSettings(
            service_url="pulsar+ssl://broker:6651",
            auth_token="top-secret",  # type: ignore[arg-type]
        )
        assert s.auth_token is not None

    def test_blank_auth_token_is_rejected_without_retention(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(
                service_url="pulsar+ssl://broker:6651",
                auth_token="   ",  # type: ignore[arg-type]
            )

        assert exc_info.value.setting_name == "auth_token"
        assert exc_info.value.setting_value == "***REDACTED***"

    @pytest.mark.parametrize(
        ("overrides", "setting_name"),
        [
            ({"allow_insecure_connection": True}, "allow_insecure_connection"),
            ({"tls_validate_hostname": False}, "tls_validate_hostname"),
        ],
    )
    def test_authenticated_tls_cannot_disable_verification(
        self, overrides: dict[str, object], setting_name: str
    ) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(
                service_url="pulsar+ssl://broker:6651",
                auth_token="top-secret",  # type: ignore[arg-type]
                **overrides,
            )

        assert exc_info.value.setting_name == setting_name

    def test_service_url_userinfo_is_rejected_without_retention(self) -> None:
        secret = "do-not-leak"

        with pytest.raises(ConfigurationError) as exc_info:
            PulsarSettings(service_url=f"pulsar+ssl://crawler:{secret}@broker:6651")

        assert exc_info.value.setting_name == "service_url"
        assert secret not in str(exc_info.value)
        assert secret not in repr(exc_info.value.__dict__)
        assert exc_info.value.__cause__ is None

    def test_no_auth_token_with_plain_url_accepted(self) -> None:
        """No ``auth_token`` + ``pulsar://`` → accepted (validator skips)."""
        s = PulsarSettings(
            service_url="pulsar://broker:6650", allow_remote_plaintext=True
        )
        assert s.auth_token is None

    def test_tls_hostname_validation_defaults_secure(self) -> None:
        """TLS connections validate the broker hostname unless explicitly opted out."""
        s = PulsarSettings(service_url="pulsar+ssl://broker:6651")
        assert s.tls_validate_hostname is True

    def test_tls_hostname_validation_can_be_explicitly_disabled(self) -> None:
        """Local compatibility remains available as an explicit insecure choice."""
        s = PulsarSettings(
            service_url="pulsar+ssl://localhost:6651",
            tls_validate_hostname=False,
        )
        assert s.tls_validate_hostname is False


class TestSV3RedisSslRequiresCafile:
    """SV3-3 (M): ``ssl_enabled=True`` → ``ssl_cafile`` should be set.

    Without a CA bundle, the client either refuses to verify (openssl default
    may have no system roots in some containers) or silently skips validation
    → MITM risk. Raise rather than warn: no existing test in the repo sets
    ``ssl_enabled=True`` without ``ssl_cafile`` in a way that is intended to
    be valid (the lone fixture in ``test_backend_modes.py`` sets both).
    Operators with self-signed certs must still provide a CA file (their own).
    """

    def test_ssl_enabled_without_cafile_rejected(self) -> None:
        """``ssl_enabled=True`` + no ``ssl_cafile`` → reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            RedisSettings(ssl_enabled=True)
        msg = str(exc_info.value)
        assert "ssl_cafile" in msg
        assert exc_info.value.setting_name == "ssl_cafile"

    def test_ssl_enabled_with_cafile_accepted(self) -> None:
        """``ssl_enabled=True`` + ``ssl_cafile`` → valid."""
        s = RedisSettings(ssl_enabled=True, ssl_cafile="/etc/ssl/ca.pem")
        assert s.ssl_enabled is True
        assert s.ssl_cafile == "/etc/ssl/ca.pem"

    def test_ssl_disabled_without_cafile_accepted(self) -> None:
        """``ssl_enabled=False`` + no ``ssl_cafile`` → valid (the default)."""
        s = RedisSettings()
        assert s.ssl_enabled is False
        assert s.ssl_cafile is None


class TestRedisBlankCredentialsRejected:
    """R141-F5: blank Redis credentials fail fast instead of half-authenticating.

    All nine sibling backends reject blank credentials. redis-py gates AUTH on
    bare truthiness (``if self.credential_provider or (self.username or
    self.password)``), so an empty ``password`` silently skips AUTH (the operator
    believes authentication is configured when it is not), while a whitespace
    ``username`` reaches the server verbatim and fails as an opaque ACL error at
    connect time.
    """

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("username", "   "),
            ("username", ""),
            ("password", "   "),
            ("password", ""),
            ("sentinel_username", "   "),
            ("sentinel_username", ""),
            ("sentinel_password", "   "),
            ("sentinel_password", ""),
        ],
    )
    def test_blank_credential_rejected(self, field: str, value: str) -> None:
        """Empty and whitespace-only credentials raise ``ConfigurationError``."""
        kwargs: dict[str, object] = {"host": "localhost"}
        if field.startswith("sentinel_"):
            kwargs.update(
                mode=RedisMode.SENTINEL,
                sentinels=["localhost:26379"],
            )
        kwargs[field] = value
        with pytest.raises(ConfigurationError) as exc_info:
            RedisSettings(**kwargs)  # type: ignore[arg-type]
        assert exc_info.value.setting_name == field
        if value:
            assert value not in str(exc_info.value)
            assert value not in repr(exc_info.value)

    def test_unset_and_non_blank_credentials_stay_valid(self) -> None:
        """``None`` (credential unset) and real values remain accepted."""
        standalone = RedisSettings(
            host="localhost", username="acl-user", password="secret"
        )
        assert standalone.username == "acl-user"
        sentinel = RedisSettings(
            mode=RedisMode.SENTINEL,
            sentinels=["localhost:26379"],
            sentinel_username="sentinel-user",
            sentinel_password="sentinel-secret",
            ssl_enabled=True,
            ssl_cafile="/tls/ca.pem",
        )
        assert sentinel.sentinel_username == "sentinel-user"

    def test_blank_credentials_do_not_count_as_authentication_intent(self) -> None:
        """``validate_redis_transport_security`` treats blanks as unset.

        redis-py would never attempt AUTH with only-blank credentials, so a
        blank value must not force the authenticated remote-TLS boundary either.
        """
        validate_redis_transport_security(
            mode=RedisMode.STANDALONE,
            host="redis.internal",
            username="   ",
            password=SecretStr(""),
            sentinel_username="   ",
            sentinel_password=SecretStr(""),
            ssl_enabled=False,
            ssl_cafile=None,
            ssl_certfile=None,
            ssl_keyfile=None,
            ssl_check_hostname=True,
        )

    def test_model_validator_blank_credentials_do_not_count_as_authentication(
        self,
    ) -> None:
        """V2-2: the model validator's ``has_authentication`` must be blank-aware.

        The model validator computes its own authentication-intent flag for
        the remote-plaintext opt-in gate. Presence-based checking
        (``value is not None``) classified a blank credential as
        authentication, drifting from ``validate_redis_transport_security``
        (which uses the blank-aware ``_credential_has_content`` helper): a
        blank password on a remote host would skip the
        ``allow_remote_plaintext`` opt-in without ever counting as real AUTH.
        Field validators reject blanks at construction time, so the drift is
        pinned by exercising the model validator directly on an instance whose
        credential is blank.
        """
        settings = RedisSettings(host="localhost")
        object.__setattr__(settings, "host", "redis.internal")
        object.__setattr__(settings, "password", SecretStr("   "))
        with pytest.raises(ConfigurationError) as exc_info:
            RedisSettings._validate_transport_security(settings)
        assert exc_info.value.setting_name == "allow_remote_plaintext"
        assert exc_info.value.args[0] == (
            "Remote unauthenticated plaintext Redis connections require "
            "allow_remote_plaintext=True. Enable TLS or use this override only "
            "for a trusted private network."
        )


class TestSV3MongoPoolSizeOrdering:
    """SV3-4 (M): ``min_pool_size <= max_pool_size``.

    Inverted bounds surface as an opaque ``ConnectionFailure`` / deadlock under
    load once pymongo's pool tries to acquire a slot that can never exist.
    """

    def test_min_greater_than_max_rejected(self) -> None:
        """``min_pool_size > max_pool_size`` → reject."""
        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(min_pool_size=20, max_pool_size=10)
        msg = str(exc_info.value)
        assert "min_pool_size" in msg
        assert "max_pool_size" in msg

    def test_equal_sizes_accepted(self) -> None:
        """``min == max`` → valid (fixed-size pool)."""
        s = MongoDBSettings(min_pool_size=5, max_pool_size=5)
        assert s.min_pool_size == s.max_pool_size

    def test_default_sizes_accepted(self) -> None:
        """Defaults (1, 10) → valid."""
        s = MongoDBSettings()
        assert s.min_pool_size <= s.max_pool_size

    def test_zero_min_accepted(self) -> None:
        """``min_pool_size=0`` (Field allows ge=0) → valid."""
        s = MongoDBSettings(min_pool_size=0, max_pool_size=1)
        assert s.min_pool_size <= s.max_pool_size


class TestSV3ElasticsearchAuthExclusivity:
    """SV3-5 (L-M): ``api_key`` and (``username``, ``password``) mutually exclusive.

    When both are set, ``_build_kwargs`` prefers ``api_key`` and silently drops
    ``basic_auth`` → the operator believes basic_auth is enforced while it never
    reaches the broker. Fail-fast at config time.
    """

    def test_api_key_with_username_rejected(self) -> None:
        """``api_key`` + ``username`` → reject (silent basic_auth drop)."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=["https://es:9200"],
                api_key=SecretStr("key"),
                username="user",
            )
        msg = str(exc_info.value)
        assert "api_key" in msg
        assert "username" in msg

    def test_api_key_with_password_rejected(self) -> None:
        """``api_key`` + ``password`` → reject."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=["https://es:9200"],
                api_key=SecretStr("key"),
                password=SecretStr("p"),
            )
        assert exc_info.value.setting_name in {"api_key", "username"}

    def test_api_key_alone_accepted(self) -> None:
        """``api_key`` alone → valid."""
        from pydantic import SecretStr

        s = ElasticSearchSettings(hosts=["https://es:9200"], api_key=SecretStr("k"))
        assert s.api_key is not None
        assert s.username is None

    def test_basic_auth_alone_accepted(self) -> None:
        """``username`` + ``password`` (no api_key) → valid."""
        from pydantic import SecretStr

        s = ElasticSearchSettings(
            hosts=["https://es:9200"],
            username="user",
            password=SecretStr("p"),
        )
        assert s.username == "user"
        assert s.api_key is None

    def test_empty_api_key_rejected(self) -> None:
        """R45: an explicitly supplied blank API key is never anonymous auth."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(hosts=["https://es:9200"], api_key=SecretStr(""))
        assert exc_info.value.setting_name == "api_key"

    @pytest.mark.parametrize(
        ("kwargs", "setting_name"),
        [
            ({"username": "user"}, "password"),
            ({"password": "p"}, "username"),
            ({"username": " ", "password": "p"}, "username"),
            ({"username": "user", "password": " "}, "password"),
        ],
    )
    def test_partial_or_blank_basic_auth_rejected(
        self, kwargs: dict[str, object], setting_name: str
    ) -> None:
        """R45: client kwargs must not silently discard incomplete basic auth."""
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(hosts=["https://es:9200"], **kwargs)
        assert exc_info.value.setting_name == setting_name

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"api_key": "key"},
            {"username": "user", "password": "p"},
        ],
    )
    def test_authenticated_unverified_tls_rejected(
        self, kwargs: dict[str, object]
    ) -> None:
        """R45: credentials must never be sent over unverified TLS."""
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=["https://es:9200"], verify_certs=False, **kwargs
            )
        assert exc_info.value.setting_name == "verify_certs"

    def test_anonymous_local_http_and_verified_https_auth_remain_valid(self) -> None:
        """R45 retains the supported local-development and secure-auth paths."""
        from pydantic import SecretStr

        assert ElasticSearchSettings(hosts=["http://localhost:9200"]).hosts
        assert (
            ElasticSearchSettings(
                hosts=["https://es:9200"], username="user", password=SecretStr("p")
            ).username
            == "user"
        )


class TestSV3SqsAwsCredsBothOrNeither:
    """SV3-6a (M): SQS AWS creds must be both-set or both-unset.

    Lifts the round-6 SEC-7 connect-path XOR into the settings validator so it
    fires at config time, not at first boto3 RPC.
    """

    def test_key_without_secret_rejected(self) -> None:
        """``aws_access_key_id`` set, ``aws_secret_access_key`` None → reject."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            SqsSettings(
                aws_access_key_id=SecretStr("AKIA..."),
                aws_secret_access_key=None,
            )
        msg = str(exc_info.value)
        assert "aws_secret_access_key" in msg
        assert exc_info.value.setting_name == "aws_secret_access_key"

    def test_secret_without_key_rejected(self) -> None:
        """``aws_secret_access_key`` set, ``aws_access_key_id`` None → reject."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            SqsSettings(
                aws_access_key_id=None,
                aws_secret_access_key=SecretStr("orphan"),
            )
        assert "aws_access_key_id" in str(exc_info.value)
        assert exc_info.value.setting_name == "aws_access_key_id"

    def test_both_set_accepted(self) -> None:
        """Both creds → valid."""
        from pydantic import SecretStr

        s = SqsSettings(
            aws_access_key_id=SecretStr("AKIA..."),
            aws_secret_access_key=SecretStr("secret"),
        )
        assert s.aws_access_key_id is not None
        assert s.aws_secret_access_key is not None

    def test_neither_set_accepted(self) -> None:
        """Neither cred (IAM role path) → valid."""
        s = SqsSettings()
        assert s.aws_access_key_id is None
        assert s.aws_secret_access_key is None


class TestSV3DynamoDbAwsCredsBothOrNeither:
    """SV3-6b (M): DynamoDB AWS creds must be both-set or both-unset.

    Mirrors SQS (same boto3 default-chain behavior; same connect-path SEC-7
    guard lifted to settings).
    """

    def test_key_without_secret_rejected(self) -> None:
        """``aws_access_key_id`` set, ``aws_secret_access_key`` None → reject."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            DynamoDBSettings(
                aws_access_key_id=SecretStr("AKIA..."),
                aws_secret_access_key=None,
            )
        assert exc_info.value.setting_name == "aws_secret_access_key"

    def test_secret_without_key_rejected(self) -> None:
        """``aws_secret_access_key`` set, ``aws_access_key_id`` None → reject."""
        from pydantic import SecretStr

        with pytest.raises(ConfigurationError) as exc_info:
            DynamoDBSettings(
                aws_access_key_id=None,
                aws_secret_access_key=SecretStr("orphan"),
            )
        assert exc_info.value.setting_name == "aws_access_key_id"

    def test_both_set_accepted(self) -> None:
        """Both creds → valid."""
        from pydantic import SecretStr

        s = DynamoDBSettings(
            aws_access_key_id=SecretStr("AKIA..."),
            aws_secret_access_key=SecretStr("secret"),
        )
        assert s.aws_access_key_id is not None

    def test_neither_set_accepted(self) -> None:
        """Neither cred (IAM role path) → valid."""
        s = DynamoDBSettings()
        assert s.aws_access_key_id is None
        assert s.aws_secret_access_key is None


# ---------------------------------------------------------------------------
# R14-B — v1.0 breaking-change disclosure + public-contract freeze.
# ---------------------------------------------------------------------------
# Two TDD gates:
# 1. ``Settings.backend_type`` MUST accept any registry-known 3rd-party string
#    (round-5 R5-1 promised this; round-9 regressed it — ``BackendType`` enum
#    coercion rejects unknown strings with pydantic ValidationError before the
#    registry can accept them).
# 2. Unknown backend types MUST raise ``ConfigurationError`` (the project's
#    config-error family), NOT pydantic ``ValidationError`` — so operators get
#    a consistent exception family + a ``setting_name`` attribute for logging.
class TestR14BBackendTypeThirdPartyString:
    """R14-B: ``SCRAPY_BACKEND_TYPE`` accepts registered 3rd-party strings."""

    def test_backend_type_accepts_registered_third_party_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A registered 3rd-party backend string is accepted at the Settings layer.

        RED today: ``Settings.backend_type: BackendType`` uses pydantic enum
        coercion → ``ValidationError`` for any non-member string, contradicting
        round-5 R5-1 (3rd-party backends route through the same path).
        GREEN after R14-B: the field_validator accepts any ``BackendType`` OR
        any string present in ``get_registry()``.
        """
        from dataclasses import dataclass

        from scrapy_extension.backends.registry import (
            _ENTRY_POINT_GROUP,
            BackendDescriptor,
            _reset_registry_cache,
        )

        @dataclass(frozen=True)
        class _FakeEP:
            name: str
            value: str
            group: str

            def load(self) -> Any:
                # Backenddescriptor for a fake 3rd-party backend.
                return lambda: BackendDescriptor(
                    backend_type="fakebackend",
                    backend_cls_path="tests.test_settings_validation._FakeBackend",
                    settings_cls_path="tests.test_settings_validation._FakeBackendSettings",
                    capabilities=frozenset({"queue"}),
                )

        import importlib.metadata as importlib_metadata

        def _eps(group: str | None = None) -> Any:
            fake = _FakeEP("fakebackend", "x.y.z", _ENTRY_POINT_GROUP)
            if group is not None:
                return [fake] if fake.group == group else []
            return {"scrapy_extension.backends": [fake]}

        monkeypatch.setattr(importlib_metadata, "entry_points", _eps)
        _reset_registry_cache()

        # Must NOT raise — fakebackend is registered.
        settings = Settings(backend_type="fakebackend")  # type: ignore[arg-type]
        assert settings.backend_type == "fakebackend"

    def test_unknown_backend_type_raises_configuration_error_not_validation_error(
        self,
    ) -> None:
        """Unknown backend string → ``ConfigurationError``, NOT pydantic
        ``ValidationError``.

        RED today: raises ``ValidationError`` (enum coercion via
        ``BackendType._missing_`` → ``ValueError``).
        GREEN after R14-B: the field_validator routes unknown values through
        ``ConfigurationError(setting_name="SCRAPY_BACKEND_TYPE", ...)`` so the
        exception family is consistent with all other settings-validation paths
        and the ``setting_name`` attribute is preserved for downstream log
        handlers (frozen in STABILITY.md).
        """
        with pytest.raises(ConfigurationError) as exc_info:
            Settings(backend_type="totally-not-a-real-backend")  # type: ignore[arg-type]
        # pydantic ValidationError must NOT leak through.
        assert not isinstance(exc_info.value, ValidationError)
        # setting_name must be populated for downstream log handlers.
        assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"

    def test_non_string_backend_type_raises_configuration_error(self) -> None:
        """Non-str, non-BackendType input (e.g. int) → ``ConfigurationError``,
        NOT pydantic ``ValidationError`` — consistent exception family.

        Covers the validator's final ``raise`` (the non-str/non-BackendType branch).
        """
        with pytest.raises(ConfigurationError) as exc_info:
            Settings(backend_type=42)  # type: ignore[arg-type]
        assert not isinstance(exc_info.value, ValidationError)
        assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"
        assert exc_info.value.setting_value is None


class _FakeBackend:
    """Stub backend pointed at by the fake 3rd-party entry-point above."""

    def __init__(self, settings: _FakeBackendSettings) -> None:
        self.settings = settings


class _FakeBackendSettings:
    """Stub settings matching ``_FakeBackend``'s constructor."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
