"""Tests for configuration module."""

import traceback

import pytest
from pydantic import ValidationError

from scrapy_extension.backends.base import BackendType
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
    ElasticSearchSettings,
    MongoDBSettings,
    RedisSettings,
    Settings,
)


def _assert_package_traceback_locals_are_redacted(
    error: BaseException,
    marker: str,
) -> None:
    """Ensure replacement boundaries do not retain raw input in package frames."""
    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)
        trace = trace.tb_next


class TestSettings:
    """Test base Settings."""

    def test_default_backend_type(self):
        """Test default backend type is REDIS."""
        settings = Settings()
        assert settings.backend_type == BackendType.REDIS

    def test_default_serializer(self):
        """Test default serializer is json."""
        settings = Settings()
        assert settings.serializer == "json"

    def test_default_retry_attempts(self):
        """Test default retry attempts."""
        settings = Settings()
        assert settings.retry_attempts == 3

    def test_default_retry_delay(self):
        """Test default retry delay."""
        settings = Settings()
        assert settings.retry_delay == 1.0

    def test_backend_type_from_str(self):
        """Test backend type from string."""
        settings = Settings(backend_type=BackendType.MONGODB)
        assert settings.backend_type == BackendType.MONGODB

    def test_empty_backend_type_matches_unset_default(self, monkeypatch):
        """An empty environment value remains the documented unset sentinel."""
        monkeypatch.setenv("SCRAPY_BACKEND_TYPE", "")

        settings = Settings()

        assert settings.backend_type == BackendType.REDIS
        assert Settings(backend_type="").backend_type == BackendType.REDIS

    @pytest.mark.parametrize("age", [0.0, -0.1])
    def test_storage_buffer_max_age_must_be_positive(self, age):
        with pytest.raises(ValidationError):
            Settings(storage_buffer_max_age_s=age)

    @pytest.mark.parametrize("max_pending", [0, -1])
    def test_storage_buffer_max_pending_must_be_positive(self, max_pending):
        with pytest.raises(ValidationError):
            Settings(storage_buffer_max_pending=max_pending)


@pytest.mark.parametrize(
    ("factory", "marker"),
    [
        (
            lambda marker: Settings(retry_attempts=marker),
            "base-settings-validation-secret-marker",
        ),
        (
            lambda marker: ElasticSearchSettings(api_key=[marker]),
            "elasticsearch-settings-validation-secret-marker",
        ),
        (
            lambda marker: MongoDBSettings(password=[marker]),
            "mongodb-settings-validation-secret-marker",
        ),
    ],
)
def test_direct_settings_validation_errors_do_not_retain_raw_input(factory, marker):
    """Pydantic's rendered and structured error APIs must both be redacted."""
    with pytest.raises(ValidationError) as exc_info:
        factory(marker)

    error = exc_info.value
    public_forms = (
        str(error),
        repr(error),
        repr(getattr(error, "__dict__", {})),
        "".join(traceback.format_exception(error)),
        repr(error.errors()),
        error.json(),
    )
    assert all(marker not in form for form in public_forms)
    assert all(detail["input"] is None for detail in error.errors())
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_unknown_backend_type_does_not_retain_the_raw_value():
    marker = "base-backend-type-secret-marker"

    with pytest.raises(ConfigurationError) as exc_info:
        Settings(backend_type=marker)  # type: ignore[arg-type]

    error = exc_info.value
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.setting_value is None
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_spoofed_settings_class_cannot_receive_bundled_message_trust():
    """Module/name metadata must not impersonate one exact bundled model."""
    from pydantic import model_validator

    from scrapy_extension.settings._redacted import RedactedBaseSettings

    marker = "spoofed-settings-message-secret-marker"

    class _SpoofedSettings(RedactedBaseSettings):
        value: str = "configured"

        @model_validator(mode="after")
        def _raise_untrusted_error(self):
            raise ConfigurationError(marker, setting_name="value")

    _SpoofedSettings.__module__ = "scrapy_extension.settings.base"
    _SpoofedSettings.__qualname__ = "Settings"

    with pytest.raises(ConfigurationError) as exc_info:
        _SpoofedSettings()

    error = exc_info.value
    assert str(error) == "Settings contain an invalid configuration value."
    assert error.setting_name == "settings"
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_settings_boundary_does_not_introspect_hostile_error_subclasses():
    """A validator's exception subclass cannot execute code during redaction."""
    from pydantic import model_validator

    from scrapy_extension.settings._redacted import RedactedBaseSettings

    marker = "hostile-settings-configuration-error-marker"

    class _HostileConfigurationError(ConfigurationError):
        def __getattribute__(self, name: str) -> object:
            if name in {"args", "setting_name"}:
                raise RuntimeError(marker)
            return super().__getattribute__(name)

    class _UntrustedSettings(RedactedBaseSettings):
        value: str = "configured"

        @model_validator(mode="after")
        def _raise_untrusted_error(self):
            raise _HostileConfigurationError(marker)

    with pytest.raises(ConfigurationError) as exc_info:
        _UntrustedSettings()

    error = exc_info.value
    assert str(error) == "Settings contain an invalid configuration value."
    assert error.setting_name == "settings"
    assert marker not in repr(error.__dict__)
    _assert_package_traceback_locals_are_redacted(error, marker)


def test_malformed_environment_setting_does_not_retain_source_diagnostics(
    monkeypatch,
):
    marker = "direct-settings-env-secret-marker"
    monkeypatch.setenv("SCRAPY_ELASTICSEARCH_HOSTS", marker)

    with pytest.raises(ConfigurationError) as exc_info:
        ElasticSearchSettings()

    error = exc_info.value
    assert error.setting_name == "settings"
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    _assert_package_traceback_locals_are_redacted(error, marker)


class TestRedisSettings:
    """Test RedisSettings."""

    def test_default_host(self):
        """Test default host."""
        settings = RedisSettings()
        assert settings.host == "localhost"

    def test_default_port(self):
        """Test default port."""
        settings = RedisSettings()
        assert settings.port == 6379

    def test_default_db(self):
        """Test default db."""
        settings = RedisSettings()
        assert settings.db == 0

    def test_custom_host(self):
        """Test custom host."""
        settings = RedisSettings(host="redis.example.com", allow_remote_plaintext=True)
        assert settings.host == "redis.example.com"

    def test_custom_port(self):
        """Test custom port."""
        settings = RedisSettings(port=6380)
        assert settings.port == 6380

    def test_port_validation(self):
        """Test port validation."""
        with pytest.raises(ConfigurationError):
            RedisSettings(port=0)

        with pytest.raises(ConfigurationError):
            RedisSettings(port=70000)

    def test_password_optional(self):
        """Test password is optional."""
        settings = RedisSettings()
        assert settings.password is None

        settings = RedisSettings(password="secret")
        assert settings.password.get_secret_value() == "secret"

    def test_from_env_vars(self, monkeypatch):
        """Test loading from environment variables."""
        monkeypatch.setenv("SCRAPY_REDIS_HOST", "redis.example.com")
        monkeypatch.setenv("SCRAPY_REDIS_PORT", "6380")
        monkeypatch.setenv("SCRAPY_REDIS_ALLOW_REMOTE_PLAINTEXT", "true")

        settings = RedisSettings()
        assert settings.host == "redis.example.com"
        assert settings.port == 6380

    def test_ssl_check_hostname_defaults_to_true(self):
        """R2-C1: TLS hostname verification must be ON by default.

        A misconfigured env that flips ``ssl_enabled=True`` must NOT silently
        accept any valid-CA cert for an unrelated domain (MITM). Operators who
        need IP-only service discovery must opt out explicitly.
        """
        settings = RedisSettings()
        assert settings.ssl_check_hostname is True


class TestMongoDBSettings:
    """Test MongoDBSettings."""

    def test_default_values(self):
        """Test all default values."""
        from scrapy_extension.settings import MongoDBSettings

        settings = MongoDBSettings()
        assert settings.uri == "mongodb://localhost:27017"
        assert settings.database == "scrapy_extension"
        assert settings.queue_collection == "queues"
        assert settings.set_collection == "sets"
        assert settings.storage_collection == "storage"
        assert settings.min_pool_size == 1
        assert settings.max_pool_size == 10
        assert settings.max_idle_time_ms == 60000
        assert settings.wait_queue_timeout_ms == 5000
        assert settings.w == 1
        assert settings.journal is True
        assert settings.read_preference == "primary"

    def test_from_env_vars(self, monkeypatch):
        """Test loading from environment variables."""
        from scrapy_extension.settings import MongoDBSettings

        monkeypatch.setenv("SCRAPY_MONGO_URI", "mongodb://custom:27017")
        monkeypatch.setenv("SCRAPY_MONGO_DATABASE", "custom_db")
        monkeypatch.setenv("SCRAPY_MONGO_ALLOW_REMOTE_PLAINTEXT", "true")
        settings = MongoDBSettings()
        assert settings.uri == "mongodb://custom:27017"
        assert settings.database == "custom_db"

    @pytest.mark.parametrize(
        ("queue_collection", "set_collection", "storage_collection"),
        [
            ("tenant_queue_set_marker", "tenant_queue_set_marker", "storage"),
            ("tenant_queue_storage_marker", "sets", "tenant_queue_storage_marker"),
            ("queues", "tenant_set_storage_marker", "tenant_set_storage_marker"),
        ],
    )
    def test_collection_capability_domains_must_be_physically_distinct(
        self,
        queue_collection,
        set_collection,
        storage_collection,
    ):
        """Queue, set, and storage clears must never share one collection."""
        from scrapy_extension.settings import MongoDBSettings

        markers = {
            queue_collection,
            set_collection,
            storage_collection,
        } - {"queues", "sets", "storage"}

        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(
                queue_collection=queue_collection,
                set_collection=set_collection,
                storage_collection=storage_collection,
            )

        assert exc_info.value.setting_name == "collection_names"
        for marker in markers:
            assert marker not in str(exc_info.value)
            assert marker not in repr(exc_info.value)


def test_mongodb_collection_names_empty_rejected():
    """R29-A: empty collection name must reject — opaque pymongo InvalidName at
    connect otherwise. The validator checked type + distinctness but not
    non-empty, so ``('', 'sets', 'storage')`` passed (3 distinct values)."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(queue_collection="")
    assert exc_info.value.setting_name == "collection_names"


def test_mongodb_collection_names_whitespace_rejected():
    """R29-A: whitespace-only collection names also reject (strip-aware)."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(set_collection="   ")


@pytest.mark.parametrize(
    "collection_name", ["system.users", "scrapy$queue", "bad\x00name"]
)
def test_mongodb_reserved_collection_names_rejected(collection_name):
    """Capability collections must not alias MongoDB system or invalid names."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(storage_collection=collection_name)

    assert exc_info.value.setting_name == "collection_names"
    assert collection_name not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_mongodb_database_empty_rejected():
    """R29-C: empty/whitespace database name must reject — opaque InvalidName at
    ``_initialize_collections`` otherwise (no validator on the field)."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(database="")


def test_mongodb_auth_source_empty_rejected():
    """R31-A: empty ``auth_source`` must reject at the settings layer — consistent
    with R29-C ``database``. (Empty-string is benign at the backend — falsy →
    skipped → pymongo default — but reject at settings for consistency.)"""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(auth_source="")


def test_mongodb_auth_source_whitespace_rejected():
    """R31-A: whitespace ``auth_source`` must reject — the backend's bare-truthiness
    ``if self.config.auth_source:`` lets ``"   "`` through (truthy) and passes it
    verbatim as ``authSource='   '`` to MongoClient → opaque authentication failure.
    R29's whitespace sweep missed this field."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(auth_source="   ")


def test_mongodb_username_whitespace_rejected():
    """R32-A: whitespace ``username`` must reject — the backend's _auth_kwargs gates
    on bare truthiness (``if not (username and password)``), so a whitespace value
    is truthy and passed verbatim to MongoClient → opaque auth failure. R31-A's
    sweep covered auth_source but missed the username/password siblings."""
    from pydantic import SecretStr

    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(username="   ", password=SecretStr("real-pw"))


def test_mongodb_password_whitespace_rejected():
    """R32-A: whitespace ``password`` (SecretStr) must reject — same rationale as
    username; ``SecretStr('   ')`` is truthy so it bypasses the both-or-neither
    gate and reaches MongoClient verbatim → opaque auth failure."""
    from pydantic import SecretStr

    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(username="real-user", password=SecretStr("   "))


def test_mongodb_replica_set_members_empty_element_rejected():
    """R29-B: empty/whitespace elements in replica_set_members/mongos_routers
    build a malformed ``mongodb://`` URI → opaque InvalidURI otherwise."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(replica_set_members=["host1:27017", ""])


def test_mongodb_replica_set_name_whitespace_rejected():
    """R29-D: whitespace ``replica_set_name`` bypasses the REPLICA_SET truthiness
    check (``not '  '`` is False) → opaque discovery error with ``replicaSet='  '``."""
    from scrapy_extension.settings import MongoDBMode, MongoDBSettings

    with pytest.raises(ConfigurationError):
        MongoDBSettings(mode=MongoDBMode.REPLICA_SET, replica_set_name="   ")


def test_kafka_settings_defaults():
    from scrapy_extension.settings import KafkaSettings

    settings = KafkaSettings()
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.max_priority_partitions == 10
    assert settings.acks == "all"
    assert settings.group_id == "scrapy-extension"


def test_kafka_settings_from_env(monkeypatch):
    from scrapy_extension.settings import KafkaSettings

    monkeypatch.setenv("SCRAPY_KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9092")
    monkeypatch.setenv("SCRAPY_KAFKA_GROUP_ID", "my-group")
    monkeypatch.setenv("SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT", "true")
    settings = KafkaSettings()
    assert settings.bootstrap_servers == "kafka.example.com:9092"
    assert settings.group_id == "my-group"


def test_rabbitmq_settings_defaults():
    """R2-C2: username/password are required (no silent guest/guest fallback).

    Construction must fail fast when creds are missing. Tests that exercise
    non-auth fields pass explicit test credentials.
    """
    from scrapy_extension.settings import RabbitMQSettings

    settings = RabbitMQSettings(username="test-user", password="test-pass")
    assert settings.host == "localhost"
    assert settings.port == 5672
    assert settings.username == "test-user"
    assert settings.password.get_secret_value() == "test-pass"
    assert settings.max_priority == 255


def test_rabbitmq_settings_requires_username_and_password(monkeypatch):
    """R2-C2: missing creds must raise ValidationError (no guest/guest default)."""
    from scrapy_extension.settings import RabbitMQSettings

    # The ``_rabbitmq_test_credentials`` autouse fixture (conftest) sets these so
    # bare ``RabbitMQSettings()`` works elsewhere; this test asserts the
    # required-creds contract, so they must be absent here.
    monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
    monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

    with pytest.raises(ValidationError):
        RabbitMQSettings()

    with pytest.raises(ValidationError):
        RabbitMQSettings(password="p")

    with pytest.raises(ValidationError):
        RabbitMQSettings(username="u")


class TestConfigurationErrorRedaction:
    """R2-B6 / R26-C1: ConfigurationError must not retain secrets.

    Defensive design — current backend code only passes non-sensitive
    ``setting_value`` (mode, sentinels, defaults), but future contributors
    may pass credentials. The redaction at ``__init__`` time ensures the
    raw value never lives on the exception object, so ``repr(exc)`` and
    debug-logging the exception cannot leak.
    """

    def test_secretstr_setting_value_is_redacted(self):
        """A SecretStr value is masked regardless of setting_name."""
        from pydantic import SecretStr

        from scrapy_extension.exceptions import ConfigurationError

        exc = ConfigurationError(
            "invalid",
            setting_name="uri",
            setting_value=SecretStr("hunter2"),
        )
        assert exc.setting_value == "***REDACTED***"
        assert "hunter2" not in repr(exc)

    def test_sensitive_setting_name_redacts_any_value(self):
        """Names containing 'password', 'secret', 'api_key', 'token' trigger redaction."""
        from scrapy_extension.exceptions import ConfigurationError

        for sensitive_name in (
            "password",
            "rabbitmq_password",
            "API_KEY",
            "auth_token",
            "confluent_api_secret",
        ):
            exc = ConfigurationError(
                "invalid",
                setting_name=sensitive_name,
                setting_value="plain-string-secret",
            )
            assert exc.setting_value == "***REDACTED***", sensitive_name

    def test_non_sensitive_value_is_preserved(self):
        """Non-sensitive names + non-secret values pass through unchanged (for debugging)."""
        from scrapy_extension.exceptions import ConfigurationError

        exc = ConfigurationError(
            "invalid mode",
            setting_name="mode",
            setting_value="INVALID_MODE",
        )
        assert exc.setting_value == "INVALID_MODE"

    def test_no_setting_passed_preserves_none(self):
        """Default (no name/value) leaves setting_value as None."""
        from scrapy_extension.exceptions import ConfigurationError

        exc = ConfigurationError("just a message")
        assert exc.setting_name is None
        assert exc.setting_value is None


class TestBackpressureSettings:
    """Round-4 BP-1: backpressure pause/resume depth settings.

    Two additive, default-``None`` fields (zero compat break) configuring the
    scheduler's depth-gated pull-rate throttle. ``pause_at`` is the queue depth
    at/above which ``next_request`` returns None (Scrapy's contract-correct
    "slow down" signal); ``resume_at`` is the depth at/below which it resumes
    (hysteresis, prevents flapping). When only ``pause_at`` is set the scheduler
    defaults ``resume_at := pause_at`` at consume time, so no cross-check fires.
    """

    def test_both_unset_defaults_to_none(self):
        """Default-off: both None → feature disabled (byte-identical to pre-BP)."""
        settings = Settings()
        assert settings.backpressure_pause_at is None
        assert settings.backpressure_resume_at is None

    def test_only_pause_at_set_accepted(self):
        """Only pause_at set → cross-check skipped (resume defaults to pause later)."""
        settings = Settings(backpressure_pause_at=10)
        assert settings.backpressure_pause_at == 10
        assert settings.backpressure_resume_at is None

    def test_resume_below_pause_accepted(self):
        """resume_at=5, pause_at=10 → valid hysteresis band."""
        settings = Settings(backpressure_pause_at=10, backpressure_resume_at=5)
        assert settings.backpressure_pause_at == 10
        assert settings.backpressure_resume_at == 5

    def test_resume_above_pause_rejected(self):
        """resume_at > pause_at → ConfigurationError (would never resume)."""
        from scrapy_extension.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError) as exc_info:
            Settings(backpressure_pause_at=10, backpressure_resume_at=15)
        assert exc_info.value.setting_name == "backpressure_resume_at"

    def test_negative_pause_at_rejected(self):
        """pause_at < 0 → ConfigurationError (depth cannot be negative)."""
        from scrapy_extension.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError):
            Settings(backpressure_pause_at=-1)

    def test_negative_resume_at_rejected(self):
        """resume_at < 0 → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError):
            Settings(backpressure_pause_at=10, backpressure_resume_at=-1)

    def test_resume_equals_pause_accepted(self):
        """resume_at == pause_at → valid (no hysteresis, but not invalid)."""
        settings = Settings(backpressure_pause_at=10, backpressure_resume_at=10)
        assert settings.backpressure_pause_at == 10
        assert settings.backpressure_resume_at == 10

    def test_only_resume_at_set_accepted(self):
        """Only resume_at set → accepted (pause_at None at settings layer; scheduler treats feature off)."""
        settings = Settings(backpressure_resume_at=5)
        assert settings.backpressure_pause_at is None
        assert settings.backpressure_resume_at == 5

    def test_pause_at_from_env(self, monkeypatch):
        """Loads from SCRAPY_BACKPRESSURE_PAUSE_AT env var."""
        monkeypatch.setenv("SCRAPY_BACKPRESSURE_PAUSE_AT", "10")
        settings = Settings()
        assert settings.backpressure_pause_at == 10

    def test_resume_at_from_env(self, monkeypatch):
        """Loads from SCRAPY_BACKPRESSURE_RESUME_AT env var."""
        monkeypatch.setenv("SCRAPY_BACKPRESSURE_RESUME_AT", "5")
        settings = Settings()
        assert settings.backpressure_resume_at == 5


# =============================================================================
# Round-6 SEC-SET: settings-file security validators (file-disjoint from SEC-BE)
# =============================================================================


class TestSec2MongoTlsModeGuard:
    """SEC-2: tls_allow_invalid_certificates=True forbidden in production modes.

    Disabling certificate validation breaks the MITM protection TLS provides.
    In ATLAS / SHARDED_CLUSTER / REPLICA_SET deployments (multi-host, production-
    tier) this is virtually always a misconfiguration or a developer shortcut
    that must not ship. STANDALONE stays permissive for local dev (e.g. a
    self-signed local mongod). Mirrors the Redis ``ssl_check_hostname``
    guidance and the RabbitMQ guest-guard pattern (raise, not warn).
    """

    def test_atlas_with_insecure_tls_rejected(self):
        """ATLAS + tls_allow_invalid_certificates=True → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import MongoDBSettings
        from scrapy_extension.settings.mongodb import MongoDBMode

        with pytest.raises(ConfigurationError) as exc_info:
            MongoDBSettings(mode=MongoDBMode.ATLAS, tls_allow_invalid_certificates=True)
        assert exc_info.value.setting_name == "tls_allow_invalid_certificates"
        assert exc_info.value.setting_value is None

    def test_sharded_cluster_with_insecure_tls_rejected(self):
        """SHARDED_CLUSTER + True → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import MongoDBSettings
        from scrapy_extension.settings.mongodb import MongoDBMode

        with pytest.raises(ConfigurationError):
            MongoDBSettings(
                mode=MongoDBMode.SHARDED_CLUSTER, tls_allow_invalid_certificates=True
            )

    def test_replica_set_with_insecure_tls_rejected(self):
        """REPLICA_SET + True → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import MongoDBSettings
        from scrapy_extension.settings.mongodb import MongoDBMode

        with pytest.raises(ConfigurationError):
            MongoDBSettings(
                mode=MongoDBMode.REPLICA_SET, tls_allow_invalid_certificates=True
            )

    def test_standalone_with_insecure_tls_accepted(self):
        """STANDALONE + True → accepted (local dev with self-signed certs)."""
        from scrapy_extension.settings import MongoDBSettings
        from scrapy_extension.settings.mongodb import MongoDBMode

        settings = MongoDBSettings(
            mode=MongoDBMode.STANDALONE,
            tls_enabled=True,
            tls_allow_invalid_certificates=True,
        )
        assert settings.tls_allow_invalid_certificates is True

    def test_any_mode_with_secure_tls_accepted(self):
        """tls_allow_invalid_certificates=False (default) accepted in all modes.

        R9-b SV2: ATLAS requires a ``mongodb+srv://`` URI; REPLICA_SET requires
        ``replica_set_name`` (or a URI carrying ``?replicaSet=``). The loop now
        supplies the per-mode required fields so the secure-TLS acceptance check
        runs across all four modes.
        """
        from scrapy_extension.settings import MongoDBSettings
        from scrapy_extension.settings.mongodb import MongoDBMode

        def kwargs_for(mode: MongoDBMode) -> dict:
            if mode == MongoDBMode.ATLAS:
                return {"uri": "mongodb+srv://cluster0.example.mongodb.net"}
            if mode == MongoDBMode.REPLICA_SET:
                return {"replica_set_name": "rs0"}
            return {}

        for mode in MongoDBMode:
            settings = MongoDBSettings(
                mode=mode, tls_allow_invalid_certificates=False, **kwargs_for(mode)
            )
            assert settings.tls_allow_invalid_certificates is False


class TestSec3ElasticsearchCleartextCredsGuard:
    """SEC-3: credentials over http:// (cleartext) forbidden.

    Sending ``api_key`` / ``password`` over an ``http://`` host leaks them on
    the wire. Reject at config time. ``https://`` + creds is fine; ``http://``
    with no creds is fine (e.g. a no-auth local dev node).
    """

    def test_http_host_with_basic_auth_rejected(self):
        """http:// host + complete basic auth → ConfigurationError."""
        from pydantic import SecretStr

        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import ElasticSearchSettings

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=["http://es:9200"],
                username="crawler",
                password=SecretStr("s3cr3t"),
            )
        assert exc_info.value.setting_name == "hosts"

    def test_http_host_with_api_key_rejected(self):
        """http:// host + api_key → ConfigurationError."""
        from pydantic import SecretStr

        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import ElasticSearchSettings

        with pytest.raises(ConfigurationError):
            ElasticSearchSettings(
                hosts=["http://es:9200"], api_key=SecretStr("key-123")
            )

    def test_cleartext_error_does_not_echo_host_or_credential(self):
        from pydantic import SecretStr

        from scrapy_extension.settings import ElasticSearchSettings

        host_marker = "elasticsearch-host-secret-marker"
        credential_marker = "elasticsearch-api-secret-marker"
        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchSettings(
                hosts=[f"http://{host_marker}:9200"],
                api_key=SecretStr(credential_marker),
            )

        error = exc_info.value
        for marker in (host_marker, credential_marker):
            assert marker not in str(error)
            assert marker not in repr(error.__dict__)
            assert marker not in "".join(traceback.format_exception(error))
        assert error.__cause__ is None
        assert error.__context__ is None

    def test_https_host_with_basic_auth_accepted(self):
        """https:// host + complete basic auth → accepted."""
        from pydantic import SecretStr

        from scrapy_extension.settings import ElasticSearchSettings

        settings = ElasticSearchSettings(
            hosts=["https://es:9200"], username="crawler", password=SecretStr("s3cr3t")
        )
        assert settings.password.get_secret_value() == "s3cr3t"

    def test_http_host_without_creds_accepted(self):
        """http:// host + no creds → accepted (local no-auth dev node)."""
        from scrapy_extension.settings import ElasticSearchSettings

        settings = ElasticSearchSettings(hosts=["http://localhost:9200"])
        assert settings.api_key is None
        assert settings.password is None

    def test_mixed_scheme_with_creds_rejected(self):
        """One http:// + one https:// host + creds → ConfigurationError (any http)."""
        from pydantic import SecretStr

        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import ElasticSearchSettings

        with pytest.raises(ConfigurationError):
            ElasticSearchSettings(
                hosts=["https://es.prod:9200", "http://es.dev:9200"],
                api_key=SecretStr("key"),
            )


class TestSec4EndpointUrlSchemeGuard:
    """SEC-4: SQS/DynamoDB endpoint_url must be http:// or https://.

    Catches typos and bare host:port values that would otherwise fall through
    to boto3's default chain (silent wrong target). ``http://`` is allowed
    (LocalStack). In standalone mode, unset uses the safe LocalStack default;
    cloud mode alone may use the real AWS default chain.
    """

    def test_sqs_no_scheme_rejected(self):
        """SQS endpoint_url without scheme → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import SqsSettings

        with pytest.raises(ConfigurationError) as exc_info:
            SqsSettings(endpoint_url="localstack:4566")
        assert exc_info.value.setting_name == "endpoint_url"

    def test_sqs_http_accepted(self):
        """SQS endpoint_url=http://localhost:4566 → accepted (LocalStack)."""
        from scrapy_extension.settings import SqsSettings

        settings = SqsSettings(endpoint_url="http://localhost:4566")
        assert settings.endpoint_url == "http://localhost:4566"

    def test_sqs_https_accepted(self):
        """SQS endpoint_url=https://... → accepted."""
        from scrapy_extension.settings import SqsSettings

        settings = SqsSettings(endpoint_url="https://sqs.example.com")
        assert settings.endpoint_url == "https://sqs.example.com"

    def test_sqs_standalone_unset_uses_safe_local_default(self):
        """SQS standalone without endpoint_url targets local LocalStack."""
        from scrapy_extension.settings import SqsSettings

        settings = SqsSettings()
        assert settings.endpoint_url == "http://localhost:4566"

    def test_dynamodb_no_scheme_rejected(self):
        """DynamoDB endpoint_url without scheme → ConfigurationError."""
        from scrapy_extension.exceptions import ConfigurationError
        from scrapy_extension.settings import DynamoDBSettings

        with pytest.raises(ConfigurationError):
            DynamoDBSettings(endpoint_url="localstack:8000")

    def test_dynamodb_http_accepted(self):
        """DynamoDB endpoint_url=http://localhost:8000 → accepted (LocalStack)."""
        from scrapy_extension.settings import DynamoDBSettings

        settings = DynamoDBSettings(endpoint_url="http://localhost:8000")
        assert settings.endpoint_url == "http://localhost:8000"

    def test_dynamodb_https_accepted(self):
        """DynamoDB endpoint_url=https://... → accepted."""
        from scrapy_extension.settings import DynamoDBSettings

        settings = DynamoDBSettings(endpoint_url="https://dynamodb.example.com")
        assert settings.endpoint_url == "https://dynamodb.example.com"

    def test_dynamodb_standalone_unset_uses_safe_local_default(self):
        """DynamoDB standalone without endpoint_url targets local LocalStack."""
        from scrapy_extension.settings import DynamoDBSettings

        settings = DynamoDBSettings()
        assert settings.endpoint_url == "http://localhost:4566"


# =============================================================================
# Round-14 R14-C: operability configurability (deferred settings-wiring)
# =============================================================================


class TestR14COperabilitySettings:
    """R14-C: the U4/U5/U2 knobs the runbook promises are now real SCRAPY_* settings.

    Round-9 (U4/U5) + round-12 (U2) shipped depth-sampling, max-item-bytes,
    delay-max-held, backpressure-threshold, and the pop-rate window as
    constructor defaults ONLY — ``BackendScheduler.from_settings`` never
    threaded them, so they were stuck at defaults. R14-C adds the 5 settings
    fields and threads them through. These tests pin the settings-layer half
    of the contract (defaults + env-var loading); the threading half lives in
    ``test_scheduler_settings_threading``.
    """

    def test_queue_depth_sample_every_default(self):
        """Default ``depth_sample_every`` is 100 (U4 default)."""
        settings = Settings()
        assert settings.queue_depth_sample_every == 100

    def test_queue_depth_sample_every_from_env(self, monkeypatch):
        """Loads from SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY env var."""
        monkeypatch.setenv("SCRAPY_QUEUE_DEPTH_SAMPLE_EVERY", "5")
        settings = Settings()
        assert settings.queue_depth_sample_every == 5

    def test_queue_max_item_bytes_default(self):
        """Default ``queue_max_item_bytes`` is 1 MiB (matches Memcached ceiling)."""
        settings = Settings()
        assert settings.queue_max_item_bytes == 1_048_576

    def test_queue_max_item_bytes_from_env(self, monkeypatch):
        """Loads from SCRAPY_QUEUE_MAX_ITEM_BYTES env var."""
        monkeypatch.setenv("SCRAPY_QUEUE_MAX_ITEM_BYTES", "2048")
        settings = Settings()
        assert settings.queue_max_item_bytes == 2048

    def test_queue_delay_max_held_default(self):
        """Default ``queue_delay_max_held`` is 100_000 (U5 default)."""
        settings = Settings()
        assert settings.queue_delay_max_held == 100_000

    def test_queue_delay_max_held_from_env(self, monkeypatch):
        """Loads from SCRAPY_QUEUE_DELAY_MAX_HELD env var."""
        monkeypatch.setenv("SCRAPY_QUEUE_DELAY_MAX_HELD", "5000")
        settings = Settings()
        assert settings.queue_delay_max_held == 5000

    def test_monitor_backpressure_threshold_default(self):
        """Default ``monitor_backpressure_threshold`` is 1000 (U2 default)."""
        settings = Settings()
        assert settings.monitor_backpressure_threshold == 1000

    def test_monitor_backpressure_threshold_from_env(self, monkeypatch):
        """Loads from SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD env var."""
        monkeypatch.setenv("SCRAPY_MONITOR_BACKPRESSURE_THRESHOLD", "2500")
        settings = Settings()
        assert settings.monitor_backpressure_threshold == 2500

    def test_monitor_pop_rate_window_s_default(self):
        """Default ``monitor_pop_rate_window_s`` is 60.0 (U2 default)."""
        settings = Settings()
        assert settings.monitor_pop_rate_window_s == 60.0

    def test_monitor_pop_rate_window_s_from_env(self, monkeypatch):
        """Loads from SCRAPY_MONITOR_POP_RATE_WINDOW_S env var."""
        monkeypatch.setenv("SCRAPY_MONITOR_POP_RATE_WINDOW_S", "30.0")
        settings = Settings()
        assert settings.monitor_pop_rate_window_s == 30.0


def test_mongodb_uri_userinfo_is_rejected_without_secret_leakage():
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri="mongodb://alice:super-secret@db.example.test:27017")

    assert exc_info.value.setting_name == "uri"
    assert "super-secret" not in str(exc_info.value)


def test_mongodb_malformed_uri_is_normalized_to_configuration_error():
    """Malformed authorities must not leak raw parser exceptions or credentials."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri="mongodb://alice:malformed-uri-secret@[")

    assert exc_info.value.setting_name == "uri"
    assert "malformed-uri-secret" not in str(exc_info.value)


@pytest.mark.parametrize("uri", ["mongodb://", "mongodb+srv://"])
def test_mongodb_uri_requires_an_endpoint(uri):
    """A syntactically valid scheme without an authority cannot reach PyMongo."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=uri)

    assert exc_info.value.setting_name == "uri"


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://db.example.test:not-a-port",
        "mongodb://db.example.test:65536",
        "mongodb://,db.example.test:27017",
        "mongodb://db.example.test:27017,,other.example.test:27017",
        "mongodb://db.example.test:27017,",
        "mongodb+srv://cluster.example.test:27017",
        "mongodb+srv://first.example.test,second.example.test",
        "mongodb+srv://[::1]",
        "mongodb+srv://192.0.2.1",
    ],
)
def test_mongodb_uri_rejects_malformed_authorities_before_driver_io(uri):
    """Every URI authority is parsed before PyMongo sees a connection string."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=uri)

    assert exc_info.value.setting_name == "uri"


@pytest.mark.parametrize(
    "query",
    [
        "tlsAllowInvalidCertificates=true",
        "tls=true;tlsAllowInvalidHostnames=true",
        "tls=true&tlsDisableOCSPEndpointCheck=true",
        "tlsInsecure=true",
    ],
)
def test_mongodb_uri_tls_policy_options_are_rejected(query):
    """Both PyMongo URI separators must not bypass TLS verification policy."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=f"mongodb://db.example.test:27017/?{query}")

    assert exc_info.value.setting_name == "uri"


@pytest.mark.parametrize(
    "query",
    [
        "proxyHost=proxy-secret.example.test",
        "proxyPort=1080;proxyUsername=proxy-user",
        "proxyPassword=proxy-secret",
        "PrOxYhOsT=proxy-secret.example.test&PROXYPORT=1080",
    ],
)
def test_mongodb_uri_proxy_options_are_rejected_without_leakage(query):
    """Proxy authority and credentials stay outside an untyped URI channel."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=f"mongodb://db.example.test:27017/?{query}")

    assert exc_info.value.setting_name == "uri"
    assert "proxy-secret" not in str(exc_info.value)
    assert "proxy-secret" not in repr(exc_info.value)


@pytest.mark.parametrize(
    "query",
    [
        "authMechanismProperties=AWS_SESSION_TOKEN:uri-secret",
        "tlsCertificateKeyFilePassword=uri-secret",
    ],
)
def test_mongodb_uri_credential_options_do_not_leak(query):
    """Credential query values are rejected without becoming error text."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(uri=f"mongodb://db.example.test:27017/?{query}")

    assert exc_info.value.setting_name == "uri"
    assert "uri-secret" not in str(exc_info.value)
    assert "uri-secret" not in repr(exc_info.value)


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://db.example.test:27017/#?tlsAllowInvalidHostnames=true",
        "mongodb://db.example.test:27017/#?authMechanismProperties=AWS_SESSION_TOKEN:fragment-secret",
    ],
)
def test_mongodb_uri_fragment_policy_bypass_is_rejected_without_leakage(uri):
    """Fragments must not hide options from stdlib parsing but not PyMongo."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            uri=uri,
            username="crawler",
            password="fragment-secret",
            tls_enabled=True,
        )

    assert exc_info.value.setting_name == "uri"
    assert "fragment-secret" not in str(exc_info.value)
    assert "fragment-secret" not in repr(exc_info.value)


@pytest.mark.parametrize(
    ("setting_name", "value"),
    [
        ("replica_set_members", ["alice:seed-secret@db.example.test:27017"]),
        ("replica_set_members", ["db.example.test:27017/?tlsInsecure=true"]),
        ("mongos_routers", ["db.example.test:27017#seed-secret"]),
        ("mongos_routers", ["mongodb://db.example.test:27017"]),
    ],
)
def test_mongodb_seed_endpoints_reject_uri_injection_without_leakage(
    setting_name, value
):
    """Generated replica/mongos URIs accept host syntax only, never URI pieces."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(**{setting_name: value})

    assert exc_info.value.setting_name == setting_name
    assert "seed-secret" not in str(exc_info.value)


def test_mongodb_seed_endpoints_normalize_bracketed_and_bare_ipv6():
    """IPv6 remains supported while the generated authority stays unambiguous."""
    from scrapy_extension.settings import MongoDBSettings

    settings = MongoDBSettings(replica_set_members=["[::1]:27017", "2001:db8::7"])

    assert settings.replica_set_members == ["[::1]:27017", "[2001:db8::7]"]


@pytest.mark.parametrize(
    "value",
    [
        object(),
        ["-bad-host:27017"],
        ["db.example.test:0"],
        ["[127.0.0.1]:27017"],
        ["[not-an-ip]:27017"],
        ["[::1]suffix"],
        ["one:two:three"],
    ],
)
def test_mongodb_seed_endpoint_parser_fails_closed_for_invalid_authorities(value):
    """Every malformed generated-URI authority is rejected before concatenation."""
    from scrapy_extension.settings.mongodb import validate_mongodb_seed_endpoints

    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_seed_endpoints(value, "replica_set_members")

    assert exc_info.value.setting_name == "replica_set_members"


@pytest.mark.parametrize(
    ("settings_kwargs", "missing_name"),
    [
        ({"username": "crawler"}, "password"),
        ({"password": "partial-secret"}, "username"),
    ],
)
def test_mongodb_partial_credentials_are_rejected(settings_kwargs, missing_name):
    """One-sided credentials must never become an anonymous MongoDB connection."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(**settings_kwargs)

    assert exc_info.value.setting_name == missing_name
    assert "partial-secret" not in str(exc_info.value)


@pytest.mark.parametrize(
    "settings_kwargs",
    [
        {
            "uri": "mongodb://db.example.test:27017",
            "username": "crawler",
            "password": "transport-secret",
        },
        {
            "mode": "replica_set",
            "replica_set_name": "rs0",
            "replica_set_members": ["db.example.test:27017"],
            "username": "crawler",
            "password": "transport-secret",
        },
        {
            "mode": "sharded_cluster",
            "mongos_routers": ["db.example.test:27017"],
            "username": "crawler",
            "password": "transport-secret",
        },
    ],
)
def test_mongodb_remote_authenticated_connections_require_tls(settings_kwargs):
    """URI, replica member, and mongos router endpoints all control TLS policy."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(**settings_kwargs)

    assert exc_info.value.setting_name == "tls_enabled"
    assert "transport-secret" not in str(exc_info.value)


def test_mongodb_loopback_authenticated_connection_allows_local_plaintext():
    """The explicitly direct local development compatibility path remains available."""
    from scrapy_extension.settings import MongoDBSettings

    settings = MongoDBSettings(
        uri="mongodb://127.0.0.1:27017",
        username="crawler",
        password="local-secret",
    )

    assert settings.tls_enabled is False


def test_mongodb_lookalike_localhost_requires_tls_for_authenticated_connections():
    """Only exact localhost and literal loopback IPs receive the dev exception."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            uri="mongodb://attacker.localhost:27017",
            username="crawler",
            password="lookalike-localhost-secret",
        )

    assert exc_info.value.setting_name == "tls_enabled"
    assert "lookalike-localhost-secret" not in str(exc_info.value)


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://localhost:27017/?replicaSet=rs0",
        "mongodb://localhost:27017/?directConnection=false",
        "mongodb://localhost:27017/?loadBalanced=true",
        "mongodb://127.0.0.1:27017,localhost:27018",
    ],
)
def test_mongodb_topology_bearing_loopback_uri_requires_tls_for_auth(uri):
    """The local plaintext exception cannot discover a remote topology."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            uri=uri,
            username="crawler",
            password="topology-uri-secret",
        )

    assert exc_info.value.setting_name == "tls_enabled"
    assert "topology-uri-secret" not in str(exc_info.value)


@pytest.mark.parametrize(
    "settings_kwargs",
    [
        {
            "mode": "replica_set",
            "replica_set_name": "rs0",
            "replica_set_members": ["127.0.0.1:27017"],
            "username": "crawler",
            "password": "topology-secret",
        },
        {
            "mode": "sharded_cluster",
            "mongos_routers": ["127.0.0.1:27017"],
            "username": "crawler",
            "password": "topology-secret",
        },
    ],
)
def test_mongodb_authenticated_cluster_topologies_require_tls(settings_kwargs):
    """Loopback seeds can discover remote members, so cluster auth needs TLS."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(**settings_kwargs)

    assert exc_info.value.setting_name == "tls_enabled"
    assert "topology-secret" not in str(exc_info.value)


def test_mongodb_authenticated_srv_uri_uses_implicit_verified_tls():
    """PyMongo's ``mongodb+srv`` transport is TLS-enabled even without a flag."""
    from scrapy_extension.settings import MongoDBSettings

    settings = MongoDBSettings(
        uri="mongodb+srv://cluster.example.mongodb.net",
        username="crawler",
        password="srv-secret",
    )

    assert settings.tls_enabled is False


def test_mongodb_remote_invalid_certificate_setting_is_rejected():
    """Self-signed certificate exemptions are constrained to loopback dev only."""
    from scrapy_extension.settings import MongoDBSettings

    with pytest.raises(ConfigurationError) as exc_info:
        MongoDBSettings(
            uri="mongodb://db.example.test:27017",
            tls_enabled=True,
            tls_allow_invalid_certificates=True,
        )

    assert exc_info.value.setting_name == "tls_allow_invalid_certificates"


@pytest.mark.parametrize(
    ("username", "password", "auth_mechanism", "setting_name"),
    [
        (" ", None, None, "username"),
        (None, " ", None, "password"),
        (None, object(), None, "password"),
        (None, None, "not-a-mongodb-mechanism", "auth_mechanism"),
    ],
)
def test_mongodb_runtime_authentication_validation_rejects_invalid_mutations(
    username, password, auth_mechanism, setting_name
):
    """Runtime settings mutation retains the same typed authentication boundary."""
    from scrapy_extension.settings.mongodb import validate_mongodb_authentication

    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_authentication(username, password, auth_mechanism)

    assert exc_info.value.setting_name == setting_name


@pytest.mark.parametrize("endpoint", [None, " ", "[", ":"])
def test_mongodb_loopback_classifier_fails_closed_for_malformed_endpoints(endpoint):
    """Unparseable endpoints never receive the local plaintext exception."""
    from scrapy_extension.settings.mongodb import _mongodb_endpoint_is_loopback

    assert _mongodb_endpoint_is_loopback(endpoint) is False


@pytest.mark.parametrize(
    ("tls_enabled", "tls_allow_invalid_certificates", "setting_name"),
    [
        ("true", False, "tls_enabled"),
        (False, "false", "tls_allow_invalid_certificates"),
    ],
)
def test_mongodb_transport_validation_rejects_runtime_nonboolean_tls_flags(
    tls_enabled, tls_allow_invalid_certificates, setting_name
):
    """Mutable TLS flags must not fall through Python truthiness semantics."""
    from scrapy_extension.settings import MongoDBMode
    from scrapy_extension.settings.mongodb import validate_mongodb_transport_security

    with pytest.raises(ConfigurationError) as exc_info:
        validate_mongodb_transport_security(
            mode=MongoDBMode.STANDALONE,
            uri="mongodb://localhost:27017",
            replica_set_members=[],
            mongos_routers=[],
            tls_enabled=tls_enabled,
            tls_allow_invalid_certificates=tls_allow_invalid_certificates,
            username=None,
            password=None,
            auth_mechanism=None,
        )

    assert exc_info.value.setting_name == setting_name
