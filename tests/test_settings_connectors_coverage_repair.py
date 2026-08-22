"""Focused contract coverage for settings and connection-manager hardening."""

from __future__ import annotations

from collections.abc import Generator
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import SecretStr, ValidationError, model_validator
from pytest_mock import MockerFixture
from scrapy.settings import Settings as ScrapySettings
from typing_extensions import Self

from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.backends.connectors._capabilities import capable_backends
from scrapy_extension.backends.connectors._config import resolve_backend_config
from scrapy_extension.backends.connectors._plugin_contract import (
    _is_safe_capability_message,
    _is_safe_manager_configuration_message,
    _load_descriptor_object,
)
from scrapy_extension.backends.registry import BackendDescriptor
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import (
    KafkaSettings,
    MemcachedSettings,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RocketMQSettings,
    Settings,
)
from scrapy_extension.settings._aws import (
    is_remote_http_endpoint,
    validate_aws_endpoint,
)
from scrapy_extension.settings._broker_endpoints import (
    normalize_kafka_broker_endpoints,
    normalize_rocketmq_namesrv_endpoints,
)
from scrapy_extension.settings._endpoint_validation import (
    has_invalid_percent_escape,
    parse_endpoint_host,
    parse_endpoint_port,
    parse_host_port_authority,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings
from scrapy_extension.settings.kafka import validate_kafka_authentication
from scrapy_extension.settings.mongodb import (
    MongoDBMode,
    is_mongodb_direct_loopback_uri,
)
from scrapy_extension.settings.redis import normalize_redis_host, normalize_redis_port


@pytest.mark.parametrize(
    ("value", "invalid"),
    [
        (None, True),
        ("path%2Fsegment", False),
        ("path%", True),
        ("path%G0", True),
        ("path%0g", True),
    ],
)
def test_endpoint_percent_and_port_boundaries_are_static(
    value: object, invalid: bool
) -> None:
    assert has_invalid_percent_escape(value) is invalid


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("[2001:0db8::1]", "2001:db8::1"),
        ("example.internal.", "example.internal."),
        ("127.0.0.1", "127.0.0.1"),
        ("[127.0.0.1]", None),
        ("[2001:db8::1", None),
        ("example/internal", None),
        ("127.1", None),
    ],
)
def test_endpoint_host_parser_handles_bracket_and_legacy_literal_boundaries(
    value: str, expected: str | None
) -> None:
    assert parse_endpoint_host(value) == expected


@pytest.mark.parametrize("value", [None, "", "0", "65536", "9" * 5000, "\uff11\uff12"])
def test_endpoint_port_parser_rejects_non_tcp_values(value: object) -> None:
    assert parse_endpoint_port(value) is None


def test_endpoint_authority_parser_distinguishes_default_required_and_ipv6_ports() -> (
    None
):
    assert parse_host_port_authority("broker", default_port=6650) == ("broker", 6650)
    assert parse_host_port_authority("broker", require_port=True) is None
    assert parse_host_port_authority("[2001:db8::1]:06650") == (
        "2001:db8::1",
        6650,
    )
    assert parse_host_port_authority("2001:db8::1", default_port=6650) == (
        "2001:db8::1",
        6650,
    )
    assert parse_host_port_authority("[2001:db8::1]tail") is None
    assert parse_host_port_authority("broker:6650", default_port=0) is None


def test_broker_endpoint_normalizers_canonicalize_valid_edges_and_reject_mixed_lists() -> (
    None
):
    assert (
        normalize_kafka_broker_endpoints(
            " broker:00090,[2001:0db8::1]:09092,::1 ", "bootstrap_servers"
        )
        == "broker:90,[2001:db8::1]:9092,[::1]"
    )
    assert normalize_rocketmq_namesrv_endpoints("10.0.0.1:08081;10.0.0.2:8081") == (
        "10.0.0.1:8081;10.0.0.2:8081"
    )
    assert normalize_rocketmq_namesrv_endpoints("namesrv.internal:8081") == (
        "namesrv.internal:8081"
    )
    with pytest.raises(ConfigurationError):
        normalize_kafka_broker_endpoints("broker:9092,", "bootstrap_servers")
    with pytest.raises(ConfigurationError):
        normalize_kafka_broker_endpoints("bröker:9092", "bootstrap_servers")
    with pytest.raises(ConfigurationError):
        normalize_rocketmq_namesrv_endpoints("namesrv.internal:8081;10.0.0.1:8081")


@pytest.mark.parametrize("hostile", [None, [], {}, object()])
def test_endpoint_policy_helpers_fail_closed_for_hostile_scalar_and_container_inputs(
    hostile: object,
) -> None:
    assert is_remote_http_endpoint(hostile) is False
    if hostile is not None:
        with pytest.raises(ConfigurationError):
            validate_aws_endpoint(hostile, cloud=False)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError):
        normalize_redis_host(hostile)
    with pytest.raises(ConfigurationError):
        normalize_redis_port(hostile)
    with pytest.raises(ConfigurationError):
        normalize_kafka_broker_endpoints(hostile, "bootstrap_servers")
    with pytest.raises(ConfigurationError):
        normalize_rocketmq_namesrv_endpoints(hostile)
    with pytest.raises(ConfigurationError):
        validate_kafka_authentication(
            "standalone", hostile, None, None, None, None, None
        )


def test_mongodb_loopback_classifier_rejects_non_uri_containers() -> None:
    assert is_mongodb_direct_loopback_uri([]) is False  # type: ignore[arg-type]
    assert is_mongodb_direct_loopback_uri({}) is False  # type: ignore[arg-type]


def test_aws_unsigned_endpoint_policy_has_local_remote_and_cloud_boundaries() -> None:
    assert validate_aws_endpoint(None, cloud=False) is None
    assert is_remote_http_endpoint(None) is False
    assert is_remote_http_endpoint("https://aws.example") is False
    assert is_remote_http_endpoint("http://127.0.0.1:4566") is False
    assert is_remote_http_endpoint("http://aws.example:4566") is True

    with pytest.raises(ConfigurationError, match="endpoint_url is required"):
        validate_aws_endpoint(None, cloud=False, require_endpoint=True)
    with pytest.raises(ConfigurationError, match="non-empty HTTP"):
        validate_aws_endpoint(123, cloud=False)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="not a valid"):
        validate_aws_endpoint("https://aws.example:not-a-port", cloud=False)
    with pytest.raises(ConfigurationError, match="absolute"):
        validate_aws_endpoint("file://aws.example", cloud=False)
    with pytest.raises(ConfigurationError, match="allow_remote_http"):
        validate_aws_endpoint("http://aws.example:4566", cloud=False)

    endpoint = "http://aws.example:4566"
    assert (
        validate_aws_endpoint(endpoint, cloud=False, allow_remote_http=True) == endpoint
    )
    with pytest.raises(ConfigurationError, match="Explicit AWS credentials"):
        validate_aws_endpoint(endpoint, cloud=False, explicit_credentials=True)


def test_settings_model_validate_and_safelist_drop_recursive_private_diagnostics() -> (
    None
):
    marker = "settings-model-private-marker"
    supplied = {"retry_delay": marker}
    with pytest.raises(ValidationError) as exc_info:
        Settings.model_validate(supplied, strict=True)
    error = exc_info.value
    assert supplied == {"retry_delay": marker}
    assert str(error) != marker
    assert marker not in repr(error.__dict__)
    assert marker not in error.json()
    assert error.__cause__ is None
    assert error.__context__ is None

    with pytest.raises(ConfigurationError) as safe_error:
        KafkaSettings.model_validate({"retries": marker}, strict=True)
    assert str(safe_error.value) == "Settings contain an invalid configuration value."
    assert safe_error.value.setting_name == "retries"

    with pytest.raises(ConfigurationError) as safe_error:
        MongoDBSettings.model_validate(
            {"mode": MongoDBMode.REPLICA_SET},
            strict=True,
        )
    assert "REPLICA_SET mode requires" in str(safe_error.value)
    assert safe_error.value.setting_name == "replica_set_name"


class _UntrustedModelValidateSettings(RedactedBaseSettings):
    marker: str

    @model_validator(mode="after")
    def _raise_private_error(self) -> Self:
        raise ConfigurationError(
            f"untrusted marker: {self.marker}", setting_name="marker"
        )


class _UnexpectedModelValidateSettings(RedactedBaseSettings):
    @model_validator(mode="after")
    def _raise_unexpected_error(self) -> Self:
        raise RuntimeError("unexpected private settings failure")


def test_settings_model_validate_sanitizes_untrusted_and_unexpected_validator_failures() -> (
    None
):
    marker = "untrusted-model-validator-marker"
    with pytest.raises(ConfigurationError) as private_error:
        _UntrustedModelValidateSettings.model_validate({"marker": marker})
    assert (
        str(private_error.value) == "Settings contain an invalid configuration value."
    )
    assert private_error.value.setting_name == "settings"
    assert marker not in repr(private_error.value.__dict__)

    with pytest.raises(ConfigurationError) as unexpected_error:
        _UnexpectedModelValidateSettings.model_validate({})
    assert (
        str(unexpected_error.value)
        == "Settings contain an invalid configuration value."
    )
    assert unexpected_error.value.setting_name == "settings"


def test_secret_assignment_keeps_plaintext_out_of_the_model_error_graph() -> None:
    settings = MongoDBSettings()
    settings.password = "assignment-secret"  # type: ignore[assignment]
    assert isinstance(settings.password, SecretStr)
    assert settings.password.get_secret_value() == "assignment-secret"
    settings.password = SecretStr("replacement-secret")
    assert settings.password.get_secret_value() == "replacement-secret"
    with pytest.raises(TypeError, match="matching plaintext"):
        settings.password = 7  # type: ignore[assignment]


@pytest.mark.parametrize(
    ("settings", "mutation", "validator", "setting_name"),
    [
        (
            MemcachedSettings(),
            {"host": "cache.example", "allow_remote_plaintext": False},
            "_validate_connection",
            "allow_remote_plaintext",
        ),
        (
            PulsarSettings(service_url="pulsar+ssl://localhost:6651"),
            {
                "service_url": "pulsar://localhost:6650",
                "auth_token": SecretStr("token"),
            },
            "_validate_connection",
            "service_url",
        ),
        (
            RabbitMQSettings(
                username="crawler", password=SecretStr("secret"), ssl_enabled=True
            ),
            {"host": "rabbit.example", "ssl_enabled": False},
            "_validate_mode_requirements",
            "ssl_enabled",
        ),
        (
            RocketMQSettings(),
            {
                "access_key": SecretStr("access"),
                "secret_key": SecretStr("secret"),
                "tls_enabled": False,
            },
            "_validate_connection",
            "tls_enabled",
        ),
        (
            MongoDBSettings(),
            {"storage_collection": "system.users"},
            "_validate_collection_domains",
            "collection_names",
        ),
    ],
)
def test_mutated_settings_are_revalidated_before_driver_use(
    settings: object,
    mutation: dict[str, object],
    validator: str,
    setting_name: str,
) -> None:
    for name, value in mutation.items():
        setattr(settings, name, value)
    with pytest.raises(ConfigurationError) as exc_info:
        getattr(settings, validator)()
    assert exc_info.value.setting_name == setting_name
    assert "system.users" not in str(exc_info.value)
    assert "cache.example" not in str(exc_info.value)


def test_capability_failure_is_static_and_capability_discovery_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = BackendDescriptor(
        backend_type="test_plugin",
        backend_cls_path="tests.missing.PluginBackend",
        settings_cls_path="tests.missing.PluginSettings",
        capabilities=frozenset({"queue"}),
    )
    monkeypatch.setattr(
        "scrapy_extension.backends.connectors._capabilities.get_registry",
        lambda: {"test_plugin": descriptor},
    )
    assert capable_backends("queue") == frozenset({"test_plugin"})
    assert capable_backends("storage") == frozenset()

    settings = ScrapySettings({"SCRAPY_BACKEND_TYPE": "kafka"})
    with pytest.raises(ConfigurationError) as exc_info:
        resolve_backend_config(
            settings,
            "SCRAPY_QUEUE_BACKEND_TYPE",
            "SCRAPY_QUEUE_BACKEND_SETTINGS",
            required_capabilities={"storage"},
            component_name="storage",
        )
    error = exc_info.value
    assert "does not support the storage interface" in str(error)
    assert "kafka" in str(error)
    assert "password" not in repr(error.__dict__)


def test_plugin_descriptor_loader_and_safelist_fail_closed() -> None:
    descriptor = BackendDescriptor(
        backend_type="third_party",
        backend_cls_path="no_such_module.PluginBackend",
        settings_cls_path="no_such_module.PluginSettings",
        capabilities=frozenset(),
    )
    with pytest.raises(ConfigurationError) as exc_info:
        _load_descriptor_object(descriptor, descriptor.backend_cls_path)
    assert str(exc_info.value) == "Selected backend has an invalid plugin class path."
    assert exc_info.value.setting_name == "SCRAPY_BACKEND_TYPE"

    assert _is_safe_capability_message(
        "Selected kafka does not support the storage interface and is missing "
        "capabilities. Capable bundled backends: ['elasticsearch', 'mongodb', 'redis']."
    )
    assert not _is_safe_capability_message(
        "Selected evil does not support the storage interface and is missing "
        "capabilities. Capable bundled backends: ['redis']."
    )
    assert _is_safe_manager_configuration_message(
        "Selected third-party backend does not implement its declared contract: "
        "missing QueueBackend."
    )
    assert not _is_safe_manager_configuration_message(
        "Selected third-party backend does not implement its declared contract: "
        "missing QueueBackend, SecretBackend."
    )


@pytest.fixture(autouse=True)
def _clear_manager_registry() -> Generator[None, None, None]:
    ConnectionManager.clear_registry()
    yield
    ConnectionManager.clear_registry()


def test_retry_pending_release_failures_are_nonfatal_and_diagnosed(
    mocker: MockerFixture,
) -> None:
    lease = SimpleNamespace(released=False)
    manager = SimpleNamespace()

    def fail_lease() -> None:
        lease.released = True
        raise KeyboardInterrupt("lease rollback")

    def fail_manager() -> None:
        raise RuntimeError("manager rollback")

    lease.release = fail_lease
    manager.close = fail_manager
    warning = mocker.patch(
        "scrapy_extension.backends.connectors._manager.logger.warning"
    )
    with ConnectionManager._registry_lock:
        ConnectionManager._pending_release_leases.append(lease)  # type: ignore[arg-type]
        ConnectionManager._pending_release_managers.append(manager)  # type: ignore[arg-type]
    try:
        ConnectionManager.retry_pending_releases()
        assert lease.released is True
        warning.assert_called_once_with("A pending connection-manager release failed.")
    finally:
        with ConnectionManager._registry_lock:
            ConnectionManager._pending_release_leases.clear()
            ConnectionManager._pending_release_managers.clear()


def test_forced_candidate_teardown_logs_without_leaking_cleanup_exception(
    mocker: MockerFixture,
) -> None:
    manager = ConnectionManager("redis")
    backend = Mock()
    backend.disconnect.side_effect = KeyboardInterrupt("candidate cleanup marker")
    manager._backend = backend
    warning = mocker.patch(
        "scrapy_extension.backends.connectors._manager.logger.warning"
    )

    ConnectionManager._disconnect_backend_safely(manager)

    backend.disconnect.assert_called_once_with()
    warning.assert_called_once_with("Error disconnecting evicted backend")
    assert manager._retired is True
    assert manager._backend is None


@pytest.mark.parametrize("typed_error", [False, True])
def test_retry_release_race_rebuilds_untrusted_connection_errors(
    typed_error: bool,
) -> None:
    manager = ConnectionManager(
        "redis", {"retry_attempts": 2, "retry_delay": 0, "reactor_io_timeout": 1}
    )
    marker = f"retry-release-private-marker-{typed_error}"

    def fail_after_release() -> None:
        with manager._lock:
            manager._retired = True
        if typed_error:
            raise BackendConnectionError(marker, backend_type=marker)
        raise RuntimeError(marker)

    manager._attempt_connection = fail_after_release  # type: ignore[method-assign]
    with pytest.raises(BackendConnectionError) as exc_info:
        manager._connect_with_retries([])
    if typed_error:
        assert (
            str(exc_info.value)
            == "Connection manager failed to connect to the selected backend."
        )
    else:
        assert str(exc_info.value) == "Connection manager was released while connecting"
    assert marker not in repr(exc_info.value.__dict__)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_direct_close_finishes_cleanup_before_rethrowing_control_validation_error(
    mocker: MockerFixture,
) -> None:
    manager = ConnectionManager("redis")
    backend = Mock()
    manager._backend = backend
    control = KeyboardInterrupt("registry validation control marker")
    mocker.patch.object(ConnectionManager, "_registry_key", side_effect=control)

    with pytest.raises(KeyboardInterrupt) as exc_info:
        manager.close()
    assert exc_info.value is control
    assert exc_info.value.__traceback__ is not None
    backend.disconnect.assert_called_once_with()
    assert manager._retired is True


def test_direct_close_reports_configuration_validation_after_finalizer(
    mocker: MockerFixture,
) -> None:
    manager = ConnectionManager("redis")
    backend = Mock()
    manager._backend = backend
    mocker.patch.object(
        ConnectionManager,
        "_registry_key",
        side_effect=ConfigurationError("private invalid key", setting_name="private"),
    )

    with pytest.raises(ConfigurationError) as exc_info:
        manager.close()
    assert str(exc_info.value) == "Connection manager configuration is invalid."
    assert exc_info.value.setting_name == "configuration"
    backend.disconnect.assert_called_once_with()
    assert manager._retired is True
