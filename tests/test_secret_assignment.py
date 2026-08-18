"""Targeted secret-wrapper preservation for mutable bundled settings."""

from __future__ import annotations

import traceback
from typing import Annotated, Any

import pytest
from pydantic import SecretBytes, SecretStr
from pydantic_settings import SettingsConfigDict

from scrapy_extension.settings import (
    DynamoDBSettings,
    ElasticSearchSettings,
    KafkaSettings,
    MongoDBSettings,
    PulsarSettings,
    RabbitMQSettings,
    RedisSettings,
    RocketMQSettings,
    SqsSettings,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings

_BUNDLED_SECRET_FIELDS: tuple[
    tuple[type[Any], str, type[SecretStr] | type[SecretBytes]], ...
] = (
    (RedisSettings, "password", SecretStr),
    (RedisSettings, "sentinel_password", SecretStr),
    (MongoDBSettings, "password", SecretStr),
    (ElasticSearchSettings, "api_key", SecretStr),
    (ElasticSearchSettings, "password", SecretStr),
    (KafkaSettings, "sasl_password", SecretStr),
    (KafkaSettings, "confluent_api_key", SecretStr),
    (KafkaSettings, "confluent_api_secret", SecretStr),
    (PulsarSettings, "auth_token", SecretStr),
    (RabbitMQSettings, "url", SecretStr),
    (RabbitMQSettings, "password", SecretStr),
    (RocketMQSettings, "access_key", SecretStr),
    (RocketMQSettings, "secret_key", SecretStr),
    (SqsSettings, "aws_access_key_id", SecretStr),
    (SqsSettings, "aws_secret_access_key", SecretStr),
    (DynamoDBSettings, "aws_access_key_id", SecretStr),
    (DynamoDBSettings, "aws_secret_access_key", SecretStr),
)


def _settings_instance(settings_type: type[Any]) -> Any:
    if settings_type is RabbitMQSettings:
        return settings_type(
            username="crawler", password="initial-secret", ssl_enabled=True
        )
    return settings_type()


@pytest.mark.parametrize(
    "settings_type,field_name,wrapper_type",
    _BUNDLED_SECRET_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
def test_every_bundled_secret_field_wraps_plaintext_before_publication(
    settings_type: type[Any],
    field_name: str,
    wrapper_type: type[SecretStr] | type[SecretBytes],
) -> None:
    marker = f"{settings_type.__name__}-{field_name}-assignment-marker"
    settings = _settings_instance(settings_type)

    setattr(settings, field_name, marker)

    stored = getattr(settings, field_name)
    assert type(stored) is wrapper_type
    assert stored.get_secret_value() == marker
    assert marker not in repr(settings)
    assert marker not in repr(settings.__dict__)
    dumped = settings.model_dump()
    assert type(dumped[field_name]) is wrapper_type
    assert marker not in repr(dumped)
    assert marker not in settings.model_dump_json()


@pytest.mark.parametrize(
    "settings_type,field_name,_wrapper_type",
    _BUNDLED_SECRET_FIELDS,
    ids=lambda value: value.__name__ if isinstance(value, type) else None,
)
def test_every_bundled_secret_field_rejects_unrelated_assignment_without_retention(
    settings_type: type[Any],
    field_name: str,
    _wrapper_type: type[SecretStr] | type[SecretBytes],
) -> None:
    marker = f"{settings_type.__name__}-{field_name}-invalid-marker"
    settings = _settings_instance(settings_type)

    with pytest.raises(TypeError) as exc_info:
        setattr(settings, field_name, [marker])

    error = exc_info.value
    assert str(error) == (
        "Secret setting assignment requires the matching plaintext or "
        "secret-wrapper type."
    )
    assert marker not in repr(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    current_traceback = error.__traceback__
    while current_traceback is not None:
        module_name = current_traceback.tb_frame.f_globals.get("__name__", "")
        if module_name.startswith("scrapy_extension"):
            assert marker not in repr(current_traceback.tb_frame.f_locals)
        current_traceback = current_traceback.tb_next


class _AnnotatedSecretSettings(RedactedBaseSettings):
    model_config = SettingsConfigDict(env_prefix="TEST_SECRET_ASSIGNMENT_")

    optional_text: Annotated[SecretStr | None, "test metadata"] = None
    required_bytes: Annotated[SecretBytes, "test metadata"]


def test_annotated_secret_bytes_and_optional_none_assignment() -> None:
    settings = _AnnotatedSecretSettings(required_bytes=b"initial")

    settings.optional_text = "plain-text"  # type: ignore[assignment]
    settings.required_bytes = b"plain-bytes"  # type: ignore[assignment]

    assert type(settings.optional_text) is SecretStr
    assert settings.optional_text.get_secret_value() == "plain-text"
    assert type(settings.required_bytes) is SecretBytes
    assert settings.required_bytes.get_secret_value() == b"plain-bytes"
    settings.optional_text = None
    assert settings.optional_text is None


def test_required_secret_rejects_none_immediately() -> None:
    settings = RabbitMQSettings(
        username="crawler", password="initial-secret", ssl_enabled=True
    )

    with pytest.raises(TypeError):
        settings.password = None  # type: ignore[assignment]

    assert settings.password.get_secret_value() == "initial-secret"


def test_nonsecret_assignment_remains_unvalidated_and_models_remain_mutable() -> None:
    settings = RedisSettings()

    settings.port = "post-construction-value"  # type: ignore[assignment]

    assert settings.port == "post-construction-value"
