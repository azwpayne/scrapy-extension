"""Fail-closed standalone AWS HTTP endpoint policy regression tests."""

from __future__ import annotations

import traceback
from typing import Any

import pytest

from scrapy_extension.backends import dynamodb as dynamodb_mod
from scrapy_extension.backends import sqs as sqs_mod
from scrapy_extension.backends.dynamodb import DynamoDBBackend
from scrapy_extension.backends.sqs import SqsBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import DynamoDBSettings, SqsSettings

_AWS_SETTINGS_CASES: tuple[tuple[type[Any], str], ...] = (
    (SqsSettings, "SCRAPY_SQS_ALLOW_REMOTE_HTTP"),
    (DynamoDBSettings, "SCRAPY_DYNAMODB_ALLOW_REMOTE_HTTP"),
)
_AWS_BACKEND_CASES: tuple[tuple[type[Any], type[Any], str], ...] = (
    (SqsSettings, SqsBackend, "scrapy_extension.backends.sqs.boto3.session.Session"),
    (
        DynamoDBSettings,
        DynamoDBBackend,
        "scrapy_extension.backends.dynamodb.boto3.session.Session",
    ),
)
_AWS_LOOPBACK_HTTP_ENDPOINTS = (
    "http://localhost:4566",
    "http://localhost.:4566",
    "http://127.0.0.1:4566",
    "http://[::1]:4566",
)
_AWS_REMOTE_HTTP_HOSTS = (
    "aws-proxy.example",
    "attacker.localhost",
    "localhost..",
    "127.1",
    "0177.0.0.1",
    "2130706433",
    "[::ffff:127.0.0.1]",
    "0.0.0.0",
    "[::]",
    "[::1%25lo0]",
    "192.0.2.1",
    "[2001:db8::1]",
)


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
@pytest.mark.parametrize("host", _AWS_REMOTE_HTTP_HOSTS)
def test_remote_standalone_http_fails_closed(
    settings_type: type[Any], _environment_name: str, host: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(endpoint_url=f"http://{host}:4566")

    assert exc_info.value.setting_name == "allow_remote_http"
    _assert_marker_absent(exc_info.value, host)


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
def test_remote_standalone_http_requires_exact_opt_in(
    settings_type: type[Any], _environment_name: str
) -> None:
    settings = settings_type(
        endpoint_url="http://aws-proxy.example:4566", allow_remote_http=True
    )

    assert settings.allow_remote_http is True


@pytest.mark.parametrize(("settings_type", "environment_name"), _AWS_SETTINGS_CASES)
def test_remote_standalone_http_environment_opt_in(
    settings_type: type[Any],
    environment_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(environment_name, "true")

    settings = settings_type(endpoint_url="http://aws-proxy.example:4566")

    assert settings.allow_remote_http is True


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
@pytest.mark.parametrize("lookalike", [1, 0, "yes", object()])
def test_remote_http_opt_in_rejects_non_boolean_lookalikes(
    settings_type: type[Any], _environment_name: str, lookalike: object
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(
            endpoint_url="http://aws-proxy.example:4566",
            allow_remote_http=lookalike,
        )

    assert exc_info.value.setting_name == "allow_remote_http"


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
def test_explicit_credentials_over_remote_http_fail_even_with_opt_in(
    settings_type: type[Any], _environment_name: str
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        settings_type(
            endpoint_url="http://aws-proxy.example:4566",
            allow_remote_http=True,
            aws_access_key_id="access-marker",
            aws_secret_access_key="secret-marker",
        )

    error = exc_info.value
    assert error.setting_name == "endpoint_url"
    assert "aws-proxy" not in str(error)
    assert "access-marker" not in str(error)
    assert "secret-marker" not in str(error)


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
@pytest.mark.parametrize("endpoint_url", _AWS_LOOPBACK_HTTP_ENDPOINTS)
def test_loopback_http_remains_available(
    settings_type: type[Any], _environment_name: str, endpoint_url: str
) -> None:
    settings_type(
        endpoint_url=endpoint_url,
        aws_access_key_id="local-access",
        aws_secret_access_key="local-secret",
    )


@pytest.mark.parametrize(("settings_type", "_environment_name"), _AWS_SETTINGS_CASES)
def test_remote_https_remains_available(
    settings_type: type[Any], _environment_name: str
) -> None:
    settings_type(endpoint_url="https://aws-proxy.example:4566")


def _assert_marker_absent(error: BaseException, marker: str) -> None:
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)
    assert marker not in "".join(traceback.format_exception(error))
    assert getattr(error, "setting_value", None) is None
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("settings_type", "backend_type", "session_target"), _AWS_BACKEND_CASES
)
@pytest.mark.parametrize("host", _AWS_REMOTE_HTTP_HOSTS)
def test_mutated_remote_http_rejects_before_session_construction(
    settings_type: type[Any],
    backend_type: type[Any],
    session_target: str,
    host: str,
    mocker,
) -> None:
    settings = settings_type()
    settings.endpoint_url = f"http://{host}:4566"
    session = mocker.patch(session_target)

    with pytest.raises(ConfigurationError) as exc_info:
        backend_type(settings).connect()

    assert exc_info.value.setting_name == "allow_remote_http"
    session.assert_not_called()
    _assert_marker_absent(exc_info.value, host)


def test_sqs_generation_captures_remote_http_policy_immutably(mocker) -> None:
    settings = SqsSettings(
        endpoint_url="http://aws-proxy.example:4566", allow_remote_http=True
    )
    session = mocker.MagicMock()
    mocker.patch.object(sqs_mod.boto3.session, "Session", return_value=session)
    backend = SqsBackend(settings)

    backend.connect()
    settings.allow_remote_http = False

    assert backend._generation is not None
    assert backend._generation.snapshot.allow_remote_http is True


def test_dynamodb_generation_captures_remote_http_policy_immutably(mocker) -> None:
    settings = DynamoDBSettings(
        endpoint_url="http://aws-proxy.example:4566", allow_remote_http=True
    )
    session = mocker.MagicMock()
    resource = mocker.MagicMock()
    table = mocker.MagicMock()
    table.load.return_value = None
    table.table_status = "ACTIVE"
    resource.Table.return_value = table
    session.resource.return_value = resource
    mocker.patch.object(dynamodb_mod.boto3.session, "Session", return_value=session)
    backend = DynamoDBBackend(settings)

    backend.connect()
    settings.allow_remote_http = False

    assert backend._generation is not None
    assert backend._generation.snapshot.allow_remote_http is True
