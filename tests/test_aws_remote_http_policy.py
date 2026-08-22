"""Fail-closed standalone AWS HTTP endpoint policy regression tests."""

from __future__ import annotations

import os
import traceback
from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.credentials import Credentials

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


@pytest.mark.parametrize(
    "endpoint_url",
    [
        "https://aws-proxy.example?token=endpoint-secret",
        "https://aws-proxy.example#endpoint-secret",
        "https://aws-proxy.example?",
        "https://aws-proxy.example#",
        "https://aws" + chr(0) + "-proxy.example",
    ],
)
@pytest.mark.parametrize(
    ("settings_type", "backend_type", "session_target"), _AWS_BACKEND_CASES
)
def test_mutated_endpoint_structure_rejects_before_session_construction(
    endpoint_url: str,
    settings_type: type[Any],
    backend_type: type[Any],
    session_target: str,
    mocker,
) -> None:
    """Post-construction endpoint changes cannot smuggle URL data into boto3."""
    settings = settings_type()
    settings.endpoint_url = endpoint_url
    session = mocker.patch(session_target)

    with pytest.raises(ConfigurationError) as exc_info:
        backend_type(settings).connect()

    assert exc_info.value.setting_name == "endpoint_url"
    assert "endpoint-secret" not in str(exc_info.value)
    session.assert_not_called()


@pytest.mark.parametrize(
    ("settings_type", "backend_type", "session_target"), _AWS_BACKEND_CASES
)
def test_mutated_remote_http_explicit_keys_reject_before_session_construction(
    settings_type: type[Any],
    backend_type: type[Any],
    session_target: str,
    mocker,
) -> None:
    """A post-construction key pair cannot bypass the remote-HTTP guard."""
    settings = settings_type()
    settings.endpoint_url = "http://aws-proxy.example:4566"
    settings.allow_remote_http = True
    settings.aws_access_key_id = "mutated-access-marker"
    settings.aws_secret_access_key = "mutated-secret-marker"
    session = mocker.patch(session_target)

    with pytest.raises(ConfigurationError) as exc_info:
        backend_type(settings).connect()

    assert exc_info.value.setting_name == "endpoint_url"
    session.assert_not_called()
    _assert_marker_absent(exc_info.value, "aws-proxy.example")
    _assert_marker_absent(exc_info.value, "mutated-access-marker")
    _assert_marker_absent(exc_info.value, "mutated-secret-marker")


@pytest.mark.parametrize(
    ("settings_type", "backend_type", "session_target"), _AWS_BACKEND_CASES
)
def test_mutated_remote_http_noncanonical_opt_in_rejects_before_session_construction(
    settings_type: type[Any],
    backend_type: type[Any],
    session_target: str,
    mocker,
) -> None:
    """A truthy string cannot become a remote-HTTP policy decision."""
    settings = settings_type()
    settings.endpoint_url = "http://aws-proxy.example:4566"
    settings.allow_remote_http = "true"
    session = mocker.patch(session_target)

    with pytest.raises(ConfigurationError) as exc_info:
        backend_type(settings).connect()

    assert exc_info.value.setting_name == "allow_remote_http"
    session.assert_not_called()


@pytest.mark.parametrize(
    ("settings_type", "backend_type", "session_target"), _AWS_BACKEND_CASES
)
def test_remote_http_generation_passes_unsigned_config_before_sdk_io(
    settings_type: type[Any],
    backend_type: type[Any],
    session_target: str,
    mocker,
) -> None:
    """The backend route, not only the helper, installs the unsigned control."""
    session = mocker.MagicMock(name="private-session")
    if backend_type is SqsBackend:
        session.client.return_value = mocker.MagicMock(name="sqs-client")
    else:
        resource = mocker.MagicMock(name="dynamodb-resource")
        table = mocker.MagicMock(name="dynamodb-table")
        table.load.return_value = None
        table.table_status = "ACTIVE"
        resource.Table.return_value = table
        session.resource.return_value = resource
    mocker.patch(session_target, return_value=session)

    backend = backend_type(
        settings_type(
            endpoint_url="http://aws-proxy.example:4566",
            allow_remote_http=True,
        )
    )
    backend.connect()

    if backend_type is SqsBackend:
        kwargs = session.client.call_args.kwargs
    else:
        kwargs = session.resource.call_args.kwargs
    assert kwargs["endpoint_url"] == "http://aws-proxy.example:4566"
    assert kwargs["config"].signature_version is UNSIGNED
    assert kwargs["config"].ignore_configured_endpoint_urls is True


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


@pytest.mark.parametrize(
    ("settings_type", "backend_type"),
    [(SqsSettings, SqsBackend), (DynamoDBSettings, DynamoDBBackend)],
)
@pytest.mark.parametrize(
    "credential_source",
    ["environment", "profile", "session", "custom-session", "metadata-provider"],
)
def test_remote_http_generation_is_unsigned_for_ambient_credentials(
    settings_type: type[Any],
    backend_type: type[Any],
    credential_source: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """Ambient credentials cannot add an Authorization header to opted-in HTTP."""
    for name in tuple(os.environ):
        if name.startswith("AWS_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    if credential_source == "environment":
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "environment-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
    elif credential_source == "profile":
        credentials_file = tmp_path / "credentials"
        credentials_file.write_text(
            "[remote-http]\naws_access_key_id = profile-key\n"
            "aws_secret_access_key = profile-secret\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("AWS_PROFILE", "remote-http")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_file))
    elif credential_source == "session":
        # A session token is an ambient temporary-credential input, not an
        # explicit package credential pair.
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "session-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "session-secret")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "session-token")

    metadata_provider: Any | None = None
    if credential_source == "custom-session":
        # Credentials supplied directly to a caller-owned boto3 Session are a
        # separate ambient source from the process environment. The backend's
        # Config must still force anonymous request signing.
        session = boto3.session.Session(
            aws_access_key_id="custom-session-key",
            aws_secret_access_key="custom-session-secret",
            aws_session_token="custom-session-token",
            region_name="us-east-1",
        )
    elif credential_source == "metadata-provider":
        # A metadata-style provider must not even be consulted while building an
        # unsigned remote-HTTP client. This avoids a provider network call before
        # the transport policy has become effective.
        class _MetadataProvider:
            METHOD = "review-metadata"
            CANONICAL_NAME = "review-metadata"

            def __init__(self) -> None:
                self.called = False

            def load(self) -> Credentials:
                self.called = True
                return Credentials("metadata-key", "metadata-secret", "metadata-token")

        metadata_provider = _MetadataProvider()
        session = boto3.session.Session(region_name="us-east-1")
        resolver = session._session.get_component("credential_provider")
        resolver.providers.insert(0, metadata_provider)
    else:
        session = boto3.session.Session()

    # Keep an ambient endpoint override hostile: only the validated endpoint
    # supplied by the backend may select the destination.
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://ambient.invalid:9999")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SQS", "http://ambient.invalid:9998")
    monkeypatch.setenv("AWS_ENDPOINT_URL_DYNAMODB", "http://ambient.invalid:9997")

    backend = backend_type(
        settings_type(
            endpoint_url="http://aws-proxy.example:4566",
            allow_remote_http=True,
        )
    )
    _snapshot, kwargs = backend._capture_connection_snapshot()

    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
    config = kwargs["config"]
    assert config.signature_version is UNSIGNED
    assert config.ignore_configured_endpoint_urls is True

    # Build the real botocore client/resource without making a network call. The
    # unsigned config must also remove ambient credentials from its request signer.
    if backend_type is SqsBackend:
        client = session.client("sqs", **kwargs)
    else:
        resource = session.resource("dynamodb", **kwargs)
        client = resource.meta.client
    try:
        assert client.meta.config.signature_version is UNSIGNED
        assert client._request_signer._credentials is None
    finally:
        client.close()
    if metadata_provider is not None:
        assert metadata_provider.called is False
    assert client.meta.endpoint_url == "http://aws-proxy.example:4566"


@pytest.mark.parametrize(
    ("settings_type", "backend_type"),
    [(SqsSettings, SqsBackend), (DynamoDBSettings, DynamoDBBackend)],
)
@pytest.mark.parametrize(
    "endpoint_url",
    [
        "http://localhost:4566",
        "http://127.0.0.1:4566",
        "http://[::1]:4566",
        "https://aws-proxy.example",
    ],
)
def test_loopback_and_https_generations_keep_normal_signing_controls(
    settings_type: type[Any],
    backend_type: type[Any],
    endpoint_url: str,
) -> None:
    settings = settings_type(
        endpoint_url=endpoint_url,
        aws_access_key_id="explicit-key",
        aws_secret_access_key="explicit-secret",
    )
    _backend = backend_type(settings)
    _snapshot, kwargs = _backend._capture_connection_snapshot()

    assert kwargs["config"].signature_version is None
    assert kwargs["aws_access_key_id"] == "explicit-key"
    assert kwargs["aws_secret_access_key"] == "explicit-secret"

    session = boto3.session.Session(region_name="us-east-1")
    if backend_type is SqsBackend:
        client = session.client("sqs", **kwargs)
    else:
        resource = session.resource("dynamodb", **kwargs)
        client = resource.meta.client
    try:
        assert client.meta.config.signature_version is not UNSIGNED
        assert client._request_signer._credentials is not None
        assert client._request_signer._credentials.access_key == "explicit-key"
    finally:
        client.close()
