"""Shared AWS credential and endpoint security invariants."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

from pydantic import SecretStr

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings._endpoint_validation import parse_endpoint_host
from scrapy_extension.settings._transport_security import is_loopback_host

# AWS partition region identifiers are not all three labels: GovCloud/ISO use
# four (``us-gov-west-1``), while the European Sovereign Cloud starts with a
# longer label (``eusc-de-east-1``). Validate the stable structural grammar
# without a frozen region allowlist that would reject future launches.
_AWS_REGION_PATTERN = re.compile(r"^[a-z][a-z0-9]+(?:-[a-z][a-z0-9]*)+-[0-9]+$")

# These are the exact diagnostics emitted by the validators in this module.
# They contain only fixed field names and policy text, so terminal backend
# boundaries may retain them without exposing endpoint or credential values.
_AWS_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        (
            "region_name must be a lowercase, hyphen-delimited AWS region "
            "identifier ending in a numeric label (e.g. 'us-east-1', "
            "'us-gov-west-1', 'eusc-de-east-1')."
        ),
        "aws_access_key_id must be a string when explicitly configured.",
        "aws_access_key_id must be non-empty when explicitly configured.",
        "aws_secret_access_key must be a string when explicitly configured.",
        "aws_secret_access_key must be non-empty when explicitly configured.",
        (
            "aws_access_key_id is required when aws_secret_access_key is set; "
            "set both or leave both unset to use the ambient credential chain."
        ),
        (
            "aws_secret_access_key is required when aws_access_key_id is set; "
            "set both or leave both unset to use the ambient credential chain."
        ),
        (
            "endpoint_url is required in standalone mode to prevent an accidental "
            "fallback to the real AWS endpoint chain."
        ),
        "endpoint_url must be a non-empty HTTP(S) URL.",
        "endpoint_url must not contain surrounding whitespace or control characters.",
        "endpoint_url is not a valid HTTP(S) URL.",
        "endpoint_url must be an absolute HTTP(S) URL with a hostname.",
        (
            "endpoint_url must not contain URL userinfo; configure AWS credentials "
            "through the dedicated credential fields."
        ),
        "endpoint_url must not contain a query or fragment.",
        "An explicit endpoint_url in cloud mode must use HTTPS.",
        "allow_remote_http must be a boolean.",
        (
            "Remote standalone HTTP endpoints require allow_remote_http=True. "
            "Use this override only for an explicitly trusted private network."
        ),
        (
            "Explicit AWS credentials cannot be sent to a remote HTTP endpoint; "
            "use HTTPS or a loopback LocalStack endpoint."
        ),
    }
)


def validate_aws_region_name(region_name: object) -> str:
    """Return one AWS-style region name or raise a typed config error."""
    if type(region_name) is not str or not _AWS_REGION_PATTERN.fullmatch(region_name):
        raise ConfigurationError(
            (
                "region_name must be a lowercase, hyphen-delimited AWS region "
                "identifier ending in a numeric label (e.g. 'us-east-1', "
                "'us-gov-west-1', 'eusc-de-east-1')."
            ),
            setting_name="region_name",
        )
    return region_name


def _credential_value(value: SecretStr | str | None, setting_name: str) -> str | None:
    """Extract one explicitly configured credential and reject blank values."""
    if value is None:
        return None
    if type(value) is SecretStr:
        text = value.get_secret_value()
    elif type(value) is str:
        text = value
    else:
        raise ConfigurationError(
            f"{setting_name} must be a string when explicitly configured.",
            setting_name=setting_name,
        )
    if not text.strip():
        raise ConfigurationError(
            f"{setting_name} must be non-empty when explicitly configured.",
            setting_name=setting_name,
        )
    return text


def validate_aws_credentials(
    access_key: SecretStr | str | None,
    secret_key: SecretStr | str | None,
) -> tuple[str | None, str | None]:
    """Return a non-empty explicit pair or the intentional ambient sentinel."""
    key_text = _credential_value(access_key, "aws_access_key_id")
    secret_text = _credential_value(secret_key, "aws_secret_access_key")
    if key_text is None and secret_text is not None:
        raise ConfigurationError(
            "aws_access_key_id is required when aws_secret_access_key is set; "
            "set both or leave both unset to use the ambient credential chain.",
            setting_name="aws_access_key_id",
        )
    if key_text is not None and secret_text is None:
        raise ConfigurationError(
            "aws_secret_access_key is required when aws_access_key_id is set; "
            "set both or leave both unset to use the ambient credential chain.",
            setting_name="aws_secret_access_key",
        )
    return key_text, secret_text


def normalize_allow_remote_http(value: object) -> bool:
    """Parse canonical environment booleans without accepting truthy lookalikes."""
    if type(value) is bool:
        return value
    if type(value) is str:
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise ConfigurationError(
        "allow_remote_http must be a boolean.",
        setting_name="allow_remote_http",
    )


def validate_allow_remote_http(value: object) -> bool:
    """Require an exact boolean when mutable settings bypass Pydantic."""
    if type(value) is not bool:
        raise ConfigurationError(
            "allow_remote_http must be a boolean.",
            setting_name="allow_remote_http",
        )
    return value


def is_remote_http_endpoint(endpoint_url: object) -> bool:
    """Return whether a validated endpoint is non-loopback plaintext HTTP.

    This helper is also used by mutable-settings revalidation.  Treat malformed
    scalars and containers as non-endpoints instead of handing them to
    ``urlsplit`` (which attempts bytes coercion and can invoke hostile methods).
    """
    if type(endpoint_url) is not str:
        return False
    parsed = urlsplit(endpoint_url)
    hostname = parsed.hostname
    return (
        parsed.scheme.lower() == "http"
        and hostname is not None
        and not is_loopback_host(hostname)
    )


def validate_aws_endpoint(
    endpoint_url: str | None,
    *,
    cloud: bool,
    require_endpoint: bool = False,
    allow_remote_http: object = False,
    explicit_credentials: bool = False,
) -> str | None:
    """Validate an AWS endpoint override without retaining or echoing userinfo."""
    remote_http_allowed = validate_allow_remote_http(allow_remote_http)
    if type(cloud) is not bool or type(require_endpoint) is not bool:
        raise ConfigurationError(
            "AWS endpoint policy flags must be booleans.",
            setting_name="endpoint_url",
        )
    if type(explicit_credentials) is not bool:
        raise ConfigurationError(
            "AWS credential policy flag must be a boolean.",
            setting_name="endpoint_url",
        )
    if endpoint_url is None:
        if require_endpoint:
            raise ConfigurationError(
                "endpoint_url is required in standalone mode to prevent an accidental "
                "fallback to the real AWS endpoint chain.",
                setting_name="endpoint_url",
            )
        return None
    if type(endpoint_url) is not str or not endpoint_url.strip():
        raise ConfigurationError(
            "endpoint_url must be a non-empty HTTP(S) URL.",
            setting_name="endpoint_url",
        )
    if endpoint_url != endpoint_url.strip() or any(
        ord(character) < 32 or ord(character) == 127 for character in endpoint_url
    ):
        raise ConfigurationError(
            "endpoint_url must not contain surrounding whitespace or control characters.",
            setting_name="endpoint_url",
        )
    try:
        parsed = urlsplit(endpoint_url)
        # Accessing ``port`` validates malformed/non-numeric port text.
        _ = parsed.port
        hostname = parsed.hostname
    except ValueError:
        raise ConfigurationError(
            "endpoint_url is not a valid HTTP(S) URL.",
            setting_name="endpoint_url",
        ) from None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or hostname is None:
        raise ConfigurationError(
            "endpoint_url must be an absolute HTTP(S) URL with a hostname.",
            setting_name="endpoint_url",
        )
    # Apply the transport policy before strict host canonicalization. A
    # malformed-but-routable-looking HTTP authority is still remote intent and
    # must fail on the explicit opt-in field rather than being reclassified as a
    # harmless endpoint-shape error.
    if not cloud and is_remote_http_endpoint(endpoint_url):
        if explicit_credentials:
            raise ConfigurationError(
                "Explicit AWS credentials cannot be sent to a remote HTTP endpoint; "
                "use HTTPS or a loopback LocalStack endpoint.",
                setting_name="endpoint_url",
            )
        if remote_http_allowed is not True:
            raise ConfigurationError(
                "Remote standalone HTTP endpoints require allow_remote_http=True. "
                "Use this override only for an explicitly trusted private network.",
                setting_name="allow_remote_http",
            )
    if (
        type(hostname) is not str
        or parse_endpoint_host(hostname, allow_brackets=False) is None
    ):
        raise ConfigurationError(
            "endpoint_url must be an absolute HTTP(S) URL with a hostname.",
            setting_name="endpoint_url",
        )
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError(
            "endpoint_url must not contain URL userinfo; configure AWS credentials "
            "through the dedicated credential fields.",
            setting_name="endpoint_url",
        )
    # ``urlsplit`` discards an empty trailing ``?``/``#`` delimiter, but
    # those delimiters are still not part of an endpoint origin. Reject the raw
    # delimiters as well as non-empty parsed values so mutable settings cannot
    # smuggle URL-shaped data through a syntactically empty query or fragment.
    if parsed.query or parsed.fragment or "?" in endpoint_url or "#" in endpoint_url:
        raise ConfigurationError(
            "endpoint_url must not contain a query or fragment.",
            setting_name="endpoint_url",
        )
    if cloud and scheme != "https":
        raise ConfigurationError(
            "An explicit endpoint_url in cloud mode must use HTTPS.",
            setting_name="endpoint_url",
        )
    return endpoint_url
