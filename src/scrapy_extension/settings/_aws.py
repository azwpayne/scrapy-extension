"""Shared AWS credential and endpoint security invariants."""

from __future__ import annotations

from urllib.parse import urlsplit

from pydantic import SecretStr

from scrapy_extension.exceptions import ConfigurationError


def _credential_value(
  value: SecretStr | str | None, setting_name: str
) -> str | None:
  """Extract one explicitly configured credential and reject blank values."""
  if value is None:
    return None
  if isinstance(value, SecretStr):
    text = value.get_secret_value()
  elif isinstance(value, str):
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


def validate_aws_endpoint(
  endpoint_url: str | None,
  *,
  cloud: bool,
  require_endpoint: bool = False,
) -> str | None:
  """Validate an AWS endpoint override without retaining or echoing userinfo."""
  if endpoint_url is None:
    if require_endpoint:
      raise ConfigurationError(
        "endpoint_url is required in standalone mode to prevent an accidental "
        "fallback to the real AWS endpoint chain.",
        setting_name="endpoint_url",
      )
    return None
  if not isinstance(endpoint_url, str) or not endpoint_url.strip():
    raise ConfigurationError(
      "endpoint_url must be a non-empty HTTP(S) URL.",
      setting_name="endpoint_url",
    )
  if endpoint_url != endpoint_url.strip() or any(
    ord(character) < 32 for character in endpoint_url
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
  if parsed.username is not None or parsed.password is not None:
    raise ConfigurationError(
      "endpoint_url must not contain URL userinfo; configure AWS credentials "
      "through the dedicated credential fields.",
      setting_name="endpoint_url",
    )
  if cloud and scheme != "https":
    raise ConfigurationError(
      "An explicit endpoint_url in cloud mode must use HTTPS.",
      setting_name="endpoint_url",
    )
  return endpoint_url
