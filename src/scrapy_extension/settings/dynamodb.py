# @author  : azwpayne(https://github.com/azwpayne)
# @name    : dynamodb.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Amazon DynamoDB settings (subsystem ③ — new NoSQL backend)
from __future__ import annotations

import re
from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError

# AWS region: two lowercase letters, hyphen, lowercase word, hyphen, digits.
# Rejects typos like "us-eat-1" (the round-6 SPEC motivator).
_AWS_REGION_PATTERN = re.compile(r"^[a-z]{2}-[a-z]+-\d+$")


class DynamoDBMode(str, Enum):
  """DynamoDB deployment modes.

  Attributes:
      STANDALONE: LocalStack (or compatible) via endpoint_url (default).
      CLOUD: Real AWS DynamoDB.
  """

  STANDALONE = "standalone"
  CLOUD = "cloud"


class DynamoDBSettings(BaseSettings):
  """Amazon DynamoDB settings (StorageBackend — NoSQL KV).

  Configurable via environment variables with the SCRAPY_DYNAMODB_ prefix.
  One table per backend (auto-created on connect if missing); items are keyed
  by ``pk``. TTL is application-level (an ``expire_at`` attribute checked on
  read), not the native DynamoDB TTL feature.
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_DYNAMODB_", case_sensitive=False, extra="ignore"
  )

  mode: DynamoDBMode = Field(
    default=DynamoDBMode.STANDALONE,
    description="DynamoDB mode (standalone=LocalStack, cloud=AWS)",
  )
  table_name: str = Field(
    default="scrapy-extension", description="DynamoDB table name (auto-created)"
  )
  region_name: str = Field(default="us-east-1", description="AWS region")
  endpoint_url: str | None = Field(
    default=None, description="Endpoint URL for LocalStack (standalone mode)"
  )
  aws_access_key_id: SecretStr | None = Field(
    default=None, description="AWS access key id (optional; IAM role otherwise)"
  )
  aws_secret_access_key: SecretStr | None = Field(
    default=None, description="AWS secret access key"
  )

  @model_validator(mode="after")
  def _validate_endpoint_url_scheme(self) -> Self:
    """SEC-4: ``endpoint_url`` (when set) must be ``http://`` or ``https://``.

    Catches typos and bare ``host:port`` values (e.g. ``localstack:8000``)
    that would otherwise fall through to boto3's default credential/endpoint
    chain (silent wrong target). ``http://`` is allowed — LocalStack and
    compatible local emulators serve over plaintext. Unset is allowed (real
    AWS via the default chain). Mirrors the SQS validator and the RabbitMQ
    guest-guard pattern (raise, not warn).

    Raises:
        ConfigurationError: if ``endpoint_url`` is set and does not start
            with ``http://`` or ``https://``.
    """
    if self.endpoint_url is None:
      return self
    lowered = self.endpoint_url.lower()
    if not (lowered.startswith("http://") or lowered.startswith("https://")):
      raise ConfigurationError(
        (
          "endpoint_url must start with 'http://' or 'https://' "
          "('http://' is LocalStack-only). "
          f"Got endpoint_url={self.endpoint_url!r}."
        ),
        setting_name="endpoint_url",
        setting_value=self.endpoint_url,
      )
    return self

  @model_validator(mode="after")
  def _validate_region_name_format(self) -> Self:
    """SV4: ``region_name`` must match the AWS region pattern.

    Catches typos like ``"us-eat-1"`` (should be ``"us-east-1"``) that would
    otherwise surface as an opaque ``InvalidLocationConstraint`` / endpoint
    resolution failure inside boto3 at the first API call. Mirrors the SQS
    validator.

    Raises:
        ConfigurationError: if ``region_name`` does not match the AWS region
            pattern.
    """
    if not _AWS_REGION_PATTERN.match(self.region_name):
      raise ConfigurationError(
        (
          "region_name must match the AWS region pattern "
          "'<aa>-<region>-<n>' (e.g. 'us-east-1', 'ap-southeast-2'). "
          f"Got region_name={self.region_name!r}."
        ),
        setting_name="region_name",
        setting_value=self.region_name,
      )
    return self
