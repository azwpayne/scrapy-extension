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
_DEFAULT_LOCAL_ENDPOINT = "http://localhost:4566"


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
    env_prefix="SCRAPY_DYNAMODB_", case_sensitive=False, extra="forbid"
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
    default=None,
    description=(
      "Endpoint URL for LocalStack. STANDALONE defaults safely to the local "
      "LocalStack edge endpoint; CLOUD may leave this unset for real AWS."
    ),
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

    In STANDALONE mode, an unset endpoint is normalized to the loopback-only
    LocalStack default so zero-config startup cannot silently target real AWS.
    CLOUD deliberately preserves ``None`` for boto3's real-AWS endpoint chain.

    Catches typos and bare ``host:port`` values (e.g. ``localstack:8000``)
    that would otherwise fall through to boto3's default credential/endpoint
    chain (silent wrong target). ``http://`` is allowed — LocalStack and
    compatible local emulators serve over plaintext. Mirrors the SQS validator
    and the RabbitMQ guest-guard pattern (raise, not warn).

    Raises:
        ConfigurationError: if ``endpoint_url`` is set and does not start
            with ``http://`` or ``https://``.
    """
    if self.endpoint_url is None:
      if self.mode == DynamoDBMode.STANDALONE:
        self.endpoint_url = _DEFAULT_LOCAL_ENDPOINT
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

  @model_validator(mode="after")
  def _validate_aws_credentials_both_or_neither(self) -> Self:
    """SV3-6b (M): AWS creds must be both-set or both-unset.

    boto3's default credential chain silently ignores a lone
    ``aws_access_key_id`` without ``aws_secret_access_key`` (and vice versa)
    — it falls through to IAM role / env / config, masking the
    half-configured state. The round-6 SEC-7 connect-path XOR catches this
    at ``connect()`` time; this validator lifts the same check into the
    settings layer so it fires at config time (fail-fast, closer to the
    misconfiguration). Mirrors the SQS validator.

    Verified safe: all existing repo fixtures set both creds or neither.

    Raises:
        ConfigurationError: if exactly one of ``aws_access_key_id`` /
            ``aws_secret_access_key`` is set.
    """
    key_set = self.aws_access_key_id is not None
    secret_set = self.aws_secret_access_key is not None
    if key_set and not secret_set:
      raise ConfigurationError(
        (
          "aws_access_key_id is set but aws_secret_access_key is None — "
          "AWS credentials must be both-set or both-unset. boto3's default "
          "chain silently falls through to IAM/env/config when only one is "
          "provided, masking the half-configured state. Either set "
          "aws_secret_access_key or remove aws_access_key_id (to use the "
          "default chain)."
        ),
        setting_name="aws_secret_access_key",
        setting_value=self.aws_secret_access_key,
      )
    if secret_set and not key_set:
      raise ConfigurationError(
        (
          "aws_secret_access_key is set but aws_access_key_id is None — "
          "AWS credentials must be both-set or both-unset. Either set "
          "aws_access_key_id or remove aws_secret_access_key (to use the "
          "default chain)."
        ),
        setting_name="aws_access_key_id",
        setting_value=self.aws_access_key_id,
      )
    return self
