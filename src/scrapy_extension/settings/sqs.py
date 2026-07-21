# @author  : azwpayne(https://github.com/azwpayne)
# @name    : sqs.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Amazon SQS settings (subsystem ③ — new MQ backend)
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.settings._aws import (
  validate_aws_credentials,
  validate_aws_endpoint,
  validate_aws_region_name,
)

_DEFAULT_LOCAL_ENDPOINT = "http://localhost:4566"


class SqsMode(str, Enum):
  """SQS deployment modes.

  Attributes:
      STANDALONE: LocalStack (or compatible) via endpoint_url (default).
      CLOUD: Real AWS SQS.
  """

  STANDALONE = "standalone"
  CLOUD = "cloud"


class SqsSettings(BaseSettings):
  """Amazon SQS settings (queue-only MQ backend).

  Configurable via environment variables with the SCRAPY_SQS_ prefix. SQS
  has no native priority queue — ``priority`` on push is ignored (Standard
  queues, best-effort ordering).
  """

  model_config = SettingsConfigDict(
    env_prefix="SCRAPY_SQS_", case_sensitive=False, extra="forbid"
  )

  mode: SqsMode = Field(
    default=SqsMode.STANDALONE,
    description="SQS deployment mode (standalone=LocalStack, cloud=AWS)",
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
  queue_name_prefix: str = Field(
    default="scrapy-", description="Prefix applied to queue names"
  )
  visibility_timeout: int = Field(
    default=300,
    ge=1,
    le=12 * 60 * 60,
    description="Visibility timeout (seconds) — redelivery delay for unacked msgs",
  )

  @model_validator(mode="after")
  def _validate_endpoint_url_scheme(self) -> Self:
    """Normalize local defaults and enforce the shared AWS URL policy."""
    if self.endpoint_url is None:
      if self.mode == SqsMode.STANDALONE:
        self.endpoint_url = _DEFAULT_LOCAL_ENDPOINT
    validate_aws_endpoint(
      self.endpoint_url,
      cloud=self.mode == SqsMode.CLOUD,
      require_endpoint=self.mode == SqsMode.STANDALONE,
    )
    return self

  @model_validator(mode="after")
  def _validate_region_name_format(self) -> Self:
    """SV4: ``region_name`` must match the AWS structural grammar.

    Rejects malformed casing, separators, and suffixes before boto3 I/O while
    accepting multi-label partitions such as GovCloud, ISO, and EUSC. It is
    intentionally not a known-region allowlist, so same-shape word typos remain
    the SDK/service's responsibility.

    Raises:
        ConfigurationError: if ``region_name`` does not match the AWS region
            pattern.
    """
    validate_aws_region_name(self.region_name)
    return self

  @model_validator(mode="after")
  def _validate_aws_credentials_both_or_neither(self) -> Self:
    """Require either an intentional ambient path or a non-empty pair."""
    validate_aws_credentials(
      self.aws_access_key_id, self.aws_secret_access_key
    )
    return self
