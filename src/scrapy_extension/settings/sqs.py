# @author  : azwpayne(https://github.com/azwpayne)
# @name    : sqs.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Amazon SQS settings (subsystem ③ — new MQ backend)
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    env_prefix="SCRAPY_SQS_", case_sensitive=False, extra="ignore"
  )

  mode: SqsMode = Field(
    default=SqsMode.STANDALONE,
    description="SQS deployment mode (standalone=LocalStack, cloud=AWS)",
  )
  region_name: str = Field(default="us-east-1", description="AWS region")
  endpoint_url: str | None = Field(
    default=None,
    description="Endpoint URL for LocalStack (standalone mode)",
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
    default=30,
    ge=1,
    description="Visibility timeout (seconds) — redelivery delay for unacked msgs",
  )
