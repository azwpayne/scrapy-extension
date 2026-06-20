# @author  : azwpayne(https://github.com/azwpayne)
# @name    : dynamodb.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Amazon DynamoDB settings (subsystem ③ — new NoSQL backend)
from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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
