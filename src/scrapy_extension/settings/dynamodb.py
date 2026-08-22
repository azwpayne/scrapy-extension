# @author  : azwpayne(https://github.com/azwpayne)
# @name    : dynamodb.py
# @time    : 2026/6/20
# @blog    : https://paynewu.com/
# @mail    : paynewu0719@gmail.com
# @desc    : Amazon DynamoDB settings (subsystem ③ — new NoSQL backend)
from __future__ import annotations

import re
from enum import Enum

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from typing_extensions import Self

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._aws import (
    normalize_allow_remote_http,
    validate_aws_credentials,
    validate_aws_endpoint,
    validate_aws_region_name,
)
from scrapy_extension.settings._redacted import RedactedBaseSettings

_DEFAULT_LOCAL_ENDPOINT = "http://localhost:4566"
_DYNAMODB_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{3,255}$")
_DYNAMODB_TABLE_NAME_ERROR = (
    "DynamoDB table_name must be 3-255 letters, digits, dots, hyphens, or underscores."
)


def validate_dynamodb_table_name(value: object) -> str:
    """Validate one physical table identifier before boto3 resource I/O."""
    if type(value) is not str or _DYNAMODB_TABLE_NAME_PATTERN.fullmatch(value) is None:
        raise ConfigurationError(_DYNAMODB_TABLE_NAME_ERROR, setting_name="table_name")
    return value


class DynamoDBMode(str, Enum):
    """DynamoDB deployment modes.

    Attributes:
        STANDALONE: LocalStack (or compatible) via endpoint_url (default).
        CLOUD: Real AWS DynamoDB.
    """

    STANDALONE = "standalone"
    CLOUD = "cloud"


class DynamoDBSettings(RedactedBaseSettings):
    """Amazon DynamoDB settings (StorageBackend — NoSQL KV).

    Configurable via environment variables with the SCRAPY_DYNAMODB_ prefix.
    One table per backend (auto-created on connect if missing); items are keyed
    by ``pk``. Package writes carry a reserved opaque ``_scrapy_revision``
    attribute used to fence maintenance clears. TTL is application-level (an
    ``expire_at`` attribute checked on read), not the native DynamoDB TTL feature.
    """

    model_config = SettingsConfigDict(
        env_prefix="SCRAPY_DYNAMODB_",
        case_sensitive=False,
        extra="forbid",
        hide_input_in_errors=True,
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
    allow_remote_http: bool = Field(
        default=False,
        description=(
            "Allow a standalone non-loopback HTTP endpoint only on an explicitly "
            "trusted private network; never permits explicit credentials"
        ),
    )
    aws_access_key_id: SecretStr | None = Field(
        default=None, description="AWS access key id (optional; IAM role otherwise)"
    )
    aws_secret_access_key: SecretStr | None = Field(
        default=None, description="AWS secret access key"
    )
    allow_unfenced_legacy_clear: bool = Field(
        default=False,
        description=(
            "High-risk maintenance override permitting clear_storage() to delete "
            "revisionless legacy rows. Enable only while every table writer is stopped."
        ),
    )

    @field_validator("table_name", mode="after")
    @classmethod
    def _validate_table_name(cls, value: str) -> str:
        """Reject malformed physical table names before client construction."""
        return validate_dynamodb_table_name(value)

    @field_validator("allow_remote_http", mode="before")
    @classmethod
    def _normalize_remote_http_opt_in(cls, value: object) -> bool:
        """Accept canonical environment booleans but reject truthy lookalikes."""
        return normalize_allow_remote_http(value)

    @model_validator(mode="after")
    def _validate_endpoint_url_scheme(self) -> Self:
        """Normalize local defaults and enforce the shared AWS URL policy."""
        if self.endpoint_url is None:
            if self.mode == DynamoDBMode.STANDALONE:
                self.endpoint_url = _DEFAULT_LOCAL_ENDPOINT
        validate_aws_endpoint(
            self.endpoint_url,
            cloud=self.mode == DynamoDBMode.CLOUD,
            require_endpoint=self.mode == DynamoDBMode.STANDALONE,
            allow_remote_http=self.allow_remote_http,
            explicit_credentials=(
                self.aws_access_key_id is not None
                or self.aws_secret_access_key is not None
            ),
        )
        return self

    @model_validator(mode="after")
    def _validate_region_name_format(self) -> Self:
        """SV4: ``region_name`` must match the AWS structural grammar.

        Rejects malformed casing, separators, and suffixes before boto3 I/O while
        accepting multi-label partitions such as GovCloud, ISO, and EUSC. It is
        intentionally not a known-region allowlist, so same-shape word typos remain
        the SDK/service's responsibility. Mirrors the SQS validator.

        Raises:
            ConfigurationError: if ``region_name`` does not match the AWS region
                pattern.
        """
        validate_aws_region_name(self.region_name)
        return self

    @model_validator(mode="after")
    def _validate_aws_credentials_both_or_neither(self) -> Self:
        """Require either an intentional ambient path or a non-empty pair."""
        validate_aws_credentials(self.aws_access_key_id, self.aws_secret_access_key)
        return self
