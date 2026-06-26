"""Round 9a — SV1 + SV5 settings-validation tests (RED → GREEN).

This file pins parse-time rejection of invalid values for:

- SV1 (10 fields): free-form ``str`` fields that hold values from a closed set
  are converted to ``Literal[...]``. Typos that previously surfaced as opaque
  client-lib errors at first backend RPC now raise ``ValidationError`` at
  config time.
- SV5 (5 fields): empty-string ``host`` gaps and one unbounded int
  (``MemcachedSettings.port``) get pydantic ``Field`` constraints
  (``min_length``, ``ge``/``le``).

Honest TDD: each test constructs with the INVALID input and asserts
``ValidationError`` post-fix. No ``xfail`` / ``skip`` / weakening.

Scope note: SV2/SV3/SV4 (mode-conditional, cross-field, URL-scheme validators)
land in a later round — they raise ``ConfigurationError``, not
``ValidationError``, so they are out of scope here.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from scrapy_extension.settings import (
  KafkaSettings,
  MemcachedSettings,
  MongoDBSettings,
  PulsarSettings,
  RabbitMQSettings,
  RedisSettings,
)
from scrapy_extension.settings.base import Settings

# ---------------------------------------------------------------------------
# SV1 — Literal enum types (10 fields)
# ---------------------------------------------------------------------------
# Each closed set is pulled from the corresponding client lib's valid options
# (kafka-python, pulsar-client, pika, pymongo). Values currently accepted by
# any valid config or exercised by any existing test MUST remain valid.


class TestKafkaLiterals:
  """KafkaSettings Literal fields (SV1)."""

  def test_security_protocol_rejects_typo(self) -> None:
    """`security_protocol="SAS_SSL"` (missing underscore) must reject."""
    with pytest.raises(ValidationError):
      KafkaSettings(security_protocol="SAS_SSL")  # type: ignore[arg-type]

  def test_security_protocol_rejects_lowercase(self) -> None:
    """Case-sensitive — `"plaintext"` is not a valid client-lib value."""
    with pytest.raises(ValidationError):
      KafkaSettings(security_protocol="plaintext")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "value",
    ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
  )
  def test_security_protocol_accepts_valid(self, value: str) -> None:
    """All four documented kafka-python security protocols stay valid."""
    assert KafkaSettings(security_protocol=value).security_protocol == value

  def test_sasl_mechanism_rejects_lowercase(self) -> None:
    """`sasl_mechanism="plain"` silently fails auth today — must reject."""
    with pytest.raises(ValidationError):
      KafkaSettings(sasl_mechanism="plain")  # type: ignore[arg-type]

  def test_sasl_mechanism_rejects_typo(self) -> None:
    """`"SCRAM-SH-256"` (truncated) must reject."""
    with pytest.raises(ValidationError):
      KafkaSettings(sasl_mechanism="SCRAM-SH-256")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "value",
    ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"],
  )
  def test_sasl_mechanism_accepts_valid(self, value: str) -> None:
    """All documented kafka-python SASL mechanisms stay valid."""
    assert KafkaSettings(sasl_mechanism=value).sasl_mechanism == value

  def test_compression_type_rejects_typo(self) -> None:
    """`"snapy"` typo must reject (currently surfaces at producer create)."""
    with pytest.raises(ValidationError):
      KafkaSettings(compression_type="snapy")  # type: ignore[arg-type]

  @pytest.mark.parametrize("value", ["gzip", "snappy", "lz4", "zstd"])
  def test_compression_type_accepts_valid(self, value: str) -> None:
    """All four documented kafka-python codecs stay valid."""
    assert KafkaSettings(compression_type=value).compression_type == value

  def test_auto_offset_reset_rejects_typo(self) -> None:
    """`"earliet"` typo must reject."""
    with pytest.raises(ValidationError):
      KafkaSettings(auto_offset_reset="earliet")  # type: ignore[arg-type]

  @pytest.mark.parametrize("value", ["earliest", "latest", "none"])
  def test_auto_offset_reset_accepts_valid(self, value: str) -> None:
    """All three documented kafka-python offset resets stay valid."""
    assert KafkaSettings(auto_offset_reset=value).auto_offset_reset == value


class TestPulsarLiterals:
  """PulsarSettings Literal fields (SV1) — PascalCase per pulsar-client."""

  def test_consumer_type_rejects_lowercase_shared(self) -> None:
    """`consumer_type="shared"` (lowercase) must reject — client lib wants "Shared"."""
    with pytest.raises(ValidationError):
      PulsarSettings(consumer_type="shared")  # type: ignore[arg-type]

  def test_consumer_type_rejects_typo(self) -> None:
    """`"Faileover"` typo must reject."""
    with pytest.raises(ValidationError):
      PulsarSettings(consumer_type="Faileover")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "value", ["Shared", "Failover", "Exclusive", "Key_Shared"]
  )
  def test_consumer_type_accepts_valid(self, value: str) -> None:
    """All four pulsar ConsumerType mappings stay valid (backend _consumer_type)."""
    assert PulsarSettings(consumer_type=value).consumer_type == value

  def test_initial_position_rejects_lowercase(self) -> None:
    """`"earliest"` (lowercase) must reject — client lib wants "Earliest"."""
    with pytest.raises(ValidationError):
      PulsarSettings(initial_position="earliest")  # type: ignore[arg-type]

  @pytest.mark.parametrize("value", ["Earliest", "Latest"])
  def test_initial_position_accepts_valid(self, value: str) -> None:
    """Both pulsar InitialPosition mappings stay valid."""
    assert PulsarSettings(initial_position=value).initial_position == value


class TestRabbitMQLiterals:
  """RabbitMQSettings Literal fields (SV1)."""

  def test_ssl_verify_mode_rejects_typo(self) -> None:
    """`"CERT_REQ"` typo must reject (currently silently falls back)."""
    with pytest.raises(ValidationError):
      RabbitMQSettings(
        username="u", password="p", ssl_verify_mode="CERT_REQ"  # type: ignore[arg-type]
      )

  @pytest.mark.parametrize(
    "value", ["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"]
  )
  def test_ssl_verify_mode_accepts_valid(self, value: str) -> None:
    """All three ssl.VerifyMode string mappings stay valid."""
    s = RabbitMQSettings(
      username="u", password="p", ssl_verify_mode=value  # type: ignore[arg-type]
    )
    assert s.ssl_verify_mode == value

  def test_cluster_node_type_rejects_disk_typo(self) -> None:
    """`"disk"` (should be `"disc"`) must reject."""
    with pytest.raises(ValidationError):
      RabbitMQSettings(
        username="u", password="p", cluster_node_type="disk"  # type: ignore[arg-type]
      )

  @pytest.mark.parametrize("value", ["disc", "ram"])
  def test_cluster_node_type_accepts_valid(self, value: str) -> None:
    """Both RabbitMQ node types stay valid."""
    s = RabbitMQSettings(
      username="u", password="p", cluster_node_type=value  # type: ignore[arg-type]
    )
    assert s.cluster_node_type == value


class TestMongoDBLiterals:
  """MongoDBSettings Literal fields (SV1)."""

  def test_read_preference_rejects_typo(self) -> None:
    """`"primry"` typo must reject."""
    with pytest.raises(ValidationError):
      MongoDBSettings(read_preference="primry")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "value",
    [
      "primary",
      "primaryPreferred",
      "secondary",
      "secondaryPreferred",
      "nearest",
    ],
  )
  def test_read_preference_accepts_valid(self, value: str) -> None:
    """All five pymongo ReadPreference modes stay valid (camelCase)."""
    assert MongoDBSettings(read_preference=value).read_preference == value

  def test_auth_mechanism_rejects_typo(self) -> None:
    """`"SCRAM-SHA-25"` (truncated) must reject."""
    with pytest.raises(ValidationError):
      MongoDBSettings(auth_mechanism="SCRAM-SHA-25")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "value",
    [
      "SCRAM-SHA-1",
      "SCRAM-SHA-256",
      "MONGODB-CR",
      "PLAIN",
      "GSSAPI",
      "MONGODB-X509",
      "MONGODB-AWS",
    ],
  )
  def test_auth_mechanism_accepts_valid(self, value: str) -> None:
    """All documented pymongo auth mechanisms stay valid."""
    assert MongoDBSettings(auth_mechanism=value).auth_mechanism == value


# ---------------------------------------------------------------------------
# SV5 — Empty-string + unbounded-int gaps (5 fields)
# ---------------------------------------------------------------------------


class TestMemcachedBounds:
  """MemcachedSettings Field constraints (SV5)."""

  def test_port_rejects_negative(self) -> None:
    """`port=-1` must reject — only unbounded int in the project."""
    with pytest.raises(ValidationError):
      MemcachedSettings(port=-1)

  def test_port_rejects_above_65535(self) -> None:
    """`port=99999` must reject."""
    with pytest.raises(ValidationError):
      MemcachedSettings(port=99999)

  def test_port_accepts_valid_range(self) -> None:
    """Boundaries 1 and 65535 stay valid."""
    assert MemcachedSettings(port=1).port == 1
    assert MemcachedSettings(port=65535).port == 65535

  def test_host_rejects_empty_string(self) -> None:
    """`host=""` must reject (opaque DNS failure today)."""
    with pytest.raises(ValidationError):
      MemcachedSettings(host="")


class TestRedisHostBounds:
  """RedisSettings host min_length (SV5)."""

  def test_host_rejects_empty_string(self) -> None:
    """`host=""` must reject."""
    with pytest.raises(ValidationError):
      RedisSettings(host="")


class TestRabbitMQHostBounds:
  """RabbitMQSettings host min_length (SV5)."""

  def test_host_rejects_empty_string(self) -> None:
    """`host=""` must reject."""
    with pytest.raises(ValidationError):
      RabbitMQSettings(username="u", password="p", host="")


class TestBaseRetryAttemptsCap:
  """Settings.retry_attempts sane upper cap (SV5)."""

  def test_retry_attempts_rejects_huge_value(self) -> None:
    """`retry_attempts=999999` is a DoS — must reject at the sane cap (le=20)."""
    with pytest.raises(ValidationError):
      Settings(retry_attempts=999999)

  def test_retry_attempts_accepts_zero_through_cap(self) -> None:
    """`0` (no retries) through 20 stay valid; 0 documented as no-retry."""
    assert Settings(retry_attempts=0).retry_attempts == 0
    assert Settings(retry_attempts=20).retry_attempts == 20

  def test_retry_attempts_rejects_above_cap(self) -> None:
    """`21` is above the cap — must reject."""
    with pytest.raises(ValidationError):
      Settings(retry_attempts=21)
