"""Round 9a/9b — settings-validation tests (RED → GREEN).

This file pins parse-time rejection of invalid values for:

- SV1 (10 fields): free-form ``str`` fields that hold values from a closed set
  are converted to ``Literal[...]``. Typos that previously surfaced as opaque
  client-lib errors at first backend RPC now raise ``ValidationError`` at
  config time.
- SV5 (5 fields): empty-string ``host`` gaps and one unbounded int
  (``MemcachedSettings.port``) get pydantic ``Field`` constraints
  (``min_length``, ``ge``/``le``).
- SV2 (round 9b): mode-conditional ``model_validator(mode="after")`` rules
  raise ``ConfigurationError`` when a mode-specific required field is missing
  (MongoDB REPLICA_SET/ATLAS, Kafka CONFLUENT, RabbitMQ CLUSTER/MIRRORED_QUEUES).
- SV4 (round 9b): URL/scheme format guards raise ``ConfigurationError`` for
  bad schemes/patterns (MongoDB URI, Pulsar service_url, RocketMQ namesrv,
  ElasticSearch hosts, SQS/DynamoDB region_name).

Honest TDD: each test constructs with the INVALID input and asserts the
project's ``ConfigurationError`` (SV2/SV4) or pydantic ``ValidationError``
(SV1/SV5) post-fix. No ``xfail`` / ``skip`` / weakening. ``# type:
ignore[arg-type]`` is used ONLY where intentionally passing invalid input
(mirror the SV1 reject-test pattern).

Scope note: SV3 (security cross-field) lands in a separate round.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import (
  KafkaSettings,
  MemcachedSettings,
  MongoDBSettings,
  PulsarSettings,
  RabbitMQSettings,
  RedisSettings,
)
from scrapy_extension.settings.base import Settings
from scrapy_extension.settings.dynamodb import DynamoDBSettings
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings
from scrapy_extension.settings.kafka import KafkaMode
from scrapy_extension.settings.mongodb import MongoDBMode
from scrapy_extension.settings.rabbitmq import RabbitMQMode
from scrapy_extension.settings.rocketmq import RocketMQSettings
from scrapy_extension.settings.sqs import SqsSettings

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


# ---------------------------------------------------------------------------
# SV2 — Mode-conditional required-field validators (round 9b)
# ---------------------------------------------------------------------------
# Each validator mirrors the existing Redis SENTINEL pattern (now upgraded to
# raise the project's ``ConfigurationError`` with ``setting_name=``). Honest
# TDD: construct with the mode-but-missing-required-field and assert
# ``ConfigurationError`` naming the missing field.


class TestMongoDBModeConditional:
  """MongoDBSettings SV2 mode-conditional validators."""

  def test_replica_set_requires_replica_set_name(self) -> None:
    """REPLICA_SET mode without ``replica_set_name`` (and no ``?replicaSet=``
    in URI) must fail fast — driver otherwise can't find the RS."""
    with pytest.raises(ConfigurationError) as exc_info:
      MongoDBSettings(mode=MongoDBMode.REPLICA_SET)
    assert exc_info.value.setting_name == "replica_set_name"
    assert "replica_set_name" in str(exc_info.value)

  def test_replica_set_accepts_uri_with_replicaset_query(self) -> None:
    """REPLICA_SET mode + URI carrying ``?replicaSet=`` is valid (no name)."""
    s = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET,
      uri="mongodb://fallback-host:27017/?replicaSet=existing",
    )
    assert s.replica_set_name is None  # URI hint satisfies the requirement

  def test_replica_set_accepts_explicit_name(self) -> None:
    """REPLICA_SET mode + explicit ``replica_set_name`` is valid."""
    s = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET, replica_set_name="rs0"
    )
    assert s.replica_set_name == "rs0"

  def test_atlas_requires_srv_uri_or_cluster_name(self) -> None:
    """ATLAS mode with plain ``mongodb://`` URI and no ``atlas_cluster_name``
    must fail fast — Atlas resolves brokers via DNS SRV (``+srv``)."""
    with pytest.raises(ConfigurationError) as exc_info:
      MongoDBSettings(
        mode=MongoDBMode.ATLAS, uri="mongodb://localhost:27017"
      )
    assert exc_info.value.setting_name == "atlas_cluster_name"

  def test_atlas_accepts_srv_uri(self) -> None:
    """ATLAS mode + ``mongodb+srv://`` URI is valid."""
    s = MongoDBSettings(
      mode=MongoDBMode.ATLAS,
      uri="mongodb+srv://cluster0.example.mongodb.net",
    )
    assert s.uri.startswith("mongodb+srv://")


class TestKafkaModeConditional:
  """KafkaSettings SV2 CONFLUENT mode validator."""

  def test_confluent_requires_api_key_and_secret(self) -> None:
    """CONFLUENT mode without ``confluent_api_key``/``confluent_api_secret``
    must fail fast — silent PLAINTEXT-localhost fallback today."""
    with pytest.raises(ConfigurationError) as exc_info:
      KafkaSettings(mode=KafkaMode.CONFLUENT)
    # The first missing field is named.
    assert exc_info.value.setting_name in {
      "confluent_api_key",
      "confluent_api_secret",
    }
    msg = str(exc_info.value)
    assert "confluent_api_key" in msg
    assert "confluent_api_secret" in msg

  def test_confluent_rejects_key_without_secret(self) -> None:
    """CONFLUENT + key but no secret must reject (incomplete credentials)."""
    with pytest.raises(ConfigurationError) as exc_info:
      KafkaSettings(
        mode=KafkaMode.CONFLUENT,
        confluent_api_key="key",  # type: ignore[arg-type]
        confluent_api_secret=None,
      )
    assert exc_info.value.setting_name == "confluent_api_secret"

  def test_confluent_accepts_key_and_secret(self) -> None:
    """CONFLUENT + key + secret is valid (the intended Confluent Cloud path)."""
    s = KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_api_key="key",  # type: ignore[arg-type]
      confluent_api_secret="secret",  # type: ignore[arg-type]
    )
    assert s.confluent_api_key is not None


class TestRabbitMQModeConditional:
  """RabbitMQSettings SV2 CLUSTER/MIRRORED_QUEUES validators."""

  def test_cluster_requires_cluster_nodes(self) -> None:
    """CLUSTER mode without ``cluster_nodes`` must fail fast — operator asked
    for a cluster but only one host:port is wired."""
    with pytest.raises(ConfigurationError) as exc_info:
      RabbitMQSettings(
        username="u", password="p", mode=RabbitMQMode.CLUSTER
      )
    assert exc_info.value.setting_name == "cluster_nodes"

  def test_cluster_accepts_cluster_nodes(self) -> None:
    """CLUSTER mode + ``cluster_nodes`` is valid."""
    s = RabbitMQSettings(
      username="u",
      password="p",
      mode=RabbitMQMode.CLUSTER,
      cluster_nodes=["node2:5672", "node3:5672"],
    )
    assert len(s.cluster_nodes) == 2

  def test_mirrored_queues_requires_ha_mode(self) -> None:
    """MIRRORED_QUEUES mode without ``ha_mode`` must fail fast — connect path
    silently skips HA policy setup otherwise."""
    with pytest.raises(ConfigurationError) as exc_info:
      RabbitMQSettings(
        username="u", password="p", mode=RabbitMQMode.MIRRORED_QUEUES
      )
    assert exc_info.value.setting_name == "ha_mode"

  def test_mirrored_queues_accepts_ha_mode_without_cluster_nodes(self) -> None:
    """MIRRORED_QUEUES + ``ha_mode`` is valid even without ``cluster_nodes``
    (single-node-mirrored is a supported dev topology — backend uses
    ``host:port``). Pins the no-API-break scope decision."""
    s = RabbitMQSettings(
      username="u",
      password="p",
      mode=RabbitMQMode.MIRRORED_QUEUES,
      ha_mode="all",
    )
    assert s.ha_mode == "all"


# ---------------------------------------------------------------------------
# SV4 — URL/scheme format guards (round 9b)
# ---------------------------------------------------------------------------


class TestMongoDBUriScheme:
  """MongoDBSettings.uri SV4 scheme guard."""

  def test_uri_rejects_bare_host_port(self) -> None:
    """``uri="localhost:27017"`` must reject — opaque InvalidURI today."""
    with pytest.raises(ConfigurationError) as exc_info:
      MongoDBSettings(uri="localhost:27017")  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "uri"

  def test_uri_rejects_empty_string(self) -> None:
    """``uri=""`` must reject (rejected by the field validator)."""
    with pytest.raises(ConfigurationError):
      MongoDBSettings(uri="")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "uri",
    [
      "mongodb://localhost:27017",
      "mongodb+srv://cluster0.example.mongodb.net",
      "mongodb://user:pass@host:27017/?replicaSet=rs0",
    ],
  )
  def test_uri_accepts_valid_schemes(self, uri: str) -> None:
    """Valid ``mongodb://`` and ``mongodb+srv://`` URIs stay accepted."""
    assert MongoDBSettings(uri=uri).uri == uri


class TestPulsarServiceUrlScheme:
  """PulsarSettings.service_url SV4 scheme guard."""

  def test_service_url_rejects_bare_host_port(self) -> None:
    """``service_url="broker:6650"`` must reject — opaque ValueError today."""
    with pytest.raises(ConfigurationError) as exc_info:
      PulsarSettings(service_url="broker:6650")  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "service_url"

  def test_service_url_rejects_http_scheme(self) -> None:
    """``http://`` is not a Pulsar scheme — must reject."""
    with pytest.raises(ConfigurationError):
      PulsarSettings(service_url="http://broker:6650")  # type: ignore[arg-type]

  def test_service_url_rejects_empty(self) -> None:
    """Empty string must reject."""
    with pytest.raises(ConfigurationError):
      PulsarSettings(service_url="")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "url",
    ["pulsar://localhost:6650", "pulsar+ssl://broker:6651"],
  )
  def test_service_url_accepts_valid_schemes(self, url: str) -> None:
    """Valid ``pulsar://`` and ``pulsar+ssl://`` URLs stay accepted."""
    assert PulsarSettings(service_url=url).service_url == url


class TestRocketMQNamesrvFormat:
  """RocketMQSettings.namesrv_address SV4 ``host:port`` guard."""

  def test_namesrv_rejects_scheme_prefix(self) -> None:
    """``http://namesrv:9876`` must reject — client wants bare ``host:port``."""
    with pytest.raises(ConfigurationError) as exc_info:
      RocketMQSettings(namesrv_address="http://namesrv:9876")  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "namesrv_address"

  def test_namesrv_rejects_bare_host(self) -> None:
    """``localhost`` (no port) must reject."""
    with pytest.raises(ConfigurationError):
      RocketMQSettings(namesrv_address="localhost")  # type: ignore[arg-type]

  def test_namesrv_rejects_non_numeric_port(self) -> None:
    """``host:abc`` must reject (port must be digits)."""
    with pytest.raises(ConfigurationError):
      RocketMQSettings(namesrv_address="namesrv:abc")  # type: ignore[arg-type]

  def test_namesrv_rejects_empty(self) -> None:
    """Empty string must reject."""
    with pytest.raises(ConfigurationError):
      RocketMQSettings(namesrv_address="")  # type: ignore[arg-type]

  @pytest.mark.parametrize(
    "addr",
    ["localhost:9876", "rocketmq-cluster:9876", "10.0.0.1:9876"],
  )
  def test_namesrv_accepts_valid_host_port(self, addr: str) -> None:
    """Valid ``host:port`` values stay accepted (incl. DNS, IPv4)."""
    assert RocketMQSettings(namesrv_address=addr).namesrv_address == addr


class TestElasticSearchHostsScheme:
  """ElasticSearchSettings.hosts SV4 scheme guard (no-creds case)."""

  def test_hosts_rejects_bare_host_port(self) -> None:
    """``hosts=["localhost:9200"]`` must reject — opaque transport error today
    (elasticsearch-py does not infer a default scheme)."""
    with pytest.raises(ConfigurationError) as exc_info:
      ElasticSearchSettings(hosts=["localhost:9200"])  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "hosts"

  def test_hosts_rejects_empty_entry(self) -> None:
    """Empty string in ``hosts`` must reject."""
    with pytest.raises(ConfigurationError):
      ElasticSearchSettings(hosts=[""])  # type: ignore[arg-type]

  def test_hosts_rejects_any_bad_entry_in_mixed_list(self) -> None:
    """One bad entry in a mixed list must reject (reports the bad entries)."""
    with pytest.raises(ConfigurationError) as exc_info:
      ElasticSearchSettings(
        hosts=["https://good:9200", "bad:9200"]  # type: ignore[arg-type]
      )
    assert exc_info.value.setting_name == "hosts"
    assert "bad:9200" in str(exc_info.value)

  @pytest.mark.parametrize(
    "hosts",
    [
      ["http://localhost:9200"],
      ["https://es.example.com:9200"],
      ["http://h1:9200", "https://h2:9200"],
    ],
  )
  def test_hosts_accepts_valid_schemes(self, hosts: list[str]) -> None:
    """All-valid ``http://`` / ``https://`` lists stay accepted."""
    assert ElasticSearchSettings(hosts=hosts).hosts == hosts


class TestAwsRegionNameFormat:
  """SQS + DynamoDB ``region_name`` SV4 regex guard.

  Catches structural typos (missing parts, wrong casing, extra suffixes,
  empty). Note: the chosen regex ``^[a-z]{2}-[a-z]+-\\d+$`` cannot catch
  same-shape word typos like ``us-eat-1`` (intended ``us-east-1``) because
  ``eat`` is also valid ``[a-z]+`` — that requires a known-region allowlist,
  which is out of SV4 scope (and would break on new AWS regions). The cases
  below are the genuine structural catches.
  """

  @pytest.mark.parametrize(
    "bad_region",
    ["US-EAST-1", "us-east", "us-east-1-extra", "region1", "", "us-east-one"],
  )
  def test_sqs_region_rejects_invalid(self, bad_region: str) -> None:
    """Structurally-malformed region names must reject at config time."""
    with pytest.raises(ConfigurationError) as exc_info:
      SqsSettings(region_name=bad_region)  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "region_name"

  @pytest.mark.parametrize(
    "bad_region",
    ["US-EAST-1", "us-east", ""],
  )
  def test_dynamodb_region_rejects_invalid(self, bad_region: str) -> None:
    """Structurally-malformed region names must reject at config time."""
    with pytest.raises(ConfigurationError) as exc_info:
      DynamoDBSettings(region_name=bad_region)  # type: ignore[arg-type]
    assert exc_info.value.setting_name == "region_name"

  @pytest.mark.parametrize(
    "good_region",
    ["us-east-1", "us-west-2", "ap-southeast-2", "eu-central-1", "me-central-1"],
  )
  def test_sqs_region_accepts_valid(self, good_region: str) -> None:
    """Valid AWS region names stay accepted (incl. multi-word middle)."""
    assert SqsSettings(region_name=good_region).region_name == good_region

  @pytest.mark.parametrize(
    "good_region",
    ["us-east-1", "ap-southeast-3", "me-central-1"],
  )
  def test_dynamodb_region_accepts_valid(self, good_region: str) -> None:
    """Valid AWS region names stay accepted."""
    assert (
      DynamoDBSettings(region_name=good_region).region_name == good_region
    )
