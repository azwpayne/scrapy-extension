"""Connection-generation security contracts for Kafka."""

from __future__ import annotations

import pytest

from scrapy_extension.backends.kafka import KafkaBackend
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.settings import KafkaMode, KafkaSettings
from scrapy_extension.settings.kafka import validate_kafka_transport_security


def _connect_backend(mocker, settings: KafkaSettings) -> KafkaBackend:
  backend = KafkaBackend(settings)
  mocker.patch(
    "scrapy_extension.backends.kafka.KafkaProducer", return_value=mocker.MagicMock()
  )
  mocker.patch(
    "scrapy_extension.backends.kafka.KafkaAdminClient",
    return_value=mocker.MagicMock(),
  )
  backend.connect()
  return backend


def test_common_config_keeps_verified_tls_after_validation_use_race(mocker) -> None:
  """A transport downgrade after validation cannot alter the built config."""
  settings = KafkaSettings(security_protocol="SSL")
  backend = KafkaBackend(settings)

  def mutate_after_validation(
    mode: object, security_protocol: object, ssl_check_hostname: object
  ) -> None:
    validate_kafka_transport_security(
      mode, security_protocol, ssl_check_hostname
    )
    settings.ssl_check_hostname = False

  mocker.patch(
    "scrapy_extension.backends.kafka.validate_kafka_transport_security",
    side_effect=mutate_after_validation,
  )
  mocker.patch(
    "scrapy_extension.settings.kafka.validate_kafka_transport_security",
    side_effect=mutate_after_validation,
  )

  built = backend._build_common_config()

  assert built["ssl_check_hostname"] is True


def test_connection_snapshot_repr_redacts_credentials() -> None:
  backend = KafkaBackend(
    KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="snapshot-user",
      sasl_password="snapshot-kafka-secret",
    )
  )

  snapshot = backend._capture_connection_snapshot()

  assert "snapshot-kafka-secret" not in repr(snapshot)
  assert "<redacted>" in repr(snapshot)


@pytest.mark.parametrize(
  "settings",
  [
    KafkaSettings(security_protocol="SSL"),
    KafkaSettings(
      security_protocol="SASL_SSL",
      sasl_mechanism="PLAIN",
      sasl_username="runtime-user",
      sasl_password="runtime-kafka-secret",
    ),
    KafkaSettings(
      mode=KafkaMode.CONFLUENT,
      confluent_bootstrap_servers="pkc.example.confluent.cloud:9092",
      confluent_api_key="runtime-api-key",
      confluent_api_secret="runtime-kafka-secret",
    ),
  ],
)
def test_connect_rejects_runtime_tls_hostname_downgrade_before_sdk_io(
  mocker, settings: KafkaSettings
) -> None:
  backend = KafkaBackend(settings)
  settings.ssl_check_hostname = False
  producer = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
  admin = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()

  assert exc_info.value.setting_name == "ssl_check_hostname"
  assert "runtime-kafka-secret" not in str(exc_info.value)
  assert "runtime-kafka-secret" not in repr(exc_info.value)
  producer.assert_not_called()
  admin.assert_not_called()


def test_connected_generation_uses_tls_snapshot_for_late_consumer(mocker) -> None:
  settings = KafkaSettings(security_protocol="SSL")
  backend = _connect_backend(mocker, settings)
  settings.ssl_check_hostname = False
  consumer = mocker.MagicMock()
  consumer.poll.return_value = {}
  constructor = mocker.patch(
    "scrapy_extension.backends.kafka.KafkaConsumer", return_value=consumer
  )

  assert backend.pop("jobs") is None

  assert constructor.call_args.kwargs["ssl_check_hostname"] is True


def test_connected_generation_uses_tls_snapshot_for_temporary_consumer(mocker) -> None:
  settings = KafkaSettings(security_protocol="SSL")
  backend = _connect_backend(mocker, settings)
  settings.ssl_check_hostname = False
  consumer = mocker.MagicMock()
  consumer.partitions_for_topic.return_value = None
  constructor = mocker.patch(
    "scrapy_extension.backends.kafka.KafkaConsumer", return_value=consumer
  )

  assert backend.queue_len("jobs") == 0

  assert constructor.call_args.kwargs["ssl_check_hostname"] is True
  consumer.close.assert_called_once()


def test_confluent_clients_receive_explicit_verified_tls_configuration(mocker) -> None:
  settings = KafkaSettings(
    mode=KafkaMode.CONFLUENT,
    confluent_bootstrap_servers="pkc.example.confluent.cloud:9092",
    confluent_api_key="confluent-api-key",
    confluent_api_secret="confluent-api-secret",
  )
  producer = mocker.patch(
    "scrapy_extension.backends.kafka.KafkaProducer", return_value=mocker.MagicMock()
  )
  admin = mocker.patch(
    "scrapy_extension.backends.kafka.KafkaAdminClient",
    return_value=mocker.MagicMock(),
  )

  KafkaBackend(settings).connect()

  assert producer.call_args.kwargs["security_protocol"] == "SASL_SSL"
  assert producer.call_args.kwargs["ssl_check_hostname"] is True
  assert admin.call_args.kwargs["security_protocol"] == "SASL_SSL"
  assert admin.call_args.kwargs["ssl_check_hostname"] is True
