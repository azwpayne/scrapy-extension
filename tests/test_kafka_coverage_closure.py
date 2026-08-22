"""Semantic Kafka lifecycle, broker-response, and safety boundary tests."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from kafka import TopicPartition
from kafka.errors import KafkaError, TopicAlreadyExistsError

from scrapy_extension.backends import kafka as kafka_module
from scrapy_extension.backends._generation import GenerationUnavailable
from scrapy_extension.backends.kafka import (
    KafkaBackend,
    _KafkaAckToken,
    _KafkaConnectionAttemptFenced,
    _KafkaGenerationHandles,
)
from scrapy_extension.exceptions import (
    ConfigurationError,
    QueueError,
    QueueOutcomeIndeterminateError,
)
from scrapy_extension.settings import KafkaMode, KafkaSettings


@pytest.fixture(autouse=True)
def _authorize_legacy_remote_kafka_fixtures(monkeypatch) -> None:
    monkeypatch.setenv("SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT", "true")


def _backend(config: KafkaSettings | None = None) -> KafkaBackend:
    return KafkaBackend(config or KafkaSettings())


def _publish(
    backend: KafkaBackend,
    mocker,
    *,
    consumer: Any = None,
) -> tuple[Any, Any, Any, Any]:
    producer = mocker.MagicMock(name="producer")
    admin = mocker.MagicMock(name="admin")
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer
    backend._publish_generation_locked()
    generation = backend._generation_gate.current
    assert generation is not None
    return producer, admin, consumer, generation


def _record(mocker, topic: str, offset: int, partition: int = 0, value: bytes = b"x"):
    record = mocker.MagicMock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    record.value = value
    return record


@pytest.mark.parametrize(
    ("mode", "method"),
    [
        (KafkaMode.STANDALONE, "_connect_standalone"),
        (KafkaMode.CLUSTER, "_connect_cluster"),
        (KafkaMode.CONFLUENT, "_connect_confluent"),
    ],
)
def test_fenced_connection_candidate_closes_both_clients(
    mocker, mode: KafkaMode, method: str
) -> None:
    """Teardown fencing never publishes a half-created producer/admin pair."""
    if mode is KafkaMode.CLUSTER:
        config = KafkaSettings(mode=mode, cluster_brokers=["cluster:9092"])
    elif mode is KafkaMode.CONFLUENT:
        config = KafkaSettings(
            mode=mode,
            confluent_api_key="api-key",
            confluent_api_secret="api-secret",
            confluent_bootstrap_servers="cloud:9092",
        )
    else:
        config = KafkaSettings(mode=mode)
    backend = _backend(config)
    producer = mocker.MagicMock(name="candidate-producer")
    admin = mocker.MagicMock(name="candidate-admin")
    mocker.patch("scrapy_extension.backends.kafka.KafkaProducer", return_value=producer)
    mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient", return_value=admin)
    backend._connect_attempt_epoch = 0
    backend._lifecycle_epoch = 1

    with pytest.raises(_KafkaConnectionAttemptFenced):
        getattr(backend, method)()

    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()
    assert backend._producer is None
    assert backend._admin_client is None


@pytest.mark.parametrize("mode", [KafkaMode.CLUSTER, KafkaMode.CONFLUENT])
def test_mode_admin_constructor_control_error_closes_candidate(
    mocker, mode: KafkaMode
) -> None:
    """A control-flow failure from admin construction cannot leak the producer."""
    if mode is KafkaMode.CLUSTER:
        config = KafkaSettings(mode=mode, cluster_brokers=["cluster:9092"])
    else:
        config = KafkaSettings(
            mode=mode,
            confluent_api_key="api-key",
            confluent_api_secret="api-secret",
            confluent_bootstrap_servers="cloud:9092",
        )
    backend = _backend(config)
    producer = mocker.MagicMock(name="candidate-producer")
    mocker.patch("scrapy_extension.backends.kafka.KafkaProducer", return_value=producer)
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaAdminClient",
        side_effect=KeyboardInterrupt("stop"),
    )

    with pytest.raises(KeyboardInterrupt):
        getattr(
            backend,
            "_connect_cluster" if mode is KafkaMode.CLUSTER else "_connect_confluent",
        )()

    producer.close.assert_called_once_with()
    assert backend._producer is None


def test_failed_residual_close_is_diagnostic_only_and_logger_is_redacted(
    mocker,
) -> None:
    """A retry remains usable when stale close and its diagnostic both fail."""
    backend = _backend()
    residual = mocker.MagicMock(name="residual")
    residual.close.side_effect = RuntimeError("driver-secret")
    backend._producer = residual
    producer, admin = mocker.MagicMock(), mocker.MagicMock()
    producer_factory = mocker.patch(
        "scrapy_extension.backends.kafka.KafkaProducer", return_value=producer
    )
    admin_factory = mocker.patch(
        "scrapy_extension.backends.kafka.KafkaAdminClient", return_value=admin
    )
    debug = mocker.patch.object(kafka_module.logger, "debug", side_effect=SystemExit)

    backend.connect()

    residual.close.assert_called_once_with()
    assert debug.called
    producer_factory.assert_called_once()
    admin_factory.assert_called_once()
    assert backend.is_connected()


def test_residual_close_callback_owned_generation_is_not_overwritten(mocker) -> None:
    """A close callback may publish a replacement while connect is retrying."""
    backend = _backend()
    residual = mocker.MagicMock(name="residual")
    replacement_producer = mocker.MagicMock(name="replacement-producer")
    replacement_admin = mocker.MagicMock(name="replacement-admin")

    def publish_replacement() -> None:
        backend._producer = replacement_producer
        backend._admin_client = replacement_admin
        backend._publish_generation_locked()

    residual.close.side_effect = publish_replacement
    backend._producer = residual
    producer_factory = mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    admin_factory = mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")

    backend.connect()

    producer_factory.assert_not_called()
    admin_factory.assert_not_called()
    assert backend._producer is replacement_producer
    assert backend._admin_client is replacement_admin
    assert backend.is_connected()


def test_snapshot_revalidation_covers_strict_validation_and_durable_guards(
    mocker,
) -> None:
    """Mutated settings fail before any Kafka constructor and expose no raw value."""
    backend = _backend()
    backend.config.__dict__["retries"] = "retry-secret"
    with pytest.raises(ConfigurationError) as invalid:
        backend._capture_connection_snapshot()
    assert invalid.value.setting_name == "retries"
    assert "retry-secret" not in str(invalid.value)

    backend = _backend()
    backend.config.__dict__["request_timeout_ms"] = 0
    with pytest.raises(ConfigurationError) as timeout_error:
        backend._capture_connection_snapshot()
    assert timeout_error.value.setting_name == "request_timeout_ms"

    backend = _backend()
    backend.config.__dict__["enable_auto_commit"] = True
    # R141-F18: the settings-layer validator now rejects this inside the
    # snapshot's ``model_validate`` revalidation, so the boundary surfaces its
    # static message; the setting name still pinpoints the mutation.
    with pytest.raises(ConfigurationError) as auto_commit_error:
        backend._capture_connection_snapshot()
    assert auto_commit_error.value.setting_name == "enable_auto_commit"
    mocker.patch("scrapy_extension.backends.kafka.KafkaProducer")
    mocker.patch("scrapy_extension.backends.kafka.KafkaAdminClient")


def test_empty_handle_graph_is_not_published() -> None:
    """A disconnected backend cannot wedge a later real connect with an empty lease."""
    backend = _backend()
    backend._publish_generation_locked()
    assert backend._generation_gate.current is None


def test_generation_admission_race_is_reported_as_disconnected(mocker) -> None:
    """A lease lost between admission and use cannot call an unowned client."""
    backend = _backend()
    _publish(backend, mocker)
    mocker.patch.object(
        backend._generation_gate,
        "lease",
        side_effect=GenerationUnavailable("ping"),
    )

    with pytest.raises(QueueError, match="disconnected"):
        with backend._lease_generation("ping"):
            pass


def test_security_snapshot_emits_ssl_config_without_optional_sasl_fields() -> None:
    """SSL-only brokers receive hostname verification but no absent SASL fields."""
    backend = _backend(KafkaSettings(security_protocol="SSL", ssl_cafile="/ca.pem"))
    config = backend._build_common_config()
    assert config["security_protocol"] == "SSL"
    assert config["ssl_cafile"] == "/ca.pem"
    assert config["ssl_check_hostname"] is True
    assert "sasl_mechanism" not in config
    assert "sasl_plain_username" not in config


def test_confluent_security_fallback_keeps_client_config_safe() -> None:
    """A malformed cloud credential snapshot cannot fall back to producer args."""
    backend = _backend(
        KafkaSettings(
            mode=KafkaMode.CONFLUENT,
            confluent_api_key="cloud-key",
            confluent_api_secret="cloud-secret",
            confluent_bootstrap_servers="cloud:9092",
        )
    )
    snapshot = backend._capture_connection_snapshot()
    without_cloud_creds = replace(
        snapshot, confluent_api_key=None, confluent_api_secret=None
    )
    config = backend._build_client_security_config(without_cloud_creds)
    assert "bootstrap_servers" not in config
    assert "sasl_plain_username" not in config
    assert "sasl_plain_password" not in config


def test_rebalance_listener_is_replaced_when_identity_changes(mocker) -> None:
    """A generation never reuses a listener bound to another consumer."""
    backend = _backend()
    _, _, consumer, generation = _publish(backend, mocker, consumer=mocker.MagicMock())
    generation.value.rebalance_listener = object()
    replacement = mocker.MagicMock()

    listener = backend._ensure_rebalance_listener_locked(generation, replacement)

    assert listener._generation is generation
    assert listener._consumer is replacement
    assert generation.value.rebalance_listener is listener


def test_topic_creation_response_variants_fail_closed_without_secret_echo() -> None:
    """Every malformed broker response is a static, non-retryable contract error."""
    cases = [
        (SimpleNamespace(), "no valid topic_errors"),
        (SimpleNamespace(topic_errors=[("scrapy-q",)]), "malformed topic entry"),
        (SimpleNamespace(topic_errors=[("scrapy-other", 0, None)]), "did not identify"),
        (
            SimpleNamespace(topic_errors=[("scrapy-q", True, None)]),
            "invalid error code",
        ),
        (SimpleNamespace(topic_errors=[("scrapy-q", "0", None)]), "invalid error code"),
    ]
    for response, message in cases:
        with pytest.raises(QueueError, match=message) as exc_info:
            KafkaBackend._validate_topic_creation_response(
                response, topic_name="scrapy-q", queue_name="secret-queue"
            )
        assert "secret-queue" not in str(exc_info.value)

    assert KafkaBackend._validate_topic_creation_response(
        SimpleNamespace(
            topic_errors=[("scrapy-other", 0, None), ("scrapy-q", 0, None)]
        ),
        topic_name="scrapy-q",
        queue_name="q",
    )
    assert not KafkaBackend._validate_topic_creation_response(
        SimpleNamespace(
            topic_errors=[
                ("scrapy-other", 0, None),
                ("scrapy-q", TopicAlreadyExistsError.errno, None),
            ]
        ),
        topic_name="scrapy-q",
        queue_name="q",
    )


def _valid_policy_admin(mocker, topic: str = "scrapy-q") -> Any:
    admin = mocker.MagicMock()
    admin.describe_topics.return_value = [
        {
            "error_code": 0,
            "topic": topic,
            "partitions": [{"partition": i, "replicas": [0]} for i in range(10)],
        }
    ]
    config_response = SimpleNamespace(
        resources=[
            (
                0,
                None,
                2,
                topic,
                [
                    ("retention.ms", "604800000", False, False, False),
                    ("min.insync.replicas", "1", False, False, False),
                ],
            )
        ]
    )
    admin.describe_configs.return_value = [config_response]
    return admin


@pytest.mark.parametrize(
    "mutation",
    [
        lambda admin: setattr(admin.describe_topics, "return_value", None),
        lambda admin: setattr(
            admin.describe_topics, "return_value", [{"topic": "other"}]
        ),
        lambda admin: setattr(
            admin.describe_topics,
            "return_value",
            [{"topic": "scrapy-q", "error_code": 5, "partitions": []}],
        ),
        lambda admin: setattr(
            admin.describe_topics,
            "return_value",
            [{"topic": "scrapy-q", "error_code": 0, "partitions": []}],
        ),
        lambda admin: setattr(
            admin.describe_topics,
            "return_value",
            [{"topic": "scrapy-q", "error_code": 0, "partitions": ["bad"] * 10}],
        ),
        lambda admin: setattr(
            admin.describe_topics,
            "return_value",
            [
                {
                    "topic": "scrapy-q",
                    "error_code": 0,
                    "partitions": [{"replicas": [0, 1]}] + [{"replicas": [0]}] * 9,
                }
            ],
        ),
        lambda admin: setattr(admin.describe_configs, "return_value", None),
        lambda admin: setattr(
            admin.describe_configs, "return_value", [SimpleNamespace(resources=None)]
        ),
        lambda admin: setattr(
            admin.describe_configs,
            "return_value",
            [SimpleNamespace(resources=[("short",)])],
        ),
        lambda admin: setattr(
            admin.describe_configs,
            "return_value",
            [SimpleNamespace(resources=[(1, None, 2, "scrapy-q", [])])],
        ),
        lambda admin: setattr(
            admin.describe_configs,
            "return_value",
            [SimpleNamespace(resources=[(0, None, 2, "scrapy-q", None)])],
        ),
        lambda admin: setattr(
            admin.describe_configs,
            "return_value",
            [SimpleNamespace(resources=[(0, None, 2, "scrapy-q", [("short",)])])],
        ),
        lambda admin: setattr(
            admin.describe_configs,
            "return_value",
            [
                SimpleNamespace(
                    resources=[(0, None, 2, "scrapy-q", [("retention.ms", 1)])]
                )
            ],
        ),
    ],
)
def test_existing_topic_policy_malformed_responses_fail_closed(
    mocker, mutation
) -> None:
    """Topic inspection rejects incomplete broker structures before caching."""
    backend = _backend()
    admin = _valid_policy_admin(mocker)
    mutation(admin)
    with pytest.raises(QueueError, match="policy"):
        backend._validate_existing_topic_policy(
            topic_name="scrapy-q",
            queue_name="q",
            partitions=10,
            replicas=1,
            retention=604800000,
            min_isr=1,
            admin_client=admin,
        )


def test_existing_topic_policy_ignores_non_string_config_values_then_rejects(
    mocker,
) -> None:
    """Unknown/non-string config entries never satisfy the required policy."""
    backend = _backend()
    admin = _valid_policy_admin(mocker)
    admin.describe_configs.return_value[0].resources[0] = (
        0,
        None,
        2,
        "scrapy-q",
        [("retention.ms", "604800000"), ("min.insync.replicas", 1), (None, None)],
    )
    with pytest.raises(QueueError, match="policy"):
        backend._validate_existing_topic_policy(
            topic_name="scrapy-q",
            queue_name="q",
            partitions=10,
            replicas=1,
            retention=604800000,
            min_isr=1,
            admin_client=admin,
        )


def test_existing_topic_inspection_kafka_failure_is_wrapped_by_push(mocker) -> None:
    """A broker error while inspecting an already-existing topic is not retried as success."""
    backend = _backend()
    admin = mocker.MagicMock()
    admin.create_topics.side_effect = TopicAlreadyExistsError("exists")
    admin.describe_topics.side_effect = KafkaError("broker-secret")
    backend._admin_client = admin

    with pytest.raises(QueueError) as exc_info:
        backend._ensure_topic_exists("q")
    assert exc_info.value.operation == "push"
    assert "broker-secret" not in str(exc_info.value)


def test_push_timeout_is_indeterminate_and_never_retried(mocker) -> None:
    """A future timeout makes outcome unknown; the backend sends exactly once."""
    backend = _backend()
    producer, _, _, _ = _publish(backend, mocker)
    backend._known_topics.add("scrapy-q")
    future = mocker.MagicMock()
    future.get.side_effect = TimeoutError("credential-secret")
    producer.send.return_value = future

    with pytest.raises(QueueError) as exc_info:
        backend.push("q", b"payload")
    assert isinstance(exc_info.value, QueueOutcomeIndeterminateError)
    assert "credential-secret" not in str(exc_info.value)
    producer.send.assert_called_once_with("scrapy-q", value=b"payload", partition=0)
    future.get.assert_called_once()


def test_push_rejects_a_generation_retired_after_future_resolution(mocker) -> None:
    """A future result from a retired lease cannot become a durable success."""
    backend = _backend()
    producer, admin, _, generation = _publish(backend, mocker)
    backend._known_topics.add("scrapy-q")
    future = mocker.MagicMock()
    producer.send.return_value = future

    def retire_after_resolution(*_args: object, **_kwargs: object) -> None:
        backend.disconnect()

    future.get.side_effect = retire_after_resolution
    with pytest.raises(QueueError, match="connection changed"):
        backend.push("q", b"payload")
    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()
    assert not generation.accepting


def test_pop_constructor_fence_closes_private_candidate(mocker) -> None:
    """Disconnect during consumer construction fences and closes that candidate."""
    backend = _backend()
    candidate = mocker.MagicMock(name="consumer-candidate")

    def construct(**_kwargs):
        backend.disconnect()
        return candidate

    mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", side_effect=construct)

    with pytest.raises(QueueError, match="connection changed"):
        backend.pop("q")
    candidate.close.assert_called_once_with()
    assert backend._consumer is None
    assert backend._generation_gate.current is None


def test_pop_subscribe_failure_detaches_and_closes_published_candidate(mocker) -> None:
    """A candidate that fails before poll is removed from both mirrors and lease."""
    backend = _backend()
    candidate = mocker.MagicMock(name="consumer-candidate")
    candidate.subscribe.side_effect = KafkaError("subscribe failed")
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=candidate
    )

    with pytest.raises(QueueError):
        backend.pop("q")
    candidate.close.assert_called_once_with()
    assert backend._consumer is None
    assert backend._generation_gate.current is None


def test_retired_consumer_candidate_is_finalized_by_its_generation(mocker) -> None:
    """A retired generation, not its operation callback, owns a late candidate."""
    backend = _backend()
    _, _, _, generation = _publish(backend, mocker)
    retired = backend._generation_gate.retire()
    assert retired is generation
    retired.value.retired = True
    candidate = mocker.MagicMock(name="late-consumer")
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=candidate
    )

    with pytest.raises(QueueError, match="connection changed"):
        backend._poll_record_unlocked("q", 0.0, retired.value)
    # The candidate was not registered before the retired-generation guard, so
    # this operation owns its one close; the generation finalizer owns only its
    # original producer/admin graph.
    candidate.close.assert_called_once_with()
    assert backend._generation_gate.drain(retired) is None


def test_stale_generation_poll_is_rejected_before_subscribe(mocker) -> None:
    """A detached operation cannot use a replacement consumer graph."""
    backend = _backend()
    consumer = mocker.MagicMock()
    _, _, _, generation = _publish(backend, mocker, consumer=consumer)
    retired = backend._generation_gate.retire()
    assert retired is generation
    retired.value.retired = True

    with pytest.raises(QueueError, match="connection changed"):
        backend._poll_record_unlocked("q", 0.0, retired.value)
    consumer.subscribe.assert_not_called()


def test_direct_consumer_injection_attaches_to_existing_generation(mocker) -> None:
    """A consumer injected after producer/admin setup is leased, not duplicated."""
    backend = _backend()
    _, _, _, generation = _publish(backend, mocker)
    consumer = mocker.MagicMock()
    consumer.poll.return_value = {}
    backend._consumer = consumer

    assert backend._poll_record_unlocked("q", 0.0, generation.value) is None
    assert generation.value.consumer is consumer
    assert backend._consumer is consumer


def test_pop_legacy_fallback_retains_record_without_publishing_empty_generation(
    mocker,
) -> None:
    """Legacy helper callers still get a last-record slot without fake connectivity."""
    backend = _backend()
    record = _record(mocker, "scrapy-q", 3, value=b"legacy")
    mocker.patch.object(backend, "_poll_record", return_value=record)

    assert backend.pop("q") == b"legacy"
    assert backend._last_record is record
    assert backend._generation_gate.current is None


def test_redelivery_replaces_attempt_identity_and_preserves_legacy_cohort(
    mocker,
) -> None:
    """A repeated offset cannot let the old attempt settle the new delivery."""
    backend = _backend()
    consumer = mocker.MagicMock()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer.assignment.return_value = {tp}
    consumer.poll.side_effect = [
        {tp: [_record(mocker, topic, 1, value=b"legacy")]},
        {tp: [_record(mocker, topic, 1)]},
        {tp: [_record(mocker, topic, 1)]},
    ]
    backend._consumer = consumer

    assert backend.pop("q") == b"legacy"
    _, first = backend.pop_with_ack("q")
    _, second = backend.pop_with_ack("q")
    assert first is not None and second is not None and first != second
    assert backend._generation_gate.current is not None
    assert backend._generation_gate.current.value.legacy_record is not None
    backend.ack("q", token=first)
    consumer.commit.assert_not_called()
    backend.ack("q", token=second)
    consumer.commit.assert_called_once()


def test_finish_attempt_handles_retired_record_without_maps() -> None:
    """A malformed/retired generation's absent attempt map is a safe no-op."""
    backend = _backend()
    token = _KafkaAckToken(0, 1, "scrapy-q")
    handles = _KafkaGenerationHandles(None, None, active_attempts=None)
    backend._finish_attempt_locked(token, handles)
    assert handles.active_attempts is None


def test_ack_retired_generation_does_not_touch_replacement_maps(mocker) -> None:
    """An admitted old ack may finish old state but never resurrect new state."""
    backend = _backend()
    consumer = mocker.MagicMock()
    _, _, _, generation = _publish(backend, mocker, consumer=consumer)
    token = _KafkaAckToken(
        0,
        1,
        "scrapy-q",
        consumer_generation=generation.value.consumer_generation,
        assignment_epoch=generation.value.assignment_epoch,
        delivery_attempt=1,
    )
    backend._in_flight[("scrapy-q", 0)].add(1)
    backend._watermarks[("scrapy-q", 0)] = 1
    backend._high_water[("scrapy-q", 0)] = 2
    backend._active_attempts[("scrapy-q", 0, 1)] = 1
    backend._active_attempts_by_partition[("scrapy-q", 0)] = {1}
    retired = generation
    backend.disconnect()

    with pytest.raises(QueueError, match="connection changed"):
        backend._ack_token(token, retired.value)
    consumer.commit.assert_called_once()
    assert backend._in_flight == {}
    assert retired.value.in_flight is not backend._in_flight


def test_ack_missing_maps_and_missing_offset_are_idempotent(mocker) -> None:
    """Retired map shape and already-settled offsets never trigger a commit."""
    backend = _backend()
    consumer = mocker.MagicMock()
    token = _KafkaAckToken(0, 2, "scrapy-q", delivery_attempt=0)
    handles = _KafkaGenerationHandles(
        None,
        None,
        consumer=consumer,
        consumer_generation=0,
        assignment_epoch=0,
        active_attempts={},
        in_flight=None,
        watermarks=None,
        high_water=None,
    )
    backend._ack_token(token, handles)
    assert consumer.commit.call_count == 0

    handles.in_flight = {("scrapy-q", 0): {1}}
    handles.watermarks = {("scrapy-q", 0): 1}
    handles.high_water = {("scrapy-q", 0): 2}
    handles.active_attempts = {("scrapy-q", 0, 2): 2}
    token = _KafkaAckToken(0, 2, "scrapy-q", delivery_attempt=2)
    backend._ack_token(token, handles)
    consumer.commit.assert_not_called()


def test_ack_completed_base_prunes_maps_without_commit(mocker) -> None:
    """A token at an already-complete frontier cleans local state without SDK I/O."""
    backend = _backend()
    consumer = mocker.MagicMock()
    backend._consumer = consumer
    token = _KafkaAckToken(0, 4, "scrapy-q", delivery_attempt=1)
    key = ("scrapy-q", 0)
    backend._in_flight[key] = {4}
    backend._watermarks[key] = 4
    backend._high_water[key] = 4
    backend._active_attempts[("scrapy-q", 0, 4)] = 1
    backend._active_attempts_by_partition[key] = {1}

    backend._ack_token(token)

    consumer.commit.assert_not_called()
    assert key not in backend._in_flight
    assert key not in backend._watermarks
    assert key not in backend._high_water


def test_compatibility_ack_restores_offset_after_commit_failure(mocker) -> None:
    """A legacy zero-attempt token is restored when its commit outcome is known failed."""
    backend = _backend()
    consumer = mocker.MagicMock()
    consumer.commit.side_effect = KafkaError("commit failed")
    backend._consumer = consumer
    key = ("scrapy-q", 0)
    backend._in_flight[key] = {0}
    backend._watermarks[key] = 0
    backend._high_water[key] = 1
    token = _KafkaAckToken(0, 0, "scrapy-q", delivery_attempt=0)

    with pytest.raises(QueueError):
        backend._ack_token(token)
    assert backend._in_flight[key] == {0}


def test_nack_legacy_seek_and_cleanup_are_safe(mocker) -> None:
    """A real legacy record is sought, then its local delivery state is removed."""
    backend = _backend()
    consumer = mocker.MagicMock()
    tp = TopicPartition("scrapy-q", 2)
    consumer.assignment.return_value = {tp}
    record = _record(mocker, "scrapy-q", 7, partition=2)
    backend._consumer = consumer
    backend._last_record = record
    backend._in_flight[(record.topic, record.partition)] = {record.offset}
    backend._watermarks[(record.topic, record.partition)] = record.offset
    backend._high_water[(record.topic, record.partition)] = record.offset + 1

    backend.nack("q")

    consumer.seek.assert_called_once_with(tp, 7)
    assert backend._last_record is None
    assert (record.topic, record.partition) not in backend._in_flight
    assert (record.topic, record.partition) not in backend._watermarks


def test_nack_legacy_does_not_remove_an_exact_token_attempt(mocker) -> None:
    """A legacy nack cannot erase a concurrent token's retry bookkeeping."""
    backend = _backend()
    consumer = mocker.MagicMock()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer.assignment.return_value = {tp}
    consumer.poll.side_effect = [
        {tp: [_record(mocker, topic, 1, value=b"legacy")]},
        {tp: [_record(mocker, topic, 2)]},
    ]
    backend._consumer = consumer
    backend.pop("q")
    _, token = backend.pop_with_ack("q")
    backend.nack("q")

    assert token is not None
    assert backend._in_flight[(topic, 0)] == {1, 2}
    consumer.commit.assert_not_called()


def test_nack_token_retirement_after_seek_is_reported(mocker) -> None:
    """A callback retiring the generation during seek cannot report success."""
    backend = _backend()
    consumer = mocker.MagicMock()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic, 1)]}
    backend._consumer = consumer
    _, token = backend.pop_with_ack("q")
    generation = backend._generation_gate.current
    assert generation is not None

    def retire(*_args: object, **_kwargs: object) -> None:
        generation.value.retired = True

    consumer.seek.side_effect = retire
    with pytest.raises(QueueError, match="connection changed"):
        backend.nack("q", token=token)


def test_nack_legacy_seek_failure_is_wrapped(mocker) -> None:
    """Legacy seek errors are retryable QueueErrors and retain no false ack."""
    backend = _backend()
    consumer = mocker.MagicMock()
    tp = TopicPartition("scrapy-q", 0)
    consumer.assignment.return_value = {tp}
    consumer.seek.side_effect = KafkaError("seek failed")
    record = _record(mocker, "scrapy-q", 2)
    backend._consumer = consumer
    backend._last_record = record
    backend._in_flight[(record.topic, 0)] = {2}

    with pytest.raises(QueueError, match="nack"):
        backend.nack("q")
    assert backend._last_record is record


def test_queue_len_rejects_malformed_offsets_and_clamps_retention_lag(mocker) -> None:
    """Queue depth fails closed for malformed broker metadata and clamps retention."""
    backend = _backend()
    tp = TopicPartition("scrapy-q", 0)
    cases = [
        ({tp: True}, {tp: 0}, 0, "invalid end"),
        ({tp: 5}, {tp: -1}, 0, "invalid beginning"),
    ]
    for ends, beginnings, committed, message in cases:
        consumer = mocker.MagicMock()
        consumer.end_offsets.return_value = ends
        consumer.beginning_offsets.return_value = beginnings
        consumer.committed.return_value = committed
        with pytest.raises(QueueError, match=message):
            backend._consumer_group_lag(
                consumer, {tp}, queue_name="q", auto_offset_reset="earliest"
            )

    consumer = mocker.MagicMock()
    consumer.end_offsets.return_value = {tp: 5}
    consumer.beginning_offsets.return_value = {tp: 3}
    consumer.committed.return_value = "bad"
    with pytest.raises(QueueError, match="invalid committed"):
        backend._consumer_group_lag(
            consumer, {tp}, queue_name="q", auto_offset_reset="earliest"
        )

    consumer.committed.return_value = 1
    assert (
        backend._consumer_group_lag(
            consumer, {tp}, queue_name="q", auto_offset_reset="earliest"
        )
        == 2
    )
    consumer.committed.return_value = 8
    assert (
        backend._consumer_group_lag(
            consumer, {tp}, queue_name="q", auto_offset_reset="earliest"
        )
        == 0
    )


def test_queue_len_rejects_non_mapping_metadata_and_retired_live_generation(
    mocker,
) -> None:
    """Metadata shape and retirement are both terminal depth outcomes."""
    backend = _backend()
    tp = TopicPartition("scrapy-q", 0)
    consumer = mocker.MagicMock()
    consumer.end_offsets.return_value = []
    consumer.beginning_offsets.return_value = {}
    with pytest.raises(QueueError, match="invalid offset metadata"):
        backend._consumer_group_lag(
            consumer, {tp}, queue_name="q", auto_offset_reset="latest"
        )

    consumer.end_offsets.return_value = {tp: 5}
    consumer.beginning_offsets.return_value = {tp: 0}
    consumer.committed.return_value = 0
    consumer.assignment.return_value = {tp}
    producer, admin, _, generation = _publish(backend, mocker, consumer=consumer)
    generation.value.retired = True
    with pytest.raises(QueueError, match="connection changed"):
        backend._queue_len_unlocked("q", generation.value.snapshot, generation.value)
    producer.close.assert_not_called()
    admin.close.assert_not_called()


def test_temp_queue_probe_closes_after_success_even_when_close_fails(mocker) -> None:
    """A successful temporary probe returns its depth despite ordinary close failure."""
    backend = _backend()
    temp = mocker.MagicMock()
    tp = TopicPartition("scrapy-q", 0)
    temp.partitions_for_topic.return_value = {0}
    temp.end_offsets.return_value = {tp: 8}
    temp.beginning_offsets.return_value = {tp: 0}
    temp.committed.return_value = 3
    temp.close.side_effect = RuntimeError("close only")
    mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=temp)

    assert backend.queue_len("q") == 5
    temp.close.assert_called_once_with()


def test_disconnect_defers_generation_finalizer_until_lease_exit(mocker) -> None:
    """Detached handles stay owned by an admitted operation until it exits."""
    backend = _backend()
    producer, admin, _, generation = _publish(backend, mocker)
    with backend._lease_generation("ping") as admitted:
        assert admitted is generation
        backend.disconnect()
        producer.close.assert_not_called()
        admin.close.assert_not_called()
    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_close_diagnostics_cannot_raise_after_all_clients_are_closed(mocker) -> None:
    """An ordinary close failure remains diagnostic-only even if logging fails."""
    first = mocker.MagicMock()
    second = mocker.MagicMock()
    first.close.side_effect = RuntimeError("close")
    debug = mocker.patch.object(
        kafka_module.logger, "debug", side_effect=KeyboardInterrupt
    )

    KafkaBackend._close_detached_clients(first, second)

    first.close.assert_called_once_with()
    second.close.assert_called_once_with()
    debug.assert_called_once()


def test_public_push_redacts_queue_identity_and_future_exception(mocker) -> None:
    """Public push diagnostics remain static even when inputs contain secrets."""
    backend = _backend()
    producer, _, _, _ = _publish(backend, mocker)
    backend._known_topics.add("scrapy-secret-queue")
    future = mocker.MagicMock()
    future.get.side_effect = RuntimeError("future-password")
    producer.send.return_value = future

    with pytest.raises(QueueError) as exc_info:
        backend.push("secret-queue", b"payload")
    assert str(exc_info.value) == (
        "Kafka push outcome is indeterminate; retry may duplicate the message."
    )
    assert "secret-queue" not in str(exc_info.value)
    assert "future-password" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_public_malformed_topic_response_has_static_diagnostic(mocker) -> None:
    """Malformed broker data cannot expose the logical queue through the boundary."""
    backend = _backend()
    producer, admin, _, _ = _publish(backend, mocker)
    admin.create_topics.return_value = SimpleNamespace(topic_errors=[("wrong", 0)])

    with pytest.raises(QueueError) as exc_info:
        backend.push("secret-queue", b"payload")
    assert str(exc_info.value) == (
        "Kafka create-topics response did not identify the requested topic."
    )
    assert "secret-queue" not in str(exc_info.value)
    producer.send.assert_not_called()
