"""Additional Kafka edge contracts whose outcomes must remain observable."""

from __future__ import annotations

from typing import Any

import pytest
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from scrapy_extension.backends.kafka import KafkaBackend, _KafkaAckToken
from scrapy_extension.exceptions import ConfigurationError, QueueError
from scrapy_extension.settings import KafkaSettings


@pytest.fixture(autouse=True)
def _authorize_legacy_remote_kafka_fixtures(monkeypatch) -> None:
    monkeypatch.setenv("SCRAPY_KAFKA_ALLOW_REMOTE_PLAINTEXT", "true")


def _backend() -> KafkaBackend:
    return KafkaBackend(KafkaSettings())


def _record(mocker, topic: str = "scrapy-q", offset: int = 0):
    record = mocker.MagicMock()
    record.topic = topic
    record.partition = 0
    record.offset = offset
    record.value = b"payload"
    return record


def _published(backend: KafkaBackend, mocker, consumer: Any = None):
    backend._producer = mocker.MagicMock(name="producer")
    backend._admin_client = mocker.MagicMock(name="admin")
    backend._consumer = consumer
    backend._publish_generation_locked()
    generation = backend._generation_gate.current
    assert generation is not None
    return generation


def test_snapshot_explicit_timeout_guard_is_not_bypassed(mocker) -> None:
    """The durable-push deadline guard rejects a model that was mutated in place."""
    backend = _backend()
    validated = KafkaSettings().model_copy(update={"request_timeout_ms": 0})
    mocker.patch.object(KafkaSettings, "model_validate", return_value=validated)

    with pytest.raises(ConfigurationError) as exc_info:
        backend._capture_connection_snapshot()
    assert exc_info.value.setting_name == "request_timeout_ms"


def test_ping_false_when_accepting_generation_lacks_admin(mocker) -> None:
    """An incomplete accepting graph cannot claim broker health."""
    backend = _backend()
    generation = _published(backend, mocker)
    generation.value.admin_client = None

    assert backend.ping() is False


def test_push_rejects_missing_producer_inside_admitted_generation(mocker) -> None:
    """A producer disappearing after admission fails closed without an SDK call."""
    backend = _backend()
    generation = _published(backend, mocker)
    generation.value.producer = None
    backend._producer = None
    backend._known_topics.add("scrapy-q")

    with pytest.raises(QueueError, match="Failed to push Kafka message"):
        backend.push("q", b"payload")


def test_policy_inspection_requires_an_admin_client() -> None:
    """Durability verification cannot silently pass without an admin handle."""
    with pytest.raises(Exception) as exc_info:
        _backend()._validate_existing_topic_policy(
            topic_name="scrapy-q",
            queue_name="q",
            partitions=10,
            replicas=1,
            retention=604800000,
            min_isr=1,
            admin_client=None,
        )
    assert isinstance(exc_info.value, Exception)
    assert "admin client is None" in str(exc_info.value)


def test_partial_cleanup_uses_current_generation_when_handles_are_omitted(
    mocker,
) -> None:
    """A candidate callback with no explicit handles still detaches its live mirror."""
    backend = _backend()
    _published(backend, mocker)
    candidate = mocker.MagicMock(name="candidate")
    backend._consumer = candidate

    backend._cleanup_partial_consumer(candidate, None, True)

    candidate.close.assert_called_once_with()
    assert backend._consumer is None


def test_constructor_candidate_is_registered_when_existing_generation_is_live(
    mocker,
) -> None:
    """A lazy consumer joins the accepting generation before subscribe/poll."""
    backend = _backend()
    generation = _published(backend, mocker)
    candidate = mocker.MagicMock(name="candidate")
    candidate.poll.return_value = {}
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=candidate
    )

    assert backend._poll_record_unlocked("q", 0.0, generation.value) is None
    assert generation.value.consumer is candidate
    candidate.close.assert_not_called()


@pytest.mark.parametrize("phase", ["subscribe", "poll"])
def test_retired_live_consumer_is_fenced_after_sdk_callback(mocker, phase: str) -> None:
    """Retirement observed after subscribe or poll is not reported as empty work."""
    consumer = mocker.MagicMock()
    consumer.poll.return_value = {}
    backend = _backend()
    generation = _published(backend, mocker, consumer=consumer)
    generation.value.retired = True
    backend._subscribed_topic = "scrapy-q" if phase == "poll" else None

    with pytest.raises(QueueError, match="connection changed"):
        backend._poll_record_unlocked("q", 0.0, generation.value)

    if phase == "subscribe":
        consumer.subscribe.assert_called_once()
    else:
        consumer.poll.assert_called_once()


def test_repeated_offset_without_partition_attempt_set_is_safe(mocker) -> None:
    """A stale attempt can be replaced even when its partition index was pruned."""
    consumer = mocker.MagicMock()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer.assignment.return_value = {tp}
    record = _record(mocker, topic, 2)
    consumer.poll.side_effect = [{tp: [record]}, {tp: [_record(mocker, topic, 2)]}]
    backend = _backend()
    backend._consumer = consumer

    _, first = backend.pop_with_ack("q")
    backend._active_attempts_by_partition.clear()
    _, second = backend.pop_with_ack("q")

    assert first != second
    backend.ack("q", token=first)
    consumer.commit.assert_not_called()


def test_legacy_ack_singleton_cleans_unleased_maps(mocker) -> None:
    """The compatibility ack commits only its own partition and prunes its maps."""
    backend = _backend()
    consumer = mocker.MagicMock()
    record = _record(mocker, offset=4)
    backend._consumer = consumer
    backend._last_record = record
    key = (record.topic, 0)
    backend._in_flight[key] = {record.offset}
    backend._watermarks[key] = record.offset
    backend._high_water[key] = record.offset + 1

    backend._ack_unleased_unlocked("q", handles=None)

    consumer.commit.assert_called_once()
    assert backend._last_record is None
    assert key not in backend._in_flight
    assert key not in backend._watermarks
    assert key not in backend._high_water


def test_legacy_ack_refuses_extra_pending_offset(mocker) -> None:
    """Bare ack never advances a partition past another pending delivery."""
    backend = _backend()
    backend._consumer = mocker.MagicMock()
    backend._last_record = _record(mocker, offset=4)
    backend._in_flight[("scrapy-q", 0)] = {4, 5}

    with pytest.raises(QueueError, match="un-acked"):
        backend._ack_unleased_unlocked("q", handles=None)
    backend._consumer.commit.assert_not_called()


def test_ack_retry_settles_attempt_stranded_by_postcommit_interruption(mocker) -> None:
    """A retry after a post-commit interruption must not strand the attempt.

    The offset leaves the in-flight set *before* the broker commit, so a
    process-control exception between the successful ``consumer.commit`` and
    the post-commit bookkeeping leaves the attempt entry behind with no
    settle path: every retried ack hits the in-flight guard and used to
    return early, keeping the entry resident (and refusing legacy bare acks
    on that topic-partition with QueueError) until a rebalance/reconnect.
    """
    backend = _backend()
    consumer = mocker.MagicMock()
    backend._consumer = consumer
    record = _record(mocker, offset=5)
    backend._last_record = record
    topic_partition = ("scrapy-q", 0)
    attempt_key = ("scrapy-q", 0, 5)
    backend._in_flight[topic_partition] = {5}
    backend._watermarks[topic_partition] = 5
    backend._high_water[topic_partition] = 6
    backend._active_attempts[attempt_key] = 1
    backend._active_attempts_by_partition[topic_partition] = {1}
    token = _KafkaAckToken(0, 5, "scrapy-q", delivery_attempt=1)

    original_finish = backend._finish_attempt_locked
    interrupted = {"done": False}

    def _interrupt_first_pass(*args: Any, **kwargs: Any) -> None:
        if not interrupted["done"]:
            interrupted["done"] = True
            raise KeyboardInterrupt
        original_finish(*args, **kwargs)

    mocker.patch.object(
        backend, "_finish_attempt_locked", side_effect=_interrupt_first_pass
    )

    with pytest.raises(KeyboardInterrupt):
        backend.ack("q", token=token)
    consumer.commit.assert_called_once()

    # The commit reached the broker but the attempt entry is now stranded.
    assert backend._active_attempts == {attempt_key: 1}
    assert backend._active_attempts_by_partition == {topic_partition: {1}}

    # Retrying the same token settles the stranded attempt instead of
    # returning early at the in-flight guard.
    backend.ack("q", token=token)
    assert backend._active_attempts == {}
    assert backend._active_attempts_by_partition == {}
    assert consumer.commit.call_count == 1

    # The legacy bare ack on that topic-partition is usable again.
    consumer.commit.reset_mock()
    backend.ack("q")
    consumer.commit.assert_called_once_with(
        {TopicPartition("scrapy-q", 0): OffsetAndMetadata(6, "")}
    )


def test_nested_ack_restores_thread_local_lease_handle(mocker) -> None:
    """Nested helper use restores the caller's admitted generation context."""
    backend = _backend()
    previous = object()
    backend._delivery_lease.handles = previous

    backend.ack("q", token=object())

    assert backend._delivery_lease.handles is previous


def test_nack_foreign_token_and_missing_legacy_delivery_are_noops() -> None:
    """Foreign tokens and empty legacy slots must not touch Kafka."""
    backend = _backend()
    backend.nack("q", token=object())
    backend._nack_unleased_unlocked("q", token=None, handles=None)
    assert backend._consumer is None


def test_retired_token_nack_is_refused_before_seek(mocker) -> None:
    """A retired generation cannot seek an old token."""
    backend = _backend()
    consumer = mocker.MagicMock()
    generation = _published(backend, mocker, consumer=consumer)
    token = _KafkaAckToken(
        0,
        1,
        "scrapy-q",
        consumer_generation=generation.value.consumer_generation,
        assignment_epoch=generation.value.assignment_epoch,
        delivery_attempt=1,
    )
    generation.value.in_flight = {("scrapy-q", 0): {1}}
    generation.value.active_attempts = {("scrapy-q", 0, 1): 1}
    generation.value.retired = True

    with pytest.raises(QueueError, match="connection changed"):
        backend._nack_unleased_unlocked("q", token=token, handles=generation.value)
    consumer.seek.assert_not_called()


def test_nack_token_becomes_stale_during_assignment_check(mocker) -> None:
    """A rebalance during assignment lookup turns nack into an idempotent no-op."""
    backend = _backend()
    consumer = mocker.MagicMock()
    consumer.assignment.side_effect = lambda: backend._active_attempts.clear() or set()
    backend._consumer = consumer
    token = _KafkaAckToken(0, 1, "scrapy-q", delivery_attempt=1)
    backend._in_flight[("scrapy-q", 0)] = {1}
    backend._active_attempts[("scrapy-q", 0, 1)] = 1

    backend._nack_unleased_unlocked("q", token=token, handles=None)
    consumer.seek.assert_not_called()


def test_nack_token_becomes_stale_after_seek(mocker) -> None:
    """A seek callback that fences the attempt prevents final settlement."""
    backend = _backend()
    consumer = mocker.MagicMock()
    tp = TopicPartition("scrapy-q", 0)
    consumer.assignment.return_value = {tp}
    consumer.seek.side_effect = lambda *_args: backend._active_attempts.clear()
    backend._consumer = consumer
    token = _KafkaAckToken(0, 1, "scrapy-q", delivery_attempt=1)
    backend._in_flight[("scrapy-q", 0)] = {1}
    backend._active_attempts[("scrapy-q", 0, 1)] = 1

    backend._nack_unleased_unlocked("q", token=token, handles=None)
    consumer.seek.assert_called_once_with(tp, 1)


def test_legacy_nack_same_state_guard_returns_without_settling(mocker) -> None:
    """A legacy record replaced during assignment cannot be cleared by the old nack."""
    backend = _backend()
    consumer = mocker.MagicMock()
    record = _record(mocker, offset=3)
    backend._consumer = consumer
    backend._last_record = record
    backend._in_flight[("scrapy-q", 0)] = {3}
    consumer.assignment.side_effect = lambda: (
        setattr(backend, "_last_record", None) or set()
    )

    backend._nack_unleased_unlocked("q", handles=None)
    consumer.seek.assert_not_called()


def test_legacy_nack_retired_after_seek_is_refused(mocker) -> None:
    """A legacy seek callback cannot make a retired operation succeed."""
    backend = _backend()
    consumer = mocker.MagicMock()
    generation = _published(backend, mocker, consumer=consumer)
    record = _record(mocker, offset=3)
    generation.value.legacy_record = record
    generation.value.in_flight = {("scrapy-q", 0): {3}}
    generation.value.retired = False
    consumer.assignment.return_value = {TopicPartition("scrapy-q", 0)}
    consumer.seek.side_effect = lambda *_args: setattr(
        generation.value, "retired", True
    )

    with pytest.raises(QueueError, match="connection changed"):
        backend._nack_unleased_unlocked("q", handles=generation.value)


def test_malformed_empty_temp_probe_has_no_cleanup_handle(mocker) -> None:
    """A constructor that returns no consumer cannot be closed or leaked."""
    backend = _backend()
    mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=None)

    with pytest.raises(QueueError):
        backend.queue_len("q")
