"""Resilience / contract tests for KafkaBackend (initiative #29).

kafka.py was 90.51% (24 uncovered lines + 11 partial branches) — the
largest coverage gap and below the 95% floor. This pins the clearest
contract clusters (validation, ack-token semantics, TOCTOU None-guards,
the watermark-commit error path, queue_len's temp-consumer fallback).
Each test maps to a documented contract, not a line-hit.
"""

from __future__ import annotations

from collections import defaultdict
from threading import Event, Thread

import pytest
from kafka import TopicPartition
from kafka.errors import KafkaError

from scrapy_extension.backends.kafka import (
    KafkaBackend,
    _KafkaAckToken,
    _validate_topic_name,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    QueueOutcomeIndeterminateError,
)
from scrapy_extension.settings import KafkaMode, KafkaSettings


def _backend() -> KafkaBackend:
    """Constructed-but-not-connected backend (clients are None)."""
    return KafkaBackend(KafkaSettings())


def _record(mocker, topic: str, offset: int = 0, partition: int = 0):
    record = mocker.MagicMock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    record.value = b"payload"
    return record


# ---------------------------------------------------------------------------
# _validate_topic_name (line 59)
# ---------------------------------------------------------------------------


def test_validate_topic_name_raises_on_invalid_name() -> None:
    """Line 59: a name with characters outside [A-Za-z0-9._-] raises
    ValueError — the input-validation contract at the system boundary
    (prevents malformed topic names reaching the broker)."""
    with pytest.raises(ValueError, match="Invalid topic/queue name"):
        _validate_topic_name("bad name with spaces!")
    # Sanity: a valid name passes.
    _validate_topic_name("valid.topic-name_1")


# ---------------------------------------------------------------------------
# _KafkaAckToken semantics (lines 102-104, 111)
# ---------------------------------------------------------------------------


def test_ack_token_eq_not_implemented_for_other_types() -> None:
    """Lines 102-103: __eq__ vs a non-_KafkaAckToken returns NotImplemented
    (Python's __eq__ protocol — dict/set membership for tokens survives a
    refactor that would silently return False)."""
    token = _KafkaAckToken(partition=0, offset=1, topic="t")
    assert token.__eq__("not-a-token") is NotImplemented


def test_ack_token_eq_true_for_equal_tokens_and_hash_stable() -> None:
    """Line 104 + 111: two tokens with identical (partition, offset, topic)
    compare equal AND hash equal — the contract that lets the watermark
    algorithm's in-flight set correctly dedup acks."""
    a = _KafkaAckToken(partition=0, offset=1, topic="t")
    b = _KafkaAckToken(partition=0, offset=1, topic="t")
    assert a == b
    assert hash(a) == hash(b)
    assert _KafkaAckToken(partition=0, offset=2, topic="t") != a
    assert _KafkaAckToken(partition=0, offset=1, topic="t", consumer_generation=1) != a
    assert (
        _KafkaAckToken(
            partition=0,
            offset=1,
            topic="t",
            delivery_attempt=1,
        )
        != a
    )


# ---------------------------------------------------------------------------
# TOCTOU None-guards (lines 454-455 push, 570-571 _poll_record, 657
# _ack_token, 804-805 clear_queue)
# ---------------------------------------------------------------------------


def test_push_raises_when_producer_becomes_none(mocker) -> None:
    """Lines 454-455: ``is_connected()`` passed but the producer became None
    before ``send()`` (concurrent disconnect) → the public static ``QueueError``
    rather than ``AttributeError`` on ``None.send()``. ``_ensure_topic_exists``
    is stubbed so the topic-check (which would itself raise on a None admin
    client) doesn't preempt the producer-None guard under test."""
    backend = _backend()
    backend._producer = None
    mocker.patch.object(backend, "is_connected", return_value=True)
    mocker.patch.object(
        backend, "_ensure_topic_exists"
    )  # don't let topic-check preempt
    with pytest.raises(QueueError) as exc_info:
        backend.push("q", b"x")
    assert str(exc_info.value) == "Failed to push Kafka message."
    assert exc_info.value.queue_name is None
    assert exc_info.value.operation == "push"


def test_ensure_topic_exists_raises_when_admin_client_is_none() -> None:
    """Lines 424-425: ``_ensure_topic_exists`` with no admin client (never
    connected / concurrent disconnect) → BackendConnectionError rather than
    ``None.create_topics()`` — distinct from clear_queue's own guard (804-805)
    but the same TOCTOU shape on the admin client."""
    backend = _backend()
    backend._admin_client = None
    with pytest.raises(BackendConnectionError, match="admin client is None"):
        backend._ensure_topic_exists("q")


def test_poll_record_raises_when_consumer_construction_returns_none(mocker) -> None:
    """Lines 570-571: ``KafkaConsumer(...)`` returning None (client-lib
    contract violation) fails fast with BackendConnectionError rather than
    dispatching ``.subscribe()`` / ``.poll()`` on None."""
    backend = _backend()
    backend._consumer = None
    mocker.patch("scrapy_extension.backends.kafka.KafkaConsumer", return_value=None)
    with pytest.raises(BackendConnectionError, match="consumer is None"):
        backend._poll_record("q", 0.0)


def test_ack_token_is_noop_when_consumer_is_none() -> None:
    """Line 657: ``_ack_token`` with no consumer silently returns — ack is
    best-effort, and a vanished consumer (post-disconnect) must not crash
    the caller (the message re-delivers via the broker's at-least-once)."""
    backend = _backend()
    backend._consumer = None
    backend._ack_token(
        _KafkaAckToken(partition=0, offset=1, topic="t")
    )  # must not raise


def test_clear_queue_fails_closed_even_without_admin_client() -> None:
    """Kafka clear is unsupported instead of reporting an unsafe reset."""
    backend = _backend()
    backend._admin_client = None
    with pytest.raises(QueueError, match="Kafka clear_queue is unsupported"):
        backend.clear_queue("q")


def test_disconnect_invalidates_known_topic_cache(mocker) -> None:
    backend = _backend()
    backend._producer = mocker.MagicMock()
    backend._admin_client = mocker.MagicMock()
    backend._known_topics.add("scrapy-stale")

    backend.disconnect()

    assert backend._known_topics == set()


def test_is_connected_requires_an_accepting_generation(mocker) -> None:
    """Compatibility mirrors alone must not bypass generation admission."""
    backend = _backend()
    backend._producer = mocker.MagicMock()
    backend._admin_client = mocker.MagicMock()

    assert backend.is_connected() is False
    assert backend._generation_gate.current is None


def test_partial_consumer_cleanup_closes_unregistered_candidate_once(mocker) -> None:
    backend = _backend()
    candidate = mocker.MagicMock()
    backend._consumer = candidate

    backend._cleanup_partial_consumer(candidate, None, True)

    assert backend._consumer is None
    candidate.close.assert_called_once_with()
    assert backend._generation_gate.current is None


def test_retired_consumer_cleanup_is_owned_by_generation_finalizer(mocker) -> None:
    backend = _backend()
    producer = mocker.MagicMock()
    admin = mocker.MagicMock()
    consumer = mocker.MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer
    backend._publish_generation_locked()
    retired = backend._generation_gate.retire()
    assert retired is not None
    retired.value.retired = True

    backend._cleanup_partial_consumer(consumer, retired.value, True)

    # A retired generation owns its consumer; the candidate path must not close it
    # a second time. The detached finalizer performs the sole close.
    consumer.close.assert_not_called()
    assert backend._consumer is None
    assert backend._generation_gate.drain(retired, consumer.close) is None
    consumer.close.assert_called_once_with()


def test_abort_partial_connect_detaches_retired_maps_without_cross_generation_mutation(
    mocker,
) -> None:
    backend = _backend()
    producer = mocker.MagicMock()
    admin = mocker.MagicMock()
    consumer = mocker.MagicMock()
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer
    backend._in_flight[("scrapy-q", 0)].add(3)
    backend._watermarks[("scrapy-q", 0)] = 3
    backend._high_water[("scrapy-q", 0)] = 4
    backend._publish_generation_locked()
    retired = backend._generation_gate.current
    assert retired is not None
    old_in_flight = retired.value.in_flight

    assert backend._abort_partial_connect() is False

    assert retired.value.retired is True
    assert retired.value.in_flight is old_in_flight
    assert old_in_flight is not None
    assert old_in_flight[("scrapy-q", 0)] == {3}
    assert backend._in_flight is not old_in_flight
    backend._in_flight[("scrapy-q", 0)].add(9)
    assert old_in_flight[("scrapy-q", 0)] == {3}
    assert backend._generation_gate.current is None
    producer.close.assert_called_once_with()
    consumer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_push_driver_failure_is_indeterminate_without_generation_leak(mocker) -> None:
    backend = _backend()
    producer = mocker.MagicMock()
    admin = mocker.MagicMock()
    future = mocker.MagicMock()
    future.get.side_effect = KafkaError("broker outcome unknown")
    producer.send.return_value = future
    backend._producer = producer
    backend._admin_client = admin
    backend._known_topics.add("scrapy-q")

    with pytest.raises(QueueError) as exc_info:
        backend.push("q", b"payload")

    assert exc_info.value.operation == "push"
    assert isinstance(exc_info.value, QueueOutcomeIndeterminateError)
    assert "indeterminate" in str(exc_info.value).lower()
    producer.send.assert_called_once()
    future.get.assert_called_once()
    generation = backend._generation_gate.current
    assert generation is not None and generation.accepting
    backend.disconnect()
    producer.close.assert_called_once_with()
    admin.close.assert_called_once_with()


def test_ack_commit_failure_restores_exact_attempt_for_retry(mocker) -> None:
    backend = _backend()
    consumer = mocker.MagicMock()
    consumer.commit.side_effect = [KafkaError("commit lost"), None]
    backend._consumer = consumer
    topic_partition = ("scrapy-q", 0)
    token = _KafkaAckToken(
        partition=0,
        offset=1,
        topic="scrapy-q",
        consumer_generation=0,
        assignment_epoch=0,
        delivery_attempt=1,
    )
    backend._in_flight[topic_partition].add(token.offset)
    backend._watermarks[topic_partition] = 0
    backend._high_water[topic_partition] = 2
    backend._active_attempts[(token.topic, token.partition, token.offset)] = 1
    backend._active_attempts_by_partition[topic_partition] = {1}

    with pytest.raises(QueueError, match="Failed to ack Kafka message"):
        backend.ack("q", token=token)
    assert backend._in_flight[topic_partition] == {1}
    assert backend._active_attempts[(token.topic, token.partition, token.offset)] == 1

    backend.ack("q", token=token)

    assert consumer.commit.call_count == 2
    assert topic_partition not in backend._in_flight
    assert topic_partition not in backend._watermarks
    assert topic_partition not in backend._high_water
    assert (token.topic, token.partition, token.offset) not in backend._active_attempts
    backend.disconnect()


def test_rebalance_listener_without_identity_is_a_noop() -> None:
    backend = _backend()
    backend._rebalance_listener.on_partitions_revoked([])
    backend._rebalance_listener.on_partitions_assigned([])
    assert backend._assignment_epoch == 0


# ---------------------------------------------------------------------------
# _ack_token error paths
# ---------------------------------------------------------------------------


def test_ack_unknown_token_is_idempotent_noop(mocker) -> None:
    """A duplicate/stale token must not seed state or query consumer position."""
    backend = _backend()
    backend._consumer = mocker.MagicMock()
    backend._in_flight = defaultdict(set)
    backend._watermarks = {}

    backend._ack_token(_KafkaAckToken(partition=0, offset=1, topic="t"))

    backend._consumer.position.assert_not_called()
    backend._consumer.commit.assert_not_called()
    assert backend._watermarks == {}


def test_ack_token_raises_when_commit_fails(mocker) -> None:
    """Lines 685-687: ``_consumer.commit()`` raising KafkaError (after the
    watermark advanced past the base) surfaces as a QueueError — the
    contiguous-run commit failed, so the caller sees the failure rather than
    a silent data loss. Setup: pre-seed the watermark base + high-water so
    the walk advances and triggers commit."""
    backend = _backend()
    backend._consumer = mocker.MagicMock()
    backend._consumer.commit.side_effect = KafkaError("commit boom")
    backend._in_flight = defaultdict(set)
    topic_partition = ("t", 0)
    backend._in_flight[topic_partition].add(1)
    # Pre-seeded base (0); high-water (5) lets the watermark walk advance past
    # the base -> commit path taken.
    backend._watermarks = {topic_partition: 0}
    backend._high_water = {topic_partition: 5}
    with pytest.raises(QueueError, match="Failed to ack Kafka message"):
        backend._ack_token(_KafkaAckToken(partition=0, offset=1, topic="t"))
    assert 1 in backend._in_flight[topic_partition]


# ---------------------------------------------------------------------------
# nack-with-token re-adds the offset (line 732->734)
# ---------------------------------------------------------------------------


def test_nack_with_unknown_token_does_not_create_in_flight_state(mocker) -> None:
    """A forged or already-completed token cannot rewind or create ack state."""
    backend = _backend()
    backend._consumer = mocker.MagicMock()
    backend._in_flight = defaultdict(set)
    token = _KafkaAckToken(partition=0, offset=7, topic="t")
    backend.nack("q", token=token)
    assert backend._in_flight == {}
    backend._consumer.seek.assert_not_called()


def test_nack_with_non_kafka_token_is_a_silent_noop() -> None:
    """Line 732->734 (false branch): nack(token=<non-_KafkaAckToken>) skips
    the in-flight re-add (can't partition/offset a foreign token type) and
    returns — defensive against a caller passing a legacy/external token
    shape, must not raise."""
    backend = _backend()
    backend._in_flight = defaultdict(set)
    backend.nack("q", token="some-legacy-opaque-token")  # must not raise
    assert backend._in_flight == {}  # nothing added


def test_nack_redelivery_gets_new_attempt_and_old_token_cannot_commit(
    mocker,
) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.side_effect = [
        {tp: [_record(mocker, topic)]},
        {tp: [_record(mocker, topic)]},
    ]
    backend._consumer = consumer

    _, old_token = backend.pop_with_ack("q")
    backend.nack("q", token=old_token)
    _, new_token = backend.pop_with_ack("q")

    assert old_token != new_token
    backend.ack("q", token=old_token)
    consumer.commit.assert_not_called()
    backend.ack("q", token=new_token)
    consumer.commit.assert_called_once()


def test_nack_success_is_terminal_but_seek_failure_is_retryable(mocker) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    consumer.seek.side_effect = [KafkaError("seek failed"), None]
    backend._consumer = consumer
    _, token = backend.pop_with_ack("q")

    with pytest.raises(QueueError, match="nack"):
        backend.nack("q", token=token)
    assert token.offset in backend._in_flight[(topic, 0)]
    backend.nack("q", token=token)
    assert token.offset in backend._in_flight[(topic, 0)]
    backend.nack("q", token=token)
    backend.ack("q", token=token)

    assert consumer.seek.call_count == 2
    assert token.offset in backend._in_flight[(topic, 0)]
    consumer.commit.assert_not_called()


def test_partition_revocation_fences_old_assignment_token(mocker) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.side_effect = [
        {tp: [_record(mocker, topic)]},
        {tp: [_record(mocker, topic)]},
    ]
    backend._consumer = consumer

    _, old_token = backend.pop_with_ack("q")
    listener = consumer.subscribe.call_args.kwargs["listener"]
    listener.on_partitions_revoked([tp])
    listener.on_partitions_assigned([tp])
    _, new_token = backend.pop_with_ack("q")

    assert old_token != new_token
    backend.ack("q", token=old_token)
    consumer.commit.assert_not_called()
    backend.ack("q", token=new_token)
    consumer.commit.assert_called_once()


def test_retired_rebalance_listener_cannot_mutate_its_generation(mocker) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    backend._consumer = consumer

    _, token = backend.pop_with_ack("q")
    listener = consumer.subscribe.call_args.kwargs["listener"]
    retired = backend._generation_gate.current
    assert retired is not None
    retired_epoch = retired.value.assignment_epoch

    backend.disconnect()
    listener.on_partitions_revoked([tp])
    listener.on_partitions_assigned([tp])

    assert retired.value.assignment_epoch == retired_epoch
    assert backend._generation_gate.current is None
    assert backend._assignment_epoch == retired_epoch + 1
    backend.ack("q", token=token)
    consumer.commit.assert_not_called()


def test_old_rebalance_listener_cannot_mutate_or_commit_replacement_generation(
    mocker,
) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    old_consumer = mocker.MagicMock()
    old_consumer.assignment.return_value = {tp}
    old_consumer.poll.return_value = {tp: [_record(mocker, topic, offset=0)]}
    backend._consumer = old_consumer

    _, old_token = backend.pop_with_ack("q")
    old_listener = old_consumer.subscribe.call_args.kwargs["listener"]
    old_generation = backend._generation_gate.current
    assert old_generation is not None

    backend.disconnect()

    new_consumer = mocker.MagicMock()
    new_consumer.assignment.return_value = {tp}
    new_consumer.poll.return_value = {tp: [_record(mocker, topic, offset=0)]}
    backend._consumer = new_consumer
    _, new_token = backend.pop_with_ack("q")
    new_generation = backend._generation_gate.current
    assert new_generation is not None
    new_epoch = new_generation.value.assignment_epoch

    old_listener.on_partitions_revoked([tp])
    old_listener.on_partitions_assigned([tp])

    assert old_generation.value.assignment_epoch != new_epoch
    assert new_generation.value.assignment_epoch == new_epoch
    backend.ack("q", token=old_token)
    new_consumer.commit.assert_not_called()
    backend.ack("q", token=new_token)
    new_consumer.commit.assert_called_once()


def test_rebalance_clears_generation_legacy_delivery(mocker) -> None:
    """A bare ack cannot settle a record from before a rebalance."""
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    backend._consumer = consumer

    assert backend.pop("q") == b"payload"
    listener = consumer.subscribe.call_args.kwargs["listener"]
    listener.on_partitions_revoked([tp])
    listener.on_partitions_assigned([tp])

    backend.ack("q")

    consumer.commit.assert_not_called()
    assert backend._last_record is None
    generation = backend._generation_gate.current
    assert generation is not None
    assert generation.value.legacy_record is None


def test_ack_rebalance_callback_fences_active_lease(mocker) -> None:
    """A rebalance callback during commit cannot make an old ack succeed."""
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    backend._consumer = consumer

    _, token = backend.pop_with_ack("q")
    listener = consumer.subscribe.call_args.kwargs["listener"]
    consumer.commit.side_effect = lambda *_args, **_kwargs: (
        listener.on_partitions_revoked([tp])
    )

    with pytest.raises(QueueError, match="connection changed"):
        backend.ack("q", token=token)

    generation = backend._generation_gate.current
    assert generation is not None
    assert generation.value.assignment_epoch == backend._assignment_epoch
    assert consumer.commit.call_count == 1


def test_ack_reentrant_disconnect_does_not_deadlock_with_teardown(
    mocker,
) -> None:
    """A competing teardown cannot invert connection and delivery locks."""
    backend = _backend()
    producer = mocker.MagicMock()
    admin = mocker.MagicMock()
    consumer = mocker.MagicMock()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    backend._producer = producer
    backend._admin_client = admin
    backend._consumer = consumer
    _, token = backend.pop_with_ack("q")

    teardown_entered = Event()
    allow_teardown = Event()
    competitor_started = Event()
    original_retire = backend._generation_gate.retire

    def retire():
        if competitor_started.is_set() and not allow_teardown.is_set():
            teardown_entered.set()
            assert allow_teardown.wait(timeout=2.0)
        return original_retire()

    backend._generation_gate.retire = retire  # type: ignore[method-assign]

    def competing_teardown() -> None:
        competitor_started.set()
        backend.disconnect()

    competitor = Thread(target=competing_teardown, daemon=True)

    def commit(*_args: object, **_kwargs: object) -> None:
        competitor.start()
        assert teardown_entered.wait(timeout=2.0)
        # Release the competing teardown only after it has tried disconnect.
        # Both operations then pass the real gate's retirement fence without a
        # scheduler-dependent delay.
        allow_teardown.set()
        backend.disconnect()

    consumer.commit.side_effect = commit
    errors: list[BaseException] = []

    def acknowledge() -> None:
        try:
            backend.ack("q", token=token)
        except BaseException as error:
            errors.append(error)

    acknowledgment = Thread(target=acknowledge, daemon=True)
    acknowledgment.start()
    acknowledgment.join(timeout=3.0)
    competitor.join(timeout=3.0)

    assert not acknowledgment.is_alive()
    assert not competitor.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], QueueError)


def test_subscription_change_fences_prior_topic_token(mocker) -> None:
    backend = _backend()
    topic_a = "scrapy-a"
    topic_b = "scrapy-b"
    tp_a = TopicPartition(topic_a, 0)
    tp_b = TopicPartition(topic_b, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp_a, tp_b}
    consumer.poll.side_effect = [
        {tp_a: [_record(mocker, topic_a)]},
        {tp_b: [_record(mocker, topic_b)]},
    ]
    backend._consumer = consumer

    _, old_token = backend.pop_with_ack("a")
    _, current_token = backend.pop_with_ack("b")

    assert old_token != current_token
    backend.ack("a", token=old_token)
    consumer.commit.assert_not_called()
    backend.ack("b", token=current_token)
    consumer.commit.assert_called_once()


def test_token_nack_cannot_be_followed_by_legacy_bare_commit(mocker) -> None:
    backend = _backend()
    topic = "scrapy-q"
    tp = TopicPartition(topic, 0)
    consumer = mocker.MagicMock()
    consumer.assignment.return_value = {tp}
    consumer.poll.return_value = {tp: [_record(mocker, topic)]}
    backend._consumer = consumer
    _, token = backend.pop_with_ack("q")

    backend.nack("q", token=token)
    backend.ack("q")

    consumer.commit.assert_not_called()


# ---------------------------------------------------------------------------
# queue_len temp-consumer fallback (lines 777, 785-786)
# ---------------------------------------------------------------------------


def test_queue_len_returns_zero_when_topic_has_no_partitions(mocker) -> None:
    """Line 777: with no connected consumer, queue_len builds a temp consumer;
    if the topic has no partitions (doesn't exist) → 0 (eventual-consistency
    monitoring signal, not an error)."""
    backend = _backend()
    backend._consumer = None
    mock_temp = mocker.MagicMock()
    mock_temp.partitions_for_topic.return_value = None  # topic missing
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp
    )
    assert backend.queue_len("q") == 0


def test_queue_len_raises_on_temp_consumer_kafka_error(mocker) -> None:
    """R-kqlen: a KafkaError during the temp-consumer depth query (end_offsets /
    position) must raise QueueError, not return 0 — otherwise a broker outage
    looks like an empty queue and the scheduler drops the backpressure signal
    (parity with R-sqs-qlen #62 / R-es-qlen #65 / redis)."""
    backend = _backend()
    backend._consumer = None
    mock_temp = mocker.MagicMock()
    mock_temp.partitions_for_topic.return_value = {0}  # topic exists
    mock_temp.end_offsets.side_effect = KafkaError("end_offsets boom")
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp
    )
    with pytest.raises(QueueError) as exc_info:
        backend.queue_len("q")
    assert exc_info.value.operation == "queue_len"


def test_queue_len_preserves_terminal_error_when_failed_probe_close_interrupts(
    mocker,
) -> None:
    """A cleanup interrupt cannot replace the terminal failed-depth error."""
    backend = _backend()
    backend._consumer = None
    primary = KafkaError("end_offsets boom")
    close_interrupt = KeyboardInterrupt("temporary consumer cleanup")
    mock_temp = mocker.MagicMock()
    mock_temp.partitions_for_topic.return_value = {0}
    mock_temp.end_offsets.side_effect = primary
    mock_temp.close.side_effect = close_interrupt
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp
    )

    with pytest.raises(QueueError) as raised:
        backend.queue_len("q")

    assert str(raised.value) == "Failed to inspect Kafka queue."
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert raised.value.operation == "queue_len"
    mock_temp.close.assert_called_once_with()


def test_queue_len_propagates_close_interrupt_after_successful_probe(mocker) -> None:
    """A close interrupt remains observable when the probe itself succeeded."""
    backend = _backend()
    backend._consumer = None
    close_interrupt = KeyboardInterrupt("temporary consumer cleanup")
    mock_temp = mocker.MagicMock()
    mock_temp.partitions_for_topic.return_value = None
    mock_temp.close.side_effect = close_interrupt
    mocker.patch(
        "scrapy_extension.backends.kafka.KafkaConsumer", return_value=mock_temp
    )

    with pytest.raises(KeyboardInterrupt) as raised:
        backend.queue_len("q")

    assert raised.value is close_interrupt
    mock_temp.close.assert_called_once_with()


# ---------------------------------------------------------------------------
# Niche edge cases (initiative #33 — completing kafka to 100%)
# ---------------------------------------------------------------------------


def test_build_client_security_config_confluent_without_keys_fails_closed(
    mocker,
) -> None:
    """A mutated half-configured Confluent backend cannot fall back to plaintext."""
    backend = _backend()
    backend.config.mode = KafkaMode.CONFLUENT  # confluent keys stay None (default)
    common_config = mocker.patch.object(
        backend, "_build_common_config", return_value={"security_protocol": "PLAINTEXT"}
    )

    with pytest.raises(ConfigurationError) as exc_info:
        backend._build_client_security_config()

    assert exc_info.value.setting_name == "confluent_api_key"
    common_config.assert_not_called()


def test_pop_with_ack_returns_none_tuple_when_queue_empty(mocker) -> None:
    """Line 517: pop_with_ack on an empty queue (``_poll_record`` returned
    None) returns ``(None, None)`` — the empty-queue sentinel before any
    token / in-flight bookkeeping."""
    backend = _backend()
    mocker.patch.object(backend, "_poll_record", return_value=None)
    assert backend.pop_with_ack("q", 0.0) == (None, None)


def test_poll_record_returns_none_when_poll_yields_empty_records(mocker) -> None:
    """Line 584->583: when ``consumer.poll`` returns a non-empty messages
    dict whose record list IS empty (a TP key with no records — common
    during long-poll idle), the inner loop doesn't iterate and _poll_record
    falls through to return None rather than mishandling the empty batch."""
    backend = _backend()
    backend._consumer = mocker.MagicMock()
    # Non-empty dict (one TopicPartition) but an EMPTY record list:
    backend._consumer.poll.return_value = {mocker.MagicMock(name="tp"): []}
    assert backend._poll_record("q", 0.0) is None
