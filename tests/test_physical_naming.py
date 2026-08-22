"""Physical broker destination compatibility vectors."""

from __future__ import annotations

import re

import pytest

from scrapy_extension.backends.physical_naming import kafka_physical_topic_name
from scrapy_extension.settings import KafkaTopicNameGeneration


def test_kafka_auto_preserves_safe_legacy_topic() -> None:
    assert (
        kafka_physical_topic_name("scrapy-", "queue_1", KafkaTopicNameGeneration.AUTO)
        == "scrapy-queue_1"
    )


@pytest.mark.parametrize("logical", ["spider:queue", "café", "x" * 400])
def test_kafka_auto_hashes_names_that_are_not_safe_legacy_topics(logical: str) -> None:
    topic = kafka_physical_topic_name("scrapy-", logical, KafkaTopicNameGeneration.AUTO)
    assert re.fullmatch(r"scrapyext-v2-[a-f0-9]{40}", topic)
    assert len(topic.encode("utf-8")) <= 249


def test_kafka_v2_is_tuple_unambiguous_and_does_not_dual_read() -> None:
    left = kafka_physical_topic_name("a", "bc", KafkaTopicNameGeneration.V2)
    right = kafka_physical_topic_name("ab", "c", KafkaTopicNameGeneration.V2)
    assert left != right
    assert kafka_physical_topic_name("scrapy-", "q", KafkaTopicNameGeneration.V2) != (
        "scrapy-q"
    )


def test_kafka_legacy_rejects_unrepresentable_name() -> None:
    with pytest.raises(ValueError, match="legacy"):
        kafka_physical_topic_name(
            "scrapy-", "spider:queue", KafkaTopicNameGeneration.LEGACY_V1
        )


def test_kafka_v2_rejects_non_string_identity_parts_before_hashing() -> None:
    with pytest.raises(ValueError, match="inputs must be strings"):
        kafka_physical_topic_name("scrapy-", 123, KafkaTopicNameGeneration.V2)


def test_legacy_topic_validator_rejects_non_string_input() -> None:
    from scrapy_extension.backends.kafka import _validate_topic_name

    with pytest.raises(ValueError, match="Invalid topic/queue name") as raised:
        _validate_topic_name(123)  # type: ignore[arg-type]
    assert "123" not in str(raised.value)


def test_legacy_topic_validator_does_not_echo_rejected_identity() -> None:
    from scrapy_extension.backends.kafka import _validate_topic_name

    marker = "secret-password"
    with pytest.raises(ValueError) as raised:
        _validate_topic_name(marker + "!")
    assert marker not in str(raised.value)


def test_kafka_mapping_rejects_unknown_generation_without_fallback_collision() -> None:
    with pytest.raises(ValueError, match="Unsupported Kafka topic-name generation"):
        kafka_physical_topic_name("scrapy-", "queue", "future")

    # The legacy concatenation collision is not allowed to reappear in v2.
    assert kafka_physical_topic_name("a", "bc", KafkaTopicNameGeneration.V2) != (
        kafka_physical_topic_name("ab", "c", KafkaTopicNameGeneration.V2)
    )
