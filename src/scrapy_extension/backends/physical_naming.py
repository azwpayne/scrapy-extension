"""Stable logical-to-physical names for broker destinations."""

from __future__ import annotations

import hashlib
import re

from scrapy_extension.settings.kafka import KafkaTopicNameGeneration

_KAFKA_TOPIC_PATTERN = re.compile(r"[A-Za-z0-9._-]+\Z")
_KAFKA_MAX_TOPIC_BYTES = 249
_KAFKA_V2_PREFIX = "scrapyext-v2-"
_KAFKA_V2_DOMAIN = b"scrapy-extension:kafka:physical-topic:v2\x00"


def _length_prefixed_digest(domain: bytes, *parts: str, digest_size: int) -> str:
    """Hash a domain-separated tuple without concatenation collisions."""
    digest = hashlib.blake2s(digest_size=digest_size)
    digest.update(domain)
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _is_valid_kafka_topic(name: str) -> bool:
    """Return whether ``name`` is accepted by Kafka's topic-name contract."""
    encoded = name.encode("utf-8")
    return (
        1 <= len(encoded) <= _KAFKA_MAX_TOPIC_BYTES
        and name not in {".", ".."}
        and _KAFKA_TOPIC_PATTERN.fullmatch(name) is not None
    )


def kafka_physical_topic_name(
    prefix: str,
    logical_queue_name: str,
    generation: KafkaTopicNameGeneration,
) -> str:
    """Resolve one logical queue to exactly one Kafka topic.

    ``AUTO`` never dual-reads: safe historical names use the legacy mapping and
    every other identity uses v2. ``LEGACY_V1`` deliberately fails for a name
    that could not have existed under the old direct mapping, making migrations
    explicit rather than silently creating a second queue.
    """
    if type(prefix) is not str or type(logical_queue_name) is not str:
        raise ValueError("Kafka physical topic inputs must be strings.")
    candidate = f"{prefix}{logical_queue_name}"
    if generation is KafkaTopicNameGeneration.LEGACY_V1:
        if not _is_valid_kafka_topic(candidate):
            raise ValueError(
                "Kafka legacy topic mapping cannot represent this logical queue."
            )
        return candidate
    if generation is KafkaTopicNameGeneration.AUTO and _is_valid_kafka_topic(candidate):
        return candidate
    if generation not in (KafkaTopicNameGeneration.AUTO, KafkaTopicNameGeneration.V2):
        raise ValueError("Unsupported Kafka topic-name generation.")
    return f"{_KAFKA_V2_PREFIX}{_length_prefixed_digest(_KAFKA_V2_DOMAIN, prefix, logical_queue_name, digest_size=20)}"


__all__ = ["kafka_physical_topic_name"]
