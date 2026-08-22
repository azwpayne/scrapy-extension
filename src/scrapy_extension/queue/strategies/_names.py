"""Portable physical queue names for strategies that fan out one logical queue."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING

from scrapy_extension.exceptions import ConfigurationError

if TYPE_CHECKING:
    from scrapy_extension.backends.connectors import ConnectionManager

logger = logging.getLogger(__name__)

# One-shot latch (V4-2): requesting ``legacy_v1`` on a backend outside the
# legacy-colon set is a no-op that keeps the v2 names; the advisory must not
# spam once per strategy build or per queue-name resolution.
_legacy_generation_noop_warned = False


# These bundled backends accepted the published colon-delimited physical names
# and must keep using them so an upgrade sees existing backlog in place. Strict
# topic/queue-name backends never had valid queues under those names.
_LEGACY_COLON_QUEUE_BACKENDS = frozenset(
    {"elasticsearch", "mongodb", "pulsar", "rabbitmq", "redis"}
)

# The old ``{logical}:{discriminator}`` layout is deliberately migration-only.
# A literal passthrough queue may itself be named ``jobs:p0`` or ``jobs:w1``;
# keeping the old layout as the default would therefore let a fan-out strategy
# consume another strategy's backlog.  ``legacy_v1`` remains an explicit read/
# write mode for a stopped drain, never an automatic dual-read fallback.
CURRENT_FANOUT_NAME_GENERATION = "v2"
LEGACY_FANOUT_NAME_GENERATION = "legacy_v1"
_FANOUT_NAME_GENERATIONS = frozenset(
    {CURRENT_FANOUT_NAME_GENERATION, LEGACY_FANOUT_NAME_GENERATION}
)

_UNSUPPORTED_FANOUT_BACKENDS = {
    "kafka": (
        "KafkaBackend owns one consumer subscription; scanning strategy-created "
        "topics repeatedly subscribes and rebalances, invalidating in-flight "
        "ack/nack isolation"
    ),
    "rocketmq": (
        "RocketMQ SimpleConsumer receives across its accumulated subscriptions "
        "and cannot isolate a pop to the strategy-requested physical topic"
    ),
}


def _backend_type_name(connection_manager: ConnectionManager) -> str | None:
    """Return the manager's normalized backend registry name when available."""
    raw = getattr(connection_manager, "backend_type", None)
    value = getattr(raw, "value", raw)
    return value if isinstance(value, str) else None


def ensure_fanout_backend_supported(
    connection_manager: ConnectionManager,
    *,
    strategy: str,
) -> None:
    """Reject backends whose single consumer cannot isolate physical queues."""
    backend_type = _backend_type_name(connection_manager)
    reason = _UNSUPPORTED_FANOUT_BACKENDS.get(backend_type or "")
    if reason is None:
        return
    raise ConfigurationError(
        f"Queue strategy {strategy!r} is incompatible with backend "
        f"{backend_type!r}: {reason}. Use 'passthrough' with this backend.",
        setting_name="SCRAPY_QUEUE_STRATEGY",
        setting_value=strategy,
    )


def _warn_legacy_generation_noop(backend_type: str | None) -> None:
    """Warn once that ``legacy_v1`` cannot restore colon names on a backend.

    Strict topic/queue-name backends (e.g. SQS) are outside the legacy-colon
    set and never hosted the colon-delimited names, so the drain knob is a
    no-op there. The latch keeps repeated resolution quiet; a logging failure
    must not break physical-name selection.
    """
    global _legacy_generation_noop_warned  # noqa: PLW0603
    if _legacy_generation_noop_warned:
        return
    _legacy_generation_noop_warned = True
    try:
        logger.warning(
            "SCRAPY_QUEUE_NAME_GENERATION='legacy_v1' has no effect on backend %r: "
            "strict topic/queue-name backends such as SQS are outside the "
            "legacy-colon set and never hosted the colon-delimited fan-out "
            "names, so the strategy keeps reading and writing the v2 digest "
            "names. Pre-flip hash names (scrapyext-<ns>-<blake2s16>, including "
            "scrapyext-worker-<digest> for colon-bearing identities) are "
            "unreachable under both knob values and must be located and "
            "drained manually.",
            backend_type,
        )
    except BaseException:  # noqa: BLE001 - name resolution must not fail on logging
        pass


def physical_strategy_queue_name(
    connection_manager: ConnectionManager,
    *,
    queue_name: str,
    namespace: str,
    discriminator: str,
    legacy_name: str,
    name_generation: str = CURRENT_FANOUT_NAME_GENERATION,
) -> str:
    """Return the physical name for one strategy sub-queue.

    ``v2`` is the safe default: it is a versioned, length-delimited digest of
    the complete ``(namespace, queue_name, discriminator)`` identity and can
    never alias a literal passthrough queue using the old colon layout.  The
    old name is available only when callers explicitly select ``legacy_v1``;
    that mode is for a quiescent, one-time backlog drain and does not dual-read.
    Backends on which the old colon name could not have existed always use v2,
    even when legacy mode is requested (warned once).  Colon-bearing
    discriminators keep the digest under ``legacy_v1`` too: they hashed away
    from the colon layout before the flip, so no drainable backlog is lost.
    """
    if (
        not isinstance(name_generation, str)
        or name_generation not in _FANOUT_NAME_GENERATIONS
    ):
        raise ValueError("name_generation must be 'v2' or 'legacy_v1'")
    backend_type = _backend_type_name(connection_manager)
    legacy_supported = backend_type in _LEGACY_COLON_QUEUE_BACKENDS
    rabbit_name_fits = backend_type != "rabbitmq" or len(legacy_name.encode()) <= 255
    if name_generation == LEGACY_FANOUT_NAME_GENERATION and not legacy_supported:
        _warn_legacy_generation_noop(backend_type)
    # Invariant (R138-F1): "{queue}:{discriminator}" is collision-free only
    # while the discriminator is colon-free — a colon there lets (q, "a:b")
    # and (q + ":a", "b") land on one physical queue. Queue names may still
    # carry colons: every would-be collision partner has a colon-bearing
    # discriminator and hashes away from all legacy names, even under
    # legacy_v1.
    discriminator_unambiguous = ":" not in discriminator
    if (
        name_generation == LEGACY_FANOUT_NAME_GENERATION
        and legacy_supported
        and rabbit_name_fits
        and discriminator_unambiguous
    ):
        return legacy_name
    return strategy_queue_name(
        queue_name,
        namespace=namespace,
        discriminator=discriminator,
    )


def strategy_queue_name(
    queue_name: str,
    *,
    namespace: str,
    discriminator: str,
) -> str:
    """Return a stable, backend-portable name for one strategy sub-queue.

    Kafka rejects the colon separator accepted by the package's generic key
    validator, while SQS limits queue names to 80 characters. Hashing a
    length-prefixed tuple keeps names short, prevents delimiter ambiguity, and
    isolates namespaces used by different strategies.
    """
    digest = hashlib.blake2s(digest_size=16)
    for part in (queue_name, namespace, discriminator):
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return f"scrapyext-v2-{namespace}-{digest.hexdigest()}"
