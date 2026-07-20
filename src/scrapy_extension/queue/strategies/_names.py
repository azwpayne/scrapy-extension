"""Portable physical queue names for strategies that fan out one logical queue."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from scrapy_extension.exceptions import ConfigurationError

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager


# These bundled backends accepted the published colon-delimited physical names
# and must keep using them so an upgrade sees existing backlog in place. Strict
# topic/queue-name backends never had valid queues under those names.
_LEGACY_COLON_QUEUE_BACKENDS = frozenset(
  {"elasticsearch", "mongodb", "pulsar", "rabbitmq", "redis"}
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


def physical_strategy_queue_name(
  connection_manager: ConnectionManager,
  *,
  queue_name: str,
  namespace: str,
  discriminator: str,
  legacy_name: str,
) -> str:
  """Select one backlog-compatible physical name for the active backend.

  Backends that accepted the package's published colon-delimited names keep
  using them for both reads and writes, so upgrades see the existing backlog
  without permanent dual queues or doubled RPCs. Strict-name backends use the
  portable hash. RabbitMQ is the one compatible backend with a relevant hard
  name limit; a legacy name over 255 UTF-8 bytes could never have existed, so
  the portable name is safe there.
  """
  backend_type = _backend_type_name(connection_manager)
  legacy_supported = backend_type in _LEGACY_COLON_QUEUE_BACKENDS
  rabbit_name_fits = backend_type != "rabbitmq" or len(legacy_name.encode()) <= 255
  if legacy_supported and rabbit_name_fits:
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
  return f"scrapyext-{namespace}-{digest.hexdigest()}"
