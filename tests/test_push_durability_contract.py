"""U1 — pin ``_push_is_durable`` on the 7 real durable queue backends.

The distributed-dedup value proposition depends on the scheduler seeing a durable
push so it publishes a persistent cross-worker dedup marker (rather than a
process-local shadow) on the enqueue path. Each of these 7 backends sets
``_push_is_durable = True``; the production durability gate is
``self._push_is_durable is True`` (base.py:590). This test pins that identity
check on each real backend class so a regression (the ClassVar deleted or
flipped to False) fails the build instead of silently breaking cross-worker
dedup with zero signal — the same "mock the helper, not the real client"
false-green anti-pattern that bit R-es-qlen/#65 and R-kqlen/#68.

Only RabbitMQ overrides ``_push_with_durability`` (rabbitmq.py:935); it is
covered by tests/test_rabbitmq_backend.py and is intentionally excluded here.
The classification method itself is shared base code; the per-backend risk is
the flag, which this pins directly against the real classes.
"""
from __future__ import annotations

import pytest

from scrapy_extension.backends.base import QueueBackend
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.backends.kafka import KafkaBackend
from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.backends.pulsar import PulsarBackend
from scrapy_extension.backends.redis import RedisBackend
from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.backends.sqs import SqsBackend

DURABLE_BACKEND_CLASSES = [
  RedisBackend,
  MongoDBBackend,
  ElasticSearchBackend,
  KafkaBackend,
  PulsarBackend,
  RocketMQBackend,
  SqsBackend,
]


@pytest.mark.parametrize(
  "backend_cls", DURABLE_BACKEND_CLASSES, ids=lambda cls: cls.__name__
)
def test_real_durable_backend_pinned(backend_cls: type[QueueBackend]) -> None:
  # Production durability gate: `self._push_is_durable is True` (base.py:590).
  assert backend_cls._push_is_durable is True


def test_base_default_is_not_durable() -> None:
  """Discrimination proof: the QueueBackend base default is False, so only an
  explicit ``_push_is_durable = True`` declaration passes the pin above — a
  backend that dropped the ClassVar would inherit False and fail."""
  assert QueueBackend._push_is_durable is False
