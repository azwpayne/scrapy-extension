"""Unit A (A1-A3): capability-aware ack-concurrency gate.

Round-2 hardening (C1 CRITICAL): the H commit removed the
``CONCURRENT_REQUESTS>1`` fail-fast guard for ALL ack-using backends,
but only Kafka/RabbitMQ got the real in-flight-set fix. SQS/Pulsar still
track a single ack slot, so under default ``CONCURRENT_REQUESTS=16`` they
silently lose 15/16 acks. The scheduler docstring over-claimed universal
correctness.

A1/A2 add a per-backend capability contract
(``QueueBackend.requires_ack`` / ``supports_concurrent_ack``); A3 makes
``BackendScheduler.from_settings`` raise ``ConfigurationError`` for
single-slot ack backends under ``CONCURRENT_REQUESTS>1`` unless the
explicit ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is set.

This test RED-first pins the gate. It does NOT instantiate real backends
(the manager is mocked); the gate resolves the backend class via
``_BACKEND_FACTORIES`` and reads the class-level capabilities.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, Mock

import pytest

# Pulsar-client (C++ binding) and boto3 (SQS) are NOT installed in the test
# env (only redis/pymongo/kafka-python-ng/pika/elasticsearch are — see
# pyproject [dependency-groups].test). Inject mocks so this file can import
# SqsBackend/PulsarBackend for the capability-class lookup WITHOUT relying
# on another test file (e.g. test_sqs_backend.py) having populated
# sys.modules first — that cross-file ordering dependency made the file fail
# when run standalone. Self-sufficient now.
sys.modules.setdefault("pulsar", MagicMock())
sys.modules.setdefault("boto3", MagicMock())


@pytest.fixture(scope="module", autouse=True)
def _cleanup_sys_modules_mocks():
  """Pop the module-level ``pulsar``/``boto3`` mocks after this module's tests.

  R14-G flake fix: module-top-level ``sys.modules.setdefault`` pollutes the
  session for later modules; pop at module teardown.
  """
  yield
  for key in ("pulsar", "boto3"):
    sys.modules.pop(key, None)

from scrapy_extension.backends.base import BackendType  # noqa: E402
from scrapy_extension.backends.connectors import ConnectionManager  # noqa: E402
from scrapy_extension.exceptions import ConfigurationError  # noqa: E402
from scrapy_extension.schedule.scheduler import BackendScheduler  # noqa: E402


def _make_settings(
  backend_type: str,
  *,
  concurrent: int,
  opt_out: bool = False,
) -> Mock:
  """Build a Scrapy-Settings-like mock resolving queue + concurrency + opt-out."""
  settings = Mock()
  backend_map = {
    "SCRAPY_BACKEND_TYPE": backend_type,
    "SCRAPY_QUEUE_KEY": "scheduler:queue",
    "SCRAPY_QUEUE_STRATEGY": "passthrough",
  }

  def get(key, default=None):
    if key == "CONCURRENT_REQUESTS":
      return concurrent
    if key == "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS":
      return opt_out
    return backend_map.get(key, default)

  def getint(key, default=0):
    if key == "CONCURRENT_REQUESTS":
      return concurrent
    return default

  settings.get.side_effect = get
  settings.getint.side_effect = getint
  settings.getfloat.return_value = 0.0
  settings.getdict.return_value = {}
  return settings


class TestAckCapabilityGate:
  """A3: scheduler.from_settings gates single-slot-ack backends under concurrency."""

  def test_sqs_with_concurrency_gt_1_passes(self, mocker) -> None:
    """SQS has a real in-flight set (round-3) -> concurrency-safe, no raise."""
    settings = _make_settings("sqs", concurrent=16)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_gate_fires_for_synthetic_single_slot_backend(self, mocker) -> None:
    """After round-3 every real backend is concurrency-safe, so the gate
    mechanism is covered by a synthetic single-slot stub — it must still
    fire for any future backend that declares requires_ack=True /
    supports_concurrent_ack=False."""

    class _SingleSlotStub:
      requires_ack = True
      supports_concurrent_ack = False

    mocker.patch(
      "scrapy_extension.backends.connectors._load_object",
      return_value=_SingleSlotStub,
    )
    settings = _make_settings("sqs", concurrent=16)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    with pytest.raises(ConfigurationError) as excinfo:
      BackendScheduler.from_settings(settings)
    assert "SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS" in str(excinfo.value)

  def test_gate_opt_out_for_synthetic_single_slot_backend(self, mocker) -> None:
    """The opt-out flag still disables the gate for a single-slot backend."""

    class _SingleSlotStub:
      requires_ack = True
      supports_concurrent_ack = False

    mocker.patch(
      "scrapy_extension.backends.connectors._load_object",
      return_value=_SingleSlotStub,
    )
    settings = _make_settings("sqs", concurrent=16, opt_out=True)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_pulsar_with_concurrency_gt_1_passes(self, mocker) -> None:
    """Pulsar has a real in-flight set (round-3) -> concurrency-safe, no raise."""
    settings = _make_settings("pulsar", concurrent=4)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_kafka_with_concurrency_gt_1_passes(self, mocker) -> None:
    """Kafka has a real in-flight set -> concurrency-safe, no raise."""
    settings = _make_settings("kafka", concurrent=16)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_rabbitmq_with_concurrency_gt_1_passes(self, mocker) -> None:
    """RabbitMQ has a real in-flight set -> concurrency-safe, no raise."""
    settings = _make_settings("rabbitmq", concurrent=8)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_redis_with_concurrency_gt_1_passes(self, mocker) -> None:
    """Redis (atomic pop, requires_ack=False) -> concurrency-safe, no raise."""
    settings = _make_settings("redis", concurrent=32)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_atomic_backend_with_concurrency_1_passes(self, mocker) -> None:
    """Any queue backend + CONCURRENT_REQUESTS=1 -> no gate applies."""
    settings = _make_settings("sqs", concurrent=1)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_sqs_with_concurrency_1_passes(self, mocker) -> None:
    """Single-slot ack is correct when CONCURRENT_REQUESTS=1."""
    settings = _make_settings("sqs", concurrent=1)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"

  def test_G11_synthetic_single_slot_with_concurrency_1_skips_gate(self, mocker) -> None:
    """G11: single-slot stub + CONCURRENT_REQUESTS=1 → early return, no raise.

    This is the ONLY combination reaching the ``if concurrent <= 1: return``
    arm (line 368-370): every real backend sets ``supports_concurrent_ack=True``
    (round-3 hardening) so they return at line 367 before the concurrency
    check. The synthetic stub (requires_ack=True, supports_concurrent_ack=False)
    + concurrent=1 reaches and exercises the early-return arm.
    """

    class _SingleSlotStub:
      requires_ack = True
      supports_concurrent_ack = False

    mocker.patch(
      "scrapy_extension.backends.connectors._load_object",
      return_value=_SingleSlotStub,
    )
    settings = _make_settings("sqs", concurrent=1)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    scheduler = BackendScheduler.from_settings(settings)  # must not raise
    assert scheduler.queue_key == "scheduler:queue"


class TestAckCapabilityDefaults:
  """A1: capability contract defaults on the QueueBackend ABC."""

  def test_queue_backend_defaults_require_no_ack(self) -> None:
    """QueueBackend.requires_ack default is False (atomic-pop backends)."""
    from scrapy_extension.backends.base import QueueBackend

    assert QueueBackend.requires_ack is False

  def test_queue_backend_defaults_support_concurrent_ack(self) -> None:
    """QueueBackend.supports_concurrent_ack default is True."""
    from scrapy_extension.backends.base import QueueBackend

    assert QueueBackend.supports_concurrent_ack is True


class TestAckCapabilityDeclarations:
  """A2: each backend declares its capability correctly."""

  def test_sqs_declared_concurrent_safe(self) -> None:
    """SQS: requires_ack=True, supports_concurrent_ack=True (real in-flight, round-3)."""
    from scrapy_extension.backends.sqs import SqsBackend

    assert SqsBackend.requires_ack is True
    assert SqsBackend.supports_concurrent_ack is True

  def test_pulsar_declared_concurrent_safe(self) -> None:
    """Pulsar: requires_ack=True, supports_concurrent_ack=True (real in-flight, round-3)."""
    from scrapy_extension.backends.pulsar import PulsarBackend

    assert PulsarBackend.requires_ack is True
    assert PulsarBackend.supports_concurrent_ack is True

  def test_kafka_declared_concurrent_safe(self) -> None:
    """Kafka: requires_ack=True, supports_concurrent_ack=True (real in-flight)."""
    from scrapy_extension.backends.kafka import KafkaBackend

    assert KafkaBackend.requires_ack is True
    assert KafkaBackend.supports_concurrent_ack is True

  def test_rabbitmq_declared_concurrent_safe(self) -> None:
    """RabbitMQ: requires_ack=True, supports_concurrent_ack=True (real in-flight)."""
    from scrapy_extension.backends.rabbitmq import RabbitMQBackend

    assert RabbitMQBackend.requires_ack is True
    assert RabbitMQBackend.supports_concurrent_ack is True

  def test_redis_atomic_default(self) -> None:
    """Redis: atomic pop -> requires_ack=False (default, untouched)."""
    from scrapy_extension.backends.redis import RedisBackend

    assert RedisBackend.requires_ack is False

  @pytest.mark.parametrize(
    "backend_type",
    [
      BackendType.REDIS,
      BackendType.MONGODB,
      BackendType.ELASTICSEARCH,
      BackendType.ROCKETMQ,
    ],
  )
  def test_atomic_backends_require_no_ack(self, backend_type: BackendType) -> None:
    """Atomic-pop backends (Redis/Mongo/ES/RocketMQ) keep requires_ack=False."""
    # Round-5 R5-1: dispatch routes through the registry descriptor table
    # (was the deleted ``_BACKEND_FACTORIES``).
    from scrapy_extension.backends.connectors import _load_object
    from scrapy_extension.backends.registry import get_descriptor

    descriptor = get_descriptor(backend_type.value)
    cls = _load_object(descriptor.backend_cls_path)
    assert getattr(cls, "requires_ack", False) is False, (
      f"{backend_type.value} should be atomic (requires_ack=False); "
      f"got {getattr(cls, 'requires_ack', 'MISSING')}"
    )
