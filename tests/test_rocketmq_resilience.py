"""Resilience tests for RocketMQBackend defensive branches (initiative #26).

Rewritten (#44) for the apache ``rocketmq-python-client`` 5.1.1 surface — the
prior file patched fictional import paths (``rocketmq.client.Producer``,
``rocketmq.consumer.SimpleConsumer``, ``rocketmq.endpoint.Endpoint``) that
matched no released client. These tests now stub the real top-level surface
(``rocketmq.Producer`` / ``rocketmq.SimpleConsumer`` / ``rocketmq.Message`` /
``rocketmq.ClientConfiguration`` / ``rocketmq.Credentials``) directly, mirroring
``test_rocketmq_backend._patch_rocketmq``.

Pins the two failure-detection contracts in ``connect()`` and the three
TOCTOU race guards in the push/receive paths — every branch here is a real
load-bearing guard:

- ``Producer(...)`` / ``SimpleConsumer(...)`` returning ``None`` (a client-lib
  contract violation) fails fast with ``BackendConnectionError`` rather than
  dispatching ``.startup()`` / ``.receive()`` on ``None``.
- TOCTOU race guard — ``is_connected()`` passed, but the client became ``None``
  before use (concurrent disconnect under ``CONCURRENT_REQUESTS > 1``). The
  guard raises a clean ``QueueError`` rather than ``AttributeError`` on
  ``None.send()`` / ``None.receive()``.
- ``_ensure_subscribed`` is a no-op when the consumer is ``None`` — defensive
  against being called outside the ``is_connected`` gate.
"""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import RocketMQSettings


def _patch_rocketmq(mocker, *, producer=None, consumer=None) -> None:
    """Install a stub of the apache 5.1.1 top-level client surface.

    ``producer`` / ``consumer`` are what the mock ``Producer`` / ``SimpleConsumer``
    constructors RETURN — pass ``None`` to exercise the constructor-returned-None
    fail-fast; pass a ``MagicMock`` (or leave default) to connect cleanly.
    """
    rocketmq_module = ModuleType("rocketmq")
    mock_producer_cls = MagicMock()
    mock_consumer_cls = MagicMock()
    rocketmq_module.Producer = mock_producer_cls
    rocketmq_module.SimpleConsumer = mock_consumer_cls
    rocketmq_module.Message = MagicMock()
    rocketmq_module.ClientConfiguration = MagicMock()
    rocketmq_module.Credentials = MagicMock()
    mocker.patch.dict(sys.modules, {"rocketmq": rocketmq_module})
    mock_producer_cls.return_value = producer
    mock_consumer_cls.return_value = consumer


def _connected_backend(mocker) -> RocketMQBackend:
    """A backend whose connect() succeeded (producer + consumer both started)."""
    _patch_rocketmq(mocker, producer=MagicMock(), consumer=MagicMock())
    backend = RocketMQBackend(RocketMQSettings())
    backend.connect()
    return backend


# ---------------------------------------------------------------------------
# connect() — constructor-returned-None fail-fast
# ---------------------------------------------------------------------------


def test_connect_raises_when_producer_constructor_returns_none(mocker) -> None:
    """``Producer(...)`` returning ``None`` (client-lib contract violation) fails
    fast with ``BackendConnectionError`` rather than dispatching ``.startup()`` on
    ``None``."""
    _patch_rocketmq(mocker, producer=None, consumer=MagicMock())
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(BackendConnectionError) as exc:
        backend.connect()
    assert "producer initialization returned None" in str(exc.value)


def test_connect_raises_when_consumer_constructor_returns_none(mocker) -> None:
    """``SimpleConsumer(...)`` returning ``None`` fails fast (producer
    construction must succeed first)."""
    _patch_rocketmq(mocker, producer=MagicMock(), consumer=None)
    backend = RocketMQBackend(RocketMQSettings())
    with pytest.raises(BackendConnectionError) as exc:
        backend.connect()
    assert "consumer initialization returned None" in str(exc.value)


# ---------------------------------------------------------------------------
# TOCTOU race guards — client became None between is_connected() and use
# ---------------------------------------------------------------------------


def test_push_raises_when_producer_becomes_none_after_connect_check(mocker) -> None:
    """``is_connected()`` passed, but the producer became ``None`` before
    ``send()`` (concurrent disconnect). push must raise a clean ``QueueError``
    rather than ``AttributeError`` on ``None.send()``."""
    backend = _connected_backend(mocker)
    backend._producer = None  # simulate the race window after is_connected() passed
    mocker.patch.object(backend, "is_connected", return_value=True)
    with pytest.raises(QueueError, match="producer is None"):
        backend.push("q", b"x")


def test_receive_raises_when_consumer_becomes_none_after_connect_check(mocker) -> None:
    """``is_connected()`` passed, but the consumer became ``None`` before
    ``receive()``. ``_receive_message`` must raise a clean ``QueueError``. Also
    covers the ``_ensure_subscribed`` no-op-when-consumer-None branch (called
    just before the consumer-None guard)."""
    backend = _connected_backend(mocker)
    backend._consumer = None
    mocker.patch.object(backend, "is_connected", return_value=True)
    with pytest.raises(QueueError, match="consumer is None"):
        backend._receive_message("q", 0.0)


def test_subscribe_in_flight_reconnect_does_not_poison_subscribed_topics(
    mocker,
) -> None:
    """A disconnect completing while ``subscribe()`` is in flight (R132 Finding A)
    must not record the topic into the new generation's cleared set — that would
    permanently skip subscribe on the live consumer. ``_receive_message`` must
    raise a clean retryable ``QueueError`` and leave ``_subscribed_topics``
    empty."""
    backend = _connected_backend(mocker)
    consumer = backend._consumer

    def _subscribe_then_disconnect(topic_name):
        backend.disconnect()
        return None

    consumer.subscribe.side_effect = _subscribe_then_disconnect
    with pytest.raises(QueueError, match="reconnected"):
        backend._receive_message("q", 0.0)
    assert backend._subscribed_topics == set()


def test_subscribe_records_topic_when_generation_stable(mocker) -> None:
    """Happy path: when no reconnect races the subscribe, the topic IS recorded
    in ``_subscribed_topics`` and the receive proceeds without error."""
    backend = _connected_backend(mocker)
    backend._consumer.receive.return_value = []
    backend._receive_message("q", 0.0)
    assert backend._get_topic_name("q") in backend._subscribed_topics
    backend._consumer.subscribe.assert_called_once_with(backend._get_topic_name("q"))
