"""Resilience tests for RocketMQBackend defensive branches (initiative #26).

Pins the two failure-detection contracts in ``connect()`` and the three
TOCTOU race guards in the push/receive paths — rocketmq.py was 93.53%,
below the 95% floor. Every branch here is a real load-bearing guard:

- Lines 112-113 / 124-125: ``Producer(...)`` / ``SimpleConsumer(...)``
  returning ``None`` (a client-lib contract violation) fails fast with
  ``BackendConnectionError`` rather than dispatching ``.start()`` /
  ``.receive()`` on ``None``.
- Lines 240-241 / 280-281: TOCTOU race guard — ``is_connected()`` passed,
  but the client became ``None`` before use (concurrent disconnect under
  ``CONCURRENT_REQUESTS > 1``). The guard raises a clean ``QueueError``
  rather than ``AttributeError`` on ``None.send()`` / ``None.receive()``.
- Line 211 (false branch): ``_ensure_subscribed`` is a no-op when the
  consumer is ``None`` — defensive against being called outside the
  ``is_connected`` gate.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.rocketmq import RocketMQBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import RocketMQSettings


def _patch_rocketmq(mocker, *, producer=None, consumer=None) -> None:
  """Patch ``builtins.__import__`` so the rocketmq client sub-modules resolve
  to mocks. ``producer`` / ``consumer`` are what the mock ``Producer`` /
  ``SimpleConsumer`` constructors RETURN — pass ``None`` to exercise the
  constructor-returned-None fail-fast; pass a ``MagicMock`` to connect cleanly.
  Mirrors the import-patching in ``test_rocketmq_backend._make_connected_backend``.
  """
  mock_producer_cls = mocker.MagicMock()
  mock_consumer_cls = mocker.MagicMock()
  mock_producer_cls.return_value = producer
  mock_consumer_cls.return_value = consumer
  import_modules = {
    "rocketmq.auth.credentials": {"PlainCredentials": mocker.MagicMock()},
    "rocketmq.client": {"Producer": mock_producer_cls, "PushConsumer": mocker.MagicMock()},
    "rocketmq.consumer": {"SimpleConsumer": mock_consumer_cls},
    "rocketmq.endpoint": {"Endpoint": mocker.MagicMock()},
    "rocketmq.message": {"Message": mocker.MagicMock()},
  }
  real_import = (
    __builtins__["__import__"]  # type: ignore[index]
    if isinstance(__builtins__, dict)
    else __builtins__.__import__
  )

  def _import_side_effect(name, *args, **kwargs):
    if name in import_modules:
      mod = MagicMock()
      for attr, val in import_modules[name].items():
        setattr(mod, attr, val)
      return mod
    return real_import(name, *args, **kwargs)

  mocker.patch("builtins.__import__", side_effect=_import_side_effect)


def _connected_backend(mocker) -> RocketMQBackend:
  """A backend whose connect() succeeded (producer + consumer both started)."""
  _patch_rocketmq(mocker, producer=MagicMock(), consumer=MagicMock())
  backend = RocketMQBackend(RocketMQSettings())
  backend.connect()
  return backend


# ---------------------------------------------------------------------------
# connect() — constructor-returned-None fail-fast (lines 112-113, 124-125)
# ---------------------------------------------------------------------------


def test_connect_raises_when_producer_constructor_returns_none(mocker) -> None:
  """Lines 112-113: ``Producer(...)`` returning ``None`` (client-lib contract
  violation) fails fast with ``BackendConnectionError`` rather than
  dispatching ``.start()`` on ``None``."""
  _patch_rocketmq(mocker, producer=None, consumer=MagicMock())
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(BackendConnectionError) as exc:
    backend.connect()
  assert "producer initialization returned None" in str(exc.value)


def test_connect_raises_when_consumer_constructor_returns_none(mocker) -> None:
  """Lines 124-125: ``SimpleConsumer(...)`` returning ``None`` fails fast
  (producer construction must succeed first)."""
  _patch_rocketmq(mocker, producer=MagicMock(), consumer=None)
  backend = RocketMQBackend(RocketMQSettings())
  with pytest.raises(BackendConnectionError) as exc:
    backend.connect()
  assert "consumer initialization returned None" in str(exc.value)


# ---------------------------------------------------------------------------
# TOCTOU race guards — client became None between is_connected() and use
# (lines 211, 240-241, 280-281)
# ---------------------------------------------------------------------------


def test_push_raises_when_producer_becomes_none_after_connect_check(mocker) -> None:
  """Lines 240-241: ``is_connected()`` passed, but the producer became
  ``None`` before ``send()`` (concurrent disconnect). push must raise a clean
  ``QueueError`` rather than ``AttributeError`` on ``None.send()``."""
  backend = _connected_backend(mocker)
  backend._producer = None  # simulate the race window after is_connected() passed
  mocker.patch.object(backend, "is_connected", return_value=True)
  with pytest.raises(QueueError, match="producer is None"):
    backend.push("q", b"x")


def test_receive_raises_when_consumer_becomes_none_after_connect_check(mocker) -> None:
  """Lines 280-281 (+ 211 false branch): ``is_connected()`` passed, but the
  consumer became ``None`` before ``receive()``. ``_receive_message`` must
  raise a clean ``QueueError``. Also covers line 211 — ``_ensure_subscribed``
  (called just before the consumer-None guard) is a no-op when the consumer
  is ``None``, so the guard at 280-281 is what surfaces the failure."""
  backend = _connected_backend(mocker)
  backend._consumer = None
  mocker.patch.object(backend, "is_connected", return_value=True)
  with pytest.raises(QueueError, match="consumer is None"):
    backend._receive_message("q", 0.0)
