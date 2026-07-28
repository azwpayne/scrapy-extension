"""U4 — ElasticSearch ``connect()`` discards a half-initialized client on any failure.

Parity with mongodb/kafka: an unexpected exception (or Ctrl-C) while a client
candidate is being initialized must close that candidate, so
``is_connected()`` cannot lie True and the ES transport is not leaked.
"""
from __future__ import annotations

import threading

import pytest

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions.base import BackendConnectionError
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings


def _patch_connected_client(mocker) -> None:
  """Patch Elasticsearch so connect() builds a (mock) client whose ping() passes."""
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch",
    return_value=mocker.MagicMock(ping=lambda: True),
  )


def test_connect_discards_client_on_unexpected_error(mocker) -> None:
  backend = ElasticSearchBackend(ElasticSearchSettings())
  _patch_connected_client(mocker)
  mocker.patch.object(
    ElasticSearchBackend, "_ensure_indices", side_effect=RuntimeError("boom")
  )
  with pytest.raises(BackendConnectionError):
    backend.connect()
  assert backend._client is None


def test_connect_discards_client_on_keyboard_interrupt(mocker) -> None:
  backend = ElasticSearchBackend(ElasticSearchSettings())
  _patch_connected_client(mocker)
  mocker.patch.object(
    ElasticSearchBackend, "_ensure_indices", side_effect=KeyboardInterrupt
  )
  with pytest.raises(KeyboardInterrupt):
    backend.connect()
  assert backend._client is None


def test_overlapping_connects_share_one_live_client_generation(mocker) -> None:
  """A second connect waits for and reuses the first candidate generation."""
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock()
  ping_started = threading.Event()
  allow_ping = threading.Event()

  def ping() -> bool:
    ping_started.set()
    assert allow_ping.wait(timeout=1)
    return True

  candidate.ping.side_effect = ping
  factory = mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=candidate
  )
  first = threading.Thread(target=backend.connect)
  second = threading.Thread(target=backend.connect)

  first.start()
  assert ping_started.wait(timeout=1)
  second.start()
  allow_ping.set()
  first.join(timeout=1)
  second.join(timeout=1)

  assert not first.is_alive()
  assert not second.is_alive()
  factory.assert_called_once()
  assert backend._client is candidate
  candidate.close.assert_not_called()


def test_disconnect_waits_for_connect_before_retiring_generation(mocker) -> None:
  """Disconnect cannot close or clear a private candidate during connect."""
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock()
  ping_started = threading.Event()
  allow_ping = threading.Event()
  disconnect_finished = threading.Event()

  def ping() -> bool:
    ping_started.set()
    assert allow_ping.wait(timeout=1)
    return True

  candidate.ping.side_effect = ping
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=candidate
  )
  connecting = threading.Thread(target=backend.connect)

  def disconnect() -> None:
    backend.disconnect()
    disconnect_finished.set()

  disconnecting = threading.Thread(target=disconnect)
  connecting.start()
  assert ping_started.wait(timeout=1)
  disconnecting.start()

  assert backend._client is None
  assert not disconnect_finished.wait(timeout=0.1)
  allow_ping.set()
  connecting.join(timeout=1)
  disconnecting.join(timeout=1)

  assert not connecting.is_alive()
  assert not disconnecting.is_alive()
  assert disconnect_finished.is_set()
  assert backend._client is None
  candidate.close.assert_called_once()
