"""U4 — ElasticSearch ``connect()`` discards a half-initialized client on any failure.

Parity with mongodb/kafka: an unexpected exception (or Ctrl-C) while a client
candidate is being initialized must close that candidate, so
``is_connected()`` cannot lie True and the ES transport is not leaked.
"""
from __future__ import annotations

import logging
import sys
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


def test_failed_connect_logs_after_candidate_cleanup_context_unwinds(mocker) -> None:
  """Candidate-close diagnostics cannot inherit the driver failure context."""

  class _ExceptionContextProbe(logging.Handler):
    def __init__(self) -> None:
      super().__init__()
      self.contexts: list[tuple[object, object, object]] = []

    def emit(self, _record: logging.LogRecord) -> None:
      self.contexts.append(sys.exc_info())

  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock(ping=lambda: True)
  candidate.close.side_effect = SystemExit("candidate close interrupted")
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=candidate
  )
  mocker.patch.object(
    ElasticSearchBackend, "_ensure_indices", side_effect=RuntimeError("startup failed")
  )
  logger = logging.getLogger("scrapy_extension.backends.elasticsearch")
  probe = _ExceptionContextProbe()
  previous_level = logger.level
  logger.setLevel(logging.DEBUG)
  logger.addHandler(probe)

  try:
    with pytest.raises(BackendConnectionError):
      backend.connect()
  finally:
    logger.removeHandler(probe)
    logger.setLevel(previous_level)

  candidate.close.assert_called_once_with()
  assert probe.contexts == [(None, None, None)]


def test_connect_discards_client_on_keyboard_interrupt(mocker) -> None:
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock(ping=lambda: True)
  candidate.close.side_effect = SystemExit("candidate close interrupted")
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=candidate
  )
  mocker.patch.object(
    ElasticSearchBackend, "_ensure_indices", side_effect=KeyboardInterrupt
  )
  with pytest.raises(KeyboardInterrupt):
    backend.connect()
  assert backend._client is None
  candidate.close.assert_called_once_with()


@pytest.mark.parametrize(
  "diagnostic_error",
  [
    RuntimeError("logger extension failed"),
    KeyboardInterrupt("logger interruption"),
    SystemExit("logger exit"),
  ],
)
def test_post_publish_logger_failure_preserves_live_candidate(
  mocker, diagnostic_error: BaseException
) -> None:
  """Post-publication diagnostics cannot roll back a healthy generation."""
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock(ping=lambda: True)
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=candidate
  )
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.logger.debug",
    side_effect=diagnostic_error,
  )

  backend.connect()

  assert backend._client is candidate
  assert backend._connection_snapshot is not None
  candidate.close.assert_not_called()


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


def test_disconnect_ignores_logger_control_error_after_ordinary_close_failure(
  mocker,
) -> None:
  """A diagnostic handler cannot turn best-effort disconnect into failure."""
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock()
  candidate.close.side_effect = RuntimeError("close failed")
  backend._client = candidate
  mocker.patch(
    "scrapy_extension.backends.elasticsearch.logger.debug",
    side_effect=KeyboardInterrupt("diagnostic interruption"),
  )

  backend.disconnect()

  assert backend._client is None
  assert backend._connection_snapshot is None
  candidate.close.assert_called_once()


def test_disconnect_propagates_direct_close_control_error_after_detach(mocker) -> None:
  """Direct close control flow remains visible after the client is detached."""
  backend = ElasticSearchBackend(ElasticSearchSettings())
  candidate = mocker.MagicMock()
  candidate.close.side_effect = KeyboardInterrupt("close interruption")
  backend._client = candidate
  logger = mocker.patch("scrapy_extension.backends.elasticsearch.logger.debug")

  with pytest.raises(KeyboardInterrupt, match="close interruption"):
    backend.disconnect()

  assert backend._client is None
  assert backend._connection_snapshot is None
  candidate.close.assert_called_once()
  logger.assert_not_called()
