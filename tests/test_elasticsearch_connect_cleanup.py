"""U4 — ElasticSearch ``connect()`` discards a half-initialized client on any failure.

Parity with mongodb/kafka: an unexpected exception (or Ctrl-C) raised AFTER
``self._client = Elasticsearch(...)`` must still call ``_discard_client()`` so
``is_connected()`` cannot lie True and the ES transport is not leaked.
"""
from __future__ import annotations

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
