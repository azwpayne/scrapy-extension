"""Resilience / contract tests for ElasticSearchBackend (initiative #31).

elasticsearch.py was 94.55% (8 uncovered lines + 4 partial branches),
below the 95% floor. Every gap was a real documented contract with no
direct test:

- connect() CLOUD-mode validation (lines 93-94): cloud mode without a
  ``cloud_id`` fails fast rather than constructing a broken client.
- ``_ensure_indices`` client-None TOCTOU (lines 112-113).
- ``disconnect`` idempotency when never connected (line 124->exit).
- ``client`` property post-connect guard (lines 169-170): if connect()
  returned without setting ``_client`` (e.g. a future refactor), the
  property raises rather than returning None.
- ``queue_len`` TransportError -> QueueError (lines 268-269, raise-on-
  failure so the caller sees the broker error).
"""

from __future__ import annotations

import pytest
from elasticsearch import TransportError

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings import ElasticSearchMode, ElasticSearchSettings


def _backend() -> ElasticSearchBackend:
  """Constructed-but-not-connected backend (_client is None)."""
  return ElasticSearchBackend(ElasticSearchSettings())


# ---------------------------------------------------------------------------
# connect() CLOUD-mode validation (lines 93-94)
# ---------------------------------------------------------------------------


def test_connect_cloud_mode_requires_cloud_id() -> None:
  """Lines 93-94 (defense-in-depth): connect() re-checks the CLOUD-mode
  cloud_id requirement even though ElasticSearchSettings already validates
  it at construction — so a backend whose config is mutated post-construction
  (bypassing settings validation) still fails fast rather than building a
  broken client. Reached by constructing with cloud_id then clearing it."""
  backend = ElasticSearchBackend(
    ElasticSearchSettings(mode=ElasticSearchMode.CLOUD, cloud_id="dummy-cloud-id")
  )
  backend.config.cloud_id = None  # bypass settings validation
  with pytest.raises(BackendConnectionError, match="Cloud mode requires 'cloud_id'"):
    backend.connect()


# ---------------------------------------------------------------------------
# _ensure_indices client-None TOCTOU (lines 112-113)
# ---------------------------------------------------------------------------


def test_ensure_indices_raises_when_client_is_none() -> None:
  """Lines 112-113: ``_ensure_indices`` with no client (never connected /
  concurrent disconnect) raises BackendConnectionError rather than
  ``None.indices.exists()``."""
  backend = _backend()
  backend._client = None
  with pytest.raises(BackendConnectionError, match="client is None"):
    backend._ensure_indices()


# ---------------------------------------------------------------------------
# disconnect() idempotency (line 124->exit)
# ---------------------------------------------------------------------------


def test_disconnect_before_connect_is_a_silent_noop() -> None:
  """Line 124->exit (false branch): disconnect() with no client must not
  raise (no ``None.close()``) — idempotent teardown for callers that
  didn't connect."""
  backend = _backend()
  backend._client = None
  backend.disconnect()  # must not raise
  assert backend._client is None


# ---------------------------------------------------------------------------
# client property post-connect guard (lines 169-170)
# ---------------------------------------------------------------------------


def test_client_property_raises_when_connect_does_not_set_client(mocker) -> None:
  """Lines 169-170: the ``client`` property calls connect() when _client is
  None, then re-checks — if connect() returned WITHOUT setting _client
  (a future-regression scenario), the property raises rather than silently
  returning None to a caller that would then AttributeError on it."""
  backend = _backend()
  backend._client = None
  mocker.patch.object(backend, "connect")  # no-op: does NOT set _client
  with pytest.raises(BackendConnectionError, match="client is None after connect"):
    _ = backend.client


# ---------------------------------------------------------------------------
# queue_len TransportError -> QueueError (lines 268-269)
# ---------------------------------------------------------------------------


def test_queue_len_raises_queue_error_on_transport_error(mocker) -> None:
  """Lines 268-269: a TransportError during the queue-depth count surfaces
  as a QueueError tagged operation='queue_len' — raise-on-failure so the
  caller sees the broker error rather than a silent 0 (which would mask
  backpressure / monitoring signals)."""
  backend = _backend()
  backend._client = mocker.MagicMock()
  mocker.patch.object(backend, "_count", side_effect=TransportError("boom"))
  with pytest.raises(QueueError) as exc:
    backend.queue_len("valid-queue")
  assert exc.value.operation == "queue_len"
