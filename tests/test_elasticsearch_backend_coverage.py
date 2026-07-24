"""Additional tests for ElasticSearch backend to cover missing lines."""

from __future__ import annotations

import pytest
from elasticsearch import ApiError, NotFoundError, TransportError

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError, StorageError
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings


def _make_not_found_error() -> NotFoundError:
  """Create a properly typed NotFoundError for test mocks."""
  from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

  meta = ApiResponseMeta(
    status=404,
    http_version="1.1",
    headers=HttpHeaders(),
    duration=0.0,
    node=NodeConfig(
      "localhost",
      "http",
      9200,
      path_prefix="",
      headers=HttpHeaders(),
    ),
  )
  return NotFoundError("not_found", meta, {})


def _make_api_error() -> ApiError:
  """Create a non-NotFound, non-Conflict ApiError (e.g. auth/server/query fault)."""
  from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

  meta = ApiResponseMeta(
    status=500,
    http_version="1.1",
    headers=HttpHeaders(),
    duration=0.0,
    node=NodeConfig(
      "localhost",
      "http",
      9200,
      path_prefix="",
      headers=HttpHeaders(),
    ),
  )
  return ApiError("api_failure", meta, {})


class TestBuildKwargs:
  """Test _build_kwargs method branches."""

  def test_api_key_in_kwargs(self):
    """Test api_key is included in kwargs when set."""
    # https:// host — round-6 SEC-3 forbids credentials over cleartext http://.
    settings = ElasticSearchSettings(hosts=["https://localhost:9200"], api_key="test_key")
    backend = ElasticSearchBackend(settings)
    kwargs = backend._build_kwargs()
    assert kwargs["api_key"] == "test_key"  # _RedactedStr is a str subclass → value-equal

  def test_basic_auth_in_kwargs(self):
    """Test basic_auth is included when username/password set."""
    # https:// host — round-6 SEC-3 forbids credentials over cleartext http://.
    settings = ElasticSearchSettings(
      hosts=["https://localhost:9200"], username="user", password="pass"
    )
    backend = ElasticSearchBackend(settings)
    kwargs = backend._build_kwargs()
    assert kwargs["basic_auth"] == ("user", "pass")  # password redacted but str-value-equal

  def test_ca_certs_in_kwargs(self, mocker):
    """Test ca_certs is included when set."""
    settings = ElasticSearchSettings(ca_certs="/path/to/ca.crt")
    backend = ElasticSearchBackend(settings)
    # ca_certs is added in connect(), not _build_kwargs
    # Verify it doesn't cause issues
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )
    backend.connect()
    assert backend.is_connected()

  def test_verify_certs_false(self, mocker):
    """Test verify_certs=False is passed correctly."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )
    backend = ElasticSearchBackend(ElasticSearchSettings(verify_certs=False))
    backend.connect()
    assert backend.is_connected()


class TestConnect:
  """Test connect method exception handling."""

  def test_connect_rejects_false_ping_and_discards_client(self, mocker):
    """A false health probe must not leave a reusable half-connection."""
    mock_client = mocker.MagicMock()
    mock_client.ping.return_value = False
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    with pytest.raises(BackendConnectionError):
      backend.connect()

    assert backend._client is None
    mock_client.indices.create.assert_not_called()
    mock_client.close.assert_called_once()

  def test_connect_transport_error(self, mocker):
    """Test TransportError during connect raises BackendConnectionError."""
    mock_client = mocker.MagicMock()
    mock_client.ping.side_effect = TransportError("Connection failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert "elasticsearch" in str(exc_info.value).lower()
    assert "Connection failed" in str(exc_info.value)
    assert backend._client is None
    mock_client.close.assert_called_once()


class TestIsConnected:
  """Test is_connected exception handling."""

  def test_is_connected_transport_error(self, mocker):
    """Test is_connected returns False when TransportError is raised."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()

    # Now make ping raise TransportError
    mock_client.ping.side_effect = TransportError("Ping failed")
    assert backend.is_connected() is False

  def test_is_connected_api_error(self, mocker):
    """R20-A: is_connected/ping returns False on a non-TransportError ApiError.

    TransportError (transport-layer) and ApiError (HTTP-response hierarchy) are
    siblings, not parent/child — so an AuthenticationException /
    AuthorizationException / UnsupportedProductError raised by ping() escaped the
    ``except TransportError`` arm raw, past the bool-return contract. This is the
    health-probe analog of R19-A (which fixed pop()). Every other backend's ping
    uses a broad catch; ES was the sole narrow-catch outlier.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()

    mock_client.ping.side_effect = _make_api_error()
    assert backend.is_connected() is False
    assert backend.ping() is False

  def test_is_connected_none_client(self):
    """Test is_connected returns False when client is None."""
    backend = ElasticSearchBackend(ElasticSearchSettings())
    assert backend.is_connected() is False


class TestPop:
  """Test pop method exception handling."""

  def test_pop_not_found_error(self, mocker):
    """Test pop returns None when NotFoundError is raised."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.search.side_effect = _make_not_found_error()
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    assert backend.pop("q") is None

  def test_pop_transport_error(self, mocker):
    """Test pop raises QueueError when TransportError is raised."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.search.side_effect = TransportError("Search failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(QueueError) as exc_info:
      backend.pop("q")
    assert exc_info.value.queue_name == "q"
    assert exc_info.value.operation == "pop"

  def test_pop_api_error_wrapped_as_queue_error(self, mocker):
    """R19-A: a non-NotFound, non-Conflict ApiError subclass (auth/server/query
    fault) during pop() must surface as QueueError, not propagate raw.

    Every sibling ES hot-path catches (ApiError, TransportError); pop() was the
    lone outlier (caught only TransportError). An AuthenticationError /
    ServerError / RequestError raised by indices.refresh()/search()/delete()
    escaped raw past the QueueError contract the docstring promises, breaking
    caller error-handling (the queue contract is QueueError on operational
    failure). NotFoundError (-> None) and ConflictError (inner -> continue)
    are unaffected.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.search.side_effect = _make_api_error()
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(QueueError) as exc_info:
      backend.pop("q")
    assert exc_info.value.queue_name == "q"
    assert exc_info.value.operation == "pop"


class TestAdd:
  """Test add method exception handling."""

  def test_add_transport_error(self, mocker):
    """R-dupe-1 (option b): a transient TransportError during set add is wrapped
    as BackendConnectionError so BackendDupeFilter's graceful-degradation arm
    catches it (degrade to not-seen) instead of crashing the crawl. The raw
    TransportError is chained (``from e``) for diagnosis. Supersedes R31-A1's
    "must propagate" — but preserves R31-A1's core concern: add does NOT return
    False on error (no silent mis-treatment as duplicate); it raises a typed,
    catchable exception. The dupefilter degradation arm exists now and the
    "dead spider is worse than a duplicate fetch" philosophy wins.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.index.side_effect = TransportError("Index failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.add("s", b"item")
    assert exc_info.value.backend_type == "elasticsearch"
    assert isinstance(exc_info.value.__cause__, TransportError)  # raw error chained


class TestContains:
  """Test contains method exception handling."""

  def test_contains_transport_error(self, mocker):
    """R34-A1: TransportError on contains must propagate, NOT return False.

    Returning False conflated "not in set" with "couldn't check". The
    standard ``if not set.contains(fp): set.add(fp)`` pattern would
    produce duplicates during cluster instability.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.exists.side_effect = TransportError("Exists failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(BackendConnectionError, match="Exists failed") as exc_info:
      backend.contains("s", b"item")
    assert isinstance(exc_info.value.__cause__, TransportError)


class TestRetrieve:
  """Test retrieve method exception handling."""

  def test_retrieve_transport_error(self, mocker):
    """R32-A1: TransportError on retrieve must propagate, NOT return None.

    Returning None on TransportError conflated "key doesn't exist" with
    "couldn't reach the cluster". Callers writing ``if storage.retrieve(k)
    is None: create_new()`` would silently overwrite existing data during
    any network blip / cluster red — silent data loss. Only NotFoundError
    (HTTP 404) legitimately produces None.

    2026-07-11 (#30): retrieve now wraps TransportError as ``StorageError``
    (joining Mongo/Memcached/DynamoDB). R32-A1's *propagate, don't swallow*
    intent is preserved — StorageError propagates (with the TransportError
    chained as ``__cause__``); only the type changed. ``except BackendError``
    now catches ES storage failures uniformly.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.get.side_effect = TransportError("Get failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(StorageError, match="Get failed") as ei:
      backend.retrieve("k")
    assert isinstance(ei.value.__cause__, TransportError)


class TestExists:
  """Test exists method exception handling."""

  def test_exists_transport_error(self, mocker):
    """R33-A1: TransportError on exists must propagate, NOT return False.

    Same shape as the R32 retrieve bug: returning False on TransportError
    conflated "key doesn't exist" with "couldn't reach the cluster".
    Callers writing ``if not storage.exists(k): create_new()`` would
    silently overwrite existing data during network blips.

    R-esttl (2026-07-12): exists now uses ``get`` (not the cheap ``exists``
    HEAD) so it can lazy-reap expired docs. The TransportError is wrapped as
    ``StorageError`` (joining retrieve/delete/ttl). R33-A1's *propagate, don't
    swallow* intent is preserved — StorageError propagates with the raw
    TransportError chained as ``__cause__``; only the type + mock target
    changed (exists→get).
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.get.side_effect = TransportError("Exists failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(StorageError, match="Exists failed") as ei:
      backend.exists("k")
    assert isinstance(ei.value.__cause__, TransportError)


class TestTTL:
  """Test ttl method exception handling."""

  def test_ttl_transport_error(self, mocker):
    """R34-A1: TransportError on ttl must propagate, NOT return None.

    Returning None conflated "no TTL set" with "couldn't reach the
    cluster". Callers can't distinguish "key has no expiry" from
    "couldn't check" — the contract is None = no TTL, -1 = expired.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.get.side_effect = TransportError("Get failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(StorageError, match="Get failed") as exc_info:
      backend.ttl("k")
    assert exc_info.value.operation == "ttl"
    assert exc_info.value.key == "k"
    assert isinstance(exc_info.value.__cause__, TransportError)


class TestDeleteById:
  """Test _delete_by_id method exception handling."""

  def test_delete_by_id_transport_error(self, mocker):
    """R34-A1: TransportError on _delete_by_id must propagate, NOT return False.

    Returning False conflated "doc didn't exist" (NotFoundError) with
    "couldn't reach the cluster". The storage.delete contract says
    False = "didn't exist"; real errors propagate so the public method
    can surface them to callers.
    """
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.delete.side_effect = TransportError("Delete failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(TransportError, match="Delete failed"):
      backend._delete_by_id("index", "doc_id")


class TestDeleteByQuery:
  """Test _delete_by_query method exception handling."""

  def test_delete_by_query_transport_error(self, mocker):
    """The shared helper propagates so each public surface can type the error."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.delete_by_query.side_effect = TransportError("Delete failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(TransportError, match="Delete failed"):
      backend._delete_by_query("index", {"match_all": {}})


class TestPush:
  """Test push method exception handling."""

  def test_push_transport_error(self, mocker):
    """Test push raises QueueError when TransportError is raised."""
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(return_value=True),
        create=mocker.MagicMock(),
      ),
    )
    mock_client.index.side_effect = TransportError("Index failed")
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch",
      return_value=mock_client,
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    with pytest.raises(QueueError) as exc_info:
      backend.push("q", b"data")
    assert exc_info.value.queue_name == "q"
    assert exc_info.value.operation == "push"
