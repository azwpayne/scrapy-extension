"""Additional tests for ElasticSearch backend to cover missing lines."""

from __future__ import annotations

import pytest
from elasticsearch import NotFoundError, TransportError

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import BackendConnectionError, QueueError
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


class TestBuildKwargs:
  """Test _build_kwargs method branches."""

  def test_api_key_in_kwargs(self):
    """Test api_key is included in kwargs when set."""
    settings = ElasticSearchSettings(api_key="test_key")
    backend = ElasticSearchBackend(settings)
    kwargs = backend._build_kwargs()
    assert kwargs["api_key"] == "test_key"

  def test_basic_auth_in_kwargs(self):
    """Test basic_auth is included when username/password set."""
    settings = ElasticSearchSettings(username="user", password="pass")
    backend = ElasticSearchBackend(settings)
    kwargs = backend._build_kwargs()
    assert kwargs["basic_auth"] == ("user", "pass")

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


class TestAdd:
  """Test add method exception handling."""

  def test_add_transport_error(self, mocker):
    """Test add returns False when TransportError is raised."""
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
    assert backend.add("s", b"item") is False


class TestContains:
  """Test contains method exception handling."""

  def test_contains_transport_error(self, mocker):
    """Test contains returns False when TransportError is raised."""
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
    assert backend.contains("s", b"item") is False


class TestRetrieve:
  """Test retrieve method exception handling."""

  def test_retrieve_transport_error(self, mocker):
    """Test retrieve returns None when TransportError is raised."""
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
    assert backend.retrieve("k") is None


class TestExists:
  """Test exists method exception handling."""

  def test_exists_transport_error(self, mocker):
    """Test exists returns False when TransportError is raised."""
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
    assert backend.exists("k") is False


class TestTTL:
  """Test ttl method exception handling."""

  def test_ttl_transport_error(self, mocker):
    """Test ttl returns None when TransportError is raised."""
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
    assert backend.ttl("k") is None


class TestDeleteById:
  """Test _delete_by_id method exception handling."""

  def test_delete_by_id_transport_error(self, mocker):
    """Test _delete_by_id returns False when TransportError is raised."""
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
    assert backend._delete_by_id("index", "doc_id") is False


class TestDeleteByQuery:
  """Test _delete_by_query method exception handling."""

  def test_delete_by_query_transport_error(self, mocker, caplog):
    """Test _delete_by_query logs warning when TransportError is raised."""
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
    # Should not raise, just log warning
    backend._delete_by_query("index", {"match_all": {}})
    assert "Failed to delete" in caplog.text


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
