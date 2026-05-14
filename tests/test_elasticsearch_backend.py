"""Tests for ElasticSearch backend."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from elasticsearch import NotFoundError, RequestError, TransportError

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.settings.elasticsearch import (
  ElasticSearchMode,
  ElasticSearchSettings,
)


def _mock_backend(mocker, **settings_kwargs):
  config = ElasticSearchSettings(**settings_kwargs)
  backend = ElasticSearchBackend(config)
  backend._client = mocker.MagicMock()
  return backend


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


class TestElasticSearchSettings:
  def test_defaults(self):
    s = ElasticSearchSettings()
    assert s.mode == ElasticSearchMode.STANDALONE
    assert s.hosts == ["http://localhost:9200"]
    assert s.queue_index == "scrapy_queue"
    assert s.api_key is None

  def test_custom_hosts(self):
    s = ElasticSearchSettings(hosts=["http://es1:9200"])
    assert s.hosts == ["http://es1:9200"]


class TestBackendType:
  def test_elasticsearch_value(self):
    assert BackendType.ELASTICSEARCH.value == "elasticsearch"


class TestConnection:
  def test_connect_standalone(self, mocker):
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()

    assert backend.is_connected()

  def test_connect_cloud(self, mocker):
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    backend = ElasticSearchBackend(
      ElasticSearchSettings(mode=ElasticSearchMode.CLOUD, cloud_id="test:abc")
    )
    backend.connect()

    assert backend.is_connected()

  def test_connect_cloud_missing_id(self):
    from scrapy_extension.exceptions import BackendConnectionError

    backend = ElasticSearchBackend(ElasticSearchSettings(mode=ElasticSearchMode.CLOUD))
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_disconnect(self, mocker):
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    backend.disconnect()

    assert backend._client is None
    mock_client.close.assert_called_once()

  def test_is_connected_false(self):
    backend = ElasticSearchBackend(ElasticSearchSettings())
    assert backend.is_connected() is False

  def test_backend_type(self, mocker):
    assert _mock_backend(mocker).backend_type == BackendType.ELASTICSEARCH


class TestQueue:
  def test_push(self, mocker):
    b = _mock_backend(mocker)
    b.push("q", b"data", priority=1.0)

    doc = b._client.index.call_args.kwargs["document"]
    assert doc["queue_name"] == "q"
    assert doc["priority"] == -1.0

  def test_pop_with_items(self, mocker):
    b = _mock_backend(mocker)
    b._client.search.return_value = {
      "hits": {"hits": [{"_id": "1", "_source": {"item": "aXRlbQ=="}}]}
    }

    assert b.pop("q") == b"item"
    b._client.delete.assert_called_once_with(index="scrapy_queue", id="1")

  def test_pop_empty(self, mocker):
    b = _mock_backend(mocker)
    b._client.search.return_value = {"hits": {"hits": []}}

    assert b.pop("q") is None
    b._client.delete.assert_not_called()

  def test_queue_len(self, mocker):
    b = _mock_backend(mocker)
    b._client.count.return_value = {"count": 5}
    assert b.queue_len("q") == 5

  def test_queue_len_error(self, mocker):
    b = _mock_backend(mocker)
    b._client.count.side_effect = TransportError("err")
    assert b.queue_len("q") == 0

  def test_clear_queue(self, mocker):
    b = _mock_backend(mocker)
    b.clear_queue("q")
    b._client.delete_by_query.assert_called_once()


class TestSet:
  def test_add_new(self, mocker):
    b = _mock_backend(mocker)
    assert b.add("s", b"item") is True
    assert b._client.index.call_args.kwargs["op_type"] == "create"

  def test_add_duplicate(self, mocker):
    b = _mock_backend(mocker)
    err = RequestError(
      "409", mocker.MagicMock(), {"error": "version_conflict_engine_exception"}
    )
    b._client.index.side_effect = err
    assert b.add("s", b"item") is False

  def test_remove(self, mocker):
    b = _mock_backend(mocker)
    assert b.remove("s", b"item") is True
    b._client.delete.assert_called_once()

  def test_remove_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.delete.side_effect = _make_not_found_error()
    assert b.remove("s", b"item") is False

  def test_contains(self, mocker):
    b = _mock_backend(mocker)
    b._client.exists.return_value = True
    assert b.contains("s", b"item") is True

  def test_set_len(self, mocker):
    b = _mock_backend(mocker)
    b._client.count.return_value = {"count": 3}
    assert b.set_len("s") == 3

  def test_clear_set(self, mocker):
    b = _mock_backend(mocker)
    b.clear_set("s")
    b._client.delete_by_query.assert_called_once()


class TestStorage:
  def test_store(self, mocker):
    b = _mock_backend(mocker)
    b.store("k", b"data")
    call = b._client.index.call_args.kwargs
    assert call["id"] == "k"
    assert "expireAt" not in call["document"]

  def test_store_with_ttl(self, mocker):
    b = _mock_backend(mocker)
    b.store("k", b"data", ttl=3600)
    assert "expireAt" in b._client.index.call_args.kwargs["document"]

  def test_retrieve(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.return_value = {"_source": {"data": "ZGF0YQ=="}}
    assert b.retrieve("k") == b"data"

  def test_retrieve_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.side_effect = _make_not_found_error()
    assert b.retrieve("k") is None

  def test_delete(self, mocker):
    b = _mock_backend(mocker)
    assert b.delete("k") is True
    b._client.delete.assert_called_once_with(index="scrapy_storage", id="k")

  def test_delete_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.delete.side_effect = _make_not_found_error()
    assert b.delete("k") is False

  def test_exists(self, mocker):
    b = _mock_backend(mocker)
    b._client.exists.return_value = True
    assert b.exists("k") is True

  def test_ttl_no_expire(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.return_value = {"_source": {}}
    assert b.ttl("k") is None

  def test_ttl_with_expire(self, mocker):
    b = _mock_backend(mocker)
    future = (datetime.now(tz=timezone.utc) + timedelta(seconds=3600)).isoformat()
    b._client.get.return_value = {"_source": {"expireAt": future}}
    assert 3500 < b.ttl("k") <= 3600

  def test_ttl_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.side_effect = _make_not_found_error()
    assert b.ttl("k") == -1

  def test_clear_storage(self, mocker):
    b = _mock_backend(mocker)
    b.clear_storage()
    assert b._client.delete_by_query.call_args.kwargs["query"] == {"match_all": {}}

  def test_clear_storage_prefix(self, mocker):
    b = _mock_backend(mocker)
    b.clear_storage(prefix="items:")
    assert b._client.delete_by_query.call_args.kwargs["query"] == {
      "prefix": {"key": "items:"}
    }


class TestValidation:
  def test_validate_key_name_empty_string(self):
    from scrapy_extension.backends.elasticsearch import _validate_key_name

    with pytest.raises(ValueError, match="Invalid name"):
      _validate_key_name("", "name")


class TestSet:
  def test_add_request_error_without_version_conflict(self, mocker):
    b = _mock_backend(mocker)
    err = RequestError("400", mocker.MagicMock(), {"error": "mapper_parsing_exception"})
    b._client.index.side_effect = err
    with pytest.raises(RequestError):
      b.add("s", b"item")

  def test_add_new(self, mocker):
    b = _mock_backend(mocker)
    assert b.add("s", b"item") is True
    assert b._client.index.call_args.kwargs["op_type"] == "create"

  def test_add_duplicate(self, mocker):
    b = _mock_backend(mocker)
    err = RequestError(
      "409", mocker.MagicMock(), {"error": "version_conflict_engine_exception"}
    )
    b._client.index.side_effect = err
    assert b.add("s", b"item") is False

  def test_remove(self, mocker):
    b = _mock_backend(mocker)
    assert b.remove("s", b"item") is True
    b._client.delete.assert_called_once()

  def test_remove_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.delete.side_effect = _make_not_found_error()
    assert b.remove("s", b"item") is False

  def test_contains(self, mocker):
    b = _mock_backend(mocker)
    b._client.exists.return_value = True
    assert b.contains("s", b"item") is True

  def test_set_len(self, mocker):
    b = _mock_backend(mocker)
    b._client.count.return_value = {"count": 3}
    assert b.set_len("s") == 3

  def test_clear_set(self, mocker):
    b = _mock_backend(mocker)
    b.clear_set("s")
    b._client.delete_by_query.assert_called_once()


class TestPing:
  def test_ping_connected(self, mocker):
    b = _mock_backend(mocker)
    b._client.ping.return_value = True
    assert b.ping() is True

  def test_ping_disconnected(self):
    backend = ElasticSearchBackend(ElasticSearchSettings())
    assert backend.ping() is False


class TestClientProperty:
  def test_client_auto_connect(self, mocker):
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=False)),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    _ = backend.client
    assert backend.is_connected()


class TestEnsureIndices:
  def test_ensure_indices_creates_missing_index(self, mocker):
    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(
        exists=mocker.MagicMock(side_effect=[False, False, False]),
        create=mocker.MagicMock(),
      ),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    backend = ElasticSearchBackend(ElasticSearchSettings())
    backend.connect()
    assert mock_client.indices.create.call_count == 3


class TestConnectionManager:
  def test_get_manager(self, mocker):
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_client = mocker.MagicMock(
      ping=mocker.MagicMock(return_value=True),
      indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
    )
    mocker.patch(
      "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=mock_client
    )

    manager = ConnectionManager.get_manager(BackendType.ELASTICSEARCH)
    assert isinstance(manager.get_queue_backend(), ElasticSearchBackend)
