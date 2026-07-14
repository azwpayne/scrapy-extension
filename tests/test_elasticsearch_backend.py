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

  def test_cloud_mode_missing_id_fails_at_construction(self):
    """R52: CLOUD mode without cloud_id fails at construction (fail-fast).

    Mirrors the Redis SENTINEL validator (R8). Previously this surfaced as a
    BackendConnectionError at connect() time; now it's a pydantic
    ValidationError at ElasticSearchSettings construction — closer to the
    misconfiguration.
    """
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="cloud_id"):
      ElasticSearchSettings(mode=ElasticSearchMode.CLOUD)

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
      "hits": {
        "hits": [
          {
            "_id": "1",
            "_seq_no": 42,
            "_primary_term": 1,
            "_source": {"item": "aXRlbQ=="},
          }
        ]
      }
    }

    assert b.pop("q") == b"item"
    # delete no longer passes refresh= — read-your-writes moved to a pre-search
    # indices.refresh (see #42 perf fix). The mock's indices.refresh auto-creates.
    b._client.delete.assert_called_once_with(
      index="scrapy_queue",
      id="1",
      if_seq_no=42,
      if_primary_term=1,
    )
    b._client.indices.refresh.assert_called_once_with(index="scrapy_queue")

  def test_pop_retries_on_conflict(self, mocker):
    """R1-P1-13: pop must retry the search-delete cycle on ConflictError.

    Concurrent workers may claim the same doc; optimistic locking via
    if_seq_no/if_primary_term makes the loser's delete fail with HTTP 409.
    The backend should retry to find the next available item.
    """
    from elasticsearch import ConflictError

    b = _mock_backend(mocker)
    # First search returns a doc that loses the race; second returns a winner.
    b._client.search.side_effect = [
      {
        "hits": {
          "hits": [
            {
              "_id": "1",
              "_seq_no": 10,
              "_primary_term": 1,
              "_source": {"item": "bG9zdA=="},
            }
          ]
        }
      },
      {
        "hits": {
          "hits": [
            {
              "_id": "2",
              "_seq_no": 20,
              "_primary_term": 1,
              "_source": {"item": "d29u"},
            }
          ]
        }
      },
    ]
    b._client.delete.side_effect = [
      ConflictError("conflict", 409, body={}),
      None,
    ]

    assert b.pop("q") == b"won"
    assert b._client.search.call_count == 2

  def test_pop_returns_none_when_all_attempts_lose_race(self, mocker):
    """R10: if all 3 optimistic-lock attempts lose the race (every delete
    conflicts), pop returns None — caller treats it as empty and polls again.

    Exactly-one-winner semantics without a distributed lock. This is the
    exhaustion tail of ``test_pop_retries_on_conflict`` (line 238): when no
    attempt wins within ``max_attempts``, the queue is treated as drained.
    """
    from elasticsearch import ConflictError

    b = _mock_backend(mocker)
    # Every search finds a doc; every delete loses the race (conflict).
    b._client.search.return_value = {
      "hits": {
        "hits": [
          {
            "_id": "1",
            "_seq_no": 10,
            "_primary_term": 1,
            "_source": {"item": "bG9zdA=="},
          }
        ]
      }
    }
    b._client.delete.side_effect = ConflictError("conflict", 409, body={})

    assert b.pop("q") is None
    # All 3 attempts tried (max_attempts); each searched then lost the race.
    assert b._client.search.call_count == 3

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
    """R-es-qlen: queue_len must raise QueueError on TransportError, not
    swallow to 0 (the real _client.count path).

    Pre-fix ``_count`` swallowed TransportError and returned 0, so queue_len's
    own ``except TransportError -> raise QueueError`` arm was dead code and
    queue_len returned 0 -- masking a backend failure from the scheduler's
    idle/backpressure gate (R-qlen parity with Redis + SQS).
    """
    from scrapy_extension.exceptions import QueueError

    b = _mock_backend(mocker)
    b._client.count.side_effect = TransportError("err")
    with pytest.raises(QueueError):
      b.queue_len("q")

  def test_set_len_error_returns_zero(self, mocker):
    """R-es-qlen: set_len returns 0 on TransportError (diagnostic, redis parity).

    set_len is a diagnostic count (not safety-critical for idle detection), so
    it returns 0 on error like Redis's ``set_len`` (``except RedisError: return
    0``). The asymmetry with queue_len (which raises QueueError) is intentional:
    the scheduler trusts ``len(queue)`` for idle/backpressure, but set_len is
    observational. After ``_count``'s swallow was removed, set_len owns its own
    except arm to preserve this contract.
    """
    b = _mock_backend(mocker)
    b._client.count.side_effect = TransportError("err")
    assert b.set_len("s") == 0

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

  def test_retrieve_expired_returns_none_and_reaps(self, mocker):
    """R-esttl: retrieve() must not serve stale data — an expired doc returns
    None (consistent with DynamoDB retrieve) AND is lazy-reaped so the index
    does not accumulate dead docs (ES has no native TTL; expiry is app-level
    via expireAt). Pre-fix retrieve returned the expired doc's data verbatim."""
    b = _mock_backend(mocker)
    past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
    b._client.get.return_value = {"_source": {"data": "ZGF0YQ==", "expireAt": past}}
    assert b.retrieve("k") is None
    b._client.delete.assert_called_once_with(index="scrapy_storage", id="k")

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
    b._client.get.return_value = {"_source": {"data": "ZGF0YQ=="}}
    assert b.exists("k") is True

  def test_exists_not_found(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.side_effect = _make_not_found_error()
    assert b.exists("k") is False

  def test_exists_expired_returns_false_and_reaps(self, mocker):
    """R-esttl: exists() respects TTL — an expired doc returns False (matches
    DynamoDB exists contract: 'present AND not expired') AND is reaped."""
    b = _mock_backend(mocker)
    past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
    b._client.get.return_value = {"_source": {"data": "ZGF0YQ==", "expireAt": past}}
    assert b.exists("k") is False
    b._client.delete.assert_called_once_with(index="scrapy_storage", id="k")

  def test_ttl_no_expire(self, mocker):
    b = _mock_backend(mocker)
    b._client.get.return_value = {"_source": {}}
    assert b.ttl("k") is None

  def test_ttl_with_expire(self, mocker):
    b = _mock_backend(mocker)
    future = (datetime.now(tz=timezone.utc) + timedelta(seconds=3600)).isoformat()
    b._client.get.return_value = {"_source": {"expireAt": future}}
    assert 3500 < b.ttl("k") <= 3600

  def test_ttl_expired_returns_negative_and_reaps(self, mocker):
    """R-esttl: ttl() reaps expired docs (storage hygiene) while preserving
    the R48 contract (-1 = expired, distinct from None = absent)."""
    b = _mock_backend(mocker)
    past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
    b._client.get.return_value = {"_source": {"expireAt": past}}
    assert b.ttl("k") == -1
    b._client.delete.assert_called_once_with(index="scrapy_storage", id="k")

  def test_ttl_not_found(self, mocker):
    """R48: a missing key returns None, not -1 (distinguish absent from expired).

    Pre-R48 this asserted ``== -1``, codifying the same absent/expired
    conflation that R5 fixed on Redis and MongoDB. ElasticSearch was missed
    in that sweep.
    """
    b = _mock_backend(mocker)
    b._client.get.side_effect = _make_not_found_error()
    assert b.ttl("k") is None

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

  def test_store_wraps_transport_error_as_storage_error(self, mocker):
    """#30: ES storage ops must join the StorageError family (Mongo/Memcached/
    DynamoDB already do) so callers' ``except BackendError`` catches ES storage
    failures uniformly instead of crashing on a raw TransportError.

    R-dupe-1 (option b) now wraps the SET ``add()`` op's TransportError as
    BackendConnectionError too (see test_add_transport_error_wrapped); this
    test covers the storage ``store`` op, which wraps as StorageError — the two
    ops raise different typed exceptions for their respective surfaces.
    """
    from elasticsearch import TransportError

    from scrapy_extension.exceptions import StorageError

    b = _mock_backend(mocker)
    b._client.index.side_effect = TransportError("connection refused")
    with pytest.raises(StorageError) as ei:
      b.store("k", b"data")
    assert ei.value.operation == "store"
    assert ei.value.key == "k"
    # The original TransportError is chained (not swallowed).
    assert isinstance(ei.value.__cause__, TransportError)

  def test_retrieve_wraps_transport_error_as_storage_error(self, mocker):
    from elasticsearch import TransportError

    from scrapy_extension.exceptions import StorageError

    b = _mock_backend(mocker)
    b._client.get.side_effect = TransportError("timeout")
    with pytest.raises(StorageError) as ei:
      b.retrieve("k")
    assert ei.value.operation == "retrieve"
    assert isinstance(ei.value.__cause__, TransportError)

  def test_delete_wraps_transport_error_as_storage_error(self, mocker):
    from elasticsearch import TransportError

    from scrapy_extension.exceptions import StorageError

    b = _mock_backend(mocker)
    b._client.delete.side_effect = TransportError("timeout")
    with pytest.raises(StorageError) as ei:
      b.delete("k")
    assert ei.value.operation == "delete"
    assert ei.value.key == "k"
    assert isinstance(ei.value.__cause__, TransportError)


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

  def test_add_duplicate_via_conflict_error(self, mocker):
    """R31-A1: modern ES client raises ConflictError on op_type=create + existing doc.

    The RequestError-with-string-match path is defensive legacy support.
    ConflictError is the canonical 8.x signal for HTTP 409 version conflict.
    """
    from elasticsearch import ConflictError

    b = _mock_backend(mocker)
    b._client.index.side_effect = ConflictError(
      "version conflict", mocker.MagicMock(), "body"
    )
    assert b.add("s", b"item") is False

  def test_add_transport_error_wrapped(self, mocker):
    """R-dupe-1 (option b): TransportError during set add is wrapped as
    BackendConnectionError so BackendDupeFilter's graceful-degradation arm
    catches it (degrade to not-seen) instead of crashing the crawl. The raw
    TransportError is chained (``from e``) for diagnosis.

    Supersedes R31-A1's "must propagate raw" — but preserves R31-A1's core
    concern: add does NOT return False on error. Previously the broad
    ``except TransportError: return False`` conflated any transport failure
    with "already existed" — the dupefilter's ``return not added`` then
    treated every backend error as a duplicate, silently dropping new
    requests during network blips / cluster red. It still raises a typed,
    catchable exception; only the type changed from raw ``TransportError``
    to ``BackendConnectionError`` so ``except BackendError`` (the dupefilter's
    degradation arm) catches it uniformly across backends.
    """
    from elasticsearch import TransportError

    from scrapy_extension.exceptions import BackendConnectionError

    b = _mock_backend(mocker)
    b._client.index.side_effect = TransportError("connection refused")
    with pytest.raises(BackendConnectionError) as exc_info:
      b.add("s", b"item")
    assert exc_info.value.backend_type == "elasticsearch"
    assert isinstance(exc_info.value.__cause__, TransportError)  # raw error chained


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


# ---------------------------------------------------------------------------
# SEC-1 (round-6): ElasticSearch api_key / password redaction in _build_kwargs.
# ---------------------------------------------------------------------------


def test_elasticsearch_api_key_redacted_in_kwargs_repr():
  """SEC-1: the api_key plumbed into Elasticsearch() kwargs is wrapped in
  _RedactedStr so ``repr(kwargs)`` doesn't leak it. Value preserved for auth.
  """
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

  config = ElasticSearchSettings(
    mode="standalone",
    hosts=["https://localhost:9200"],
    api_key="top-secret-es-api-key",
  )
  backend = ElasticSearchBackend(config)
  kwargs = backend._build_kwargs()

  key = kwargs["api_key"]
  assert str(key) == "top-secret-es-api-key"
  assert "top-secret-es-api-key" not in repr(kwargs)
  assert isinstance(key, _RedactedStr)


def test_elasticsearch_basic_auth_password_redacted_in_kwargs_repr():
  """SEC-1: the basic_auth password tuple element is wrapped in _RedactedStr."""
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

  config = ElasticSearchSettings(
    mode="standalone",
    hosts=["https://localhost:9200"],
    username="alice",
    password="top-secret-es-pwd",
  )
  backend = ElasticSearchBackend(config)
  kwargs = backend._build_kwargs()

  username, password = kwargs["basic_auth"]
  assert username == "alice"
  assert str(password) == "top-secret-es-pwd"
  assert "top-secret-es-pwd" not in repr(kwargs)
  assert isinstance(password, _RedactedStr)


# ---------------------------------------------------------------------------
# R-es-validate: _validate_key_name parity with Redis/MongoDB — every public
# name-taking method rejects invalid names (defense-in-depth vs query/prefix
# injection via a special-char or non-string name). Validation fires before
# self.client is accessed, so these need no connection.
# ---------------------------------------------------------------------------


def test_es_invalid_queue_name_rejected_before_backend_call():
  backend = ElasticSearchBackend(ElasticSearchSettings())
  with pytest.raises(ValueError, match="queue_name"):
    backend.clear_queue("bad queue name!")  # space + ! outside KEY_NAME_PATTERN


def test_es_invalid_set_name_rejected_before_backend_call():
  backend = ElasticSearchBackend(ElasticSearchSettings())
  with pytest.raises(ValueError, match="set_name"):
    backend.remove("bad/set", b"x")  # slash outside KEY_NAME_PATTERN


def test_es_invalid_storage_key_rejected_before_backend_call():
  backend = ElasticSearchBackend(ElasticSearchSettings())
  with pytest.raises(ValueError, match="key"):
    backend.retrieve("bad key")  # space outside KEY_NAME_PATTERN


def test_es_clear_storage_invalid_prefix_rejected():
  backend = ElasticSearchBackend(ElasticSearchSettings())
  with pytest.raises(ValueError, match="prefix"):
    backend.clear_storage("bad prefix!")  # space + ! outside KEY_NAME_PATTERN


def test_es_valid_names_pass_validation(mocker):
  """Guard: pattern-valid names (the default queue/storage naming) keep working."""
  b = _mock_backend(mocker)
  b._client.exists.return_value = False
  b._client.get.return_value = {"_source": {}}
  # None should raise ValueError.
  b.contains("dedup:spider.name", b"x")
  b.ttl("items:a-b_c.1")
