"""Tests for ElasticSearch backend."""

from __future__ import annotations

import traceback
from datetime import datetime, timedelta, timezone

import pytest
from elasticsearch import ApiError, NotFoundError, RequestError, TransportError

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    StorageError,
    StorageOutcomeIndeterminateError,
)
from scrapy_extension.settings.elasticsearch import (
    ElasticSearchMode,
    ElasticSearchSettings,
)


def _assert_package_traceback_locals_are_redacted(
    error: BaseException,
    marker: str,
) -> None:
    """Verify startup failures do not retain mutable endpoint snapshots."""
    trace = error.__traceback__
    while trace is not None:
        frame = trace.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            assert marker not in repr(frame.f_locals)
        trace = trace.tb_next


_SHARDS = {"total": 1, "successful": 1, "failed": 0}
_INDEX_RESPONSE = {"result": "created", "_shards": _SHARDS}
_DELETE_RESPONSE = {"result": "deleted", "_shards": _SHARDS}
_DELETE_BY_QUERY_RESPONSE = {
    "took": 3,
    "timed_out": False,
    "total": 1,
    "deleted": 1,
    "batches": 1,
    "version_conflicts": 0,
    "noops": 0,
    "retries": {"bulk": 0, "search": 0},
    "throttled_millis": 0,
    "requests_per_second": -1.0,
    "throttled_until_millis": 0,
    "failures": [],
}


def _search_response(hits):
    return {
        "timed_out": False,
        "_shards": _SHARDS,
        "hits": {
            "total": {"value": len(hits), "relation": "eq"},
            "hits": hits,
        },
    }


def _count_response(count):
    return {"count": count, "_shards": _SHARDS}


def _mock_backend(mocker, **settings_kwargs):
    config = ElasticSearchSettings(**settings_kwargs)
    backend = ElasticSearchBackend(config)
    backend._client = mocker.MagicMock()
    backend._client.index.return_value = _INDEX_RESPONSE
    backend._client.delete.return_value = _DELETE_RESPONSE
    backend._client.delete_by_query.return_value = _DELETE_BY_QUERY_RESPONSE
    backend._connection_snapshot = backend._capture_connection_snapshot()
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
        s = ElasticSearchSettings(
            hosts=["http://es1:9200"], allow_remote_plaintext=True
        )
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
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
        )

        backend = ElasticSearchBackend(ElasticSearchSettings())
        backend.connect()

        assert backend.is_connected()

    def test_connect_startup_error_does_not_retain_driver_diagnostics(self, mocker):
        marker = "elasticsearch-driver-secret"
        mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            side_effect=RuntimeError(f"transport dump included {marker}"),
        )

        with pytest.raises(BackendConnectionError) as exc_info:
            ElasticSearchBackend(ElasticSearchSettings()).connect()

        assert marker not in str(exc_info.value)
        assert marker not in repr(exc_info.value.__dict__)
        assert marker not in "".join(traceback.format_exception(exc_info.value))
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None

    def test_connect_backend_error_does_not_retain_driver_diagnostics(self, mocker):
        marker = "elasticsearch-backend-secret"
        mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            side_effect=BackendConnectionError(
                f"custom transport diagnostic included {marker}", backend_type="custom"
            ),
        )

        with pytest.raises(BackendConnectionError) as exc_info:
            ElasticSearchBackend(ElasticSearchSettings()).connect()

        assert marker not in str(exc_info.value)
        assert marker not in repr(exc_info.value.__dict__)
        assert marker not in "".join(traceback.format_exception(exc_info.value))
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None

    def test_mutated_remote_plaintext_fails_before_sdk_and_hides_endpoint(self, mocker):
        marker = "elasticsearch-mutable-endpoint-marker"
        config = ElasticSearchSettings()
        config.hosts = [f"http://{marker}:9200"]
        client_factory = mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch"
        )

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchBackend(config).connect()

        client_factory.assert_not_called()
        error = exc_info.value
        assert marker not in str(error)
        assert marker not in repr(error.__dict__)
        assert marker not in "".join(traceback.format_exception(error))
        assert error.__cause__ is None
        assert error.__context__ is None
        _assert_package_traceback_locals_are_redacted(error, marker)

    def test_connect_cloud(self, mocker):
        mock_client = mocker.MagicMock(
            ping=mocker.MagicMock(return_value=True),
            indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
        )
        mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
        )

        backend = ElasticSearchBackend(
            ElasticSearchSettings(
                mode=ElasticSearchMode.CLOUD, cloud_id="test:abc", api_key="test_key"
            )
        )
        backend.connect()

        assert backend.is_connected()

    def test_cloud_mode_missing_id_fails_at_construction(self):
        """R52/R66: CLOUD mode without cloud_id fails at construction.

        The misconfig surfaces as a ``ConfigurationError`` whose precise message
        survives the RedactedBaseSettings sanitization layer (R14-B uniform-
        exception-family invariant). Previously it was a ``ValueError`` that
        pydantic wrapped into a generic-sanitized ``ValidationError``.
        """
        with pytest.raises(ConfigurationError, match="requires 'cloud_id'") as exc_info:
            ElasticSearchSettings(mode=ElasticSearchMode.CLOUD)

        assert exc_info.value.setting_name == "cloud_id"

    def test_cloud_mode_without_auth_fails_at_construction(self):
        """R26-F/R66: CLOUD mode with cloud_id but NO auth method fails at
        construction with a ``ConfigurationError`` whose precise message survives
        sanitization (R14-B invariant). Previously a generic-sanitized
        ``ValueError``/``ValidationError``.
        """
        # cloud_id present but no api_key and no basic_auth → must fail.
        with pytest.raises(
            ConfigurationError, match="requires an auth method"
        ) as exc_info:
            ElasticSearchSettings(mode=ElasticSearchMode.CLOUD, cloud_id="test:abc")

        assert exc_info.value.setting_name == "api_key"

    def test_cloud_mode_empty_api_key_fails_at_construction(self):
        """R45: an empty-string ``api_key`` fails before client construction.

        R26-F used ``is not None``, so ``SecretStr("")`` — an env var set but
        unpopulated (e.g. ``SCRAPY_ELASTICSEARCH_API_KEY=""`` in CI drift) — passed
        fail-fast validation. But ``_build_kwargs`` uses truthiness and skipped the
        empty key, constructing an *anonymous* client that Elastic Cloud 401s on
        ping → the exact opaque ``BackendConnectionError('health check returned
        false')`` R26-F exists to surface at config time. An empty value is the
        same operator error as an unset one and must fail at the same point.
        """
        with pytest.raises(ConfigurationError, match="api_key"):
            ElasticSearchSettings(
                mode=ElasticSearchMode.CLOUD, cloud_id="test:abc", api_key=""
            )

    def test_cloud_mode_empty_basic_auth_fails_at_construction(self):
        """R45: blank basic-auth fields fail before client construction.

        Same root cause as the empty api_key case: ``is not None`` treats
        ``username=""`` / ``password=SecretStr("")`` as present, but
        ``_build_kwargs`` drops them via truthiness → anonymous client → 401.
        """
        with pytest.raises(ConfigurationError, match="username"):
            ElasticSearchSettings(
                mode=ElasticSearchMode.CLOUD,
                cloud_id="test:abc",
                username="",
                password="",
            )

    def test_blank_password_message_survives_sanitization(self):
        """R67: the blank-password ConfigurationError message must survive the
        RedactedBaseSettings sanitization layer (R14-B invariant). The
        api_key/username siblings are safe-listed; the password message in this
        same validator was overlooked (R74 CLOUD-mode sibling gap, caught by
        ndiff-regression)."""
        with pytest.raises(
            ConfigurationError, match="password must not be blank"
        ) as exc_info:
            ElasticSearchSettings(username="user", password="   ")

        assert exc_info.value.setting_name == "password"

    def test_connect_rejects_mutated_authenticated_cleartext_host(self, mocker):
        """A post-construction downgrade must not send credentials over HTTP."""
        config = ElasticSearchSettings(
            hosts=["https://es.example:9200"], api_key="top-secret-es-key"
        )
        config.hosts = ["http://downgraded.example:9200"]
        client_factory = mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch"
        )

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchBackend(config).connect()

        assert exc_info.value.setting_name == "hosts"
        assert "top-secret-es-key" not in str(exc_info.value)
        client_factory.assert_not_called()

    def test_snapshot_validation_does_not_retain_mutated_secret_input(self, mocker):
        marker = "elasticsearch-snapshot-secret-marker"
        config = ElasticSearchSettings(hosts=["https://es.example:9200"])
        config.api_key = [marker]  # type: ignore[assignment]
        client_factory = mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch"
        )

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchBackend(config).connect()

        error = exc_info.value
        assert error.setting_name == "api_key"
        assert marker not in str(error)
        assert marker not in repr(error.__dict__)
        assert marker not in "".join(traceback.format_exception(error))
        assert error.__cause__ is None
        assert error.__context__ is None
        client_factory.assert_not_called()

    def test_connect_rejects_mutated_capability_index_overlap(self, mocker):
        """A later mutation cannot collapse destructive capability boundaries."""
        config = ElasticSearchSettings()
        config.storage_index = config.queue_index
        client_factory = mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch"
        )

        with pytest.raises(ConfigurationError) as exc_info:
            ElasticSearchBackend(config).connect()

        assert exc_info.value.setting_name == "queue_index"
        client_factory.assert_not_called()

    def test_live_client_keeps_original_capability_indices_after_mutation(self, mocker):
        """Live operations use the client generation's immutable index snapshot."""
        client = mocker.MagicMock(ping=mocker.MagicMock(return_value=True))
        client.index.return_value = _INDEX_RESPONSE
        mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch", return_value=client
        )
        config = ElasticSearchSettings(
            queue_index="queue-a", set_index="set-a", storage_index="storage-a"
        )
        backend = ElasticSearchBackend(config)
        backend.connect()

        config.queue_index = "attacker-index"
        config.storage_index = "attacker-index"
        backend.push("jobs", b"payload")
        backend.store("key", b"payload")

        assert [call.kwargs["index"] for call in client.index.call_args_list] == [
            "queue-a",
            "storage-a",
        ]

    def test_standalone_empty_api_key_rejected(self):
        """R45: explicitly supplied blank credentials cannot become anonymous auth."""
        with pytest.raises(ConfigurationError, match="api_key"):
            ElasticSearchSettings(mode=ElasticSearchMode.STANDALONE, api_key="")

    def test_disconnect(self, mocker):
        mock_client = mocker.MagicMock(
            ping=mocker.MagicMock(return_value=True),
            indices=mocker.MagicMock(exists=mocker.MagicMock(return_value=True)),
        )
        mocker.patch(
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
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
        b._client.search.return_value = _search_response(
            [
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                }
            ]
        )

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

    @pytest.mark.parametrize(
        "hit",
        [
            pytest.param([], id="hit-not-mapping"),
            pytest.param(
                {
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                },
                id="missing-id",
            ),
            pytest.param(
                {
                    "_id": "",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                },
                id="empty-id",
            ),
            pytest.param(
                {
                    "_id": 1,
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                },
                id="non-string-id",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": True,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                },
                id="bool-sequence-number",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1.0,
                    "_source": {"item": "aXRlbQ=="},
                },
                id="non-integer-primary-term",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": [],
                },
                id="source-not-mapping",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {},
                },
                id="missing-item",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": b"aXRlbQ=="},
                },
                id="item-not-string",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "not base64!"},
                },
                id="invalid-base64",
            ),
            pytest.param(
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": "\N{SNOWMAN}"},
                },
                id="non-ascii-item",
            ),
        ],
    )
    def test_pop_rejects_malformed_hit_before_delete(self, mocker, hit):
        b = _mock_backend(mocker)
        b._client.search.return_value = _search_response([hit])

        with pytest.raises(QueueError) as exc_info:
            b.pop("q")

        assert str(exc_info.value) == "ElasticSearch queue pop failed."
        assert exc_info.value.operation == "pop"
        assert exc_info.value.queue_name is None
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        b._client.delete.assert_not_called()

    def test_pop_preserves_valid_empty_bytes(self, mocker):
        b = _mock_backend(mocker)
        b._client.search.return_value = _search_response(
            [
                {
                    "_id": "1",
                    "_seq_no": 42,
                    "_primary_term": 1,
                    "_source": {"item": ""},
                }
            ]
        )

        assert b.pop("q") == b""
        b._client.delete.assert_called_once_with(
            index="scrapy_queue",
            id="1",
            if_seq_no=42,
            if_primary_term=1,
        )

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
            _search_response(
                [
                    {
                        "_id": "1",
                        "_seq_no": 10,
                        "_primary_term": 1,
                        "_source": {"item": "bG9zdA=="},
                    }
                ]
            ),
            _search_response(
                [
                    {
                        "_id": "2",
                        "_seq_no": 20,
                        "_primary_term": 1,
                        "_source": {"item": "d29u"},
                    }
                ]
            ),
        ]
        b._client.delete.side_effect = [
            ConflictError("conflict", 409, body={}),
            _DELETE_RESPONSE,
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
        b._client.search.return_value = _search_response(
            [
                {
                    "_id": "1",
                    "_seq_no": 10,
                    "_primary_term": 1,
                    "_source": {"item": "bG9zdA=="},
                }
            ]
        )
        b._client.delete.side_effect = ConflictError("conflict", 409, body={})

        assert b.pop("q") is None
        # All 3 attempts tried (max_attempts); each searched then lost the race.
        assert b._client.search.call_count == 3

    def test_pop_delete_not_found_is_not_empty_success(self, mocker):
        b = _mock_backend(mocker)
        b._client.search.return_value = _search_response(
            [
                {
                    "_id": "1",
                    "_seq_no": 10,
                    "_primary_term": 1,
                    "_source": {"item": "aXRlbQ=="},
                }
            ]
        )
        b._client.delete.side_effect = _make_not_found_error()

        with pytest.raises(QueueError, match="queue pop failed"):
            b.pop("q")

    def test_pop_empty(self, mocker):
        b = _mock_backend(mocker)
        b._client.search.return_value = _search_response([])

        assert b.pop("q") is None
        b._client.delete.assert_not_called()

    def test_queue_len(self, mocker):
        b = _mock_backend(mocker)
        b._client.count.return_value = _count_response(5)
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

    def test_set_len_error_is_not_reported_as_empty(self, mocker):
        from scrapy_extension.exceptions import BackendConnectionError

        b = _mock_backend(mocker)
        b._client.count.side_effect = TransportError("err")
        with pytest.raises(BackendConnectionError) as exc_info:
            b.set_len("s")
        assert exc_info.value.backend_type == "elasticsearch"
        assert str(exc_info.value) == "ElasticSearch set length read failed."
        assert exc_info.value.__cause__ is None

    def test_clear_queue(self, mocker):
        b = _mock_backend(mocker)
        b.clear_queue("q")
        b._client.delete_by_query.assert_called_once()

    def test_clear_queue_wraps_transport_error(self, mocker):
        from scrapy_extension.exceptions import QueueError

        b = _mock_backend(mocker)
        b._client.delete_by_query.side_effect = TransportError("cluster unavailable")

        with pytest.raises(QueueError) as exc_info:
            b.clear_queue("q")

        assert str(exc_info.value) == "ElasticSearch queue clear failed."
        assert exc_info.value.queue_name is None
        assert exc_info.value.operation == "clear_queue"
        assert exc_info.value.__cause__ is None


class TestSetCore:
    def test_add_new(self, mocker):
        b = _mock_backend(mocker)
        assert b.add("s", b"item") is True
        assert b._client.index.call_args.kwargs["op_type"] == "create"

    def test_add_duplicate(self, mocker):
        b = _mock_backend(mocker)
        err = RequestError(
            "409",
            mocker.MagicMock(),
            {"error": {"type": "version_conflict_engine_exception"}},
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

    @pytest.mark.parametrize(
        ("error_type", "expected_type"),
        ((ApiError, ConfigurationError), (TransportError, BackendConnectionError)),
    )
    def test_remove_normalizes_backend_error(self, mocker, error_type, expected_type):

        b = _mock_backend(mocker)
        error = (
            error_type("delete failed", mocker.MagicMock(status=500), {})
            if error_type is ApiError
            else error_type("delete failed")
        )
        b._client.delete.side_effect = error
        with pytest.raises(expected_type) as exc_info:
            b.remove("s", b"item")
        assert exc_info.value.__cause__ is None
        if expected_type is BackendConnectionError:
            assert str(exc_info.value) == "ElasticSearch set remove failed."
            assert exc_info.value.backend_type == "elasticsearch"
        else:
            assert str(exc_info.value) == "ElasticSearch set request was rejected."
            assert exc_info.value.setting_name == "operation"

    def test_contains(self, mocker):
        b = _mock_backend(mocker)
        b._client.exists.return_value = True
        assert b.contains("s", b"item") is True

    def test_contains_wraps_transport_error(self, mocker):
        from scrapy_extension.exceptions import BackendConnectionError

        b = _mock_backend(mocker)
        error = TransportError("exists failed")
        b._client.exists.side_effect = error
        with pytest.raises(BackendConnectionError) as exc_info:
            b.contains("s", b"item")
        assert str(exc_info.value) == "ElasticSearch set membership check failed."
        assert exc_info.value.backend_type == "elasticsearch"
        assert exc_info.value.__cause__ is None

    def test_set_len(self, mocker):
        b = _mock_backend(mocker)
        b._client.count.return_value = _count_response(3)
        assert b.set_len("s") == 3

    def test_clear_set(self, mocker):
        b = _mock_backend(mocker)
        b.clear_set("s")
        b._client.delete_by_query.assert_called_once()

    def test_clear_set_wraps_transport_error(self, mocker):
        from scrapy_extension.exceptions import BackendConnectionError

        b = _mock_backend(mocker)
        b._client.delete_by_query.side_effect = TransportError("cluster unavailable")

        with pytest.raises(BackendConnectionError) as exc_info:
            b.clear_set("s")

        assert exc_info.value.backend_type == "elasticsearch"
        assert str(exc_info.value) == "ElasticSearch set clear failed."
        assert exc_info.value.__cause__ is None


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
        b._client.get.return_value = {
            "_source": {"data": "ZGF0YQ==", "expireAt": past},
            "_seq_no": 4,
            "_primary_term": 2,
        }
        assert b.retrieve("k") is None
        b._client.delete.assert_called_once_with(
            index="scrapy_storage", id="k", if_seq_no=4, if_primary_term=2
        )

    def test_retrieve_expired_reap_cannot_delete_concurrent_fresh_write(self, mocker):
        """Lazy expiry cleanup is conditional on the exact GET document version."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {
            "_source": {"data": "ZGF0YQ==", "expireAt": past},
            "_seq_no": 7,
            "_primary_term": 3,
        }

        assert b.retrieve("k") is None
        b._client.delete.assert_called_once_with(
            index="scrapy_storage",
            id="k",
            if_seq_no=7,
            if_primary_term=3,
        )

    def test_retrieve_expired_without_version_metadata_fails_closed(self, mocker):
        """Missing concurrency metadata is a malformed response, not absence."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {"_source": {"data": "ZGF0YQ==", "expireAt": past}}

        with pytest.raises(StorageError):
            b.retrieve("k")
        b._client.delete.assert_not_called()

    @pytest.mark.parametrize(
        "diagnostic_error",
        [
            RuntimeError("warning handler failed"),
            KeyboardInterrupt("warning handler interrupted"),
            SystemExit("warning handler exited"),
        ],
    )
    def test_expired_reap_missing_version_fails_closed_without_warning(
        self, mocker, diagnostic_error
    ):
        """Malformed version metadata fails closed before cleanup diagnostics."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {"_source": {"data": "ZGF0YQ==", "expireAt": past}}
        warning = mocker.patch(
            "scrapy_extension.backends.elasticsearch.logger.warning",
            side_effect=diagnostic_error,
        )

        with pytest.raises(StorageError):
            b.retrieve("k")

        b._client.delete.assert_not_called()
        warning.assert_not_called()

    @pytest.mark.parametrize(
        "diagnostic_error",
        [
            RuntimeError("warning handler failed"),
            KeyboardInterrupt("warning handler interrupted"),
            SystemExit("warning handler exited"),
        ],
    )
    def test_expired_reap_transport_failure_is_indeterminate_without_warning(
        self, mocker, diagnostic_error
    ):
        """A lost reap response is surfaced instead of becoming absent success."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {
            "_source": {"data": "ZGF0YQ==", "expireAt": past},
            "_seq_no": 4,
            "_primary_term": 2,
        }
        b._client.delete.side_effect = TransportError("cleanup unavailable")
        warning = mocker.patch(
            "scrapy_extension.backends.elasticsearch.logger.warning",
            side_effect=diagnostic_error,
        )

        with pytest.raises(StorageOutcomeIndeterminateError) as exc_info:
            b.retrieve("k")

        assert exc_info.value.operation == "retrieve"
        b._client.delete.assert_called_once_with(
            index="scrapy_storage", id="k", if_seq_no=4, if_primary_term=2
        )
        warning.assert_not_called()

    @pytest.mark.parametrize(
        "control_error",
        [KeyboardInterrupt("delete interrupted"), SystemExit("delete exited")],
    )
    def test_expired_reap_preserves_direct_delete_control_error(
        self, mocker, control_error
    ):
        """Only diagnostic controls are isolated; SDK controls remain observable."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {
            "_source": {"data": "ZGF0YQ==", "expireAt": past},
            "_seq_no": 4,
            "_primary_term": 2,
        }
        b._client.delete.side_effect = control_error

        with pytest.raises(type(control_error)) as exc_info:
            b.retrieve("k")

        assert exc_info.value is control_error

    @pytest.mark.parametrize(
        "source",
        [
            {},
            {"data": 123},
            {"data": "!!!!"},
            {"data": "ZGF0YQ==", "expireAt": "not-a-date"},
        ],
    )
    def test_retrieve_corrupt_document_is_storage_error(self, mocker, source):
        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.get.return_value = {"_source": source}

        with pytest.raises(StorageError) as exc_info:
            b.retrieve("k")

        assert str(exc_info.value) == "ElasticSearch storage retrieve failed."
        assert exc_info.value.operation == "retrieve"
        assert exc_info.value.key is None

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
        b._client.get.return_value = {
            "_source": {"data": "ZGF0YQ==", "expireAt": past},
            "_seq_no": 4,
            "_primary_term": 2,
        }
        assert b.exists("k") is False
        b._client.delete.assert_called_once_with(
            index="scrapy_storage", id="k", if_seq_no=4, if_primary_term=2
        )

    @pytest.mark.parametrize("expiry_fields", [{}, {"expireAt": None}])
    def test_ttl_no_expire(self, mocker, expiry_fields):
        b = _mock_backend(mocker)
        b._client.get.return_value = {"_source": expiry_fields}
        assert b.ttl("k") is None
        b._client.delete.assert_not_called()

    def test_ttl_with_expire(self, mocker):
        b = _mock_backend(mocker)
        future = (datetime.now(tz=timezone.utc) + timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {"_source": {"expireAt": future}}
        assert 3500 < b.ttl("k") <= 3600

    def test_ttl_expired_returns_none_and_reaps(self, mocker):
        """Expired storage is absent after optimistic lazy cleanup."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {
            "_source": {"expireAt": past},
            "_seq_no": 4,
            "_primary_term": 2,
        }
        assert b.ttl("k") is None
        b._client.delete.assert_called_once_with(
            index="scrapy_storage", id="k", if_seq_no=4, if_primary_term=2
        )

    def test_ttl_expired_without_version_metadata_fails_closed(self, mocker):
        """Logical expiry with malformed metadata is not an absent success."""
        b = _mock_backend(mocker)
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=3600)).isoformat()
        b._client.get.return_value = {"_source": {"expireAt": past}}

        with pytest.raises(StorageError):
            b.ttl("k")
        b._client.delete.assert_not_called()

    def test_ttl_malformed_expiry_is_storage_error(self, mocker):
        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.get.return_value = {"_source": {"expireAt": "not-a-date"}}

        with pytest.raises(StorageError) as exc_info:
            b.ttl("k")

        assert str(exc_info.value) == "ElasticSearch storage TTL read failed."
        assert exc_info.value.operation == "ttl"
        assert exc_info.value.key is None

    def test_ttl_not_found(self, mocker):
        """R48: a missing key returns None, not -1 (distinguish absent from expired).

        Pre-R48 this asserted ``== -1``, codifying the same absent/expired
        conflation that R5 fixed on Redis and MongoDB. ElasticSearch was missed
        in that sweep.
        """
        b = _mock_backend(mocker)
        b._client.get.side_effect = _make_not_found_error()
        assert b.ttl("k") is None

    def test_ttl_wraps_transport_error_as_storage_error(self, mocker):
        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.get.side_effect = TransportError("cluster unavailable")

        with pytest.raises(StorageError) as exc_info:
            b.ttl("k")

        assert str(exc_info.value) == "ElasticSearch storage TTL read failed."
        assert exc_info.value.operation == "ttl"
        assert exc_info.value.key is None
        assert exc_info.value.__cause__ is None

    def test_clear_storage(self, mocker):
        b = _mock_backend(mocker)
        b.clear_storage()
        assert b._client.delete_by_query.call_args.kwargs["query"] == {"match_all": {}}

    def test_clear_storage_prefix(self, mocker):
        """R-es-keyword: prefix clear must target ``key.keyword`` (unanalyzed).

        ``key`` is dynamically mapped as ``text`` (standard analyzer); a ``prefix``
        query on the analyzed field matches tokens, not the full key value, so prefix
        clearing would silently over-match or no-op. The ``.keyword`` subfield is
        unanalyzed → exact-prefix match (same convention as ``_count`` /
        ``_delete_by_term`` / ``pop``). Parity with redis scan_iter(match=prefix*)
        and dynamodb begins_with (#64).
        """
        b = _mock_backend(mocker)
        b.clear_storage(prefix="items:")
        assert b._client.delete_by_query.call_args.kwargs["query"] == {
            "prefix": {"key.keyword": "items:"}
        }

    def test_clear_storage_wraps_transport_error(self, mocker):
        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.delete_by_query.side_effect = TransportError("cluster unavailable")

        with pytest.raises(StorageError) as exc_info:
            b.clear_storage("items:")

        assert str(exc_info.value) == "ElasticSearch storage clear failed."
        assert exc_info.value.operation == "clear_storage"
        assert exc_info.value.key is None
        assert exc_info.value.__cause__ is None

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
        assert str(ei.value) == "ElasticSearch storage store failed."
        assert ei.value.operation == "store"
        assert ei.value.key is None
        assert ei.value.__cause__ is None

    def test_store_wraps_api_error_as_storage_error(self, mocker):
        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        error = ApiError("cluster rejected write", mocker.MagicMock(status=500), {})
        b._client.index.side_effect = error

        with pytest.raises(StorageError) as exc_info:
            b.store("k", b"data")

        assert str(exc_info.value) == "ElasticSearch storage store failed."
        assert exc_info.value.operation == "store"
        assert exc_info.value.key is None
        assert exc_info.value.__cause__ is None

    def test_retrieve_wraps_transport_error_as_storage_error(self, mocker):
        from elasticsearch import TransportError

        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.get.side_effect = TransportError("timeout")
        with pytest.raises(StorageError) as ei:
            b.retrieve("k")
        assert str(ei.value) == "ElasticSearch storage retrieve failed."
        assert ei.value.operation == "retrieve"
        assert ei.value.key is None
        assert ei.value.__cause__ is None

    def test_delete_wraps_transport_error_as_storage_error(self, mocker):
        from elasticsearch import TransportError

        from scrapy_extension.exceptions import StorageError

        b = _mock_backend(mocker)
        b._client.delete.side_effect = TransportError("timeout")
        with pytest.raises(StorageError) as ei:
            b.delete("k")
        assert str(ei.value) == "ElasticSearch storage delete failed."
        assert ei.value.operation == "delete"
        assert ei.value.key is None
        assert ei.value.__cause__ is None


class TestValidation:
    def test_validate_key_name_empty_string(self):
        from scrapy_extension.backends.elasticsearch import _validate_key_name

        with pytest.raises(ValueError, match="Invalid name"):
            _validate_key_name("", "name")


class TestSet:
    def test_add_request_error_without_version_conflict(self, mocker):
        b = _mock_backend(mocker)
        err = RequestError(
            "400", mocker.MagicMock(), {"error": "mapper_parsing_exception"}
        )
        b._client.index.side_effect = err
        with pytest.raises(ConfigurationError) as exc_info:
            b.add("s", b"item")
        assert str(exc_info.value) == "ElasticSearch set request was rejected."
        assert exc_info.value.setting_name == "operation"
        assert exc_info.value.__cause__ is None

    def test_add_new(self, mocker):
        b = _mock_backend(mocker)
        assert b.add("s", b"item") is True
        assert b._client.index.call_args.kwargs["op_type"] == "create"

    def test_add_duplicate(self, mocker):
        b = _mock_backend(mocker)
        err = RequestError(
            "409",
            mocker.MagicMock(),
            {"error": {"type": "version_conflict_engine_exception"}},
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
        b._client.count.return_value = _count_response(3)
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
        catches it (degrade to not-seen) instead of crashing the crawl. The public
        terminal error intentionally drops the raw TransportError graph.

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
        assert str(exc_info.value) == "ElasticSearch set add failed."
        assert exc_info.value.__cause__ is None


class TestPing:
    def test_ping_connected(self, mocker):
        b = _mock_backend(mocker)
        b._client.ping.return_value = True
        assert b.ping() is True

    @pytest.mark.parametrize(
        ("response", "expected"), [(1, True), (0, False), (None, False)]
    )
    def test_is_connected_normalizes_ping_response_to_bool(
        self, mocker, response, expected
    ):
        b = _mock_backend(mocker)
        b._client.ping.return_value = response

        assert b.is_connected() is expected

    @pytest.mark.parametrize("method", ["is_connected", "ping"])
    @pytest.mark.parametrize(
        "error", [RuntimeError("unexpected failure"), ValueError("invalid state")]
    )
    def test_health_probe_returns_false_on_unexpected_exception(
        self, mocker, method, error
    ):
        """Health probes remain boolean for ordinary driver failures."""
        b = _mock_backend(mocker)
        b._client.ping.side_effect = error

        assert getattr(b, method)() is False

    @pytest.mark.parametrize("method", ["is_connected", "ping"])
    @pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit])
    def test_health_probe_propagates_control_flow(self, mocker, method, error_type):
        """Health probes do not convert terminal control flow into False."""
        b = _mock_backend(mocker)
        b._client.ping.side_effect = error_type("stop")

        with pytest.raises(error_type):
            getattr(b, method)()

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
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
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
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
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
            "scrapy_extension.backends.elasticsearch.Elasticsearch",
            return_value=mock_client,
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
