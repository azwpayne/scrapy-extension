"""Transport-level Elasticsearch mutation outcome-safety contracts."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from typing import Any, ClassVar
from urllib.parse import unquote

import pytest
from elastic_transport import ApiResponseMeta, BaseNode, HttpHeaders, NodeConfig
from elastic_transport import ConnectionError as ElasticConnectionError
from elastic_transport._node import NodeApiResponse
from elasticsearch import Elasticsearch, RequestError, TransportError
from hypothesis import example, given, seed, settings
from hypothesis import strategies as st

from scrapy_extension.backends.elasticsearch import (
    ElasticSearchBackend,
    _ElasticSearchResponseError,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    QueueError,
    QueueOutcomeIndeterminateError,
    SetOutcomeIndeterminateError,
    StorageOutcomeIndeterminateError,
)
from scrapy_extension.settings.elasticsearch import ElasticSearchSettings


class _RetryProbeNode(BaseNode):
    """In-memory transport node that fails selected requests before replying."""

    calls: ClassVar[Counter[str]] = Counter()
    failures_remaining: ClassVar[Counter[str]] = Counter()
    malformed_remaining: ClassVar[Counter[str]] = Counter()
    close_calls: ClassVar[int] = 0

    @classmethod
    def reset(cls, **failures: int) -> None:
        cls.calls = Counter()
        cls.failures_remaining = Counter(failures)
        cls.malformed_remaining = Counter()
        cls.close_calls = 0

    def close(self) -> None:
        type(self).close_calls += 1

    @staticmethod
    def _operation(method: str, target: str) -> str:
        if method == "PUT" and "/_doc/" in target:
            return "index"
        if method == "DELETE" and "/_doc/" in target:
            return "delete"
        if target.endswith("/_delete_by_query"):
            return "delete_by_query"
        if target.endswith("/_refresh"):
            return "refresh"
        if target.endswith("/_count"):
            return "count"
        return f"{method} {target}"

    def perform_request(
        self,
        method: str,
        target: str,
        body: bytes | None = None,
        headers: HttpHeaders | None = None,
        request_timeout: Any = None,
    ) -> NodeApiResponse:
        del body, headers, request_timeout
        operation = self._operation(method, target)
        type(self).calls[operation] += 1
        if type(self).failures_remaining[operation] > 0:
            type(self).failures_remaining[operation] -= 1
            raise ElasticConnectionError("simulated response loss")

        response: dict[str, Any]
        if operation in {"index", "delete"}:
            document_target = target.partition("?")[0]
            index, document_id = (
                unquote(part) for part in document_target.split("/_doc/", maxsplit=1)
            )
            index = index.removeprefix("/")
        if operation == "index":
            response = {
                "_index": index,
                "_id": document_id,
                "result": "created",
                "_shards": {"total": 2, "successful": 1, "failed": 0},
            }
            status = 201
        elif operation == "delete":
            response = {
                "_index": index,
                "_id": document_id,
                "result": "deleted",
                "_shards": {"total": 2, "successful": 1, "failed": 0},
            }
            status = 200
        elif operation == "delete_by_query":
            response = {
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
            status = 200
        elif operation == "refresh":
            response = {"_shards": {"total": 2, "successful": 1, "failed": 0}}
            status = 200
        elif operation == "count":
            response = {
                "count": 7,
                "_shards": {"total": 1, "successful": 1, "failed": 0},
            }
            status = 200
        else:  # pragma: no cover - an unexpected SDK path should be obvious
            raise AssertionError(f"unexpected transport request: {operation}")

        if type(self).malformed_remaining[operation] > 0:
            type(self).malformed_remaining[operation] -= 1
            response["_id"] = "ack-for-another-document"

        meta = ApiResponseMeta(
            status=status,
            http_version="1.1",
            headers=HttpHeaders(
                {
                    "content-type": "application/json",
                    "x-elastic-product": "Elasticsearch",
                }
            ),
            duration=0.0,
            node=self.config,
        )
        return NodeApiResponse(meta, json.dumps(response).encode())


def _transport_backend() -> tuple[ElasticSearchBackend, Elasticsearch]:
    client = Elasticsearch(
        "http://localhost:9200",
        node_class=_RetryProbeNode,
        max_retries=2,
        retry_on_timeout=True,
    )
    backend = ElasticSearchBackend(ElasticSearchSettings(max_retries=2))
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()
    return backend, client


def test_mutation_view_attempts_push_once_but_safe_read_retains_retry() -> None:
    backend, _root = _transport_backend()
    try:
        _RetryProbeNode.reset(index=1)
        with pytest.raises(QueueOutcomeIndeterminateError):
            backend.push("jobs", b"payload")
        assert _RetryProbeNode.calls["index"] == 1

        _RetryProbeNode.reset(count=1)
        assert backend.queue_len("jobs") == 7
        assert _RetryProbeNode.calls["count"] == 2
    finally:
        backend.disconnect()

    assert _RetryProbeNode.close_calls == 1


@pytest.mark.parametrize(
    ("operation", "expected_error"),
    (
        ("set", SetOutcomeIndeterminateError),
        ("storage", StorageOutcomeIndeterminateError),
        ("clear", StorageOutcomeIndeterminateError),
    ),
)
def test_all_mutation_families_surface_indeterminate_transport_outcomes(
    operation: str, expected_error: type[Exception]
) -> None:
    backend, _root = _transport_backend()
    try:
        call: Callable[[], object]
        if operation == "set":
            _RetryProbeNode.reset(index=1)
            call = lambda: backend.add("seen", b"item")
            expected_attempt = "index"
        elif operation == "storage":
            _RetryProbeNode.reset(delete=1)
            call = lambda: backend.delete("key")
            expected_attempt = "delete"
        else:
            _RetryProbeNode.reset(delete_by_query=1)
            call = lambda: backend.clear_storage()
            expected_attempt = "delete_by_query"

        with pytest.raises(expected_error):
            call()
        assert _RetryProbeNode.calls[expected_attempt] == 1
    finally:
        backend.disconnect()


def test_mutation_view_has_fixed_no_replay_options_and_shares_root_transport() -> None:
    backend, root = _transport_backend()
    try:
        with backend._lease_generation("test") as generation:
            assert generation.client is root
            assert generation.mutation_client is not root
            assert generation.mutation_client.transport is root.transport
            assert generation.mutation_client._max_retries == 0
            assert generation.mutation_client._retry_on_timeout is False
            assert generation.mutation_client._retry_on_status == ()
    finally:
        backend.disconnect()


@seed(0xE5A11C)
@settings(max_examples=40, deadline=None)
@example(["success", "response-loss", "turnover", "malformed-ack"])
@given(
    st.lists(
        st.sampled_from(("success", "response-loss", "malformed-ack", "turnover")),
        min_size=1,
        max_size=24,
    )
)
def test_seeded_mutation_outcome_model_preserves_single_transport_attempt(
    actions: list[str],
) -> None:
    """Randomized generation and outcome sequences never replay a mutation."""
    _RetryProbeNode.reset()
    backend, _root = _transport_backend()
    mutation_number = 0
    try:
        for action in actions:
            if action == "turnover":
                backend.disconnect()
                backend, _root = _transport_backend()
                continue

            before = _RetryProbeNode.calls["index"]
            if action == "response-loss":
                _RetryProbeNode.failures_remaining["index"] += 1
            elif action == "malformed-ack":
                _RetryProbeNode.malformed_remaining["index"] += 1

            call = lambda backend=backend, mutation_number=mutation_number: (
                backend.store(f"model-key-{mutation_number}", b"payload")
            )
            if action in {"response-loss", "malformed-ack"}:
                with pytest.raises(StorageOutcomeIndeterminateError):
                    call()
            else:
                call()

            assert _RetryProbeNode.calls["index"] - before == 1
            mutation_number += 1
    finally:
        backend.disconnect()


def test_mutation_view_with_unrelated_transport_fails_generation_closed(
    mocker: Any,
) -> None:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    client = mocker.MagicMock()
    client.options.return_value.transport = object()
    client.transport = object()
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()

    with pytest.raises(BackendConnectionError) as exc_info:
        backend._build_generation(client, backend._connection_snapshot)

    assert str(exc_info.value) == (
        "ElasticSearch mutation client does not share the root transport."
    )
    assert exc_info.value.backend_type == "elasticsearch"


_MUTATION_SHARDS = {"total": 2, "successful": 1, "failed": 0}
_READ_SHARDS = {"total": 1, "successful": 1, "failed": 0}
_REFRESH_RESPONSE = {"_shards": _MUTATION_SHARDS}


def _mock_backend(mocker: Any) -> tuple[ElasticSearchBackend, Any]:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    client = mocker.MagicMock()
    client.options.return_value = client
    client.indices.create.side_effect = lambda **kwargs: {
        "acknowledged": True,
        "shards_acknowledged": True,
        "index": kwargs["index"],
    }
    client.index.side_effect = lambda **kwargs: {
        "_index": kwargs["index"],
        "_id": kwargs["id"],
        "result": "created",
        "_shards": _MUTATION_SHARDS,
    }
    client.delete.side_effect = lambda **kwargs: {
        "_index": kwargs["index"],
        "_id": kwargs["id"],
        "result": "deleted",
        "_shards": _MUTATION_SHARDS,
    }
    client.indices.refresh.return_value = _REFRESH_RESPONSE
    client.delete_by_query.return_value = {
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
    backend._client = client
    backend._connection_snapshot = backend._capture_connection_snapshot()
    return backend, client


@pytest.mark.parametrize(
    "response",
    (
        {},
        {
            "timed_out": True,
            "_shards": _READ_SHARDS,
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": {"total": 1, "successful": 0, "failed": 1},
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": _READ_SHARDS,
            "hits": {"total": {"value": 10_000, "relation": "gte"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": _READ_SHARDS,
            "hits": {"total": {"value": 1, "relation": "eq"}, "hits": []},
        },
    ),
)
def test_search_partial_or_malformed_response_fails_closed(
    mocker: Any, response: object
) -> None:
    backend, client = _mock_backend(mocker)
    client.search.return_value = response

    with pytest.raises(QueueError, match="queue pop failed"):
        backend.pop("jobs")

    client.delete.assert_not_called()


_INVALID_REFRESH_RESPONSES = (
    pytest.param({}, id="missing-shards"),
    pytest.param({"_shards": None}, id="non-mapping-shards"),
    pytest.param(
        {"_shards": {"total": True, "successful": 1, "failed": 0}},
        id="non-exact-total",
    ),
    pytest.param(
        {
            "_shards": {
                "total": 1,
                "successful": 0,
                "failed": 1,
                "failures": [],
            }
        },
        id="failed-shard",
    ),
    pytest.param(
        {
            "_shards": {
                "total": 1,
                "successful": 1,
                "failed": 0,
                "failures": [{}],
            }
        },
        id="failure-details",
    ),
)


@pytest.mark.parametrize("response", _INVALID_REFRESH_RESPONSES)
def test_pop_rejects_partial_or_malformed_refresh_before_search(
    mocker: Any, response: object
) -> None:
    backend, client = _mock_backend(mocker)
    client.indices.refresh.return_value = response

    with pytest.raises(QueueError) as exc_info:
        backend.pop("jobs")

    assert type(exc_info.value) is QueueError
    assert str(exc_info.value) == "ElasticSearch queue pop failed."
    assert exc_info.value.operation == "pop"
    assert exc_info.value.__cause__ is None
    client.search.assert_not_called()


@pytest.mark.parametrize(
    ("operation", "expected_error", "expected_message"),
    (
        ("queue", QueueError, "ElasticSearch queue length read failed."),
        ("set", BackendConnectionError, "ElasticSearch set length read failed."),
    ),
)
@pytest.mark.parametrize("response", _INVALID_REFRESH_RESPONSES)
def test_count_rejects_partial_or_malformed_refresh_before_count(
    mocker: Any,
    operation: str,
    expected_error: type[Exception],
    expected_message: str,
    response: object,
) -> None:
    backend, client = _mock_backend(mocker)
    client.indices.refresh.return_value = response

    with pytest.raises(expected_error) as exc_info:
        if operation == "queue":
            backend.queue_len("jobs")
        else:
            backend.set_len("seen")

    assert type(exc_info.value) is expected_error
    assert str(exc_info.value) == expected_message
    assert exc_info.value.__cause__ is None
    client.count.assert_not_called()


@pytest.mark.parametrize("count", (None, -1, True, 1.5))
def test_count_requires_exact_nonnegative_integer(mocker: Any, count: object) -> None:
    backend, client = _mock_backend(mocker)
    client.count.return_value = {"count": count, "_shards": _READ_SHARDS}

    with pytest.raises(QueueError, match="queue length read failed"):
        backend.queue_len("jobs")


@pytest.mark.parametrize(
    "shards",
    (
        None,
        {"total": 1, "successful": 0, "failed": 1},
        {"total": 2, "successful": 1, "failed": 0},
        {"total": 1, "successful": 1, "failed": 0, "failures": [{}]},
    ),
)
def test_count_rejects_missing_failed_or_partial_shards(
    mocker: Any, shards: object
) -> None:
    backend, client = _mock_backend(mocker)
    client.count.return_value = {"count": 1, "_shards": shards}

    with pytest.raises(QueueError, match="queue length read failed"):
        backend.queue_len("jobs")


_CLEAR_FAMILIES = (
    (
        "queue",
        QueueOutcomeIndeterminateError,
        "ElasticSearch queue clear failed.",
    ),
    (
        "set",
        SetOutcomeIndeterminateError,
        "ElasticSearch set clear failed.",
    ),
    (
        "storage",
        StorageOutcomeIndeterminateError,
        "ElasticSearch storage clear failed.",
    ),
)


def _call_clear(backend: ElasticSearchBackend, operation: str) -> None:
    if operation == "queue":
        backend.clear_queue("jobs")
    elif operation == "set":
        backend.clear_set("seen")
    else:
        backend.clear_storage()


@pytest.mark.parametrize(("operation", "_error_type", "_message"), _CLEAR_FAMILIES)
def test_clear_families_accept_documented_response_without_shards(
    mocker: Any,
    operation: str,
    _error_type: type[Exception],
    _message: str,
) -> None:
    backend, client = _mock_backend(mocker)
    client.delete_by_query.return_value = {
        "took": 3,
        "timed_out": False,
        "total": 2,
        "deleted": 2,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": {"bulk": 0, "search": 0},
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "task": "node:123",
        "failures": [],
        "future_extension": {"opaque": True},
    }

    _call_clear(backend, operation)


@pytest.mark.parametrize(("operation", "error_type", "message"), _CLEAR_FAMILIES)
@pytest.mark.parametrize(
    "response",
    (
        pytest.param(
            {
                "timed_out": True,
                "total": 1,
                "deleted": 1,
                "version_conflicts": 0,
                "failures": [],
            },
            id="timeout",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 1,
                "deleted": 0,
                "version_conflicts": 0,
                "failures": [{"cause": "redacted"}],
            },
            id="failures",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 1,
                "deleted": 0,
                "version_conflicts": 1,
                "failures": [],
            },
            id="version-conflict",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 2,
                "deleted": 1,
                "version_conflicts": 0,
                "failures": [],
            },
            id="count-mismatch",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": True,
                "deleted": 1,
                "version_conflicts": 0,
                "failures": [],
            },
            id="non-exact-count",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 1,
                "deleted": 1,
                "version_conflicts": 0,
                "throttled_millis": True,
                "failures": [],
            },
            id="malformed-throttling",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 1,
                "deleted": 1,
                "version_conflicts": 0,
                "task": "",
                "failures": [],
            },
            id="malformed-task",
        ),
        pytest.param(
            {
                "timed_out": False,
                "total": 1,
                "deleted": 1,
                "version_conflicts": 0,
                "retries": None,
                "failures": [],
            },
            id="null-retries",
        ),
        *(
            pytest.param(
                {
                    "timed_out": False,
                    "total": 1,
                    "deleted": 1,
                    "version_conflicts": 0,
                    "requests_per_second": rate,
                    "failures": [],
                },
                id=f"invalid-rate-{rate!r}",
            )
            for rate in (0, True, float("nan"), float("inf"), -2)
        ),
    ),
)
def test_clear_families_reject_indeterminate_documented_responses(
    mocker: Any,
    operation: str,
    error_type: type[Exception],
    message: str,
    response: object,
) -> None:
    backend, client = _mock_backend(mocker)
    client.delete_by_query.return_value = response

    with pytest.raises(error_type) as exc_info:
        _call_clear(backend, operation)

    assert type(exc_info.value) is error_type
    assert str(exc_info.value) == message
    assert exc_info.value.__cause__ is None


def test_clear_refreshes_before_and_after_delete_by_query(mocker: Any) -> None:
    """Clear uses a refresh barrier both before and after the deletion."""
    backend, client = _mock_backend(mocker)
    calls: list[str] = []
    refresh_response = _REFRESH_RESPONSE

    def refresh(**_kwargs: Any) -> object:
        calls.append("refresh")
        return refresh_response

    def delete_by_query(**_kwargs: Any) -> object:
        calls.append("delete_by_query")
        return client.delete_by_query.return_value

    client.indices.refresh.side_effect = refresh
    client.delete_by_query.side_effect = delete_by_query

    backend.clear_queue("jobs")

    assert calls == ["refresh", "delete_by_query", "refresh"]


@pytest.mark.parametrize(
    "failure_at",
    ("before", "after"),
)
def test_clear_refresh_barrier_failure_is_indeterminate(
    mocker: Any, failure_at: str
) -> None:
    """A failed visibility barrier never reports a definite clear success."""
    backend, client = _mock_backend(mocker)
    calls: list[str] = []
    refresh_count = 0
    refresh_error = TransportError(f"{failure_at} refresh failed")

    def refresh(**_kwargs: Any) -> object:
        nonlocal refresh_count
        refresh_count += 1
        calls.append("refresh")
        if (failure_at == "before" and refresh_count == 1) or (
            failure_at == "after" and refresh_count == 2
        ):
            raise refresh_error
        return _REFRESH_RESPONSE

    def delete_by_query(**_kwargs: Any) -> object:
        calls.append("delete_by_query")
        return client.delete_by_query.return_value

    client.indices.refresh.side_effect = refresh
    client.delete_by_query.side_effect = delete_by_query

    with pytest.raises(QueueOutcomeIndeterminateError):
        backend.clear_queue("jobs")

    if failure_at == "before":
        assert calls == ["refresh"]
    else:
        assert calls == ["refresh", "delete_by_query", "refresh"]


def test_pop_response_loss_after_delete_is_indeterminate_and_not_retried(
    mocker: Any,
) -> None:
    """A lost delete response cannot be reissued against an unknown item."""
    backend, client = _mock_backend(mocker)
    client.search.return_value = {
        "timed_out": False,
        "_shards": _READ_SHARDS,
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "doc-1",
                    "_seq_no": 3,
                    "_primary_term": 2,
                    "_source": {"item": "cGF5bG9hZA=="},
                }
            ],
        },
    }
    client.delete.side_effect = TransportError("response lost with private marker")

    with pytest.raises(QueueOutcomeIndeterminateError) as exc_info:
        backend.pop("jobs")

    assert type(exc_info.value) is QueueOutcomeIndeterminateError
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None
    assert "private marker" not in str(exc_info.value)
    client.delete.assert_called_once_with(
        index="scrapy_queue",
        id="doc-1",
        if_seq_no=3,
        if_primary_term=2,
    )
    client.search.assert_called_once()


@pytest.mark.parametrize(
    ("operation", "response", "expected_error"),
    (
        (
            "push",
            {"result": "updated", "_shards": _MUTATION_SHARDS},
            QueueOutcomeIndeterminateError,
        ),
        (
            "set",
            {"result": "updated", "_shards": _MUTATION_SHARDS},
            SetOutcomeIndeterminateError,
        ),
        (
            "store",
            {"result": "noop", "_shards": _MUTATION_SHARDS},
            StorageOutcomeIndeterminateError,
        ),
        (
            "delete",
            {
                "result": "deleted",
                "_shards": {"total": 1, "successful": 0, "failed": 1},
            },
            StorageOutcomeIndeterminateError,
        ),
    ),
)
def test_mutation_requires_complete_acknowledgement(
    mocker: Any,
    operation: str,
    response: object,
    expected_error: type[Exception],
) -> None:
    backend, client = _mock_backend(mocker)
    call: Callable[[], object]
    if operation == "push":
        client.index.side_effect = None
        client.index.return_value = response
        call = lambda: backend.push("jobs", b"item")
    elif operation == "set":
        client.index.side_effect = None
        client.index.return_value = response
        call = lambda: backend.add("seen", b"item")
    elif operation == "store":
        client.index.side_effect = None
        client.index.return_value = response
        call = lambda: backend.store("key", b"item")
    else:
        client.delete.side_effect = None
        client.delete.return_value = response
        call = lambda: backend.delete("key")

    with pytest.raises(expected_error):
        call()


@pytest.mark.parametrize(
    ("operation", "expected_error"),
    (
        ("push", QueueOutcomeIndeterminateError),
        ("set", SetOutcomeIndeterminateError),
        ("store", StorageOutcomeIndeterminateError),
        ("delete", StorageOutcomeIndeterminateError),
    ),
)
def test_mutation_rejects_acknowledgement_for_another_document(
    mocker: Any, operation: str, expected_error: type[Exception]
) -> None:
    backend, client = _mock_backend(mocker)
    response = {
        "_index": "wrong-index",
        "_id": "wrong-id",
        "result": "deleted" if operation == "delete" else "created",
        "_shards": _MUTATION_SHARDS,
    }
    if operation == "delete":
        client.delete.side_effect = None
        client.delete.return_value = response
        call = lambda: backend.delete("key")
    else:
        client.index.side_effect = None
        client.index.return_value = response
        if operation == "push":
            call = lambda: backend.push("jobs", b"item")
        elif operation == "set":
            call = lambda: backend.add("seen", b"item")
        else:
            call = lambda: backend.store("key", b"item")

    with pytest.raises(expected_error):
        call()


def _request_error(body: object) -> RequestError:
    meta = ApiResponseMeta(
        status=400,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "localhost", 9200),
    )
    return RequestError("static test", meta, body)


def test_index_setup_accepts_only_structured_resource_already_exists(
    mocker: Any,
) -> None:
    backend, client = _mock_backend(mocker)
    structured = _request_error(
        {"error": {"type": "resource_already_exists_exception"}}
    )
    client.indices.create.side_effect = [
        structured,
        {
            "acknowledged": True,
            "shards_acknowledged": True,
            "index": "scrapy_set",
        },
        {
            "acknowledged": True,
            "shards_acknowledged": True,
            "index": "scrapy_storage",
        },
    ]

    backend._ensure_indices(client=client)


def test_index_setup_rejects_resource_exists_text_without_structured_type(
    mocker: Any,
) -> None:
    backend, client = _mock_backend(mocker)
    client.indices.create.side_effect = _request_error(
        {"error": {"reason": "resource_already_exists_exception"}}
    )

    with pytest.raises(RequestError):
        backend._ensure_indices(client=client)


@pytest.mark.parametrize(
    "response",
    (
        None,
        {},
        {"acknowledged": False, "shards_acknowledged": True, "index": "scrapy_queue"},
        {"acknowledged": True, "shards_acknowledged": False, "index": "scrapy_queue"},
        {"acknowledged": True, "shards_acknowledged": True, "index": "other-index"},
    ),
)
def test_index_setup_rejects_unproven_create_acknowledgement(
    mocker: Any, response: object
) -> None:
    """Create success requires the SDK's complete acknowledgement payload."""
    backend, client = _mock_backend(mocker)
    client.indices.create.side_effect = lambda **_kwargs: response

    with pytest.raises(_ElasticSearchResponseError):
        backend._ensure_indices(client=client)

    client.indices.create.assert_called_once_with(index="scrapy_queue")


# === New focused tests for ES Semantics fix ===


def test_write_operation_green_replicas_single_shard() -> None:
    """Write operation succeeds with replicas=0 (green) cluster, single shard.

    Verifies that a normal write with total=1, successful=1, failed=0 is accepted
    for write operations, representing a green-cluster single-node scenario.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 1, "successful": 1, "failed": 0}}
    try:
        _validate_shards(response, require_success=True)
    except _ElasticSearchResponseError:
        pytest.fail("Green replica write should be accepted")


def test_write_operation_yellow_replicas_single_node() -> None:
    """Write operation succeeds with replicas=1 (yellow) single-node cluster.

    Verifies that a write with total=2, successful=1, failed=0 (yellow cluster
    single-node) is accepted for write operations, as the unassigned replica
    is not a shard failure.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 2, "successful": 1, "failed": 0}}
    try:
        _validate_shards(response, require_success=True)
    except _ElasticSearchResponseError:
        pytest.fail("Yellow cluster write should be accepted")


def test_write_operation_rejects_actual_shard_failure() -> None:
    """Write operation is rejected when shards report actual failures.

    Verifies that total=1, successful=0, failed=1 is rejected for write
    operations, preserving conservative classification for failed shards.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 1, "successful": 0, "failed": 1}}
    with pytest.raises(_ElasticSearchResponseError):
        _validate_shards(response, require_success=True)


def test_read_operation_rejects_yellow_cluster() -> None:
    """Read operation is rejected for yellow cluster shard acknowledgement.

    Verifies that total=2, successful=1, failed=0 (yellow cluster) is rejected
    for read operations (count, search, pop), preserving conservative
    classification.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 2, "successful": 1, "failed": 0}}
    with pytest.raises(_ElasticSearchResponseError):
        _validate_shards(response, require_success=False)


def test_read_operation_accepts_green_replicas() -> None:
    """Read operation succeeds with green replicas (replicas=0).

    Verifies that total=1, successful=1, failed=0 is accepted for read
    operations.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 1, "successful": 1, "failed": 0}}
    try:
        _validate_shards(response, require_success=False)
    except _ElasticSearchResponseError:
        pytest.fail("Green replica read should be accepted")


def test_read_operation_rejects_shard_failure() -> None:
    """Read operation is rejected when shards report failures.

    Verifies that total=1, successful=0, failed=1 is rejected for read
    operations, preserving conservative classification for failed shards.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    response = {"_shards": {"total": 1, "successful": 0, "failed": 1}}
    with pytest.raises(_ElasticSearchResponseError):
        _validate_shards(response, require_success=False)


def test_refresh_accepts_yellow_cluster() -> None:
    """Refresh operation accepts yellow cluster shard acknowledgement.

    Verifies that total=2, successful=1, failed=0 (yellow cluster) is accepted
    for refresh operations, as refresh just flushes the indexing buffer.
    """
    from scrapy_extension.backends.elasticsearch import _validate_refresh_response

    response = {"_shards": {"total": 2, "successful": 1, "failed": 0}}
    try:
        _validate_refresh_response(response)
    except _ElasticSearchResponseError:
        pytest.fail("Yellow cluster refresh should be accepted")


def test_refresh_rejects_actual_failure() -> None:
    """Refresh operation is rejected when shards report failures.

    Verifies that total=1, successful=0, failed=1 is rejected for refresh
    operations, preserving conservative classification.
    """
    from scrapy_extension.backends.elasticsearch import _validate_refresh_response

    response = {"_shards": {"total": 1, "successful": 0, "failed": 1}}
    with pytest.raises(_ElasticSearchResponseError):
        _validate_refresh_response(response)


def test_malformed_shards_rejected() -> None:
    """Malformed shards acknowledgement is rejected.

    Verifies that various malformed shard responses are rejected for both
    read and write operations.
    """
    from scrapy_extension.backends.elasticsearch import _validate_shards

    malformed_responses = [
        {},  # missing shards
        {"_shards": None},  # non-mapping shards
        {"_shards": {"total": True, "successful": 1, "failed": 0}},  # non-exact total
    ]

    for response in malformed_responses:
        # Write operations
        with pytest.raises(_ElasticSearchResponseError):
            _validate_shards(response, require_success=True)
        # Read operations
        with pytest.raises(_ElasticSearchResponseError):
            _validate_shards(response, require_success=False)
