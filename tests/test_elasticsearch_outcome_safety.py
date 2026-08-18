"""Transport-level Elasticsearch mutation outcome-safety contracts."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from typing import Any, ClassVar

import pytest
from elastic_transport import ApiResponseMeta, BaseNode, HttpHeaders, NodeConfig
from elastic_transport import ConnectionError as ElasticConnectionError
from elastic_transport._node import NodeApiResponse
from elasticsearch import Elasticsearch, RequestError

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import (
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
    close_calls: ClassVar[int] = 0

    @classmethod
    def reset(cls, **failures: int) -> None:
        cls.calls = Counter()
        cls.failures_remaining = Counter(failures)
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
        if operation == "index":
            response = {
                "result": "created",
                "_shards": {"total": 1, "successful": 1, "failed": 0},
            }
            status = 201
        elif operation == "delete":
            response = {
                "result": "deleted",
                "_shards": {"total": 1, "successful": 1, "failed": 0},
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
            response = {"_shards": {"total": 1, "successful": 1, "failed": 0}}
            status = 200
        elif operation == "count":
            response = {
                "count": 7,
                "_shards": {"total": 1, "successful": 1, "failed": 0},
            }
            status = 200
        else:  # pragma: no cover - an unexpected SDK path should be obvious
            raise AssertionError(f"unexpected transport request: {operation}")

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


_SHARDS = {"total": 1, "successful": 1, "failed": 0}


def _mock_backend(mocker: Any) -> tuple[ElasticSearchBackend, Any]:
    backend = ElasticSearchBackend(ElasticSearchSettings())
    client = mocker.MagicMock()
    client.index.return_value = {"result": "created", "_shards": _SHARDS}
    client.delete.return_value = {"result": "deleted", "_shards": _SHARDS}
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
            "_shards": _SHARDS,
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": {"total": 1, "successful": 0, "failed": 1},
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": _SHARDS,
            "hits": {"total": {"value": 10_000, "relation": "gte"}, "hits": []},
        },
        {
            "timed_out": False,
            "_shards": _SHARDS,
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


@pytest.mark.parametrize("count", (None, -1, True, 1.5))
def test_count_requires_exact_nonnegative_integer(mocker: Any, count: object) -> None:
    backend, client = _mock_backend(mocker)
    client.count.return_value = {"count": count, "_shards": _SHARDS}

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


@pytest.mark.parametrize(
    ("operation", "response", "expected_error"),
    (
        (
            "push",
            {"result": "updated", "_shards": _SHARDS},
            QueueOutcomeIndeterminateError,
        ),
        (
            "set",
            {"result": "updated", "_shards": _SHARDS},
            SetOutcomeIndeterminateError,
        ),
        (
            "store",
            {"result": "noop", "_shards": _SHARDS},
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
        client.index.return_value = response
        call = lambda: backend.push("jobs", b"item")
    elif operation == "set":
        client.index.return_value = response
        call = lambda: backend.add("seen", b"item")
    elif operation == "store":
        client.index.return_value = response
        call = lambda: backend.store("key", b"item")
    else:
        client.delete.return_value = response
        call = lambda: backend.delete("key")

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
    client.indices.create.side_effect = [structured, None, None]

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
