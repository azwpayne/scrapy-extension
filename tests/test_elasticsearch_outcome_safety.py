"""Transport-level Elasticsearch mutation outcome-safety contracts."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from typing import Any, ClassVar

import pytest
from elastic_transport import ApiResponseMeta, BaseNode, HttpHeaders
from elastic_transport import ConnectionError as ElasticConnectionError
from elastic_transport._node import NodeApiResponse
from elasticsearch import Elasticsearch

from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
from scrapy_extension.exceptions import (
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
                "timed_out": False,
                "total": 1,
                "deleted": 1,
                "failures": [],
                "_shards": {"total": 1, "successful": 1, "failed": 0},
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
