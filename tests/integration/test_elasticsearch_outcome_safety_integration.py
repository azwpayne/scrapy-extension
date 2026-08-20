"""Live Elasticsearch response-drop checks for indeterminate mutations.

Run only against an explicitly enabled local HTTP Elasticsearch instance::

    SCRAPY_TEST_ES_OUTCOME_SAFETY=1 \
    SCRAPY_TEST_ES_HOSTS=http://localhost:9200 \
      uv run pytest tests/integration/test_elasticsearch_outcome_safety_integration.py \
      --allow-hosts=localhost,127.0.0.1,::1
"""

from __future__ import annotations

import http.client
import os
import socket
import threading
import uuid
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("SCRAPY_TEST_ES_OUTCOME_SAFETY") != "1",
        reason="Set SCRAPY_TEST_ES_OUTCOME_SAFETY=1 for response-drop tests.",
    ),
]

_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class _ResponseDropProxy(ThreadingHTTPServer):
    """Local HTTP forwarding proxy that can drop one committed response."""

    daemon_threads = True

    def __init__(self, target_host: str, target_port: int) -> None:
        super().__init__(("127.0.0.1", 0), _ProxyHandler)
        self.target_host = target_host
        self.target_port = target_port
        self._lock = threading.Lock()
        self._drop_matcher: Callable[[str, str], bool] | None = None
        self.requests: list[tuple[str, str]] = []
        self.dropped_requests: list[tuple[str, str]] = []

    def arm(self, matcher: Callable[[str, str], bool]) -> None:
        with self._lock:
            assert self._drop_matcher is None
            self._drop_matcher = matcher

    def record_and_should_drop(self, method: str, path: str) -> bool:
        with self._lock:
            request = (method, path)
            self.requests.append(request)
            matcher = self._drop_matcher
            if matcher is not None and matcher(method, path):
                self._drop_matcher = None
                self.dropped_requests.append(request)
                return True
            return False


class _ProxyHandler(BaseHTTPRequestHandler):
    """Forward one request, optionally closing after the upstream committed."""

    protocol_version = "HTTP/1.1"

    @staticmethod
    def _is_safe_header_name(name: str) -> bool:
        return "\r" not in name and "\n" not in name and ":" not in name

    @staticmethod
    def _sanitize_header_value(value: str) -> str:
        return value.replace("\r", "").replace("\n", "")

    def _forward(self) -> None:
        server = self.server
        assert isinstance(server, _ResponseDropProxy)
        content_length = int(self.headers.get("content-length", "0"))
        request_body = self.rfile.read(content_length) if content_length else None
        request_headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in _HOP_BY_HOP_HEADERS
        }
        request_headers["Host"] = f"{server.target_host}:{server.target_port}"
        request_headers["Connection"] = "close"
        request_headers["Accept-Encoding"] = "identity"

        upstream = http.client.HTTPConnection(
            server.target_host, server.target_port, timeout=10
        )
        try:
            upstream.request(
                self.command,
                self.path,
                body=request_body,
                headers=request_headers,
            )
            response = upstream.getresponse()
            response_body = response.read()
            response_headers = list(response.getheaders())
        finally:
            upstream.close()

        if server.record_and_should_drop(self.command, self.path):
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return

        self.send_response(response.status)
        for name, value in response_headers:
            if (
                name.lower() not in _HOP_BY_HOP_HEADERS | {"content-length"}
                and self._is_safe_header_name(name)
            ):
                self.send_header(name, self._sanitize_header_value(value))
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(response_body)
        self.close_connection = True

    do_DELETE = _forward
    do_GET = _forward
    do_HEAD = _forward
    do_POST = _forward
    do_PUT = _forward

    def log_message(self, _format: str, *args: Any) -> None:
        del args


@pytest.fixture(scope="module")
def response_drop_backend() -> Iterator[tuple[Any, _ResponseDropProxy]]:
    from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
    from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

    configured_hosts = [
        host.strip()
        for host in os.environ.get("SCRAPY_TEST_ES_HOSTS", "").split(",")
        if host.strip()
    ]
    if len(configured_hosts) != 1:
        pytest.skip("Response-drop tests require one SCRAPY_TEST_ES_HOSTS endpoint.")
    target = urlsplit(configured_hosts[0])
    if (
        target.scheme != "http"
        or target.hostname not in {"127.0.0.1", "localhost", "::1"}
        or target.port is None
        or target.path not in {"", "/"}
    ):
        pytest.skip("Response-drop tests accept only a localhost HTTP endpoint.")

    proxy = _ResponseDropProxy(target.hostname, target.port)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    backend = ElasticSearchBackend(
        ElasticSearchSettings(
            hosts=[f"http://127.0.0.1:{proxy.server_port}"],
            request_timeout=5.0,
            max_retries=2,
            retry_on_timeout=True,
        )
    )
    try:
        backend.connect()
        yield backend, proxy
    finally:
        backend.disconnect()
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)


def test_successful_clear_accepts_unmodified_client_response(
    response_drop_backend: tuple[Any, _ResponseDropProxy],
) -> None:
    """Exercise the real synchronous delete-by-query response without patching it."""
    backend, _proxy = response_drop_backend
    queue_name = f"outcome-clear:{uuid.uuid4().hex}"
    backend.push(queue_name, b"clear-me")
    backend.client.indices.refresh(index=backend.config.queue_index)

    backend.clear_queue(queue_name)

    assert backend.queue_len(queue_name) == 0


def test_committed_push_response_drop_is_not_replayed(
    response_drop_backend: tuple[Any, _ResponseDropProxy],
) -> None:
    from scrapy_extension.exceptions import QueueOutcomeIndeterminateError

    backend, proxy = response_drop_backend
    queue_name = f"outcome-push:{uuid.uuid4().hex}"
    queue_path = f"/{backend.config.queue_index}/_doc/"
    starting_requests = len(proxy.requests)
    starting_dropped = len(proxy.dropped_requests)
    proxy.arm(lambda method, path: method == "PUT" and queue_path in path)

    with pytest.raises(QueueOutcomeIndeterminateError):
        backend.push(queue_name, b"committed")

    newly_dropped = proxy.dropped_requests[starting_dropped:]
    assert len(newly_dropped) == 1
    _method, dropped_path = newly_dropped[0]
    document_id = unquote(urlsplit(dropped_path).path.rsplit("/", 1)[-1])
    matching_requests = [
        request
        for request in proxy.requests[starting_requests:]
        if request[0] == "PUT" and queue_path in request[1]
    ]
    assert len(matching_requests) == 1
    committed = backend.client.get(index=backend.config.queue_index, id=document_id)
    assert committed["_source"]["queue_name"] == queue_name
    backend.clear_queue(queue_name)


def test_committed_delete_response_drop_is_not_replayed_as_false(
    response_drop_backend: tuple[Any, _ResponseDropProxy],
) -> None:
    from scrapy_extension.exceptions import StorageOutcomeIndeterminateError

    backend, proxy = response_drop_backend
    key = f"outcome-delete:{uuid.uuid4().hex}"
    backend.store(key, b"committed")
    assert backend.exists(key) is True
    delete_path = f"/{backend.config.storage_index}/_doc/"
    starting_requests = len(proxy.requests)
    starting_dropped = len(proxy.dropped_requests)
    proxy.arm(lambda method, path: method == "DELETE" and delete_path in path)

    with pytest.raises(StorageOutcomeIndeterminateError):
        backend.delete(key)

    newly_dropped = proxy.dropped_requests[starting_dropped:]
    assert len(newly_dropped) == 1
    assert newly_dropped[0][0] == "DELETE"
    assert delete_path in newly_dropped[0][1]
    mutation_requests = [
        request
        for request in proxy.requests[starting_requests:]
        if request[0] == "DELETE" and delete_path in request[1]
    ]
    assert len(mutation_requests) == 1
    assert backend.exists(key) is False
