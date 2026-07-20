"""Single-process Scrapy engine probe used by ``test_scrapy_engine_e2e``.

The Twisted reactor cannot be restarted reliably inside a shared pytest
process.  This helper is therefore executed once in a child process.  It uses
Scrapy's real crawler, downloader middleware, execution engine, scheduler and
signal manager against a loopback-only HTTP server.  Only the broker boundary
is replaced by a deterministic in-memory ``QueueBackend``.
"""

from __future__ import annotations

import json
import socket
import threading
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from scrapy import Request, Spider, signals
from scrapy.crawler import CrawlerProcess

from scrapy_extension.backends.base import QueueBackend
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY
from scrapy_extension.schedule.scheduler import BackendScheduler


class ProbeState:
    """Thread-safe event log shared by the reactor and local HTTP server."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.http_attempts: Counter[str] = Counter()

    def record(self, kind: str, **details: Any) -> None:
        with self._lock:
            self.events.append({"kind": kind, **details})

    def record_http_attempt(self, path: str) -> int:
        with self._lock:
            self.http_attempts[path] += 1
            attempt = self.http_attempts[path]
            self.events.append(
                {"kind": "http_request", "url": path, "attempt": attempt}
            )
            return attempt

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "events": list(self.events),
                "http_attempts": dict(self.http_attempts),
            }


STATE = ProbeState()


class LoopbackHandler(BaseHTTPRequestHandler):
    """Scripted loopback responder for success, redirect, retry and failure."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        path = urlsplit(self.path).path
        attempt = STATE.record_http_attempt(path)

        if path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/redirect-final")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if path == "/redirect-duplicate":
            self.send_response(302)
            self.send_header("Location", "/already-seen")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if path == "/retry" and attempt == 1:
            self._send_response(500, b"retry")
            return

        if path.startswith("/download-failure"):
            # Produce a genuine downloader failure without contacting anything
            # outside the loopback server.  The request disables RetryMiddleware so
            # this one failed delivery reaches its errback exactly once.
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return

        self._send_response(200, path.encode("ascii"))

    def _send_response(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        del format, args


class InMemoryAckBackend(QueueBackend):
    """FIFO MQ boundary with explicit, per-delivery ack tokens."""

    requires_ack = True
    supports_concurrent_ack = True

    def __init__(self) -> None:
        self._items: deque[bytes] = deque()
        self._in_flight: dict[str, tuple[bytes, str]] = {}
        self._next_token = 1
        self._failed_push_urls: set[str] = set()

    @staticmethod
    def _payload_url(item: bytes) -> str:
        payload = json.loads(item)
        if not isinstance(payload, dict) or not isinstance(payload.get("url"), str):
            raise QueueError("serialized request has no string URL")
        return urlsplit(payload["url"]).path

    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        del queue_name, priority
        url = self._payload_url(item)
        if url == "/dedup-push-failure" and url not in self._failed_push_urls:
            self._failed_push_urls.add(url)
            STATE.record("push_rejected", url=url)
            raise QueueError("scripted first push failure")
        self._items.append(item)
        STATE.record("push", url=url)

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        item, _token = self.pop_with_ack(queue_name, timeout)
        return item

    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, str | None]:
        del queue_name, timeout
        if not self._items:
            return None, None
        item = self._items.popleft()
        token = f"token-{self._next_token}"
        self._next_token += 1
        url = self._payload_url(item)
        self._in_flight[token] = (item, url)
        STATE.record("pop", url=url, token=token)
        return item, token

    def queue_len(self, queue_name: str) -> int:
        del queue_name
        return len(self._items)

    def clear_queue(self, queue_name: str) -> None:
        del queue_name
        self._items.clear()

    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        del queue_name
        self._finish("ack", token)

    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        del queue_name
        # Do not requeue in this probe: the observable contract under test is the
        # scheduler's one terminal transition for the failed delivery.  Requeue
        # policy belongs to each real broker and is covered in backend tests.
        self._finish("nack", token)

    def _finish(self, kind: str, token: Any | None) -> None:
        if not isinstance(token, str) or token not in self._in_flight:
            STATE.record("invalid_terminal", terminal=kind, token=repr(token))
            raise QueueError(f"unknown or already-finished token: {token!r}")
        _item, url = self._in_flight.pop(token)
        STATE.record(kind, url=url, token=token)

    def in_flight_tokens(self) -> list[str]:
        return sorted(self._in_flight)


BACKEND = InMemoryAckBackend()


class ProbeManager:
    """Minimal connection-manager boundary consumed by production components."""

    def __init__(self, role: str) -> None:
        self.role = role
        self._connected = False

    def connect(self) -> None:
        if self._connected:
            return
        self._connected = True
        STATE.record("manager_connect", role=self.role)

    def close(self) -> None:
        self._connected = False
        STATE.record("manager_close", role=self.role)

    def set_monitor(self, monitor: Any) -> None:
        del monitor
        STATE.record("manager_monitor_set", role=self.role)

    def get_queue_backend(self) -> InMemoryAckBackend:
        self.connect()
        return BACKEND

    def get_storage_backend(self) -> None:
        raise NotImplementedError


class LifecycleDupeFilter(BackendDupeFilter):
    """Real BackendDupeFilter with observable engine-owned lifecycle calls."""

    def open(self, spider: Spider | None = None) -> None:
        STATE.record("dupefilter_open", spider=getattr(spider, "name", None))
        super().open(spider)

    def close(self, reason: str) -> None:
        STATE.record("dupefilter_close", reason=reason)
        super().close(reason)


class EngineProbeScheduler(BackendScheduler):
    """Inject the local broker boundary while retaining production scheduler code."""

    @classmethod
    def from_crawler(cls, crawler: Any) -> EngineProbeScheduler:
        dupefilter = LifecycleDupeFilter(
            ProbeManager("dupefilter"),  # type: ignore[arg-type]
            membership_filter=MemoryMembershipFilter(),
            fingerprinter=getattr(crawler, "request_fingerprinter", None),
        )
        scheduler = cls(
            ProbeManager("queue"),  # type: ignore[arg-type]
            queue_key="engine-e2e",
            stats=crawler.stats,
            dupefilter=dupefilter,
            queue_depth_sample_every=1,
        )
        scheduler._owns_dupefilter = True
        return scheduler


class ProbeSignals:
    """Observe real Scrapy signal ordering without invoking scheduler internals."""

    @classmethod
    def from_crawler(cls, crawler: Any) -> ProbeSignals:
        observer = cls()
        crawler.signals.connect(
            observer.response_received, signal=signals.response_received
        )
        crawler.signals.connect(observer.spider_error, signal=signals.spider_error)
        crawler.signals.connect(
            observer.request_dropped, signal=signals.request_dropped
        )
        crawler.signals.connect(observer.spider_closed, signal=signals.spider_closed)
        return observer

    def response_received(
        self, response: Any, request: Request, spider: Spider
    ) -> None:
        del spider
        STATE.record(
            "response_received",
            url=urlsplit(response.url).path,
            ack_token_present=BACKEND_ACK_TOKEN_META_KEY in request.meta,
        )

    def spider_error(self, failure: Any, response: Any, spider: Spider) -> None:
        del failure, spider
        request = getattr(response, "request", None)
        url = urlsplit(request.url).path if request is not None else None
        STATE.record("spider_error", url=url)

    def request_dropped(self, request: Request, spider: Spider) -> None:
        del spider
        STATE.record("request_dropped", url=urlsplit(request.url).path)

    def spider_closed(self, spider: Spider, reason: str) -> None:
        STATE.record("spider_closed", spider=spider.name, reason=reason)


class EngineProbeSpider(Spider):
    """Exercise success, replacement requests and both terminal signal paths."""

    name = "engine_probe"

    def __init__(self, base_url: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base_url = base_url

    async def start(self) -> Any:
        for path in (
            "/ok",
            "/redirect",
            "/retry",
            "/callback-error",
            "/already-seen",
            "/redirect-duplicate",
            "/download-failure",
            "/download-failure-handled",
            "/download-failure-unhandled",
            "/dedup-push-failure",
            "/dedup-push-failure",
        ):
            if path == "/download-failure-unhandled":
                errback = None
            elif path == "/download-failure-handled":
                errback = self.handled_download_error
            else:
                errback = self.download_error
            yield Request(
                f"{self.base_url}{path}",
                callback=self.parse_response,
                errback=errback,
                meta={"dont_retry": path.startswith("/download-failure")},
            )

    def parse_response(self, response: Any) -> Any:
        path = urlsplit(response.url).path
        STATE.record(
            "callback",
            url=path,
            ack_token_present=BACKEND_ACK_TOKEN_META_KEY in response.request.meta,
        )
        if path == "/callback-error":
            raise RuntimeError("scripted callback failure")
        yield {"url": response.url}

    def download_error(self, failure: Any) -> None:
        request = failure.request
        STATE.record(
            "download_errback",
            url=urlsplit(request.url).path,
            ack_token_present=BACKEND_ACK_TOKEN_META_KEY in request.meta,
        )
        raise RuntimeError("scripted errback failure")

    def handled_download_error(self, failure: Any) -> None:
        request = failure.request
        STATE.record(
            "download_errback_handled",
            url=urlsplit(request.url).path,
            ack_token_present=BACKEND_ACK_TOKEN_META_KEY in request.meta,
        )
        return None


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), LoopbackHandler)
    server.daemon_threads = True
    server_thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.01},
        name="scrapy-engine-e2e-http",
        daemon=True,
    )
    server_thread.start()
    port = int(server.server_address[1])

    component_module = __name__
    process = CrawlerProcess(
        settings={
            "CONCURRENT_REQUESTS": 1,
            "COOKIES_ENABLED": False,
            "EXTENSIONS": {f"{component_module}.ProbeSignals": 100},
            "HTTPPROXY_ENABLED": False,
            "LOG_ENABLED": False,
            "REDIRECT_ENABLED": True,
            "RETRY_ENABLED": True,
            "RETRY_HTTP_CODES": [500],
            "RETRY_TIMES": 1,
            "ROBOTSTXT_OBEY": False,
            "SCHEDULER": f"{component_module}.EngineProbeScheduler",
            "TELNETCONSOLE_ENABLED": False,
            "TWISTED_REACTOR": (
                "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
            ),
        },
        install_root_handler=False,
    )
    crawler = process.create_crawler(EngineProbeSpider)
    crawl_failures: list[str] = []

    try:
        crawl_deferred = process.crawl(crawler, base_url=f"http://127.0.0.1:{port}")

        def capture_crawl_failure(failure: Any) -> None:
            crawl_failures.append(failure.getTraceback())

        crawl_deferred.addErrback(capture_crawl_failure)
        process.start(install_signal_handlers=False)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    if crawl_failures:
        raise RuntimeError("Crawler failed:\n" + "\n".join(crawl_failures))

    result = STATE.snapshot()
    result["in_flight_tokens"] = BACKEND.in_flight_tokens()
    print(f"ENGINE_PROBE_RESULT={json.dumps(result, sort_keys=True)}")


if __name__ == "__main__":
    main()
