"""Benchmarks for request serialization without backend I/O.

The two opt-in measurements cover Scrapy request-to-dict conversion and the
JSON encode/decode pair. Representative objects are prepared before the timed
caliper. A separate deterministic full round-trip check remains in the
ordinary suite. No performance thresholds are asserted.
"""

from __future__ import annotations

from typing import Any

import pytest
from scrapy import Spider
from scrapy.http import Request, Response
from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.queue.queue import BackendQueue

_BENCHMARK_ROUNDS = 5


class _NullConnectionManager:
    """Stand-in unused by the pure serialization path."""


class _BenchSpider(Spider):
    """Spider owning the callback serialized by the representative request."""

    name = "benchmark-serialization"

    def parse_item(self, response: Response, tag: str) -> Any:
        """Provide a callback target; benchmark tests never invoke it."""
        del response, tag
        return None


@pytest.fixture(scope="module")
def bench_spider() -> _BenchSpider:
    """Return the spider used to resolve the representative callback."""
    return _BenchSpider()


@pytest.fixture(scope="module")
def backend_queue(bench_spider: _BenchSpider) -> BackendQueue:
    """Build a queue whose pure serialization methods are exercised."""
    return BackendQueue(
        _NullConnectionManager(),  # type: ignore[arg-type]
        "bench-serialization",
        spider=bench_spider,
    )


def _make_request(spider: _BenchSpider) -> Request:
    """Build a representative request with a spider-bound callback."""
    return Request(
        url="https://example.com/path/to/resource?query=value&page=42",
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Request-Id": "abc-123-def-456",
            "User-Agent": "scrapy-extension-bench/1.0",
        },
        body=b'{"key": "value", "nested": {"n": 7}, "list": [1, 2, 3]}',
        cookies={"session": "benchmark-cookie", "region": "us-west-2"},
        meta={
            "depth": 3,
            "download_timeout": 30.0,
            "source": "seed",
            "retry_times": 1,
        },
        cb_kwargs={"tag": "bench"},
        priority=100,
        encoding="utf-8",
        dont_filter=False,
        callback=spider.parse_item,
    )


@pytest.mark.benchmark
def test_request_to_dict(
    benchmark,
    backend_queue: BackendQueue,
    bench_spider: _BenchSpider,
) -> None:
    """Measure request-to-dict conversion with request creation untimed."""
    request = _make_request(bench_spider)

    result = benchmark.pedantic(
        backend_queue._request_to_dict,
        args=(request,),
        rounds=_BENCHMARK_ROUNDS,
    )

    assert isinstance(result, dict)
    assert result["url"] == request.url
    assert result["method"] == "POST"
    assert result["body"] is not None


@pytest.mark.benchmark
def test_serialize_deserialize_roundtrip(
    benchmark,
    backend_queue: BackendQueue,
    bench_spider: _BenchSpider,
) -> None:
    """Measure one JSON serialization/deserialization pair."""
    request = _make_request(bench_spider)
    request_dict = backend_queue._request_to_dict(request)
    serializer = JSONSerializer()

    def roundtrip() -> Any:
        return serializer.deserialize(serializer.serialize(request_dict))

    result = benchmark.pedantic(roundtrip, rounds=_BENCHMARK_ROUNDS)

    assert result == request_dict


def test_full_roundtrip_is_lossless(
    backend_queue: BackendQueue,
    bench_spider: _BenchSpider,
) -> None:
    """Keep full serialization correctness deterministic in the ordinary suite."""
    request = _make_request(bench_spider)
    serializer = JSONSerializer()

    restored_dict = serializer.deserialize(
        serializer.serialize(backend_queue._request_to_dict(request))
    )
    backend_queue._decode_body(restored_dict)
    restored = request_from_dict(restored_dict, spider=bench_spider)

    assert restored.url == request.url
    assert restored.method == request.method
    assert restored.body == request.body
    assert restored.priority == request.priority
    assert restored.encoding == request.encoding
    assert restored.callback == bench_spider.parse_item
    assert restored.cb_kwargs == request.cb_kwargs
    assert dict(restored.headers.to_unicode_dict()) == dict(
        request.headers.to_unicode_dict(),
    )
