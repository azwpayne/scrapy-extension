"""Benchmarks for the request-serialization hot path (round-8 F4 + F1-SER).

The scientist pass measured ``BackendQueue.push`` at ~4.30 µs and ``pop`` at
~2.81 µs per op; the dominant cost is ``_request_to_dict`` + ``JSONSerializer``
(``json.dumps``), not the broker round-trip. These benchmarks pin the CPU cost
of that pure-Python transform so a future regression (slower serialization,
heavier request dict) shows up as a defensible number rather than a vibe.

Scope — what is measured here:
  - ``_request_to_dict`` (manual Request → dict, including base64 body).
  - ``JSONSerializer.serialize`` / ``.deserialize`` (``json.dumps`` / ``json.loads``).
  - The lossless round-trip through ``request_from_dict``.

Scope — what is NOT measured here:
  - Any backend / broker I/O. The connection manager is never touched on the
    serialize path; pop's deserialization uses a pre-built ``bytes`` payload.
  - Queue strategy cost (see ``test_bench_push_pop.py``).

Opt-in: every test carries ``@pytest.mark.benchmark`` and is skipped by the
root ``conftest.py`` unless ``--benchmark-only`` / ``--benchmark-enable`` is
passed. No hard perf thresholds are asserted — the gate is "runs and reports a
defensible number"; baselines come first. One non-perf sanity assertion pins
correctness alongside the measurement (lossless round-trip on key fields).
"""

from __future__ import annotations

from typing import Any

import pytest
from scrapy.http import Request
from scrapy.utils.request import request_from_dict

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.queue.queue import BackendQueue

#: Module-level marker so every test in this file is opted-in together.
pytestmark = pytest.mark.benchmark


class _NullConnectionManager:
  """Stand-in so ``BackendQueue`` builds without a backend.

  The serialization path (``_request_to_dict`` + ``JSONSerializer``) never
  touches the connection manager — it's a pure transform. Reused verbatim from
  ``tests/test_property_serialization.py`` to avoid importing test code.
  """


@pytest.fixture(scope="module")
def backend_queue() -> BackendQueue:
  """A ``BackendQueue`` whose serialize path is exercised; backend never called."""
  return BackendQueue(_NullConnectionManager(), "bench-serialization")


def _make_request() -> Request:
  """Representative Scrapy request: URL + headers + binary body + meta + callback."""
  # ``callback`` is set to a module-level function so ``request_from_dict`` can
  # resolve it back by qualified name during the round-trip sanity assertion.
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
    cookies={"session": "s3cr3t", "region": "us-west-2"},
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
    callback=_roundtrip_callback,
  )


def _roundtrip_callback(request: Request) -> Any:  # pragma: no cover - referenced by name only
  """Callback target resolvable by ``request_from_dict`` during round-trip."""
  del request
  return None


def test_request_to_dict(benchmark, backend_queue: BackendQueue) -> None:
  """Measure ``_request_to_dict`` (Request → JSON-encodable dict, no I/O).

  No threshold asserted — the gate is "runs and reports a number". Future
  regressions surface as a slower reported mean, not a red test.
  """
  request = _make_request()

  result = benchmark(backend_queue._request_to_dict, request)

  assert isinstance(result, dict)
  assert result["url"] == request.url
  assert result["method"] == "POST"
  assert result["body"] is not None  # base64 string


def test_serialize_deserialize_roundtrip(benchmark) -> None:
  """Measure ``JSONSerializer.serialize`` + ``.deserialize`` of the request dict.

  Pairs with ``test_request_to_dict`` to isolate JSON cost from manual-dict
  cost. No threshold asserted.
  """
  queue = BackendQueue(_NullConnectionManager(), "bench-serialization")
  request = _make_request()
  request_dict = queue._request_to_dict(request)
  serializer = JSONSerializer()

  def roundtrip() -> Any:
    data = serializer.serialize(request_dict)
    return serializer.deserialize(data)

  result = benchmark(roundtrip)

  assert result == request_dict


class _StubSpider:
  """Minimal stand-in so ``request_from_dict`` can resolve the callback name.

  ``request_from_dict`` looks up ``callback``/``errback`` as attributes on the
  spider by their stored function name; a bare object with the attribute set
  is sufficient (the real ``scrapy.Spider`` is overkill for this sanity test).
  """

  def __init__(self) -> None:
    self._roundtrip_callback = _roundtrip_callback


def test_full_roundtrip_is_lossless() -> None:
  """Sanity (NOT perf): full encode → decode restores key request fields.

  Pins correctness alongside the measurements above. This is the only
  non-perf-dependent assertion in the file and does not use the ``benchmark``
  fixture, but keeps the ``benchmark`` module marker so it lives with its
  peers and is skipped by the same opt-in gate during default runs.
  """
  queue = BackendQueue(_NullConnectionManager(), "bench-serialization")
  request = _make_request()

  request_dict = queue._request_to_dict(request)
  serializer = JSONSerializer()
  data = serializer.serialize(request_dict)
  restored_dict = serializer.deserialize(data)
  # Mirror the production decode path so the body is raw bytes for Scrapy.
  queue._decode_body(restored_dict)

  restored = request_from_dict(restored_dict, spider=_StubSpider())  # type: ignore[arg-type]

  assert restored.url == request.url
  assert restored.method == request.method
  assert restored.body == request.body
  assert restored.priority == request.priority
  assert restored.encoding == request.encoding
  assert dict(restored.headers.to_unicode_dict()) == dict(
    request.headers.to_unicode_dict(),
  )
