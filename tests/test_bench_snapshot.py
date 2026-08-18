"""Representative chunked snapshot repository benchmark without pass thresholds."""

from __future__ import annotations

from typing import Any

import pytest

from scrapy_extension.queue.snapshot import SnapshotRepository

_BENCHMARK_ROUNDS = 5
_PAYLOAD = bytes(range(256)) * (8 * 1024)  # 2 MiB, 32 x 64 KiB chunks.
_KEY = "queue:snapshot:v3:0::18:benchmark-frontier"


class _MemoryStorage:
    """Minimal backend-shaped in-memory store for repository overhead."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def store(self, key: str, value: bytes) -> None:
        self.values[key] = value

    def retrieve(self, key: str) -> bytes | None:
        return self.values.get(key)


@pytest.mark.benchmark
def test_multi_chunk_snapshot_write_read_roundtrip(benchmark: Any) -> None:
    """Measure a representative 2 MiB snapshot commit plus validated read."""
    storage = _MemoryStorage()
    repository = SnapshotRepository(
        storage,
        max_bytes=len(_PAYLOAD),
        chunk_bytes=64 * 1024,
    )

    def roundtrip() -> bytes | None:
        repository.commit(_KEY, _PAYLOAD)
        return repository.read(_KEY).state

    result = benchmark.pedantic(roundtrip, rounds=_BENCHMARK_ROUNDS)

    assert result == _PAYLOAD
    assert len(storage.values) >= 33
