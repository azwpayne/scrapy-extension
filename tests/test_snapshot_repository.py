"""Regression tests for transactional chunked queue snapshots."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.snapshot import (
    SnapshotRepository,
    SnapshotRepositoryError,
)

_KEY = "queue:snapshot:v3:0::1:q"


def _storage(initial: dict[str, bytes] | None = None):
    values = dict(initial or {})
    storage = MagicMock()
    storage.retrieve.side_effect = lambda key: values.get(key)
    storage.store.side_effect = lambda key, value: values.__setitem__(key, value)
    storage.delete.side_effect = lambda key: values.pop(key, None)
    return storage, values


def test_commit_writes_chunks_before_manifest_and_round_trips() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    repository.commit(_KEY, b"abcdefghij")

    keys = [call.args[0] for call in storage.store.call_args_list]
    assert keys[-1] == _KEY
    assert all(":generation:" in key for key in keys[:-1])
    assert repository.read(_KEY).state == b"abcdefghij"
    manifest = json.loads(values[_KEY])
    assert manifest["version"] == 4
    assert manifest["length"] == 10
    assert manifest["chunks"] == 3


@pytest.mark.parametrize("failed_chunk", [0, 1, 2])
def test_each_chunk_failure_leaves_old_manifest_authoritative(failed_chunk: int) -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"old-state")
    old_manifest = values[_KEY]
    writes = 0

    def fail_selected_chunk(key: str, value: bytes) -> None:
        nonlocal writes
        if key != _KEY:
            current = writes
            writes += 1
            if current == failed_chunk:
                raise RuntimeError("secret chunk failure")
        values[key] = value

    storage.store.side_effect = fail_selected_chunk
    with pytest.raises(SnapshotRepositoryError, match="chunk write") as exc_info:
        repository.commit(_KEY, b"new-state!")

    assert exc_info.value.__cause__ is None
    assert values[_KEY] == old_manifest
    assert repository.read(_KEY).state == b"old-state"


def test_manifest_failure_leaves_old_manifest_authoritative() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"old")
    old_manifest = values[_KEY]

    def fail_manifest(key: str, value: bytes) -> None:
        if key == _KEY:
            raise RuntimeError("secret manifest failure")
        values[key] = value

    storage.store.side_effect = fail_manifest
    with pytest.raises(SnapshotRepositoryError, match="manifest write"):
        repository.commit(_KEY, b"replacement")

    assert values[_KEY] == old_manifest
    assert repository.read(_KEY).state == b"old"


def test_empty_state_is_a_committed_authoritative_manifest() -> None:
    storage, values = _storage({_KEY: b"legacy raw state"})
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    repository.commit(_KEY, None)

    result = repository.read(_KEY)
    assert result.found is True
    assert result.manifest is True
    assert result.state is None
    assert json.loads(values[_KEY])["chunks"] == 0


def test_checksum_corruption_is_rejected() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"abcdefgh")
    manifest = json.loads(values[_KEY])
    chunk_key = repository._chunk_key(_KEY, manifest["generation"], 1)
    values[chunk_key] = b"WXYZ"

    with pytest.raises(SnapshotRepositoryError, match="checksum"):
        repository.read(_KEY)


def test_chunk_length_and_manifest_schema_are_validated() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"abcdef")
    manifest = json.loads(values[_KEY])
    chunk_key = repository._chunk_key(_KEY, manifest["generation"], 0)
    values[chunk_key] = b"x"
    with pytest.raises(SnapshotRepositoryError, match="chunk length"):
        repository.read(_KEY)

    manifest["unknown"] = True
    values[_KEY] = json.dumps(manifest).encode()
    with pytest.raises(SnapshotRepositoryError, match="schema"):
        repository.read(_KEY)


def test_logical_cap_is_symmetric_and_prewrite() -> None:
    storage, values = _storage({_KEY: b"old"})
    repository = SnapshotRepository(storage, max_bytes=4, chunk_bytes=2)

    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.commit(_KEY, b"oversize")
    storage.store.assert_not_called()
    assert values[_KEY] == b"old"

    values[_KEY] = b"oversize"
    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.read(_KEY)


def test_legacy_raw_value_remains_readable() -> None:
    storage, _ = _storage({_KEY: b"legacy-v3-or-v2-payload"})
    result = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4).read(_KEY)
    assert result.found is True
    assert result.manifest is False
    assert result.state == b"legacy-v3-or-v2-payload"
