"""Regression tests for transactional chunked queue snapshots."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.queue import BackendQueue
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
    assert all(key.startswith("queue:snapshot-chunk:v1:") for key in keys[:-1])
    assert len({len(key) for key in keys[:-1]}) == 1
    assert repository.read(_KEY).state == b"abcdefghij"
    manifest = json.loads(values[_KEY])
    assert manifest["version"] == 5
    assert manifest["length"] == 10
    assert manifest["chunks"] == 3


@pytest.mark.parametrize("failed_chunk", [0, 1, 2])
def test_each_chunk_failure_leaves_old_manifest_authoritative(
    failed_chunk: int,
) -> None:
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


class _CopyBombBytearray(bytearray):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.copy_attempted = False

    def __bytes__(self) -> bytes:
        self.copy_attempted = True
        raise AssertionError("oversized backend value must not be copied")


@pytest.mark.parametrize("use_memoryview", [False, True])
def test_oversized_bytes_like_manifest_is_rejected_before_copy(
    use_memoryview: bool,
) -> None:
    hostile = _CopyBombBytearray(b"x" * (65 * 1024))
    backend_value = memoryview(hostile) if use_memoryview else hostile
    storage = MagicMock()
    storage.retrieve.return_value = backend_value
    repository = SnapshotRepository(storage, max_bytes=4, chunk_bytes=2)

    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.read(_KEY)

    assert hostile.copy_attempted is False


@pytest.mark.parametrize("use_memoryview", [False, True])
def test_oversized_bytes_like_chunk_is_rejected_before_copy(
    use_memoryview: bool,
) -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=8, chunk_bytes=4)
    repository.commit(_KEY, b"state")
    manifest = json.loads(values[_KEY])
    first_chunk = repository._chunk_key(_KEY, manifest["generation"], 0)
    hostile = _CopyBombBytearray(b"x" * 5)
    values[first_chunk] = memoryview(hostile) if use_memoryview else hostile

    with pytest.raises(SnapshotRepositoryError, match="chunk length"):
        repository.read(_KEY)

    assert hostile.copy_attempted is False


def test_bounded_memoryview_manifest_and_chunks_round_trip() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=16, chunk_bytes=4)
    repository.commit(_KEY, b"bounded-state")
    for key, value in tuple(values.items()):
        values[key] = memoryview(value)

    assert repository.read(_KEY).state == b"bounded-state"


def _logical_key(
    queue_name: str, *, owner: str | None = None, spider: str | None = None
) -> str:
    queue = object.__new__(BackendQueue)
    queue.queue_name = queue_name
    queue._snapshot_owner = owner
    queue._spider = SimpleNamespace(name=spider) if spider is not None else None
    return queue._snapshot_key()


def test_chunk_keys_hash_complete_v2_v3_identity_generation_and_index() -> None:
    repository = SnapshotRepository(MagicMock(), max_bytes=32, chunk_bytes=4)
    generation = "0" * 32
    v2_key = _logical_key("q", owner="o")
    old_v2_suffix_alias = f"{v2_key}:generation:{generation}:chunk:0"
    v2_adversarial_key = _logical_key(f"q:generation:{generation}:chunk:0", owner="o")
    v3_key = _logical_key("q")
    old_v3_suffix = f"{v3_key}:generation:{generation}:chunk:0"
    v3_adversarial_key = _logical_key(f"q:generation:{generation}:chunk:0")
    identities = [
        v2_key,
        v2_adversarial_key,
        v3_key,
        v3_adversarial_key,
        _logical_key("q" * 10_000, spider="spider:with:delimiters"),
    ]

    # The old v2 suffix scheme exactly aliases another valid logical key. V3's
    # queue-length frame avoids that exact equality, but both versions now share
    # the same dedicated namespace and fixed physical-key size.
    assert old_v2_suffix_alias == v2_adversarial_key
    assert old_v3_suffix != v3_adversarial_key

    keys = {
        repository._chunk_key(identity, generation, index)
        for identity in identities
        for index in (0, 1)
    }

    assert len(keys) == len(identities) * 2
    assert {len(key.encode("ascii")) for key in keys} == {88}
    assert all(key.startswith("queue:snapshot-chunk:v1:") for key in keys)
    assert keys.isdisjoint(identities)


def test_reserved_chunk_namespace_cannot_be_used_as_a_logical_key() -> None:
    storage, _ = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    chunk_key = repository._chunk_key(_KEY, "0" * 32, 0)

    with pytest.raises(SnapshotRepositoryError, match="reserved chunk namespace"):
        repository.commit(chunk_key, b"state")
    with pytest.raises(SnapshotRepositoryError, match="reserved chunk namespace"):
        repository.read(chunk_key)

    storage.store.assert_not_called()
    storage.retrieve.assert_not_called()


def test_chunk_size_cannot_exceed_universal_backend_safe_cap() -> None:
    with pytest.raises(ValueError, match="size limits"):
        SnapshotRepository(
            MagicMock(),
            max_bytes=(256 * 1024) + 1,
            chunk_bytes=(256 * 1024) + 1,
        )
