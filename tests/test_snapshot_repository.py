"""Regression tests for transactional chunked queue snapshots."""

from __future__ import annotations

import json
import threading
import traceback
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.snapshot import (
    SnapshotRepository,
    SnapshotRepositoryError,
)

_KEY = "queue:snapshot:v3:0::1:q"
_SECRET_MARKER = "snapshot-buffer-private-marker"


def _assert_no_secret_object_graph(value: object, seen: set[int]) -> None:
    identity = id(value)
    if identity in seen:
        return
    seen.add(identity)
    if isinstance(value, (str, bytes, bytearray)):
        assert _SECRET_MARKER not in repr(value)
        return
    if isinstance(value, memoryview):
        try:
            assert _SECRET_MARKER.encode() not in value.tobytes()
        except ValueError:
            pass
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_no_secret_object_graph(key, seen)
            _assert_no_secret_object_graph(item, seen)
        return
    if isinstance(value, Sequence):
        for item in value:
            _assert_no_secret_object_graph(item, seen)
        return
    if isinstance(value, BaseException):
        assert _SECRET_MARKER not in str(value)
        _assert_no_secret_object_graph(value.__dict__, seen)
        for linked in (value.__cause__, value.__context__):
            if linked is not None:
                _assert_no_secret_object_graph(linked, seen)


def _assert_package_frames_cleared(error: BaseException) -> None:
    current = error.__traceback__
    while current is not None:
        frame = current.tb_frame
        if "/src/scrapy_extension/" in frame.f_code.co_filename:
            for name, value in frame.f_locals.items():
                # Repository ownership intentionally reaches backend state. The
                # terminal boundary must instead remove secret operation locals.
                if name != "self":
                    _assert_no_secret_object_graph(value, set())
        current = current.tb_next


def _assert_static_repository_error(error: SnapshotRepositoryError) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _SECRET_MARKER not in "".join(traceback.format_exception(error))
    _assert_no_secret_object_graph(error, set())
    _assert_package_frames_cleared(error)


def _storage(initial: dict[str, bytes] | None = None):
    values = dict(initial or {})
    storage = MagicMock()
    storage.retrieve.side_effect = lambda key: values.get(key)
    storage.store.side_effect = lambda key, value: values.__setitem__(key, value)
    storage.delete.side_effect = lambda key: values.pop(key, None)
    return storage, values


@pytest.mark.parametrize("failure_kind", ["chunk", "manifest"])
def test_effect_then_raise_chunk_and_manifest_writes_are_verified(
    failure_kind: str,
) -> None:
    storage, values = _storage()
    failed = False

    def store(key: str, value: bytes) -> None:
        nonlocal failed
        values[key] = value
        is_chunk = key.startswith("queue:snapshot-chunk:v1:")
        if not failed and (is_chunk == (failure_kind == "chunk")):
            failed = True
            raise RuntimeError("response lost after write")

    storage.store.side_effect = store
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    repository.commit(_KEY, b"effect-then-raise")

    assert failed is True
    assert repository.read(_KEY).state == b"effect-then-raise"


def test_ambiguous_manifest_write_retry_establishes_new_authority() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"old")
    manifest_failed = False
    verification_read = False

    def store(key: str, value: bytes) -> None:
        nonlocal manifest_failed
        values[key] = value
        if key == _KEY and not manifest_failed:
            manifest_failed = True
            raise RuntimeError("response lost after manifest write")

    def retrieve(key: str) -> bytes | None:
        nonlocal verification_read
        if key == _KEY and manifest_failed and not verification_read:
            verification_read = True
            raise RuntimeError("readback unavailable")
        return values.get(key)

    storage.store.side_effect = store
    storage.retrieve.side_effect = retrieve

    with pytest.raises(SnapshotRepositoryError, match="manifest write"):
        repository.commit(_KEY, b"ambiguous")

    assert repository.read(_KEY).state == b"ambiguous"
    repository.commit(_KEY, b"retry-authoritative")
    assert repository.read(_KEY).state == b"retry-authoritative"


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
    assert manifest["version"] == 6
    assert manifest["state"] == "bytes"
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


def test_chunk_write_failure_has_no_recursive_secret_graph() -> None:
    storage, _ = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    storage.store.side_effect = RuntimeError(_SECRET_MARKER)

    with pytest.raises(SnapshotRepositoryError, match="chunk write") as exc_info:
        repository.commit(_KEY, _SECRET_MARKER.encode())

    _assert_static_repository_error(exc_info.value)


@pytest.mark.parametrize(
    "failure_point", ["uuid", "hash", "manifest", "manifest-encode"]
)
def test_commit_construction_failures_are_static_and_clear_payload_frames(
    failure_point: str, mocker: Any
) -> None:
    payload = _SECRET_MARKER.encode()
    storage, _ = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    failure = MemoryError(_SECRET_MARKER)

    if failure_point == "uuid":
        mocker.patch("scrapy_extension.queue.snapshot.uuid.uuid4", side_effect=failure)
    elif failure_point == "hash":
        real_sha256 = __import__("hashlib").sha256

        def fail_payload_hash(value: bytes = b"") -> Any:
            if value == payload:
                raise failure
            return real_sha256(value)

        mocker.patch(
            "scrapy_extension.queue.snapshot.hashlib.sha256",
            side_effect=fail_payload_hash,
        )
    elif failure_point == "manifest":
        mocker.patch("scrapy_extension.queue.snapshot._Manifest", side_effect=failure)
    else:
        mocker.patch.object(repository, "_encode_manifest", side_effect=failure)

    with pytest.raises(SnapshotRepositoryError, match="construction") as exc_info:
        repository.commit(_KEY, payload)

    _assert_static_repository_error(exc_info.value)


class _SliceBomb(bytes):
    failure: BaseException = MemoryError(_SECRET_MARKER)

    def __getitem__(self, key: object) -> bytes:
        raise self.failure


class _AssemblyBomb(bytearray):
    failure: BaseException = MemoryError(_SECRET_MARKER)

    def extend(self, value: object) -> None:
        super().extend(value)  # type: ignore[arg-type]
        raise self.failure


def test_slice_failure_is_static_and_clears_adversarial_payload(
    mocker: Any,
) -> None:
    payload = _SECRET_MARKER.encode()
    storage, _ = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    hostile = _SliceBomb(payload)
    mocker.patch.object(repository, "_copy_buffer", return_value=(hostile, None))

    with pytest.raises(SnapshotRepositoryError, match="construction") as exc_info:
        repository.commit(_KEY, payload)

    _assert_static_repository_error(exc_info.value)


def test_buffer_assembly_failure_is_static_after_clearing_partial_state() -> None:
    payload = _SECRET_MARKER.encode()
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    repository.commit(_KEY, payload)
    assembly = _AssemblyBomb()
    assembly.failure = MemoryError(_SECRET_MARKER)

    with patch("builtins.bytearray", return_value=assembly):
        with pytest.raises(SnapshotRepositoryError, match="assembly") as exc_info:
            repository.read(_KEY)

    assert assembly == b""
    _assert_static_repository_error(exc_info.value)
    assert values


@pytest.mark.parametrize("failure_point", ["slice", "assembly"])
def test_construction_control_errors_propagate_only_after_payload_cleanup(
    failure_point: str, mocker: Any
) -> None:
    class _ControlFlow(BaseException):
        pass

    payload = _SECRET_MARKER.encode()
    control_error = _ControlFlow("stop")
    storage, _ = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)

    if failure_point == "slice":
        hostile = _SliceBomb(payload)
        hostile.failure = control_error
        mocker.patch.object(repository, "_copy_buffer", return_value=(hostile, None))
        operation = lambda: repository.commit(_KEY, payload)
    else:
        repository.commit(_KEY, payload)
        assembly = _AssemblyBomb()
        assembly.failure = control_error

        def operation() -> None:
            with patch("builtins.bytearray", return_value=assembly):
                repository.read(_KEY)

    with pytest.raises(_ControlFlow) as exc_info:
        operation()

    assert exc_info.value is control_error
    _assert_package_frames_cleared(exc_info.value)


@pytest.mark.parametrize(
    ("state", "discriminator"),
    [(None, "none"), (b"", "bytes")],
)
def test_zero_length_states_are_distinct_authoritative_manifests(
    state: bytes | None, discriminator: str
) -> None:
    storage, values = _storage({_KEY: b"legacy raw state"})
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)

    repository.commit(_KEY, state)

    result = repository.read(_KEY)
    assert result.found is True
    assert result.manifest is True
    if state is None:
        assert result.state is None
    else:
        assert result.state == b""
        assert result.state is not None
    manifest = json.loads(values[_KEY])
    assert manifest["version"] == 6
    assert manifest["state"] == discriminator
    assert manifest["chunks"] == 0


def test_empty_bytes_checksum_corruption_is_rejected_and_redacted() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, b"")
    manifest = json.loads(values[_KEY])
    manifest["sha256"] = "0" * 64
    values[_KEY] = json.dumps(manifest).encode()

    with pytest.raises(SnapshotRepositoryError, match="checksum") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


def test_checksum_corruption_is_rejected() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    repository.commit(_KEY, _SECRET_MARKER.encode())
    manifest = json.loads(values[_KEY])
    manifest["sha256"] = "0" * 64
    values[_KEY] = json.dumps(manifest).encode()

    with pytest.raises(SnapshotRepositoryError, match="checksum") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


def test_chunk_length_failure_has_no_recursive_secret_graph() -> None:
    storage, values = _storage()
    payload = _SECRET_MARKER.encode() + b"x"
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    repository.commit(_KEY, payload)
    manifest = json.loads(values[_KEY])
    chunk_key = repository._chunk_key(_KEY, manifest["generation"], 0)
    values[chunk_key] = _SECRET_MARKER.encode()

    with pytest.raises(SnapshotRepositoryError, match="chunk length") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


def test_manifest_schema_failure_has_no_recursive_secret_graph() -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    repository.commit(_KEY, b"state")
    manifest = json.loads(values[_KEY])
    manifest[_SECRET_MARKER] = {_SECRET_MARKER: [_SECRET_MARKER]}
    values[_KEY] = json.dumps(manifest).encode()

    with pytest.raises(SnapshotRepositoryError, match="schema") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


@pytest.mark.parametrize("malformation", ["missing", "unknown", "none-with-bytes"])
def test_v6_discriminator_is_strictly_validated_before_chunk_retrieval(
    malformation: str,
) -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)
    repository.commit(_KEY, _SECRET_MARKER.encode())
    manifest = json.loads(values[_KEY])
    if malformation == "missing":
        del manifest["state"]
    elif malformation == "unknown":
        manifest["state"] = _SECRET_MARKER
    else:
        manifest["state"] = "none"
    values[_KEY] = json.dumps(manifest).encode()
    storage.retrieve.reset_mock()

    with pytest.raises(SnapshotRepositoryError, match="schema") as exc_info:
        repository.read(_KEY)

    assert storage.retrieve.call_args_list == [call(_KEY)]
    _assert_static_repository_error(exc_info.value)


def test_parser_recursion_error_is_terminally_redacted(mocker: Any) -> None:
    storage, _ = _storage({_KEY: _SECRET_MARKER.encode()})
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    mocker.patch(
        "scrapy_extension.queue.snapshot.json.loads",
        side_effect=RecursionError(_SECRET_MARKER),
    )

    with pytest.raises(SnapshotRepositoryError, match="parsing") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


@pytest.mark.parametrize(
    "control_error", [KeyboardInterrupt("stop"), SystemExit("stop")]
)
def test_parser_process_control_errors_propagate(
    control_error: BaseException, mocker: Any
) -> None:
    storage, _ = _storage({_KEY: b"manifest"})
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=64)
    mocker.patch(
        "scrapy_extension.queue.snapshot.json.loads", side_effect=control_error
    )

    with pytest.raises(type(control_error), match="stop"):
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


@pytest.mark.parametrize("version", [4, 5])
def test_literal_historical_manifest_fixtures_remain_readable(version: int) -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "snapshots" / f"v{version}.json"
    fixture = json.loads(fixture_path.read_text())
    values = {
        fixture["logical_key"]: fixture["manifest"].encode(),
        **{key: value.encode() for key, value in fixture["chunks"].items()},
    }
    storage, _ = _storage(values)
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=5)

    result = repository.read(fixture["logical_key"])

    assert result.state == fixture["expected"].encode()
    assert result.manifest is True


def test_literal_raw_empty_legacy_fixture_remains_present() -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "snapshots" / "raw-empty.bin"
    storage, _ = _storage({_KEY: fixture_path.read_bytes()})

    result = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4).read(_KEY)

    assert result.found is True
    assert result.state == b""
    assert result.manifest is False


def test_malicious_tiny_chunks_are_rejected_before_chunk_retrieval() -> None:
    manifest = {
        "schema": "scrapy-extension.queue-strategy-snapshot",
        "version": 5,
        "generation": "0" * 32,
        "length": 4_097,
        "chunk_bytes": 1,
        "chunks": 4_097,
        "sha256": "0" * 64,
    }
    storage, _ = _storage({_KEY: json.dumps(manifest).encode()})
    repository = SnapshotRepository(storage, max_bytes=8_192, chunk_bytes=2)

    with pytest.raises(SnapshotRepositoryError, match="size limit"):
        repository.read(_KEY)

    storage.retrieve.assert_called_once_with(_KEY)


def test_legacy_raw_value_remains_readable() -> None:
    storage, _ = _storage({_KEY: b"legacy-v3-or-v2-payload"})
    result = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4).read(_KEY)
    assert result.found is True
    assert result.manifest is False
    assert result.state == b"legacy-v3-or-v2-payload"


@pytest.mark.parametrize("version", [4, 5])
@pytest.mark.parametrize(
    ("payload", "expected"),
    [(b"migrated-state", b"migrated-state"), (b"", None)],
)
def test_v4_and_v5_manifests_remain_readable_and_next_commit_rewrites_v6(
    version: int, payload: bytes, expected: bytes | None
) -> None:
    storage, values = _storage()
    repository = SnapshotRepository(storage, max_bytes=32, chunk_bytes=4)
    repository.commit(_KEY, payload)
    manifest = json.loads(values[_KEY])
    generation = manifest["generation"]
    if version == 4:
        for index in range(manifest["chunks"]):
            current_key = repository._chunk_key(_KEY, generation, index)
            legacy_key = repository._v4_chunk_key(_KEY, generation, index)
            values[legacy_key] = values.pop(current_key)
    manifest["version"] = version
    del manifest["state"]
    values[_KEY] = json.dumps(manifest).encode()

    result = repository.read(_KEY)

    assert result.manifest is True
    assert result.state == expected
    repository.commit(_KEY, result.state)
    rewritten = json.loads(values[_KEY])
    assert rewritten["version"] == 6
    assert rewritten["state"] == ("bytes" if expected is not None else "none")


class _CopyBombBytearray(bytearray):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.copy_attempted = False

    def __bytes__(self) -> bytes:
        self.copy_attempted = True
        raise AssertionError("oversized backend value must not be copied")


class _LengthLiarBytes(bytes):
    def __len__(self) -> int:
        return 10**9


class _OversizedReturnBytes(bytes):
    conversion_attempted = False

    def __bytes__(self) -> bytes:
        type(self).conversion_attempted = True
        return _SECRET_MARKER.encode() * (1024 * 1024)


class _RaisingConversionBytes(bytes):
    conversion_attempted = False

    def __bytes__(self) -> bytes:
        type(self).conversion_attempted = True
        raise RuntimeError(_SECRET_MARKER)


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


def test_length_liar_uses_buffer_nbytes_for_commit_and_read() -> None:
    storage, values = _storage({_KEY: _LengthLiarBytes(b"legacy")})
    repository = SnapshotRepository(storage, max_bytes=16, chunk_bytes=4)

    assert repository.read(_KEY).state == b"legacy"
    repository.commit(_KEY, _LengthLiarBytes(b"bounded"))

    assert repository.read(_KEY).state == b"bounded"
    assert all(type(value) is bytes for value in values.values())


@pytest.mark.parametrize(
    "hostile_type", [_OversizedReturnBytes, _RaisingConversionBytes]
)
def test_overridden_bytes_conversion_is_never_called(
    hostile_type: type[bytes],
) -> None:
    hostile_type.conversion_attempted = False  # type: ignore[attr-defined]
    storage, _ = _storage({_KEY: hostile_type(b"legacy")})
    repository = SnapshotRepository(storage, max_bytes=16, chunk_bytes=4)

    assert repository.read(_KEY).state == b"legacy"
    repository.commit(_KEY, hostile_type(b"bounded"))

    assert repository.read(_KEY).state == b"bounded"
    assert hostile_type.conversion_attempted is False  # type: ignore[attr-defined]


@pytest.mark.timeout(10)
def test_readonly_alias_of_mutable_exporter_is_rejected_without_secret_graph() -> None:
    exporter = bytearray(_SECRET_MARKER.encode())
    readonly_alias = memoryview(exporter).toreadonly()
    storage = MagicMock()
    storage.retrieve.return_value = readonly_alias
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)

    with pytest.raises(SnapshotRepositoryError, match="mutable") as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)
    assert readonly_alias.tobytes() == _SECRET_MARKER.encode()


def test_concurrently_mutable_manifest_is_rejected_without_a_secret_graph() -> None:
    value = bytearray(_SECRET_MARKER.encode())
    storage = MagicMock()
    storage.retrieve.return_value = value
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)
    started = threading.Event()
    stop = threading.Event()

    def mutate() -> None:
        started.set()
        replacement = ord("x")
        while not stop.is_set():
            value[0] = replacement
            replacement = ord("y") if replacement == ord("x") else ord("x")

    mutator = threading.Thread(target=mutate, daemon=True)
    mutator.start()
    assert started.wait(timeout=2.0)
    try:
        with pytest.raises(SnapshotRepositoryError, match="mutable") as exc_info:
            repository.read(_KEY)
    finally:
        stop.set()
        mutator.join(timeout=2.0)

    assert not mutator.is_alive()
    _assert_static_repository_error(exc_info.value)


@pytest.mark.parametrize(
    "backend_value, message",
    [
        (memoryview(b"noncontiguous")[::2], "not contiguous"),
        (memoryview(b"released"), "conversion failed"),
    ],
    ids=["noncontiguous", "released"],
)
def test_invalid_buffer_conversion_is_static_and_context_free(
    backend_value: memoryview, message: str
) -> None:
    if message == "conversion failed":
        backend_value.release()
    storage = MagicMock()
    storage.retrieve.return_value = backend_value
    repository = SnapshotRepository(storage, max_bytes=64, chunk_bytes=4)

    with pytest.raises(SnapshotRepositoryError, match=message) as exc_info:
        repository.read(_KEY)

    _assert_static_repository_error(exc_info.value)


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
