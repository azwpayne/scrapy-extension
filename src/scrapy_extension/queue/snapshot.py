"""Transactional repository for queue-strategy recovery snapshots."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

from scrapy_extension.backends.base import BackendType, _validate_key_name

DEFAULT_SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024
MAX_SNAPSHOT_CHUNK_BYTES = 256 * 1024
MAX_SNAPSHOT_CHUNKS = 4_096
DEFAULT_SNAPSHOT_CHUNK_BYTES = MAX_SNAPSHOT_CHUNK_BYTES

_MANIFEST_SCHEMA = "scrapy-extension.queue-strategy-snapshot"
_MANIFEST_VERSION = 6
_READABLE_MANIFEST_VERSIONS = frozenset({4, 5, _MANIFEST_VERSION})
_STATE_NONE = "none"
_STATE_BYTES = "bytes"
_MAX_MANIFEST_BYTES = 64 * 1024
_CHUNK_KEY_PREFIX = "queue:snapshot-chunk:v1:"
_GENERATION_RE = re.compile(r"^[0-9a-f]{32}$")
_CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")
_BACKEND_LOGICAL_KEY_LIMITS = {
    BackendType.MEMCACHED: 250,
    BackendType.ELASTICSEARCH: 512,
    BackendType.DYNAMODB: 2_048,
}
_BUFFER_TYPES = (bytes, bytearray, memoryview)
_BUFFER_INVALID = "invalid"
_BUFFER_NONCONTIGUOUS = "noncontiguous"
_BUFFER_MUTABLE = "mutable"
_BUFFER_CONVERSION_FAILED = "conversion-failed"
_BUFFER_OVERSIZED = "oversized"


class SnapshotRepositoryError(Exception):
    """A redacted snapshot repository failure."""


@dataclass(frozen=True, slots=True)
class SnapshotRead:
    """One repository lookup, including an explicitly committed empty state."""

    found: bool
    state: bytes | None
    manifest: bool


@dataclass(frozen=True, slots=True)
class _Manifest:
    version: int
    generation: str
    length: int
    chunk_bytes: int
    chunks: int
    checksum: str
    state_present: bool


class SnapshotRepository:
    """Store immutable generation chunks and publish their manifest last.

    The logical snapshot key contains only the authoritative manifest. A failed
    chunk write cannot replace it, and a failed manifest write leaves the prior
    manifest authoritative. Existing raw values remain readable for in-place
    migration and are replaced only by a successful v6 commit.
    """

    def __init__(
        self,
        storage: Any,
        *,
        max_bytes: int = DEFAULT_SNAPSHOT_MAX_BYTES,
        chunk_bytes: int = DEFAULT_SNAPSHOT_CHUNK_BYTES,
    ) -> None:
        minimum_chunk_bytes = (
            (max_bytes + MAX_SNAPSHOT_CHUNKS - 1) // MAX_SNAPSHOT_CHUNKS
            if isinstance(max_bytes, int) and not isinstance(max_bytes, bool)
            else 1
        )
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes < 1
            or isinstance(chunk_bytes, bool)
            or not isinstance(chunk_bytes, int)
            or chunk_bytes < minimum_chunk_bytes
            or chunk_bytes > max_bytes
            or chunk_bytes > MAX_SNAPSHOT_CHUNK_BYTES
        ):
            raise ValueError("Invalid snapshot repository size limits.")
        self._storage = storage
        self._max_bytes = max_bytes
        self._chunk_bytes = chunk_bytes

    def _validate_logical_key(self, key: str) -> None:
        invalid = False
        try:
            try:
                _validate_key_name(key, "snapshot logical key")
            except (TypeError, ValueError):
                invalid = True
            if not invalid:
                if key.startswith(_CHUNK_KEY_PREFIX):
                    raise SnapshotRepositoryError(
                        "Snapshot logical key uses the reserved chunk namespace."
                    )

                raw_backend_type: object = getattr(self._storage, "backend_type", None)
                backend_type: BackendType | None = None
                if isinstance(raw_backend_type, BackendType):
                    backend_type = raw_backend_type
                elif isinstance(raw_backend_type, str):
                    try:
                        backend_type = BackendType(raw_backend_type)
                    except ValueError:
                        pass
                limit = (
                    None
                    if backend_type is None
                    else _BACKEND_LOGICAL_KEY_LIMITS.get(backend_type)
                )
                if limit is not None and len(key.encode("utf-8")) > limit:
                    raise SnapshotRepositoryError(
                        "Snapshot logical key exceeds the storage backend limit."
                    )
        finally:
            key = ""
        if invalid:
            # Raise only after the validation ValueError/TypeError and its private
            # frame have unwound, so the public error has no sensitive context.
            raise SnapshotRepositoryError("Snapshot logical key is invalid.") from None

    @staticmethod
    def _chunk_key(key: str, generation: str, index: int) -> str:
        identity = b""
        chunk_key = ""
        try:
            identity = json.dumps(
                [key, generation, index],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            chunk_key = f"{_CHUNK_KEY_PREFIX}{hashlib.sha256(identity).hexdigest()}"
            return chunk_key
        finally:
            key = ""
            generation = ""
            identity = b""
            chunk_key = ""

    @staticmethod
    def _v4_chunk_key(key: str, generation: str, index: int) -> str:
        """Return the physical chunk key used by the historical v4 format."""
        chunk_key = ""
        try:
            chunk_key = f"{key}:generation:{generation}:chunk:{index}"
            return chunk_key
        finally:
            key = ""
            generation = ""
            chunk_key = ""

    def _retrieve(self, key: str) -> tuple[Any, bool]:
        """Return backend data or a non-sensitive ordinary-failure status."""
        try:
            try:
                return self._storage.retrieve(key), False
            except Exception:
                return None, True
        finally:
            key = ""

    def _store(self, key: str, value: bytes) -> bool:
        """Store bytes, verifying an ordinary effect-then-raise result once."""
        expected = value
        observed: object = None
        copied: bytes | None = None
        try:
            try:
                self._storage.store(key, value)
            except Exception:
                # A backend may apply the write and then raise (for example, a
                # response is lost). One exact readback can prove this immutable
                # chunk or manifest committed; any ambiguity remains retryable.
                observed, retrieve_failed = self._retrieve(key)
                if retrieve_failed:
                    return False
                copied, copy_error = self._copy_buffer(observed, len(expected))
                observed = None
                return copy_error is None and copied == expected
            return True
        finally:
            key = ""
            value = b""
            expected = b""
            observed = None
            copied = None

    @staticmethod
    def _copy_buffer(value: object, maximum: int) -> tuple[bytes | None, str | None]:
        """Copy one immutable contiguous byte buffer without calling its hooks."""
        view: memoryview | None = None
        copied: bytes | None = None
        exporter: object = None
        try:
            if not isinstance(value, _BUFFER_TYPES):
                return None, _BUFFER_INVALID

            view_failed = False
            try:
                # Constructing the builtin view uses the buffer protocol directly. In
                # particular, bytes/bytearray subclass ``__len__`` and ``__bytes__``
                # overrides cannot influence either the bound or the copy below.
                view = memoryview(value)
            except Exception:
                view_failed = True
            value = None
            if view_failed or view is None:
                return None, _BUFFER_CONVERSION_FAILED

            size = view.nbytes
            if size > maximum:
                return None, _BUFFER_OVERSIZED
            if not view.c_contiguous:
                return None, _BUFFER_NONCONTIGUOUS
            # Read-only views can still alias mutable exporters (for example,
            # ``memoryview(bytearray(...)).toreadonly()``). Accept only buffers
            # whose root exporter is provably immutable bytes.
            exporter = view.obj
            while isinstance(exporter, memoryview):
                exporter = exporter.obj
            if not view.readonly or not isinstance(exporter, bytes):
                return None, _BUFFER_MUTABLE
            exporter = None

            copy_failed = False
            try:
                copied = view.tobytes()
            except Exception:
                copy_failed = True
            if copy_failed or copied is None:
                return None, _BUFFER_CONVERSION_FAILED
            # Revalidate the builtin result before any parser, hash, or backend
            # sees it. This also keeps an unexpected exporter conversion bounded.
            if len(copied) != size:
                return None, _BUFFER_CONVERSION_FAILED
            if len(copied) > maximum:
                return None, _BUFFER_OVERSIZED
            return copied, None
        finally:
            value = None
            exporter = None
            copied = None
            if view is not None:
                view.release()
            view = None

    @staticmethod
    def _decode_manifest(
        value: bytes,
    ) -> tuple[_Manifest | None, str | None]:
        """Parse a manifest without exporting parser objects into an error graph."""
        if len(value) > _MAX_MANIFEST_BYTES:
            return None, None
        decoded: Any = None
        try:
            try:
                decoded = json.loads(value)
            except (UnicodeDecodeError, json.JSONDecodeError):
                return None, None
            except Exception:
                # RecursionError and every other ordinary parser failure are
                # repository failures. Process-control BaseExceptions still pass
                # through the finally block below and propagate unchanged.
                return None, "Snapshot manifest parsing failed."
            try:
                if (
                    not isinstance(decoded, dict)
                    or decoded.get("schema") != _MANIFEST_SCHEMA
                ):
                    return None, None
                version = decoded.get("version")
                legacy_required = {
                    "schema",
                    "version",
                    "generation",
                    "length",
                    "chunk_bytes",
                    "chunks",
                    "sha256",
                }
                required = (
                    legacy_required | {"state"}
                    if version == _MANIFEST_VERSION
                    else legacy_required
                )
                if (
                    isinstance(version, bool)
                    or not isinstance(version, int)
                    or version not in _READABLE_MANIFEST_VERSIONS
                    or set(decoded) != required
                ):
                    return None, "Snapshot manifest schema is invalid."
                generation = decoded.get("generation")
                length = decoded.get("length")
                chunk_bytes = decoded.get("chunk_bytes")
                chunks = decoded.get("chunks")
                checksum = decoded.get("sha256")
                state_kind = (
                    decoded.get("state") if version == _MANIFEST_VERSION else None
                )
                if (
                    not isinstance(generation, str)
                    or _GENERATION_RE.fullmatch(generation) is None
                    or isinstance(length, bool)
                    or not isinstance(length, int)
                    or length < 0
                    or isinstance(chunk_bytes, bool)
                    or not isinstance(chunk_bytes, int)
                    or chunk_bytes < 1
                    or isinstance(chunks, bool)
                    or not isinstance(chunks, int)
                    or chunks < 0
                    or not isinstance(checksum, str)
                    or _CHECKSUM_RE.fullmatch(checksum) is None
                    or chunks != ((length + chunk_bytes - 1) // chunk_bytes)
                    or (
                        version == _MANIFEST_VERSION
                        and state_kind not in {_STATE_NONE, _STATE_BYTES}
                    )
                    or (state_kind == _STATE_NONE and length != 0)
                ):
                    return None, "Snapshot manifest schema is invalid."
                return (
                    _Manifest(
                        version,
                        generation,
                        length,
                        chunk_bytes,
                        chunks,
                        checksum,
                        state_kind == _STATE_BYTES
                        if version == _MANIFEST_VERSION
                        else length > 0,
                    ),
                    None,
                )
            except Exception:
                return None, "Snapshot manifest schema is invalid."
        finally:
            decoded = None
            value = b""

    @staticmethod
    def _encode_manifest(manifest: _Manifest) -> bytes:
        return json.dumps(
            {
                "schema": _MANIFEST_SCHEMA,
                "version": _MANIFEST_VERSION,
                "generation": manifest.generation,
                "length": manifest.length,
                "chunk_bytes": manifest.chunk_bytes,
                "chunks": manifest.chunks,
                "sha256": manifest.checksum,
                "state": _STATE_BYTES if manifest.state_present else _STATE_NONE,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def _read_terminal(self, key: str) -> tuple[SnapshotRead | None, str | None]:
        """Reconstruct a snapshot and return only a static failure status."""
        value: object = None
        raw: bytes | None = None
        manifest: _Manifest | None = None
        assembled: bytearray | None = None
        chunk: object = None
        copied_chunk: bytes | None = None
        state: bytes | None = None
        chunk_key = ""
        try:
            value, retrieve_failed = self._retrieve(key)
            if retrieve_failed:
                return None, "Snapshot manifest retrieval failed."
            if value is None:
                return SnapshotRead(False, None, False), None
            raw, buffer_error = self._copy_buffer(
                value, max(self._max_bytes, _MAX_MANIFEST_BYTES)
            )
            value = None
            if buffer_error == _BUFFER_INVALID:
                return None, "Snapshot manifest has an invalid type."
            if buffer_error == _BUFFER_NONCONTIGUOUS:
                return None, "Snapshot manifest is not contiguous."
            if buffer_error == _BUFFER_MUTABLE:
                return None, "Snapshot manifest is mutable."
            if buffer_error == _BUFFER_OVERSIZED:
                return None, "Snapshot exceeds the configured size limit."
            if buffer_error is not None or raw is None:
                return None, "Snapshot manifest conversion failed."
            manifest, manifest_error = self._decode_manifest(raw)
            if manifest_error is not None:
                return None, manifest_error
            if manifest is None:
                if len(raw) > self._max_bytes:
                    return None, "Snapshot exceeds the configured size limit."
                return SnapshotRead(True, raw, False), None
            raw = None
            minimum_chunk_bytes = (
                self._max_bytes + MAX_SNAPSHOT_CHUNKS - 1
            ) // MAX_SNAPSHOT_CHUNKS
            if (
                manifest.length > self._max_bytes
                or manifest.chunk_bytes > self._chunk_bytes
                or manifest.chunk_bytes < minimum_chunk_bytes
                or manifest.chunks > MAX_SNAPSHOT_CHUNKS
            ):
                return None, "Snapshot exceeds the configured size limit."
            if manifest.length == 0:
                if manifest.checksum != hashlib.sha256(b"").hexdigest():
                    return None, "Snapshot checksum validation failed."
                empty_state = b"" if manifest.state_present else None
                return SnapshotRead(True, empty_state, True), None
            assembled = bytearray()
            for index in range(manifest.chunks):
                chunk_key = (
                    self._v4_chunk_key(key, manifest.generation, index)
                    if manifest.version == 4
                    else self._chunk_key(key, manifest.generation, index)
                )
                chunk, retrieve_failed = self._retrieve(chunk_key)
                if retrieve_failed:
                    return None, "Snapshot chunk retrieval failed."
                expected = min(
                    manifest.chunk_bytes,
                    manifest.length - (index * manifest.chunk_bytes),
                )
                copied_chunk, buffer_error = self._copy_buffer(chunk, expected)
                chunk = None
                if buffer_error == _BUFFER_INVALID:
                    return None, "Snapshot chunk is missing or invalid."
                if buffer_error == _BUFFER_NONCONTIGUOUS:
                    return None, "Snapshot chunk is not contiguous."
                if buffer_error == _BUFFER_MUTABLE:
                    return None, "Snapshot chunk is mutable."
                if buffer_error == _BUFFER_OVERSIZED:
                    return None, "Snapshot chunk length validation failed."
                if buffer_error is not None or copied_chunk is None:
                    return None, "Snapshot chunk conversion failed."
                if len(copied_chunk) != expected:
                    return None, "Snapshot chunk length validation failed."
                assembled.extend(copied_chunk)
                copied_chunk = None
            state = bytes(assembled)
            assembled.clear()
            if len(state) != manifest.length:
                return None, "Snapshot length validation failed."
            if hashlib.sha256(state).hexdigest() != manifest.checksum:
                return None, "Snapshot checksum validation failed."
            return SnapshotRead(True, state, True), None
        except Exception:
            return None, "Snapshot assembly failed."
        finally:
            value = None
            raw = None
            manifest = None
            chunk = None
            copied_chunk = None
            state = None
            key = ""
            chunk_key = ""
            if assembled is not None:
                assembled.clear()
            assembled = None

    def read(self, key: str) -> SnapshotRead:
        """Read and fully validate one committed logical snapshot."""
        result: SnapshotRead | None = None
        try:
            self._validate_logical_key(key)
            result, failure_message = self._read_terminal(key)
            if failure_message is not None:
                raise SnapshotRepositoryError(failure_message) from None
            assert result is not None
            return result
        finally:
            key = ""
            result = None

    def _commit_terminal(self, key: str, state: bytes | None) -> str | None:
        """Write a snapshot and return only a static failure status."""
        payload = b""
        copied_payload: bytes | None = None
        chunk = b""
        manifest: _Manifest | None = None
        manifest_bytes = b""
        generation = ""
        chunk_key = ""
        state_present = state is not None
        try:
            buffer_error: str | None = None
            if state is not None:
                if not isinstance(state, bytes):
                    return "Strategy snapshot has an invalid type."
                copied_payload, buffer_error = self._copy_buffer(state, self._max_bytes)
                state = None
                if buffer_error == _BUFFER_NONCONTIGUOUS:
                    return "Strategy snapshot is not contiguous."
                if buffer_error == _BUFFER_MUTABLE:
                    return "Strategy snapshot is mutable."
                if buffer_error == _BUFFER_OVERSIZED:
                    return "Snapshot exceeds the configured size limit."
                if buffer_error is not None or copied_payload is None:
                    return "Strategy snapshot conversion failed."
                payload = copied_payload
                copied_payload = None
            length = len(payload)
            if length > self._max_bytes:
                return "Snapshot exceeds the configured size limit."
            generation = uuid.uuid4().hex
            chunks = (length + self._chunk_bytes - 1) // self._chunk_bytes
            for index in range(chunks):
                start = index * self._chunk_bytes
                chunk = payload[start : start + self._chunk_bytes]
                chunk_key = self._chunk_key(key, generation, index)
                if not self._store(chunk_key, chunk):
                    return "Snapshot chunk write failed."
                chunk_key = ""
                chunk = b""
            manifest = _Manifest(
                version=_MANIFEST_VERSION,
                generation=generation,
                length=length,
                chunk_bytes=self._chunk_bytes,
                chunks=chunks,
                checksum=hashlib.sha256(payload).hexdigest(),
                state_present=state_present,
            )
            payload = b""
            manifest_bytes = self._encode_manifest(manifest)
            manifest = None
            if not self._store(key, manifest_bytes):
                return "Snapshot manifest write failed."
            return None
        except Exception:
            return "Snapshot construction failed."
        finally:
            key = ""
            state = None
            payload = b""
            copied_payload = None
            chunk = b""
            manifest = None
            manifest_bytes = b""
            generation = ""
            chunk_key = ""

    def commit(self, key: str, state: bytes | None) -> None:
        """Commit ``state`` by writing all generation chunks before its manifest."""
        failure_message: str | None = None
        try:
            self._validate_logical_key(key)
            failure_message = self._commit_terminal(key, state)
        finally:
            key = ""
            state = None
        if failure_message is not None:
            raise SnapshotRepositoryError(failure_message) from None
