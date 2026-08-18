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
DEFAULT_SNAPSHOT_CHUNK_BYTES = MAX_SNAPSHOT_CHUNK_BYTES

_MANIFEST_SCHEMA = "scrapy-extension.queue-strategy-snapshot"
_MANIFEST_VERSION = 5
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
    generation: str
    length: int
    chunk_bytes: int
    chunks: int
    checksum: str


class SnapshotRepository:
    """Store immutable generation chunks and publish their manifest last.

    The logical snapshot key contains only the authoritative manifest. A failed
    chunk write cannot replace it, and a failed manifest write leaves the prior
    manifest authoritative. Existing raw values remain readable for in-place
    migration and are replaced only by a successful v5 commit.
    """

    def __init__(
        self,
        storage: Any,
        *,
        max_bytes: int = DEFAULT_SNAPSHOT_MAX_BYTES,
        chunk_bytes: int = DEFAULT_SNAPSHOT_CHUNK_BYTES,
    ) -> None:
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes < 1
            or isinstance(chunk_bytes, bool)
            or not isinstance(chunk_bytes, int)
            or chunk_bytes < 1
            or chunk_bytes > max_bytes
            or chunk_bytes > MAX_SNAPSHOT_CHUNK_BYTES
        ):
            raise ValueError("Invalid snapshot repository size limits.")
        self._storage = storage
        self._max_bytes = max_bytes
        self._chunk_bytes = chunk_bytes

    def _validate_logical_key(self, key: str) -> None:
        try:
            _validate_key_name(key, "snapshot logical key")
        except (TypeError, ValueError):
            raise SnapshotRepositoryError("Snapshot logical key is invalid.") from None
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

    @staticmethod
    def _chunk_key(key: str, generation: str, index: int) -> str:
        identity = json.dumps(
            [key, generation, index],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"{_CHUNK_KEY_PREFIX}{hashlib.sha256(identity).hexdigest()}"

    def _retrieve(self, key: str, failure_message: str) -> Any:
        failed = False
        value: Any = None
        try:
            value = self._storage.retrieve(key)
        except Exception:
            failed = True
        if failed:
            raise SnapshotRepositoryError(failure_message)
        return value

    def _store(self, key: str, value: bytes, failure_message: str) -> None:
        failed = False
        try:
            self._storage.store(key, value)
        except Exception:
            failed = True
        if failed:
            raise SnapshotRepositoryError(failure_message)

    @staticmethod
    def _copy_buffer(
        value: object, maximum: int
    ) -> tuple[bytes | None, str | None]:
        """Copy one immutable contiguous byte buffer without calling its hooks."""
        if not isinstance(value, _BUFFER_TYPES):
            return None, _BUFFER_INVALID

        view: memoryview | None = None
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

        try:
            size = view.nbytes
            if size > maximum:
                return None, _BUFFER_OVERSIZED
            if not view.c_contiguous:
                return None, _BUFFER_NONCONTIGUOUS
            # A writable exporter can change bytes while a snapshot is being
            # checksummed or parsed. Reject it rather than accepting a torn copy.
            if not view.readonly:
                return None, _BUFFER_MUTABLE

            copied: bytes | None = None
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
            view.release()

    @staticmethod
    def _decode_manifest(value: bytes) -> _Manifest | None:
        if len(value) > _MAX_MANIFEST_BYTES:
            return None
        try:
            decoded = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(decoded, dict) or decoded.get("schema") != _MANIFEST_SCHEMA:
            return None
        required = {
            "schema",
            "version",
            "generation",
            "length",
            "chunk_bytes",
            "chunks",
            "sha256",
        }
        if set(decoded) != required or decoded.get("version") != _MANIFEST_VERSION:
            raise SnapshotRepositoryError("Snapshot manifest schema is invalid.")
        generation = decoded.get("generation")
        length = decoded.get("length")
        chunk_bytes = decoded.get("chunk_bytes")
        chunks = decoded.get("chunks")
        checksum = decoded.get("sha256")
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
        ):
            raise SnapshotRepositoryError("Snapshot manifest schema is invalid.")
        return _Manifest(generation, length, chunk_bytes, chunks, checksum)

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
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def read(self, key: str) -> SnapshotRead:
        """Read and fully validate one committed logical snapshot."""
        self._validate_logical_key(key)
        value = self._retrieve(key, "Snapshot manifest retrieval failed.")
        if value is None:
            return SnapshotRead(False, None, False)
        raw, buffer_error = self._copy_buffer(
            value, max(self._max_bytes, _MAX_MANIFEST_BYTES)
        )
        value = None
        if buffer_error == _BUFFER_INVALID:
            raise SnapshotRepositoryError("Snapshot manifest has an invalid type.")
        if buffer_error == _BUFFER_NONCONTIGUOUS:
            raise SnapshotRepositoryError("Snapshot manifest is not contiguous.")
        if buffer_error == _BUFFER_MUTABLE:
            raise SnapshotRepositoryError("Snapshot manifest is mutable.")
        if buffer_error == _BUFFER_OVERSIZED:
            raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
        if buffer_error is not None or raw is None:
            raise SnapshotRepositoryError("Snapshot manifest conversion failed.")
        manifest = self._decode_manifest(raw)
        if manifest is None:
            if len(raw) > self._max_bytes:
                raise SnapshotRepositoryError(
                    "Snapshot exceeds the configured size limit."
                )
            return SnapshotRead(True, raw, False)
        if (
            manifest.length > self._max_bytes
            or manifest.chunk_bytes > self._chunk_bytes
        ):
            raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
        if manifest.length == 0:
            if manifest.checksum != hashlib.sha256(b"").hexdigest():
                raise SnapshotRepositoryError("Snapshot checksum validation failed.")
            return SnapshotRead(True, None, True)
        assembled = bytearray()
        for index in range(manifest.chunks):
            chunk = self._retrieve(
                self._chunk_key(key, manifest.generation, index),
                "Snapshot chunk retrieval failed.",
            )
            expected = min(
                manifest.chunk_bytes,
                manifest.length - (index * manifest.chunk_bytes),
            )
            copied_chunk, buffer_error = self._copy_buffer(chunk, expected)
            chunk = None
            if buffer_error == _BUFFER_INVALID:
                raise SnapshotRepositoryError("Snapshot chunk is missing or invalid.")
            if buffer_error == _BUFFER_NONCONTIGUOUS:
                raise SnapshotRepositoryError("Snapshot chunk is not contiguous.")
            if buffer_error == _BUFFER_MUTABLE:
                raise SnapshotRepositoryError("Snapshot chunk is mutable.")
            if buffer_error == _BUFFER_OVERSIZED:
                raise SnapshotRepositoryError(
                    "Snapshot chunk length validation failed."
                )
            if buffer_error is not None or copied_chunk is None:
                raise SnapshotRepositoryError("Snapshot chunk conversion failed.")
            if len(copied_chunk) != expected:
                raise SnapshotRepositoryError(
                    "Snapshot chunk length validation failed."
                )
            assembled.extend(copied_chunk)
        state = bytes(assembled)
        if len(state) != manifest.length:
            raise SnapshotRepositoryError("Snapshot length validation failed.")
        if hashlib.sha256(state).hexdigest() != manifest.checksum:
            raise SnapshotRepositoryError("Snapshot checksum validation failed.")
        return SnapshotRead(True, state, True)

    def commit(self, key: str, state: bytes | None) -> None:
        """Commit ``state`` by writing all generation chunks before its manifest."""
        self._validate_logical_key(key)
        payload = b""
        buffer_error: str | None = None
        if state is not None:
            if not isinstance(state, bytes):
                state = None
                raise SnapshotRepositoryError("Strategy snapshot has an invalid type.")
            copied_payload, buffer_error = self._copy_buffer(state, self._max_bytes)
            state = None
            if buffer_error == _BUFFER_NONCONTIGUOUS:
                raise SnapshotRepositoryError("Strategy snapshot is not contiguous.")
            if buffer_error == _BUFFER_MUTABLE:
                raise SnapshotRepositoryError("Strategy snapshot is mutable.")
            if buffer_error == _BUFFER_OVERSIZED:
                raise SnapshotRepositoryError(
                    "Snapshot exceeds the configured size limit."
                )
            if buffer_error is not None or copied_payload is None:
                raise SnapshotRepositoryError("Strategy snapshot conversion failed.")
            payload = copied_payload
        length = len(payload)
        if length > self._max_bytes:
            raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
        generation = uuid.uuid4().hex
        chunks = (length + self._chunk_bytes - 1) // self._chunk_bytes
        for index in range(chunks):
            start = index * self._chunk_bytes
            self._store(
                self._chunk_key(key, generation, index),
                payload[start : start + self._chunk_bytes],
                "Snapshot chunk write failed.",
            )
        manifest = _Manifest(
            generation=generation,
            length=length,
            chunk_bytes=self._chunk_bytes,
            chunks=chunks,
            checksum=hashlib.sha256(payload).hexdigest(),
        )
        self._store(
            key,
            self._encode_manifest(manifest),
            "Snapshot manifest write failed.",
        )
