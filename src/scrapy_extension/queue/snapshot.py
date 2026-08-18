"""Transactional repository for queue-strategy recovery snapshots."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

DEFAULT_SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024
DEFAULT_SNAPSHOT_CHUNK_BYTES = 256 * 1024

_MANIFEST_SCHEMA = "scrapy-extension.queue-strategy-snapshot"
_MANIFEST_VERSION = 4
_MAX_MANIFEST_BYTES = 64 * 1024
_GENERATION_RE = re.compile(r"^[0-9a-f]{32}$")
_CHECKSUM_RE = re.compile(r"^[0-9a-f]{64}$")


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
    migration and are replaced only by a successful v4 commit.
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
        ):
            raise ValueError("Invalid snapshot repository size limits.")
        self._storage = storage
        self._max_bytes = max_bytes
        self._chunk_bytes = chunk_bytes

    @staticmethod
    def _chunk_key(key: str, generation: str, index: int) -> str:
        return f"{key}:generation:{generation}:chunk:{index}"

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
        try:
            value = self._storage.retrieve(key)
        except Exception:
            raise SnapshotRepositoryError("Snapshot manifest retrieval failed.") from None
        if value is None:
            return SnapshotRead(False, None, False)
        if not isinstance(value, (bytes, bytearray)):
            raise SnapshotRepositoryError("Snapshot manifest has an invalid type.")
        raw = bytes(value)
        manifest = self._decode_manifest(raw)
        if manifest is None:
            if len(raw) > self._max_bytes:
                raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
            return SnapshotRead(True, raw, False)
        if manifest.length > self._max_bytes or manifest.chunk_bytes > self._chunk_bytes:
            raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
        if manifest.length == 0:
            if manifest.checksum != hashlib.sha256(b"").hexdigest():
                raise SnapshotRepositoryError("Snapshot checksum validation failed.")
            return SnapshotRead(True, None, True)
        assembled = bytearray()
        for index in range(manifest.chunks):
            try:
                chunk = self._storage.retrieve(
                    self._chunk_key(key, manifest.generation, index)
                )
            except Exception:
                raise SnapshotRepositoryError("Snapshot chunk retrieval failed.") from None
            if not isinstance(chunk, (bytes, bytearray)):
                raise SnapshotRepositoryError("Snapshot chunk is missing or invalid.")
            chunk_value = bytes(chunk)
            expected = min(
                manifest.chunk_bytes,
                manifest.length - (index * manifest.chunk_bytes),
            )
            if len(chunk_value) != expected:
                raise SnapshotRepositoryError("Snapshot chunk length validation failed.")
            assembled.extend(chunk_value)
        state = bytes(assembled)
        if len(state) != manifest.length:
            raise SnapshotRepositoryError("Snapshot length validation failed.")
        if hashlib.sha256(state).hexdigest() != manifest.checksum:
            raise SnapshotRepositoryError("Snapshot checksum validation failed.")
        return SnapshotRead(True, state, True)

    def commit(self, key: str, state: bytes | None) -> None:
        """Commit ``state`` by writing all generation chunks before its manifest."""
        if state is not None and not isinstance(state, bytes):
            raise SnapshotRepositoryError("Strategy snapshot has an invalid type.")
        length = 0 if state is None else len(state)
        if length > self._max_bytes:
            raise SnapshotRepositoryError("Snapshot exceeds the configured size limit.")
        generation = uuid.uuid4().hex
        payload = b"" if state is None else state
        chunks = (length + self._chunk_bytes - 1) // self._chunk_bytes
        for index in range(chunks):
            start = index * self._chunk_bytes
            try:
                self._storage.store(
                    self._chunk_key(key, generation, index),
                    payload[start : start + self._chunk_bytes],
                )
            except Exception:
                raise SnapshotRepositoryError("Snapshot chunk write failed.") from None
        manifest = _Manifest(
            generation=generation,
            length=length,
            chunk_bytes=self._chunk_bytes,
            chunks=chunks,
            checksum=hashlib.sha256(payload).hexdigest(),
        )
        try:
            self._storage.store(key, self._encode_manifest(manifest))
        except Exception:
            raise SnapshotRepositoryError("Snapshot manifest write failed.") from None
