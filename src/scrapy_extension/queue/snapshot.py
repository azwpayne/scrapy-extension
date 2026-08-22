"""Transactional repository for queue-strategy recovery snapshots."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

from scrapy_extension.backends.base import BackendType, _validate_key_name

DEFAULT_SNAPSHOT_MAX_BYTES = 128 * 1024 * 1024
MAX_SNAPSHOT_CHUNK_BYTES = 256 * 1024
MAX_SNAPSHOT_CHUNKS = 4_096
DEFAULT_SNAPSHOT_CHUNK_BYTES = MAX_SNAPSHOT_CHUNK_BYTES
# Maintenance is deliberately an offline, operator-attested operation. These
# caps bound both the backend request and a hostile/infinite listing response;
# they are not tunable through a caller-provided arbitrarily large integer.
MAX_SNAPSHOT_GC_GENERATIONS = 16
MAX_SNAPSHOT_GC_DELETIONS = MAX_SNAPSHOT_CHUNKS * 2
MAX_SNAPSHOT_GC_LISTING = MAX_SNAPSHOT_GC_DELETIONS

_MANIFEST_SCHEMA = "scrapy-extension.queue-strategy-snapshot"
_MANIFEST_VERSION = 7
_READABLE_MANIFEST_VERSIONS = frozenset({4, 5, 6, _MANIFEST_VERSION})
_STATE_NONE = "none"
_STATE_BYTES = "bytes"
_MAX_MANIFEST_BYTES = 64 * 1024
_LEGACY_CHUNK_KEY_PREFIX = "queue:snapshot-chunk:v1:"
_CURRENT_CHUNK_KEY_PREFIX = "queue:snapshot-chunk:v2:"
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
# Only a value that starts like a JSON manifest is eligible for the truncated
# current-format marker fence.  Searching the marker anywhere in a raw payload
# would misclassify perfectly valid legacy request/strategy bytes containing the
# schema text as a manifest.
_CURRENT_MANIFEST_SCHEMA = _MANIFEST_SCHEMA.encode("ascii")


class SnapshotRepositoryError(Exception):
    """A redacted snapshot repository failure.

    ``confirmed_deletions`` is populated for maintenance failures so callers can
    distinguish a clean zero-change failure from a partial collection pass
    without parsing backend-specific diagnostics.
    """

    def __init__(self, message: str, *, confirmed_deletions: int = 0) -> None:
        super().__init__(message)
        self.confirmed_deletions = confirmed_deletions


@dataclass(frozen=True, slots=True)
class SnapshotRead:
    """One repository lookup, including an explicitly committed empty state."""

    found: bool
    state: bytes | None
    manifest: bool
    generation: str | None = None


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
    migration and are replaced only by a successful v7 commit.
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
        self._lease_condition = threading.Condition(threading.RLock())
        self._active_readers = 0
        self._active_commits = 0
        self._reader_threads: dict[int, int] = {}
        self._commit_threads: dict[int, int] = {}
        self._maintenance_active = False
        self._maintenance_owner: int | None = None

    def _validate_logical_key(self, key: str) -> None:
        invalid = False
        try:
            try:
                _validate_key_name(key, "snapshot logical key")
            except (TypeError, ValueError):
                invalid = True
            if not invalid:
                if key.startswith(
                    (_LEGACY_CHUNK_KEY_PREFIX, _CURRENT_CHUNK_KEY_PREFIX)
                ):
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
    def _legacy_chunk_key(key: str, generation: str, index: int) -> str:
        """Return the fixed-length v1 chunk key used by v4/v5/v6 manifests."""
        identity = b""
        chunk_key = ""
        try:
            identity = json.dumps(
                [key, generation, index],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            chunk_key = (
                f"{_LEGACY_CHUNK_KEY_PREFIX}{hashlib.sha256(identity).hexdigest()}"
            )
            return chunk_key
        finally:
            key = ""
            generation = ""
            identity = b""
            chunk_key = ""

    @staticmethod
    def _chunk_key(key: str, generation: str, index: int) -> str:
        """Return a generation-addressable key for new manifests.

        The logical-key digest keeps the key bounded and opaque while the
        generation remains discoverable by a controlled maintenance scan. Older
        manifests continue using :meth:`_legacy_chunk_key` and are never guessed
        during GC.
        """
        logical_digest = ""
        chunk_key = ""
        try:
            logical_digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
            chunk_key = (
                f"{_CURRENT_CHUNK_KEY_PREFIX}{logical_digest}:{generation}:{index}"
            )
            return chunk_key
        finally:
            key = ""
            generation = ""
            logical_digest = ""
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
    def _has_current_manifest_marker(value: bytes) -> bool:
        """Detect a current manifest marker without treating payload text as one.

        Current manifests are JSON objects, and the schema key may not be the
        first key in older v7 encodings.  A raw strategy payload can contain the
        schema text, so only a top-level ``schema`` key with the exact current
        schema value is considered a truncated-manifest marker.  The scan is
        deliberately byte-only and tolerates an unfinished final JSON string.
        """
        stripped = value.lstrip()
        if not stripped.startswith(b"{"):
            return False

        index = 1
        depth = 1
        length = len(stripped)
        # v4-v6 and the first v7 writer used ``sort_keys=True``; their
        # manifests begin with ``chunk_bytes`` rather than ``schema``.  Preserve
        # the same fail-closed treatment for those already-persisted values,
        # including a truncation in the middle of that first key.
        first_key = stripped[index:].lstrip()
        if b'"chunk_bytes"'.startswith(first_key) or first_key.startswith(
            b'"chunk_bytes"'
        ):
            return True
        while index < length and depth:
            byte = stripped[index]
            if byte == ord('"') and depth == 1:
                start = index + 1
                index += 1
                escaped = False
                while index < length:
                    byte = stripped[index]
                    if escaped:
                        escaped = False
                    elif byte == ord("\\"):
                        escaped = True
                    elif byte == ord('"'):
                        break
                    index += 1
                token = stripped[start:index]
                index += 1
                if token == b"schema":
                    while index < length and stripped[index] in b" \t\r\n":
                        index += 1
                    if index < length and stripped[index] == ord(":"):
                        index += 1
                        while index < length and stripped[index] in b" \t\r\n":
                            index += 1
                        if index >= length:
                            # A current manifest truncated immediately after the
                            # schema key cannot provide its value, but it must not
                            # be treated as a recoverable raw payload.
                            return True
                        if stripped[index] == ord('"'):
                            candidate = stripped[index + 1 :]
                            candidate = candidate.split(b'"', 1)[0]
                            if _CURRENT_MANIFEST_SCHEMA.startswith(candidate):
                                return True
                        elif stripped[index:].startswith(_CURRENT_MANIFEST_SCHEMA):
                            return True
                continue
            if byte == ord('"'):
                index += 1
                escaped = False
                while index < length:
                    byte = stripped[index]
                    if escaped:
                        escaped = False
                    elif byte == ord("\\"):
                        escaped = True
                    elif byte == ord('"'):
                        index += 1
                        break
                    index += 1
                continue
            if byte == ord("{"):
                depth += 1
            elif byte == ord("}"):
                depth -= 1
            index += 1
        return False

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
                # A value that visibly names the current schema is not allowed to
                # fall through as a legacy raw payload merely because its JSON was
                # truncated.  Raw legacy bytes remain compatible unless they carry
                # this recognizable current-format marker.
                if SnapshotRepository._has_current_manifest_marker(value):
                    return None, "Snapshot manifest schema is invalid."
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
                    if version in {6, _MANIFEST_VERSION}
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
                    decoded.get("state") if version in {6, _MANIFEST_VERSION} else None
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
                        version in {6, _MANIFEST_VERSION}
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
                        if version in {6, _MANIFEST_VERSION}
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
                return SnapshotRead(True, empty_state, True, manifest.generation), None
            assembled = bytearray()
            for index in range(manifest.chunks):
                chunk_key = (
                    self._v4_chunk_key(key, manifest.generation, index)
                    if manifest.version == 4
                    else (
                        self._legacy_chunk_key(key, manifest.generation, index)
                        if manifest.version in {5, 6}
                        else self._chunk_key(key, manifest.generation, index)
                    )
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
            return SnapshotRead(True, state, True, manifest.generation), None
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

    @contextmanager
    def _reader_lease(self) -> Iterator[None]:
        thread_id = threading.get_ident()
        acquired = False
        try:
            with self._lease_condition:
                while self._maintenance_active:
                    if self._maintenance_owner == thread_id:
                        raise SnapshotRepositoryError(
                            "Snapshot repository lease reentry is not allowed."
                        ) from None
                    self._lease_condition.wait()
                previous_count = self._reader_threads.get(thread_id, 0)
                previous_active = self._active_readers
                try:
                    self._active_readers = previous_active + 1
                    self._reader_threads[thread_id] = previous_count + 1
                    acquired = True
                except BaseException:
                    # Do not publish a reader count without its thread
                    # registration if control flow interrupts this boundary.
                    if self._reader_threads.get(thread_id) == previous_count + 1:
                        if previous_count:
                            self._reader_threads[thread_id] = previous_count
                        else:
                            self._reader_threads.pop(thread_id, None)
                    self._active_readers = previous_active
                    raise
            yield
        finally:
            if acquired:
                release_failure: BaseException | None = None
                try:
                    with self._lease_condition:
                        release_active = self._active_readers
                        release_count = self._reader_threads.get(thread_id, 1)
                        target_active = max(0, release_active - 1)
                        target_count = max(0, release_count - 1)
                        self._active_readers = target_active
                        if target_count:
                            self._reader_threads[thread_id] = target_count
                        else:
                            self._reader_threads.pop(thread_id, None)
                        if self._active_readers == 0:
                            self._lease_condition.notify_all()
                except BaseException as error:
                    release_failure = error
                    # A signal between the paired state publications must not
                    # strand the maintenance waiter. Reconcile to the target
                    # captured before this release began.
                    try:
                        with self._lease_condition:
                            target_active = max(0, release_active - 1)
                            target_count = max(0, release_count - 1)
                            self._active_readers = target_active
                            if target_count:
                                self._reader_threads[thread_id] = target_count
                            else:
                                self._reader_threads.pop(thread_id, None)
                            self._lease_condition.notify_all()
                    except BaseException:
                        pass
                    raise release_failure

    @contextmanager
    def _commit_lease(self) -> Iterator[None]:
        thread_id = threading.get_ident()
        acquired = False
        try:
            with self._lease_condition:
                while self._maintenance_active:
                    if self._maintenance_owner == thread_id:
                        raise SnapshotRepositoryError(
                            "Snapshot repository lease reentry is not allowed."
                        ) from None
                    self._lease_condition.wait()
                previous_count = self._commit_threads.get(thread_id, 0)
                previous_active = self._active_commits
                try:
                    self._active_commits = previous_active + 1
                    self._commit_threads[thread_id] = previous_count + 1
                    acquired = True
                except BaseException:
                    if self._commit_threads.get(thread_id) == previous_count + 1:
                        if previous_count:
                            self._commit_threads[thread_id] = previous_count
                        else:
                            self._commit_threads.pop(thread_id, None)
                    self._active_commits = previous_active
                    raise
            yield
        finally:
            if acquired:
                release_failure: BaseException | None = None
                try:
                    with self._lease_condition:
                        release_active = self._active_commits
                        release_count = self._commit_threads.get(thread_id, 1)
                        target_active = max(0, release_active - 1)
                        target_count = max(0, release_count - 1)
                        self._active_commits = target_active
                        if target_count:
                            self._commit_threads[thread_id] = target_count
                        else:
                            self._commit_threads.pop(thread_id, None)
                        if self._active_commits == 0:
                            self._lease_condition.notify_all()
                except BaseException as error:
                    release_failure = error
                    try:
                        with self._lease_condition:
                            target_active = max(0, release_active - 1)
                            target_count = max(0, release_count - 1)
                            self._active_commits = target_active
                            if target_count:
                                self._commit_threads[thread_id] = target_count
                            else:
                                self._commit_threads.pop(thread_id, None)
                            self._lease_condition.notify_all()
                    except BaseException:
                        pass
                    raise release_failure

    @contextmanager
    def _maintenance_lease(self) -> Iterator[None]:
        thread_id = threading.get_ident()
        acquired = False
        try:
            with self._lease_condition:
                if self._maintenance_active and self._maintenance_owner == thread_id:
                    raise SnapshotRepositoryError(
                        "Snapshot repository lease reentry is not allowed."
                    ) from None
                # A maintenance call from inside this thread's read/commit lease
                # would wait for itself forever after publishing the fence.
                if self._reader_threads.get(thread_id) or self._commit_threads.get(
                    thread_id
                ):
                    raise SnapshotRepositoryError(
                        "Snapshot repository lease reentry is not allowed."
                    ) from None
                while self._maintenance_active:
                    self._lease_condition.wait()
                try:
                    # Publish the maintenance fence as one guarded transition.
                    # If control flow interrupts after the active flag but before
                    # the owner is recorded, future readers would otherwise wait
                    # forever with no owner able to release the lease.
                    self._maintenance_active = True
                    self._maintenance_owner = thread_id
                    acquired = True
                except BaseException:
                    self._maintenance_active = False
                    self._maintenance_owner = None
                    self._lease_condition.notify_all()
                    raise
                while self._active_readers or self._active_commits:
                    self._lease_condition.wait()
            yield
        finally:
            # Keep the cleanup in the same outer try as the fence publication:
            # KeyboardInterrupt/SystemExit/GeneratorExit after the flag is set
            # must never leave every future reader and commit blocked forever.
            if acquired:
                cleanup_failure: BaseException | None = None
                try:
                    with self._lease_condition:
                        self._maintenance_active = False
                        self._maintenance_owner = None
                        self._lease_condition.notify_all()
                except BaseException as error:
                    cleanup_failure = error
                    try:
                        with self._lease_condition:
                            self._maintenance_active = False
                            self._maintenance_owner = None
                            self._lease_condition.notify_all()
                    except BaseException:
                        pass
                    raise cleanup_failure

    def read(self, key: str) -> SnapshotRead:
        """Read and fully validate one committed logical snapshot."""
        result: SnapshotRead | None = None
        try:
            self._validate_logical_key(key)
            with self._reader_lease():
                result, failure_message = self._read_terminal(key)
            if failure_message is not None:
                raise SnapshotRepositoryError(failure_message) from None
            assert result is not None
            return result
        finally:
            key = ""
            result = None

    @staticmethod
    def _logical_key_digest(key: str) -> str:
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_current_chunk_key(key: str, prefix: str) -> tuple[str, int] | None:
        if not key.startswith(prefix):
            return None
        suffix = key[len(prefix) :]
        parts = suffix.split(":")
        if len(parts) != 2:
            return None
        generation, raw_index = parts
        if _GENERATION_RE.fullmatch(generation) is None:
            return None
        if not raw_index.isascii() or not raw_index.isdigit():
            return None
        try:
            index = int(raw_index)
        except (TypeError, ValueError):
            return None
        if index < 0 or index >= MAX_SNAPSHOT_CHUNKS or raw_index != str(index):
            return None
        return generation, index

    def _bounded_storage_listing(
        self,
        list_keys: Any,
        prefix: str,
    ) -> list[object]:
        """Call a listing capability and bound both its result and its iterator."""
        observed: object = None
        iterator: Iterator[object] | None = None
        candidates: list[object] = []
        candidate: object = None
        completed = False
        listing_failure = ""
        iterator_failure = False
        next_failure = False
        try:
            try:
                observed = list_keys(prefix, limit=MAX_SNAPSHOT_GC_LISTING)
            except NotImplementedError:
                listing_failure = "unavailable"
            except Exception:
                listing_failure = "failed"
            if listing_failure == "unavailable":
                raise SnapshotRepositoryError(
                    "Snapshot maintenance is unavailable for this storage backend."
                ) from None
            if listing_failure:
                raise SnapshotRepositoryError(
                    "Snapshot maintenance key listing failed."
                ) from None
            if observed is None or isinstance(
                observed, (str, bytes, bytearray, memoryview)
            ):
                raise SnapshotRepositoryError(
                    "Snapshot maintenance returned an invalid key listing."
                ) from None
            try:
                iterator = iter(cast(Iterable[object], observed))
            except Exception:
                iterator_failure = True
            if iterator_failure:
                raise SnapshotRepositoryError(
                    "Snapshot maintenance returned an invalid key listing."
                ) from None
            assert iterator is not None
            # Do not trust a backend's reported length or a generator's claimed
            # boundedness.  Pull one extra element to distinguish exactly-at-cap
            # from over-cap, then reject without deleting anything.
            for position in range(MAX_SNAPSHOT_GC_LISTING + 1):
                try:
                    candidate = next(iterator)
                except StopIteration:
                    break
                except Exception:
                    next_failure = True
                    break
                if position >= MAX_SNAPSHOT_GC_LISTING:
                    raise SnapshotRepositoryError(
                        "Snapshot maintenance key listing exceeds its hard limit."
                    ) from None
                candidates.append(candidate)
                candidate = None
            if next_failure:
                raise SnapshotRepositoryError(
                    "Snapshot maintenance key listing failed."
                ) from None
            completed = True
            return candidates
        finally:
            observed = None
            iterator = None
            candidate = None
            if not completed:
                candidates.clear()

    def _delete_and_confirm(self, key: str) -> bool:
        """Delete one chunk and return only an observed, confirmed deletion.

        Storage ``delete`` calls can return a stale ``False``, return no value
        in a legacy plugin, or apply the deletion before raising when its reply
        is lost. One bounded readback makes those outcomes explicit. An
        existing value is never counted; an unreadable readback is reported as
        indeterminate rather than converted into a false maintenance count.
        """
        delete_result: object = None
        delete_failed = False
        observed: object = None
        retrieve_failed = False
        try:
            try:
                delete_result = self._storage.delete(key)
            except Exception:
                delete_failed = True
            observed, retrieve_failed = self._retrieve(key)
            if retrieve_failed:
                raise SnapshotRepositoryError(
                    "Snapshot maintenance deletion outcome is indeterminate."
                ) from None
            if observed is not None:
                raise SnapshotRepositoryError(
                    "Snapshot maintenance deletion was not confirmed."
                ) from None
            if delete_failed:
                # The effect-then-raise case is confirmed by the absent readback.
                return True
            if delete_result is False:
                # The key was already absent when delete ran; this call deleted
                # nothing and must not inflate the maintenance count.
                return False
            # ``None`` is accepted for old plugins only because the readback
            # proves the key is gone; a normal StorageBackend returns bool.
            return True
        finally:
            key = ""
            delete_result = None
            observed = None
            retrieve_failed = False

    def gc(
        self,
        key: str,
        *,
        quiescent: bool = False,
        max_generations: int = MAX_SNAPSHOT_GC_GENERATIONS,
        max_deletions: int = MAX_SNAPSHOT_GC_DELETIONS,
    ) -> int:
        """Delete only bounded, unreferenced v7 generation chunks.

        Collection is explicit/offline: ``quiescent=True`` is an operator
        attestation that all writers sharing this storage namespace are stopped.
        Legacy v1 chunks, malformed entries, the current generation, and chunks
        for another logical key are never deleted.  No broad-clear fallback or
        automatic cleanup is attempted.
        """
        try:
            self._validate_logical_key(key)
            if quiescent is not True:
                raise ValueError(
                    "Snapshot maintenance requires explicit writer quiescence."
                )
            if (
                isinstance(max_generations, bool)
                or not isinstance(max_generations, int)
                or max_generations < 1
                or isinstance(max_deletions, bool)
                or not isinstance(max_deletions, int)
                or max_deletions < 1
            ):
                raise ValueError(
                    "Snapshot maintenance limits must be positive integers."
                )
            if (
                max_generations > MAX_SNAPSHOT_GC_GENERATIONS
                or max_deletions > MAX_SNAPSHOT_GC_DELETIONS
            ):
                raise SnapshotRepositoryError(
                    "Snapshot maintenance limit exceeds its hard cap."
                ) from None
        except Exception:
            key = ""
            raise
        deleted = 0
        prefix = ""
        observed: list[object] = []
        candidate: object = None
        parsed: dict[str, tuple[str, int]] = {}
        generations: list[str] = []
        try:
            with self._maintenance_lease():
                current, failure_message = self._read_terminal(key)
                if failure_message is not None:
                    raise SnapshotRepositoryError(failure_message) from None
                if current is None or not current.found or current.generation is None:
                    return 0
                list_keys_lookup_failed = False
                try:
                    list_keys = getattr(self._storage, "list_storage_keys", None)
                except Exception:
                    list_keys_lookup_failed = True
                if list_keys_lookup_failed or not callable(list_keys):
                    raise SnapshotRepositoryError(
                        "Snapshot maintenance is unavailable for this storage backend."
                    ) from None
                prefix = f"{_CURRENT_CHUNK_KEY_PREFIX}{self._logical_key_digest(key)}:"
                observed = self._bounded_storage_listing(list_keys, prefix)
                for candidate in observed:
                    if type(candidate) is not str:
                        continue
                    parsed_chunk = self._parse_current_chunk_key(candidate, prefix)
                    if parsed_chunk is None:
                        continue
                    generation, index = parsed_chunk
                    canonical = self._chunk_key(key, generation, index)
                    if candidate != canonical or generation == current.generation:
                        continue
                    # A backend listing may repeat a key.  Count and delete each
                    # canonical physical key at most once.
                    parsed.setdefault(candidate, (generation, index))
                generations = sorted({generation for generation, _ in parsed.values()})
                for generation in generations[:max_generations]:
                    for candidate, (candidate_generation, _index) in parsed.items():
                        if candidate_generation != generation:
                            continue
                        if deleted >= max_deletions:
                            return deleted
                        try:
                            confirmed = self._delete_and_confirm(candidate)
                        except SnapshotRepositoryError as error:
                            # Keep the original static exception object rather
                            # than wrapping it inside an active ``except`` suite;
                            # callers must receive truthful partial-count data
                            # without a backend exception context graph.
                            error.confirmed_deletions = deleted
                            error.args = (
                                "Snapshot maintenance was partial; confirmed "
                                f"deletions={deleted}. {error}",
                            )
                            raise
                        if confirmed:
                            deleted += 1
                return deleted
        finally:
            key = ""
            prefix = ""
            observed.clear()
            candidate = None
            parsed.clear()
            generations.clear()

    def maintenance(
        self,
        key: str,
        *,
        quiescent: bool = False,
        max_generations: int = MAX_SNAPSHOT_GC_GENERATIONS,
        max_deletions: int = MAX_SNAPSHOT_GC_DELETIONS,
    ) -> int:
        """Compatibility alias for bounded snapshot generation maintenance."""
        return self.gc(
            key,
            quiescent=quiescent,
            max_generations=max_generations,
            max_deletions=max_deletions,
        )

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
            with self._commit_lease():
                failure_message = self._commit_terminal(key, state)
        finally:
            key = ""
            state = None
        if failure_message is not None:
            raise SnapshotRepositoryError(failure_message) from None
