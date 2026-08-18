"""ElasticSearch backend implementation."""

from __future__ import annotations

import base64
import binascii
import contextlib
import hashlib
import logging
import math
import threading
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from pydantic import ValidationError

from scrapy_extension.backends._optional import _is_missing_optional_dependency

try:
    from elasticsearch import (
        ApiError,
        ConflictError,
        Elasticsearch,
        NotFoundError,
        RequestError,
        TransportError,
    )
except ImportError as e:
    if not _is_missing_optional_dependency(e, "elasticsearch"):
        raise
    raise ImportError(
        "ElasticSearch backend requires 'elasticsearch'. Install with: pip install scrapy-extension[elasticsearch]"
    ) from e

from scrapy_extension.backends._redaction import _redact
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
    _validate_key_name,
    _validate_ttl,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    QueueOutcomeIndeterminateError,
    SetOutcomeIndeterminateError,
    StorageError,
    StorageOutcomeIndeterminateError,
)
from scrapy_extension.exceptions._redaction import (
    backend_connection_error_boundary,
    configuration_error_boundary,
    queue_operation_error_boundary,
    set_operation_error_boundary,
    storage_operation_error_boundary,
)
from scrapy_extension.settings._transport_security import (
    validate_allow_remote_plaintext,
)
from scrapy_extension.settings.elasticsearch import ElasticSearchMode

if TYPE_CHECKING:
    from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

logger = logging.getLogger(__name__)

_ELASTICSEARCH_CONNECT_SETTING_NAMES: frozenset[str] = frozenset(
    {
        "api_key",
        "allow_remote_plaintext",
        "ca_certs",
        "cloud_id",
        "hosts",
        "max_retries",
        "mode",
        "password",
        "queue_index",
        "request_timeout",
        "retry_on_timeout",
        "set_index",
        "storage_index",
        "username",
        "verify_certs",
    }
)
_ELASTICSEARCH_OPERATION_SETTING_NAMES: frozenset[str] = frozenset({"operation"})
_ELASTICSEARCH_QUEUE_PUSH_ERROR = "ElasticSearch queue push failed."
_ELASTICSEARCH_QUEUE_POP_ERROR = "ElasticSearch queue pop failed."
_ELASTICSEARCH_QUEUE_LENGTH_ERROR = "ElasticSearch queue length read failed."
_ELASTICSEARCH_QUEUE_CLEAR_ERROR = "ElasticSearch queue clear failed."
_ELASTICSEARCH_SET_ADD_ERROR = "ElasticSearch set add failed."
_ELASTICSEARCH_SET_REMOVE_ERROR = "ElasticSearch set remove failed."
_ELASTICSEARCH_SET_CONTAINS_ERROR = "ElasticSearch set membership check failed."
_ELASTICSEARCH_SET_LENGTH_ERROR = "ElasticSearch set length read failed."
_ELASTICSEARCH_SET_CLEAR_ERROR = "ElasticSearch set clear failed."
_ELASTICSEARCH_SET_REQUEST_ERROR = "ElasticSearch set request was rejected."
_ELASTICSEARCH_STORAGE_STORE_ERROR = "ElasticSearch storage store failed."
_ELASTICSEARCH_STORAGE_RETRIEVE_ERROR = "ElasticSearch storage retrieve failed."
_ELASTICSEARCH_STORAGE_DELETE_ERROR = "ElasticSearch storage delete failed."
_ELASTICSEARCH_STORAGE_EXISTS_ERROR = "ElasticSearch storage existence check failed."
_ELASTICSEARCH_STORAGE_TTL_ERROR = "ElasticSearch storage TTL read failed."
_ELASTICSEARCH_STORAGE_CLEAR_ERROR = "ElasticSearch storage clear failed."


class _ElasticSearchResponseError(Exception):
    """An Elasticsearch response could not prove the requested outcome."""


def _api_error_has_type(error: ApiError, expected_type: str) -> bool:
    """Match an Elasticsearch error by structured type fields only."""
    body = getattr(error, "body", None)
    if not isinstance(body, Mapping):
        return False
    detail = body.get("error")
    if not isinstance(detail, Mapping):
        return False
    if detail.get("type") == expected_type:
        return True
    root_cause = detail.get("root_cause")
    return isinstance(root_cause, list) and any(
        isinstance(cause, Mapping) and cause.get("type") == expected_type
        for cause in root_cause
    )


def _response_mapping(response: object) -> Mapping[str, Any]:
    """Unwrap elastic-transport API responses to a response mapping."""
    if isinstance(response, Mapping):
        return cast("Mapping[str, Any]", response)
    body = getattr(response, "body", None)
    if isinstance(body, Mapping):
        return cast("Mapping[str, Any]", body)
    raise _ElasticSearchResponseError


def _exact_nonnegative_int(value: object) -> int:
    """Return an exact nonnegative integer or reject the response."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _ElasticSearchResponseError
    return value


def _validate_shards(response: object, *, require_success: bool) -> None:
    """Reject missing, malformed, partial, or failed shard acknowledgements."""
    response_body = _response_mapping(response)
    shards = response_body.get("_shards")
    if not isinstance(shards, Mapping):
        raise _ElasticSearchResponseError
    total = _exact_nonnegative_int(shards.get("total"))
    successful = _exact_nonnegative_int(shards.get("successful"))
    failed = _exact_nonnegative_int(shards.get("failed"))
    skipped = _exact_nonnegative_int(shards.get("skipped", 0))
    if failed != 0 or successful + failed != total or skipped > successful:
        raise _ElasticSearchResponseError
    if require_success and successful == 0:
        raise _ElasticSearchResponseError
    failures = shards.get("failures", [])
    if not isinstance(failures, list) or failures:
        raise _ElasticSearchResponseError


def _validate_index_response(response: object, allowed_results: frozenset[str]) -> None:
    """Require a successful shard acknowledgement and expected write result."""
    _validate_shards(response, require_success=True)
    response_body = _response_mapping(response)
    if response_body.get("result") not in allowed_results:
        raise _ElasticSearchResponseError


def _validate_delete_response(response: object) -> bool:
    """Return a server-confirmed delete result, rejecting ambiguous shapes."""
    _validate_shards(response, require_success=True)
    response_body = _response_mapping(response)
    result = response_body.get("result")
    if result == "deleted":
        return True
    if result == "not_found":
        return False
    raise _ElasticSearchResponseError


def _validate_search_response(response: object) -> list[object]:
    """Return validated exact search hits with no timeout or shard failure."""
    response_body = _response_mapping(response)
    if response_body.get("timed_out") is not False:
        raise _ElasticSearchResponseError
    _validate_shards(response_body, require_success=False)
    hits_block = response_body.get("hits")
    if not isinstance(hits_block, Mapping):
        raise _ElasticSearchResponseError
    total = hits_block.get("total")
    if not isinstance(total, Mapping) or total.get("relation") != "eq":
        raise _ElasticSearchResponseError
    total_value = _exact_nonnegative_int(total.get("value"))
    hits = hits_block.get("hits")
    if not isinstance(hits, list) or len(hits) > 1:
        raise _ElasticSearchResponseError
    if (total_value == 0) != (len(hits) == 0):
        raise _ElasticSearchResponseError
    return cast("list[object]", hits)


def _validate_count_response(response: object) -> int:
    """Return an exact nonnegative count from a complete shard response."""
    _validate_shards(response, require_success=False)
    response_body = _response_mapping(response)
    return _exact_nonnegative_int(response_body.get("count"))


def _validate_delete_by_query_response(response: object) -> None:
    """Require a complete, failure-free documented delete-by-query response.

    Unlike search, count, and single-document writes, Elasticsearch's
    delete-by-query response has no top-level ``_shards`` acknowledgement.
    Its completion counters and failure list are therefore the proof of a
    successful synchronous outcome.
    """
    response_body = _response_mapping(response)
    if response_body.get("timed_out") is not False:
        raise _ElasticSearchResponseError
    failures = response_body.get("failures")
    if not isinstance(failures, list) or failures:
        raise _ElasticSearchResponseError
    total = _exact_nonnegative_int(response_body.get("total"))
    deleted = _exact_nonnegative_int(response_body.get("deleted"))
    conflicts = _exact_nonnegative_int(response_body.get("version_conflicts"))
    if deleted != total or conflicts != 0:
        raise _ElasticSearchResponseError

    for field in (
        "took",
        "updated",
        "batches",
        "noops",
        "throttled_millis",
        "throttled_until_millis",
        "slice_id",
    ):
        if field in response_body:
            _exact_nonnegative_int(response_body[field])

    retries = response_body.get("retries")
    if retries is not None:
        if not isinstance(retries, Mapping):
            raise _ElasticSearchResponseError
        _exact_nonnegative_int(retries.get("bulk"))
        _exact_nonnegative_int(retries.get("search"))

    if "requests_per_second" in response_body:
        rate = response_body["requests_per_second"]
        if (
            isinstance(rate, bool)
            or not isinstance(rate, (int, float))
            or not math.isfinite(rate)
            or (rate < 0 and rate != -1)
        ):
            raise _ElasticSearchResponseError

    if "task" in response_body:
        task = response_body["task"]
        if not isinstance(task, str) or not task:
            raise _ElasticSearchResponseError


def _validate_queue_name_argument(
    _backend: object,
    queue_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct ElasticSearch queue name before terminal handling."""
    _validate_key_name(queue_name, "queue_name")


def _validate_set_name_argument(
    _backend: object,
    set_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct ElasticSearch set name before terminal handling."""
    _validate_key_name(set_name, "set_name")


def _validate_storage_key_argument(
    _backend: object,
    key: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct ElasticSearch storage key before terminal handling."""
    _validate_key_name(key, "key")


def _validate_store_arguments(
    _backend: object,
    key: str,
    data: bytes,
    ttl: int | None = None,
) -> None:
    """Validate storage write arguments before implementation frames exist."""
    del data
    _validate_key_name(key, "key")
    _validate_ttl(ttl)


def _validate_storage_prefix_argument(
    _backend: object,
    prefix: str | None = None,
) -> None:
    """Validate an optional ElasticSearch storage prefix outside the boundary."""
    if prefix is not None:
        _validate_key_name(prefix, "prefix")


@dataclass(frozen=True, slots=True)
class _ElasticSearchConnectionSnapshot:
    """Validated operational values fixed for one Elasticsearch client."""

    mode: ElasticSearchMode
    hosts: tuple[str, ...]
    cloud_id: str | None
    api_key: str | None
    username: str | None
    password: str | None
    verify_certs: bool
    ca_certs: str | None
    request_timeout: float
    max_retries: int
    retry_on_timeout: bool
    queue_index: str
    set_index: str
    storage_index: str


@dataclass(frozen=True, slots=True, eq=False)
class _ElasticSearchGeneration:
    """One root client, its no-replay mutation view, and immutable snapshot."""

    client: Elasticsearch
    mutation_client: Elasticsearch
    snapshot: _ElasticSearchConnectionSnapshot


def _b64encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64decode(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"), validate=True)


def _decode_queue_hit(hit: object) -> tuple[str, int, int, bytes]:
    """Validate and decode one queue hit without exposing corrupt fields."""
    if not isinstance(hit, Mapping):
        raise QueueError(_ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop")

    document_id = hit.get("_id")
    sequence_number = hit.get("_seq_no")
    primary_term = hit.get("_primary_term")
    source = hit.get("_source")
    if (
        not isinstance(document_id, str)
        or not document_id
        or not isinstance(sequence_number, int)
        or isinstance(sequence_number, bool)
        or not isinstance(primary_term, int)
        or isinstance(primary_term, bool)
        or not isinstance(source, Mapping)
    ):
        raise QueueError(_ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop")

    item = source.get("item")
    if not isinstance(item, str):
        raise QueueError(_ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop")
    try:
        decoded_item = _b64decode(item)
    except (UnicodeEncodeError, binascii.Error, ValueError):
        raise QueueError(_ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop") from None
    return document_id, sequence_number, primary_term, decoded_item


class ElasticSearchBackend(Backend, QueueBackend, SetBackend, StorageBackend):
    """ElasticSearch backend: Queue (sorted docs), Set (unique _id), Storage (key-value with TTL)."""

    _push_is_durable = True

    def __init__(self, config: ElasticSearchSettings) -> None:
        """Initialize ElasticSearch backend.

        Args:
            config: Configuration for ElasticSearch connection.
        """
        self.config = config
        self._lifecycle_lock = threading.RLock()
        self._generation_condition = threading.Condition()
        self._lease_local = threading.local()
        self._lifecycle_epoch = 0
        self._connecting = False
        self._connect_owner: int | None = None
        self._disconnecting = False
        self._disconnect_owner: int | None = None
        self._generation: _ElasticSearchGeneration | None = None
        self._active_leases = 0
        # Compatibility mirrors for diagnostics and older private test injection.
        # Internal operations use only the authoritative leased generation.
        self._client: Elasticsearch | None = None
        self._connection_snapshot: _ElasticSearchConnectionSnapshot | None = None

    @staticmethod
    def _build_generation(
        client: Elasticsearch, snapshot: _ElasticSearchConnectionSnapshot
    ) -> _ElasticSearchGeneration:
        """Bind a no-replay mutation view to the root client's transport."""
        mutation_client = client.options(
            max_retries=0,
            retry_on_timeout=False,
            retry_on_status=(),
        )
        # ``MagicMock.options()`` creates an unrelated child. Keeping legacy test
        # doubles usable does not weaken real clients: an Elasticsearch options
        # view always shares the exact root transport.
        if getattr(mutation_client, "transport", None) is not getattr(
            client, "transport", None
        ):
            mutation_client = client
        return _ElasticSearchGeneration(client, mutation_client, snapshot)

    @configuration_error_boundary(
        "Elasticsearch configuration is invalid.",
        _ELASTICSEARCH_CONNECT_SETTING_NAMES,
    )
    def _capture_connection_snapshot(self) -> _ElasticSearchConnectionSnapshot:
        """Copy and revalidate every value used by one client generation.

        Pydantic settings are mutable after construction.  Revalidating a copied
        field map preserves the settings model's transport/auth/index invariants
        before an SDK call, while the frozen result prevents later mutation from
        retargeting a live client's capability operations.
        """
        raw_values = self.config.__dict__.copy()
        validate_allow_remote_plaintext(raw_values.get("allow_remote_plaintext"))
        validated: ElasticSearchSettings | None = None
        settings_error: ConfigurationError | None = None
        try:
            validated = type(self.config).model_validate(raw_values)
        except ConfigurationError:
            raise
        except ValidationError as exc:
            errors = exc.errors()
            location = errors[0].get("loc", ()) if errors else ()
            setting_name = str(location[0]) if location else "elasticsearch"
            settings_error = ConfigurationError(
                f"Invalid Elasticsearch setting '{setting_name}'.",
                setting_name=setting_name,
            )

        if settings_error is not None:
            # Raise outside the Pydantic handler so mutable input cannot survive in
            # the public error's cause or context graph.
            raise settings_error
        assert validated is not None

        return _ElasticSearchConnectionSnapshot(
            mode=validated.mode,
            hosts=tuple(validated.hosts),
            cloud_id=validated.cloud_id,
            api_key=(
                cast(str, _redact(secret_value(validated.api_key)))
                if validated.api_key is not None
                else None
            ),
            username=validated.username,
            password=(
                cast(str, _redact(secret_value(validated.password)))
                if validated.password is not None
                else None
            ),
            verify_certs=validated.verify_certs,
            ca_certs=validated.ca_certs,
            request_timeout=validated.request_timeout,
            max_retries=validated.max_retries,
            retry_on_timeout=validated.retry_on_timeout,
            queue_index=validated.queue_index,
            set_index=validated.set_index,
            storage_index=validated.storage_index,
        )

    def _active_snapshot(self) -> _ElasticSearchConnectionSnapshot:
        """Return the current snapshot for compatibility diagnostics only."""
        with self._generation_condition:
            if self._generation is not None:
                return self._generation.snapshot
            snapshot = self._connection_snapshot
        if snapshot is None:
            snapshot = self._capture_connection_snapshot()
            with self._generation_condition:
                if self._connection_snapshot is None:
                    self._connection_snapshot = snapshot
                else:
                    snapshot = self._connection_snapshot
        return snapshot

    def _build_kwargs(
        self, snapshot: _ElasticSearchConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build common ElasticSearch client kwargs.

        Returns:
            Dictionary of client configuration options.
        """
        snapshot = (
            snapshot or self._connection_snapshot or self._capture_connection_snapshot()
        )
        kwargs: dict[str, Any] = {
            "request_timeout": snapshot.request_timeout,
            "max_retries": snapshot.max_retries,
            "retry_on_timeout": snapshot.retry_on_timeout,
        }
        if snapshot.api_key is not None:
            kwargs["api_key"] = snapshot.api_key
        elif snapshot.username is not None and snapshot.password is not None:
            kwargs["basic_auth"] = (
                snapshot.username,
                snapshot.password,
            )
        return kwargs

    @backend_connection_error_boundary(
        "Failed to connect to Elasticsearch.",
        "elasticsearch",
    )
    @configuration_error_boundary(
        "Elasticsearch configuration is invalid.",
        _ELASTICSEARCH_CONNECT_SETTING_NAMES,
        catch_unexpected=False,
    )
    def connect(self) -> None:
        """Privately build and atomically publish one client generation."""
        current_thread = threading.get_ident()
        with self._generation_condition:
            while True:
                if self._connect_owner == current_thread:
                    raise BackendConnectionError(
                        "Cannot connect to Elasticsearch re-entrantly during connect.",
                        backend_type="elasticsearch",
                    )
                if self._disconnect_owner == current_thread:
                    raise BackendConnectionError(
                        "Cannot connect to Elasticsearch re-entrantly during disconnect.",
                        backend_type="elasticsearch",
                    )
                # A generation remains published while disconnect drains its
                # leases, but it is already retiring and must not satisfy a new
                # connect. Peers wait for teardown and may publish a replacement;
                # a current lease owner cannot wait on its own lease to drain.
                if self._disconnecting:
                    if int(getattr(self._lease_local, "depth", 0)):
                        raise BackendConnectionError(
                            "Cannot connect to Elasticsearch during an active operation.",
                            backend_type="elasticsearch",
                        )
                    self._generation_condition.wait()
                    continue
                if self._generation is not None:
                    return
                if self._connecting:
                    self._generation_condition.wait()
                    continue
                break

        with self._lifecycle_lock:
            owns_startup = False
            try:
                with self._generation_condition:
                    if self._connect_owner == current_thread:
                        raise BackendConnectionError(
                            "Cannot connect to Elasticsearch re-entrantly during connect.",
                            backend_type="elasticsearch",
                        )
                    if self._generation is not None:
                        return
                    if self._disconnecting:
                        raise BackendConnectionError(
                            "Cannot connect while Elasticsearch is disconnecting.",
                            backend_type="elasticsearch",
                        )
                    if self._connecting:
                        raise BackendConnectionError(
                            "Another Elasticsearch connection startup is in progress.",
                            backend_type="elasticsearch",
                        )
                    self._connecting = True
                    self._connect_owner = current_thread
                    owns_startup = True
                    injected_client = self._client
                    injected_snapshot = self._connection_snapshot

                # Preserve narrowly scoped compatibility for code that populated the
                # historical private mirrors. Adopt both into one atomic generation.
                if injected_client is not None:
                    snapshot = injected_snapshot or self._capture_connection_snapshot()
                    injected_generation = self._build_generation(
                        injected_client, snapshot
                    )
                    try:
                        with self._generation_condition:
                            self._generation = injected_generation
                            self._client = injected_generation.client
                            self._connection_snapshot = injected_generation.snapshot
                            self._generation_condition.notify_all()
                    except BaseException:
                        # Identity publication transfers ownership to the backend.
                        # Repair compatibility mirrors and wake waiters, but never
                        # roll back or close the now-leasable generation.
                        self._preserve_published_generation(injected_generation)
                        raise
                    return

                snapshot = self._capture_connection_snapshot()
                candidate: Elasticsearch | None = None
                generation: _ElasticSearchGeneration | None = None
                startup_error: BackendConnectionError | None = None
                cleanup_diagnostic_pending = False
                try:
                    kwargs = self._build_kwargs(snapshot)
                    if snapshot.mode == ElasticSearchMode.CLOUD:
                        if not snapshot.cloud_id:
                            msg = "Cloud mode requires 'cloud_id'"
                            raise BackendConnectionError(
                                msg, backend_type="elasticsearch"
                            )
                        kwargs["cloud_id"] = snapshot.cloud_id
                    else:
                        kwargs["hosts"] = snapshot.hosts
                        kwargs["verify_certs"] = snapshot.verify_certs
                        if snapshot.ca_certs:
                            kwargs["ca_certs"] = snapshot.ca_certs
                    candidate = Elasticsearch(**kwargs)
                    if not candidate.ping():
                        raise BackendConnectionError(
                            "ElasticSearch health check returned false during connect",
                            backend_type="elasticsearch",
                        )
                    self._ensure_indices(snapshot, client=candidate)
                    generation = self._build_generation(candidate, snapshot)
                    with self._generation_condition:
                        if (
                            self._connect_owner != current_thread
                            or not self._connecting
                            or self._disconnecting
                            or self._generation is not None
                        ):
                            raise BackendConnectionError(
                                "Elasticsearch connection changed during startup.",
                                backend_type="elasticsearch",
                            )
                        self._generation = generation
                        self._client = generation.client
                        self._connection_snapshot = generation.snapshot
                        self._generation_condition.notify_all()
                    try:
                        logger.debug(
                            "Connected to ElasticSearch in %s mode", snapshot.mode.value
                        )
                    except BaseException:
                        pass
                except (BackendConnectionError, ApiError, TransportError):
                    if self._preserve_published_generation(generation):
                        raise
                    cleanup_diagnostic_pending = self._abort_failed_connect(candidate)
                    startup_error = BackendConnectionError(
                        f"Connection failed to ElasticSearch ({snapshot.mode.value}).",
                        backend_type="elasticsearch",
                    )
                except Exception:
                    if self._preserve_published_generation(generation):
                        raise
                    cleanup_diagnostic_pending = self._abort_failed_connect(candidate)
                    startup_error = BackendConnectionError(
                        f"Connection failed to ElasticSearch ({snapshot.mode.value}).",
                        backend_type="elasticsearch",
                    )
                except BaseException:
                    if not self._preserve_published_generation(generation):
                        self._abort_failed_connect(candidate)
                    raise

                if cleanup_diagnostic_pending:
                    self._log_failed_connect_cleanup_diagnostic()
                if startup_error is not None:
                    raise startup_error
            finally:
                if owns_startup:
                    with self._generation_condition:
                        self._connecting = False
                        self._connect_owner = None
                        try:
                            self._generation_condition.notify_all()
                        except BaseException:
                            # Notification is bookkeeping after any publication
                            # commit. Retry once without replacing the original
                            # control-flow exception.
                            try:
                                self._generation_condition.notify_all()
                            except BaseException:
                                pass
                            raise

    def _preserve_published_generation(
        self, generation: _ElasticSearchGeneration | None
    ) -> bool:
        """Repair mirrors for an identity-published generation.

        Publishing ``_generation`` is the ownership commit point. Once another
        caller can lease that identity, later interruption may propagate but must
        not detach or close its client.
        """
        if generation is None:
            return False
        with self._generation_condition:
            if self._generation is not generation:
                return False
            self._client = generation.client
            self._connection_snapshot = generation.snapshot
            try:
                self._generation_condition.notify_all()
            except BaseException:
                # Best effort only: preserve the primary interruption. The connect
                # finalizer also retries after clearing the startup fence.
                pass
            return True

    @staticmethod
    def _log_failed_connect_cleanup_diagnostic() -> None:
        """Report a completed failed-connect cleanup without its error object."""
        try:
            logger.debug("Failed to close ElasticSearch connect candidate")
        except BaseException:
            pass

    def _abort_failed_connect(self, candidate: Elasticsearch | None) -> bool:
        """Detach and close only this failed connect candidate.

        Roll back only if the currently published generation is this exact
        candidate; a future generation must never be cleared by stale failure
        cleanup.  This helper is deliberately separate from normal ``disconnect``
        cleanup: a cleanup ``BaseException`` must not mask the original connection
        failure.
        """
        if candidate is None:
            return False

        with self._generation_condition:
            if self._generation is not None and self._generation.client is candidate:
                self._generation = None
                self._client = None
                self._connection_snapshot = None
            elif self._client is candidate:
                self._client = None
                self._connection_snapshot = None

        try:
            candidate.close()
        except BaseException:
            # Preserve the primary error from connect, including KeyboardInterrupt
            # and SystemExit. The caller emits a fixed diagnostic after the startup
            # handler has finished, when it is safe to invoke application logging.
            return True
        return False

    def _discard_candidate(self, candidate: Elasticsearch | None) -> None:
        """Best-effort close for a client generation that was never published."""
        if candidate is not None:
            close_failed = False
            try:
                candidate.close()
            except Exception:
                close_failed = True
            if close_failed:
                # A logging handler is extension code and may itself raise a control
                # exception. Normal disconnect is best-effort for ordinary close
                # failures, so diagnostics must not turn that path into a failure or
                # observe the raw driver exception through ``sys.exc_info()``.
                # Deliberately do not catch BaseException from ``close``: once the
                # generation has been detached, direct control-flow interruption
                # remains observable to the caller.
                try:
                    logger.debug("Failed to close ElasticSearch client")
                except BaseException:
                    pass

    def _discard_client(self) -> None:
        """Detach and best-effort close a compatibility-injected client."""
        with self._generation_condition:
            generation = self._generation
            client = generation.client if generation is not None else self._client
            self._generation = None
            self._client = None
            self._connection_snapshot = None
        self._discard_candidate(client)

    def _ensure_indices(
        self,
        snapshot: _ElasticSearchConnectionSnapshot | None = None,
        *,
        client: Elasticsearch | None = None,
    ) -> None:
        """Create the queue/set/storage indices if absent.

        Uses try-create-and-ignore-``resource_already_exists`` rather than the
        prior ``if not indices.exists()`` guard. The guard's HEAD request
        (``indices.exists``) returns HTTP 400 under elasticsearch-py 9.x against
        an ES 8.x server — client/server API drift on the index-exists endpoint —
        so the existence-check path raised ``BadRequestError`` on every connect.
        Try-create is version-robust: ES replies ``resource_already_exists_exception``
        (HTTP 400) when the index is already there, which is the idempotent
        success path; any other 400 (invalid name, mapping error) is re-raised.
        """
        active_client = client if client is not None else self._client
        if active_client is None:
            msg = "ElasticSearchBackend not connected: client is None"
            raise BackendConnectionError(msg, backend_type="elasticsearch")
        snapshot = (
            snapshot or self._connection_snapshot or self._capture_connection_snapshot()
        )
        for name in (
            snapshot.queue_index,
            snapshot.set_index,
            snapshot.storage_index,
        ):
            try:
                active_client.indices.create(index=name)
            except RequestError as e:
                # HTTP 400 resource_already_exists_exception = idempotent success
                # (index created by a prior connect or a peer worker). Anything else
                # is a real config error — re-raise so it surfaces. Never inspect
                # diagnostic text: only the structured server error type is trusted.
                if not _api_error_has_type(e, "resource_already_exists_exception"):
                    raise

    @contextlib.contextmanager
    def _lease_generation(self, operation: str) -> Iterator[_ElasticSearchGeneration]:
        """Lease one complete generation, lazily connecting in the same epoch."""
        current_thread = threading.get_ident()
        with self._generation_condition:
            if self._connect_owner == current_thread and self._generation is None:
                raise BackendConnectionError(
                    f"Cannot {operation} while Elasticsearch connect is in progress.",
                    backend_type="elasticsearch",
                )
            if self._disconnecting:
                raise BackendConnectionError(
                    f"Cannot {operation} while Elasticsearch is disconnecting.",
                    backend_type="elasticsearch",
                )
            generation = self._generation
            request_epoch = self._lifecycle_epoch
            if generation is not None:
                self._active_leases += 1
                leased = True
            else:
                leased = False

        if not leased:
            # Serialize the epoch check with lifecycle changes. Without this guard,
            # a pre-disconnect operation could wait behind disconnect, reconnect a
            # replacement generation after teardown, and then fail its stale check.
            with self._lifecycle_lock:
                with self._generation_condition:
                    if request_epoch != self._lifecycle_epoch or self._disconnecting:
                        raise BackendConnectionError(
                            f"Elasticsearch connection changed while starting {operation}.",
                            backend_type="elasticsearch",
                        )
                self.connect()
            with self._generation_condition:
                generation = self._generation
                if (
                    request_epoch != self._lifecycle_epoch
                    or self._disconnecting
                    or generation is None
                ):
                    raise BackendConnectionError(
                        f"Elasticsearch connection changed while starting {operation}.",
                        backend_type="elasticsearch",
                    )
                self._active_leases += 1

        assert generation is not None
        previous_depth = int(getattr(self._lease_local, "depth", 0))
        self._lease_local.depth = previous_depth + 1
        try:
            yield generation
        finally:
            try:
                with self._generation_condition:
                    self._active_leases -= 1
                    if self._active_leases == 0:
                        self._generation_condition.notify_all()
            finally:
                self._lease_local.depth = previous_depth

    @contextlib.contextmanager
    def _lease_existing_generation(
        self,
    ) -> Iterator[_ElasticSearchGeneration | None]:
        """Lease the current generation for a non-connecting health operation."""
        with self._generation_condition:
            generation = None if self._disconnecting else self._generation
            if (
                generation is None
                and not self._disconnecting
                and self._client is not None
            ):
                snapshot = (
                    self._connection_snapshot or self._capture_connection_snapshot()
                )
                generation = self._build_generation(self._client, snapshot)
                self._generation = generation
                self._connection_snapshot = snapshot
            if generation is not None:
                self._active_leases += 1
        previous_depth = int(getattr(self._lease_local, "depth", 0))
        if generation is not None:
            self._lease_local.depth = previous_depth + 1
        try:
            yield generation
        finally:
            if generation is not None:
                try:
                    with self._generation_condition:
                        self._active_leases -= 1
                        if self._active_leases == 0:
                            self._generation_condition.notify_all()
                finally:
                    self._lease_local.depth = previous_depth

    def disconnect(self) -> None:
        """Stop admission, drain leases, detach, then close the root client."""
        current_thread = threading.get_ident()
        with self._generation_condition:
            if self._connect_owner == current_thread:
                raise BackendConnectionError(
                    "Cannot disconnect Elasticsearch re-entrantly during connect.",
                    backend_type="elasticsearch",
                )
        if int(getattr(self._lease_local, "depth", 0)):
            raise BackendConnectionError(
                "Cannot disconnect Elasticsearch re-entrantly from an active operation.",
                backend_type="elasticsearch",
            )

        pending_interrupt: BaseException | None = None
        owns_barrier = False
        generation: _ElasticSearchGeneration | None = None
        legacy_client: Elasticsearch | None = None
        with self._lifecycle_lock:
            try:
                with self._generation_condition:
                    if self._disconnect_owner == current_thread:
                        raise BackendConnectionError(
                            "Cannot disconnect Elasticsearch re-entrantly.",
                            backend_type="elasticsearch",
                        )
                    owns_barrier = True
                    self._disconnect_owner = current_thread
                    self._disconnecting = True
                    self._lifecycle_epoch += 1
                    generation = self._generation
                    legacy_client = self._client
                    while self._active_leases:
                        try:
                            self._generation_condition.wait()
                        except BaseException as error:
                            if pending_interrupt is None:
                                pending_interrupt = error
                    self._generation = None
                    self._client = None
                    self._connection_snapshot = None

                # Closing a driver may execute user/transport callbacks. It must never
                # happen under the generation state lock.
                try:
                    self._discard_candidate(
                        generation.client if generation is not None else legacy_client
                    )
                except BaseException as error:
                    if pending_interrupt is None:
                        pending_interrupt = error
            finally:
                if owns_barrier:
                    with self._generation_condition:
                        self._disconnecting = False
                        self._disconnect_owner = None
                        self._generation_condition.notify_all()

        if pending_interrupt is not None:
            raise pending_interrupt

    def is_connected(self) -> bool:
        """Check the leased current generation without implicitly connecting."""
        with self._lease_existing_generation() as generation:
            if generation is None:
                return False
            try:
                return bool(generation.client.ping())
            except Exception:
                return False

    def ping(self) -> bool:
        """Check ElasticSearch health through one operation lease."""
        return self.is_connected()

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.ELASTICSEARCH
        """
        return BackendType.ELASTICSEARCH

    @property
    def client(self) -> Elasticsearch:
        """Get ElasticSearch client, connecting if necessary.

        Returns:
            The ElasticSearch client instance.

        Raises:
            BackendConnectionError: If the client cannot be initialized.
        """
        self.connect()
        with self._generation_condition:
            generation = self._generation
            if generation is not None:
                return generation.client
            # Preserve the historical guard for patched/no-op ``connect`` methods.
            client = self._client
        if client is None:
            msg = "ElasticSearchBackend not connected: client is None after connect()"
            raise BackendConnectionError(msg, backend_type="elasticsearch")
        return client

    # ---- Queue ----

    @queue_operation_error_boundary(
        "push",
        _ELASTICSEARCH_QUEUE_PUSH_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(
            QueueError,
            QueueOutcomeIndeterminateError,
            BackendConnectionError,
        ),
    )
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (lower = more urgent).

        Raises:
            QueueError: If the push operation fails.
            ValueError: If queue_name contains invalid characters.
        """
        _validate_key_name(queue_name, "queue_name")
        document_id = uuid.uuid4().hex
        doc = {
            "queue_name": queue_name,
            "item": _b64encode(item),
            "priority": -priority,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            # No ``refresh`` on write — read-your-writes is enforced at the READ
            # side (pop/count call ``indices.refresh`` first), which amortizes one
            # forced refresh per read instead of ~1s per push. The per-push
            # ``refresh="wait_for"`` was a ~250x perf regression (1010ms/push vs
            # 4ms without — see bench_es_push_refresh.py); ES does not batch
            # ``wait_for`` across consecutive pushes, so each one pays the full
            # refresh-interval wait.
            with self._lease_generation("push") as generation:
                response = generation.mutation_client.index(
                    index=generation.snapshot.queue_index,
                    id=document_id,
                    document=doc,
                )
                _validate_index_response(response, frozenset({"created"}))
        except (TransportError, _ElasticSearchResponseError):
            raise QueueOutcomeIndeterminateError(
                _ELASTICSEARCH_QUEUE_PUSH_ERROR, operation="push"
            ) from None
        except ApiError as e:
            raise QueueError(str(e), queue_name=queue_name, operation="push") from e

    @queue_operation_error_boundary(
        "pop",
        _ELASTICSEARCH_QUEUE_POP_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(
            QueueError,
            QueueOutcomeIndeterminateError,
            BackendConnectionError,
        ),
    )
    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Atomic via optimistic locking: the search returns ``_seq_no`` and
        ``_primary_term`` for each hit, and the delete passes them as
        ``if_seq_no`` / ``if_primary_term``. If another worker deleted or
        modified the doc between search and delete, ES raises
        ``ConflictError`` (HTTP 409) and we retry the search to find the
        next available item.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (unused for ElasticSearch, blocking not supported).

        Returns:
            The popped item, or None if queue is empty (or all attempts lost
            the race to concurrent consumers).

        Raises:
            QueueError: If the pop operation fails (non-conflict transport error).
            ValueError: If queue_name contains invalid characters.
        """
        _validate_key_name(queue_name, "queue_name")
        max_attempts = 3
        with self._lease_generation("pop") as generation:
            for _attempt in range(max_attempts):
                try:
                    # Force one refresh before searching so recent pushes AND deletes
                    # from prior pops are visible. Forced ``indices.refresh`` is ms-scale
                    # (just flushes the indexing buffer to a segment) — far cheaper than
                    # the per-push ``refresh="wait_for"`` it replaces (which blocked ~1s
                    # per push). Amortized: N fast pushes + 1 refresh per read.
                    generation.client.indices.refresh(
                        index=generation.snapshot.queue_index
                    )
                    resp = generation.client.search(
                        index=generation.snapshot.queue_index,
                        # ``.keyword`` subfield for exact match: the dynamic mapping makes
                        # ``queue_name`` a ``text`` field (standard analyzer), so a name
                        # with colons (e.g. ``inttest:<uuid>:queue``) gets tokenized and a
                        # ``term`` on the analyzed field never matches. Keyword subfield is
                        # not analyzed → exact term match regardless of punctuation.
                        query={"term": {"queue_name.keyword": queue_name}},
                        sort=[{"priority": "asc"}, {"created_at": "asc"}],
                        size=1,
                        # ES 8.x omits ``_seq_no`` / ``_primary_term`` from search hits by
                        # default (7.x included them). The optimistic-locking delete below
                        # requires both, so request them explicitly — without this the pop
                        # raises ``KeyError: '_seq_no'`` on every call under ES 8.x.
                        seq_no_primary_term=True,
                        track_total_hits=True,
                    )
                    hits = _validate_search_response(resp)
                    if not hits:
                        return None
                    document_id, sequence_number, primary_term, item = (
                        _decode_queue_hit(hits[0])
                    )
                    try:
                        # No ``refresh`` on delete — the NEXT pop's pre-search refresh
                        # (above) flushes this delete, so the search won't re-find the doc.
                        delete_response = generation.mutation_client.delete(
                            index=generation.snapshot.queue_index,
                            id=document_id,
                            if_seq_no=sequence_number,
                            if_primary_term=primary_term,
                        )
                        if not _validate_delete_response(delete_response):
                            raise _ElasticSearchResponseError
                    except ConflictError:
                        # Lost the race to another worker — retry to find the next item.
                        continue
                    except NotFoundError:
                        # A DELETE 404 is a race, not proof that the queue was empty.
                        raise QueueError(
                            _ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop"
                        ) from None
                    except (TransportError, _ElasticSearchResponseError):
                        raise QueueOutcomeIndeterminateError(
                            _ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop"
                        ) from None
                    return item
                except NotFoundError:
                    return None
                except _ElasticSearchResponseError:
                    raise QueueError(
                        _ELASTICSEARCH_QUEUE_POP_ERROR, operation="pop"
                    ) from None
                except (ApiError, TransportError) as e:
                    # R19-A: catch the broad ApiError (auth/permission/server/query faults),
                    # not just TransportError — every sibling ES hot-path does. A non-NotFound,
                    # non-Conflict ApiError subclass otherwise escapes raw past the QueueError
                    # contract this method's docstring promises. (NotFoundError -> None above;
                    # ConflictError is handled by the inner delete try's `continue`.)
                    raise QueueError(
                        str(e), queue_name=queue_name, operation="pop"
                    ) from e
        return None

    @queue_operation_error_boundary(
        "pop",
        _ELASTICSEARCH_QUEUE_POP_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(
            QueueError,
            QueueOutcomeIndeterminateError,
            BackendConnectionError,
        ),
    )
    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, None]:
        """Pop atomically without retaining the inherited base operation frame."""
        return (self.pop(queue_name, timeout), None)

    @queue_operation_error_boundary(
        "queue_len",
        _ELASTICSEARCH_QUEUE_LENGTH_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(
            QueueError,
            QueueOutcomeIndeterminateError,
            BackendConnectionError,
        ),
    )
    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Args:
            queue_name: Name of the queue.

        Returns:
            Number of items in the queue.

        Raises:
            QueueError: If the operation fails.
            ValueError: If queue_name contains invalid characters.
        """
        _validate_key_name(queue_name, "queue_name")
        try:
            with self._lease_generation("queue_len") as generation:
                return self._count(
                    generation,
                    generation.snapshot.queue_index,
                    "queue_name",
                    queue_name,
                )
        except _ElasticSearchResponseError:
            raise QueueError(
                _ELASTICSEARCH_QUEUE_LENGTH_ERROR, operation="queue_len"
            ) from None
        except (ApiError, TransportError) as e:
            raise QueueError(
                str(e), queue_name=queue_name, operation="queue_len"
            ) from e

    @queue_operation_error_boundary(
        "clear_queue",
        _ELASTICSEARCH_QUEUE_CLEAR_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(
            QueueError,
            QueueOutcomeIndeterminateError,
            BackendConnectionError,
        ),
    )
    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.

        Raises:
            ValueError: If queue_name contains invalid characters.
            QueueError: If the delete-by-query request fails.
        """
        _validate_key_name(queue_name, "queue_name")
        try:
            with self._lease_generation("clear_queue") as generation:
                self._delete_by_term(
                    generation,
                    generation.snapshot.queue_index,
                    "queue_name",
                    queue_name,
                )
        except (TransportError, _ElasticSearchResponseError):
            raise QueueOutcomeIndeterminateError(
                _ELASTICSEARCH_QUEUE_CLEAR_ERROR, operation="clear_queue"
            ) from None
        except ApiError as e:
            msg = f"Failed to clear ElasticSearch queue {queue_name!r}: {e}"
            raise QueueError(msg, queue_name=queue_name, operation="clear_queue") from e

    # ---- Set ----

    def _set_doc_id(self, set_name: str, item: bytes) -> str:
        """Generate document ID for set member.

        Args:
            set_name: Name of the set.
            item: Item bytes.

        Returns:
            Document ID string.
        """
        return f"{set_name}:{hashlib.sha256(item).hexdigest()}"

    @configuration_error_boundary(
        _ELASTICSEARCH_SET_REQUEST_ERROR,
        _ELASTICSEARCH_OPERATION_SETTING_NAMES,
        fallback_setting_name="operation",
        pass_through_exception_types=(BackendConnectionError,),
        catch_unexpected=False,
    )
    @set_operation_error_boundary(
        _ELASTICSEARCH_SET_ADD_ERROR,
        "elasticsearch",
        validator=_validate_set_name_argument,
    )
    def add(self, set_name: str, item: bytes) -> bool:
        """Add item to set.

        Args:
            set_name: Name of the set.
            item: Item to add (bytes).

        Returns:
            True if added, False if already existed.

        Raises:
            ValueError: If set_name contains invalid characters.
        """
        _validate_key_name(set_name, "set_name")
        doc_id = self._set_doc_id(set_name, item)
        doc = {
            "set_name": set_name,
            "item_hash": hashlib.sha256(item).hexdigest(),
            "item": _b64encode(item),
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            # No ``refresh`` on write — ``contains`` is by-id (immediately
            # consistent); ``set_len`` refreshes in ``_count``. Same amortized
            # read-refresh rationale as push.
            with self._lease_generation("add") as generation:
                response = generation.mutation_client.index(
                    index=generation.snapshot.set_index,
                    id=doc_id,
                    document=doc,
                    op_type="create",
                )
                _validate_index_response(response, frozenset({"created"}))
        except ConflictError:
            return False
        except RequestError as e:
            if _api_error_has_type(e, "version_conflict_engine_exception"):
                return False
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e
        except ApiError as e:
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e
        except (TransportError, _ElasticSearchResponseError):
            raise SetOutcomeIndeterminateError(
                _ELASTICSEARCH_SET_ADD_ERROR, backend_type="elasticsearch"
            ) from None
        return True

    @configuration_error_boundary(
        _ELASTICSEARCH_SET_REQUEST_ERROR,
        _ELASTICSEARCH_OPERATION_SETTING_NAMES,
        fallback_setting_name="operation",
        pass_through_exception_types=(BackendConnectionError,),
        catch_unexpected=False,
    )
    @set_operation_error_boundary(
        _ELASTICSEARCH_SET_REMOVE_ERROR,
        "elasticsearch",
        validator=_validate_set_name_argument,
    )
    def remove(self, set_name: str, item: bytes) -> bool:
        """Remove item from set.

        Args:
            set_name: Name of the set.
            item: Item to remove.

        Returns:
            True if removed, False if didn't exist.

        Raises:
            ValueError: If set_name contains invalid characters.
        """
        _validate_key_name(set_name, "set_name")
        try:
            with self._lease_generation("remove") as generation:
                return self._delete_by_id_on_generation(
                    generation,
                    generation.snapshot.set_index,
                    self._set_doc_id(set_name, item),
                )
        except (TransportError, _ElasticSearchResponseError):
            raise SetOutcomeIndeterminateError(
                _ELASTICSEARCH_SET_REMOVE_ERROR, backend_type="elasticsearch"
            ) from None
        except ApiError as e:
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e

    @configuration_error_boundary(
        _ELASTICSEARCH_SET_REQUEST_ERROR,
        _ELASTICSEARCH_OPERATION_SETTING_NAMES,
        fallback_setting_name="operation",
        pass_through_exception_types=(BackendConnectionError,),
        catch_unexpected=False,
    )
    @set_operation_error_boundary(
        _ELASTICSEARCH_SET_CONTAINS_ERROR,
        "elasticsearch",
        validator=_validate_set_name_argument,
    )
    def contains(self, set_name: str, item: bytes) -> bool:
        """Check if item is in set.

        Args:
            set_name: Name of the set.
            item: Item to check.

        Returns:
            True if item exists in the set.

        Raises:
            ValueError: If set_name contains invalid characters.
        """
        _validate_key_name(set_name, "set_name")
        try:
            with self._lease_generation("contains") as generation:
                response = generation.client.exists(
                    index=generation.snapshot.set_index,
                    id=self._set_doc_id(set_name, item),
                )
        except TransportError as e:
            raise BackendConnectionError(
                f"ElasticSearch set contains failed for {set_name!r}: {e}",
                backend_type="elasticsearch",
            ) from e
        except ApiError as e:
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e
        return bool(response)

    @configuration_error_boundary(
        _ELASTICSEARCH_SET_REQUEST_ERROR,
        _ELASTICSEARCH_OPERATION_SETTING_NAMES,
        fallback_setting_name="operation",
        pass_through_exception_types=(BackendConnectionError,),
        catch_unexpected=False,
    )
    @set_operation_error_boundary(
        _ELASTICSEARCH_SET_LENGTH_ERROR,
        "elasticsearch",
        validator=_validate_set_name_argument,
    )
    def set_len(self, set_name: str) -> int:
        """Get set size.

        Args:
            set_name: Name of the set.

        Returns:
            Number of items in the set.

        Raises:
            ValueError: If set_name contains invalid characters.
        """
        _validate_key_name(set_name, "set_name")
        try:
            with self._lease_generation("set_len") as generation:
                return self._count(
                    generation, generation.snapshot.set_index, "set_name", set_name
                )
        except (TransportError, _ElasticSearchResponseError):
            raise BackendConnectionError(
                _ELASTICSEARCH_SET_LENGTH_ERROR,
                backend_type="elasticsearch",
            ) from None
        except ApiError as e:
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e

    @configuration_error_boundary(
        _ELASTICSEARCH_SET_REQUEST_ERROR,
        _ELASTICSEARCH_OPERATION_SETTING_NAMES,
        fallback_setting_name="operation",
        pass_through_exception_types=(BackendConnectionError,),
        catch_unexpected=False,
    )
    @set_operation_error_boundary(
        _ELASTICSEARCH_SET_CLEAR_ERROR,
        "elasticsearch",
        validator=_validate_set_name_argument,
    )
    def clear_set(self, set_name: str) -> None:
        """Clear all items from set.

        Args:
            set_name: Name of the set.

        Raises:
            ValueError: If set_name contains invalid characters.
            BackendConnectionError: If the delete-by-query request fails.
        """
        _validate_key_name(set_name, "set_name")
        try:
            with self._lease_generation("clear_set") as generation:
                self._delete_by_term(
                    generation, generation.snapshot.set_index, "set_name", set_name
                )
        except (TransportError, _ElasticSearchResponseError):
            raise SetOutcomeIndeterminateError(
                _ELASTICSEARCH_SET_CLEAR_ERROR, backend_type="elasticsearch"
            ) from None
        except ApiError as e:
            raise ConfigurationError(
                _ELASTICSEARCH_SET_REQUEST_ERROR,
                setting_name="operation",
            ) from e

    # ---- Storage ----

    @storage_operation_error_boundary(
        "store",
        _ELASTICSEARCH_STORAGE_STORE_ERROR,
        "elasticsearch",
        validator=_validate_store_arguments,
    )
    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        """Store data with key.

        Args:
            key: Storage key.
            data: Data to store (bytes).
            ttl: Optional time-to-live in seconds.

        Raises:
            ValueError: If key contains invalid characters.
            StorageError: If the write request fails.
        """
        _validate_key_name(key, "key")
        _validate_ttl(ttl)
        doc: dict[str, Any] = {"key": key, "data": _b64encode(data)}
        if ttl is not None:
            doc["expireAt"] = (
                datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)
            ).isoformat()
        try:
            with self._lease_generation("store") as generation:
                response = generation.mutation_client.index(
                    index=generation.snapshot.storage_index, id=key, document=doc
                )
                _validate_index_response(response, frozenset({"created", "updated"}))
        except (TransportError, _ElasticSearchResponseError):
            raise StorageOutcomeIndeterminateError(
                _ELASTICSEARCH_STORAGE_STORE_ERROR, operation="store"
            ) from None
        except ApiError as e:
            msg = f"Failed to store key {key!r} in ElasticSearch: {e}"
            raise StorageError(msg, operation="store", key=key) from e

    @staticmethod
    def _storage_source(response: Any, key: str, operation: str) -> dict[str, Any]:
        """Return a validated storage document source."""
        try:
            response_body = _response_mapping(response)
        except _ElasticSearchResponseError:
            raise StorageError(
                _ELASTICSEARCH_STORAGE_RETRIEVE_ERROR,
                operation=operation,
                key=key,
            ) from None
        source = response_body.get("_source")
        if not isinstance(source, Mapping):
            raise StorageError(
                f"Corrupt ElasticSearch storage document for key {key!r}: "
                "missing object _source",
                operation=operation,
                key=key,
            )
        return dict(source)

    @staticmethod
    def _storage_expiry(
        source: dict[str, Any], key: str, operation: str
    ) -> datetime | None:
        """Parse an optional expiry, rejecting corrupt persisted schema."""
        if "expireAt" not in source or source["expireAt"] is None:
            return None
        raw_expiry = source["expireAt"]
        if not isinstance(raw_expiry, str):
            raise StorageError(
                f"Corrupt ElasticSearch expiry for key {key!r}: expected ISO string",
                operation=operation,
                key=key,
            )
        try:
            expiry = datetime.fromisoformat(raw_expiry)
        except ValueError as e:
            raise StorageError(
                f"Corrupt ElasticSearch expiry for key {key!r}: {raw_expiry!r}",
                operation=operation,
                key=key,
            ) from e
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return expiry

    @staticmethod
    def _storage_data(source: dict[str, Any], key: str) -> bytes:
        """Strictly decode the required Base64 storage payload."""
        encoded = source.get("data")
        if not isinstance(encoded, str):
            raise StorageError(
                f"Corrupt ElasticSearch storage payload for key {key!r}",
                operation="retrieve",
                key=key,
            )
        try:
            return base64.b64decode(encoded.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error, ValueError) as e:
            raise StorageError(
                f"Corrupt ElasticSearch Base64 payload for key {key!r}",
                operation="retrieve",
                key=key,
            ) from e

    def _lazy_reap_if_expired(
        self,
        generation: _ElasticSearchGeneration,
        response: Any,
        key: str,
        operation: str,
    ) -> bool:
        """R-esttl: lazy-reap an expired storage doc; return True if expired.

        The conditional delete prevents stale documents from accumulating (ES
        has no native TTL reaper). Cleanup failures surface instead of reporting
        an absent success with unknown physical state.
        """
        source = self._storage_source(response, key, operation)
        expiry = self._storage_expiry(source, key, operation)
        if expiry is None or expiry > datetime.now(tz=timezone.utc):
            return False
        try:
            response_body = _response_mapping(response)
            seq_no = _exact_nonnegative_int(response_body.get("_seq_no"))
            primary_term = _exact_nonnegative_int(response_body.get("_primary_term"))
        except _ElasticSearchResponseError:
            raise StorageError(
                _ELASTICSEARCH_STORAGE_DELETE_ERROR,
                operation=operation,
                key=key,
            ) from None
        try:
            delete_response = generation.mutation_client.delete(
                index=generation.snapshot.storage_index,
                id=key,
                if_seq_no=seq_no,
                if_primary_term=primary_term,
            )
            _validate_delete_response(delete_response)
        except (ConflictError, NotFoundError):
            pass
        except (TransportError, _ElasticSearchResponseError):
            raise StorageOutcomeIndeterminateError(
                _ELASTICSEARCH_STORAGE_DELETE_ERROR, operation=operation
            ) from None
        except ApiError:
            raise StorageError(
                _ELASTICSEARCH_STORAGE_DELETE_ERROR,
                operation=operation,
                key=key,
            ) from None
        return True

    @storage_operation_error_boundary(
        "retrieve",
        _ELASTICSEARCH_STORAGE_RETRIEVE_ERROR,
        "elasticsearch",
        validator=_validate_storage_key_argument,
    )
    def retrieve(self, key: str) -> bytes | None:
        """Retrieve data by key.

        Returns None if the key is absent OR expired (R-esttl: expired docs are
        lazy-reaped and treated as absent — matching DynamoDB retrieve. ES has no
        native TTL so expiry is enforced on read). Pre-fix this returned expired
        data verbatim (stale reads).

        Args:
            key: Storage key.

        Returns:
            Stored data, or None if not found / expired.

        Raises:
            ValueError: If key contains invalid characters.
            StorageError: If the read request fails.
        """
        _validate_key_name(key, "key")
        try:
            with self._lease_generation("retrieve") as generation:
                try:
                    resp = generation.client.get(
                        index=generation.snapshot.storage_index, id=key
                    )
                except NotFoundError:
                    return None
                source = self._storage_source(resp, key, "retrieve")
                if self._lazy_reap_if_expired(generation, resp, key, "retrieve"):
                    return None
                return self._storage_data(source, key)
        except (ApiError, TransportError) as e:
            msg = f"Failed to retrieve key {key!r} from ElasticSearch: {e}"
            raise StorageError(msg, operation="retrieve", key=key) from e

    @storage_operation_error_boundary(
        "delete",
        _ELASTICSEARCH_STORAGE_DELETE_ERROR,
        "elasticsearch",
        validator=_validate_storage_key_argument,
    )
    def delete(self, key: str) -> bool:
        """Delete data by key.

        Args:
            key: Storage key.

        Returns:
            True if deleted, False if didn't exist.

        Raises:
            ValueError: If key contains invalid characters.
            StorageError: If the delete request fails.
        """
        _validate_key_name(key, "key")
        try:
            with self._lease_generation("delete") as generation:
                return self._delete_by_id_on_generation(
                    generation, generation.snapshot.storage_index, key
                )
        except (TransportError, _ElasticSearchResponseError):
            raise StorageOutcomeIndeterminateError(
                _ELASTICSEARCH_STORAGE_DELETE_ERROR, operation="delete"
            ) from None
        except ApiError as e:
            msg = f"Failed to delete key {key!r} from ElasticSearch: {e}"
            raise StorageError(msg, operation="delete", key=key) from e

    @storage_operation_error_boundary(
        "exists",
        _ELASTICSEARCH_STORAGE_EXISTS_ERROR,
        "elasticsearch",
        validator=_validate_storage_key_argument,
    )
    def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired.

        R-esttl: uses ``get`` (not the cheap ``exists`` HEAD) so an expired doc can
        be lazy-reaped and reported as absent — matches the DynamoDB ``exists``
        contract ("present AND not expired"). Pre-fix this returned True for
        expired docs (the cheap exists-check ignored ``expireAt``).

        Args:
            key: Storage key.

        Returns:
            True if the key exists and is current (not expired).

        Raises:
            ValueError: If key contains invalid characters.
            StorageError: On a transport failure (was previously a raw
                ``TransportError`` with no typed wrapper).
        """
        _validate_key_name(key, "key")
        try:
            with self._lease_generation("exists") as generation:
                try:
                    resp = generation.client.get(
                        index=generation.snapshot.storage_index, id=key
                    )
                except NotFoundError:
                    return False
                if self._lazy_reap_if_expired(generation, resp, key, "exists"):
                    return False
                return True
        except (ApiError, TransportError) as e:
            msg = f"Failed to check existence of key {key!r} in ElasticSearch: {e}"
            raise StorageError(msg, operation="exists", key=key) from e

    @storage_operation_error_boundary(
        "ttl",
        _ELASTICSEARCH_STORAGE_TTL_ERROR,
        "elasticsearch",
        validator=_validate_storage_key_argument,
    )
    def ttl(self, key: str) -> int | None:
        """Get remaining time-to-live.

        Args:
            key: Storage key.

        Returns:
            Non-negative seconds remaining, or None if absent, permanent, or expired.

        Raises:
            ValueError: If key contains invalid characters.
            StorageError: If the read request fails.
        """
        _validate_key_name(key, "key")
        try:
            with self._lease_generation("ttl") as generation:
                try:
                    resp = generation.client.get(
                        index=generation.snapshot.storage_index, id=key
                    )
                except NotFoundError:
                    return None
                source = self._storage_source(resp, key, "ttl")
                expiry = self._storage_expiry(source, key, "ttl")
                if expiry is None:
                    return None
                if self._lazy_reap_if_expired(generation, resp, key, "ttl"):
                    return None
                remaining = (expiry - datetime.now(tz=timezone.utc)).total_seconds()
                return max(0, int(remaining))
        except (ApiError, TransportError) as e:
            msg = f"Failed to read TTL of key {key!r} in ElasticSearch: {e}"
            raise StorageError(msg, operation="ttl", key=key) from e

    @storage_operation_error_boundary(
        "clear_storage",
        _ELASTICSEARCH_STORAGE_CLEAR_ERROR,
        "elasticsearch",
        validator=_validate_storage_prefix_argument,
    )
    def clear_storage(self, prefix: str | None = None) -> None:
        """Clear all stored data, optionally filtered by prefix.

        Args:
            prefix: If provided, only clear keys starting with this prefix.
                   If None, clear all storage data.

        Raises:
            ValueError: If a provided prefix contains invalid characters.
            StorageError: If the delete-by-query request fails.
        """
        if prefix is not None:
            _validate_key_name(prefix, "prefix")
        # R-es-keyword: target the ``.keyword`` subfield, not the analyzed ``key``
        # text field. ``key`` is dynamically mapped as text (standard analyzer); a
        # ``prefix`` query on the analyzed field matches tokens, not the full key
        # value, so prefix clearing would silently over-match or no-op. The
        # ``.keyword`` subfield is unanalyzed → exact-prefix match (same convention
        # as ``_count`` / ``_delete_by_term`` / ``pop``). Parity with redis
        # scan_iter(match=prefix*) and dynamodb begins_with (#64).
        query = {"prefix": {"key.keyword": prefix}} if prefix else {"match_all": {}}
        try:
            with self._lease_generation("clear_storage") as generation:
                self._delete_by_query_on_generation(
                    generation, generation.snapshot.storage_index, query
                )
        except (TransportError, _ElasticSearchResponseError):
            raise StorageOutcomeIndeterminateError(
                _ELASTICSEARCH_STORAGE_CLEAR_ERROR, operation="clear_storage"
            ) from None
        except ApiError as e:
            msg = f"Failed to clear ElasticSearch storage: {e}"
            raise StorageError(msg, operation="clear_storage", key=None) from e

    # ---- Shared helpers ----

    def _count(
        self, generation: _ElasticSearchGeneration, index: str, field: str, value: str
    ) -> int:
        """Count documents matching a term query.

        Args:
            index: Index name.
            field: Field to match.
            value: Value to match.

        Returns:
            Number of matching documents.

        Raises:
            TransportError: If the refresh or count request fails. Propagates to
                the caller (R-es-qlen) -- pre-fix this was swallowed to ``0``,
                which dead-coded ``queue_len``'s ``QueueError`` arm.
        """
        # R-es-qlen: do NOT swallow TransportError -> return 0. Pre-fix this
        # swallowed, making queue_len's ``except TransportError -> raise QueueError``
        # arm dead code (queue_len returned 0 on error, masking a backend failure
        # from the scheduler's idle/backpressure gate -- R-qlen violation, same as
        # sqs:507). Now TransportError propagates to the caller; each caller applies
        # its own typed error contract.
        # Forced refresh so just-written docs (push/add don't refresh) are
        # searchable — same amortized-read-refresh rationale as pop.
        generation.client.indices.refresh(index=index)
        # ``.keyword`` subfield — see pop's term-query note. ``queue_name`` /
        # ``set_name`` are dynamically mapped as ``text``; count must match the
        # exact (unanalyzed) value via the keyword subfield.
        resp = generation.client.count(
            index=index, query={"term": {f"{field}.keyword": value}}
        )
        return _validate_count_response(resp)

    def _delete_by_id(self, index: str, doc_id: str) -> bool:
        """Compatibility helper that leases one generation for a direct call."""
        with self._lease_generation("delete_by_id") as generation:
            return self._delete_by_id_on_generation(generation, index, doc_id)

    @staticmethod
    def _delete_by_id_on_generation(
        generation: _ElasticSearchGeneration, index: str, doc_id: str
    ) -> bool:
        """Delete a document by ID using only the supplied generation."""
        try:
            response = generation.mutation_client.delete(index=index, id=doc_id)
        except NotFoundError:
            return False
        return _validate_delete_response(response)

    def _delete_by_term(
        self,
        generation: _ElasticSearchGeneration,
        index: str,
        field: str,
        value: str,
    ) -> None:
        """Delete all documents matching a term query.

        Args:
            index: Index name.
            field: Field to match.
            value: Value to match.
        """
        # ``.keyword`` subfield — same exact-match rationale as ``_count``.
        self._delete_by_query_on_generation(
            generation, index, {"term": {f"{field}.keyword": value}}
        )

    def _delete_by_query(self, index: str, query: dict[str, Any]) -> None:
        """Compatibility helper that leases one generation for a direct call."""
        with self._lease_generation("delete_by_query") as generation:
            self._delete_by_query_on_generation(generation, index, query)

    @staticmethod
    def _delete_by_query_on_generation(
        generation: _ElasticSearchGeneration,
        index: str,
        query: dict[str, Any],
    ) -> None:
        """Delete matching documents using only the supplied generation."""
        response = generation.mutation_client.delete_by_query(index=index, query=query)
        _validate_delete_by_query_response(response)
