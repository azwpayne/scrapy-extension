"""RocketMQ backend implementation (apache ``rocketmq-python-client`` 5.1.1 gRPC).

Rewritten (#44) from the prior fictional-API stub. The original backend's
``connect()`` imports (``rocketmq.consumer.SimpleConsumer``,
``rocketmq.auth.credentials.PlainCredentials``, ``rocketmq.endpoint.Endpoint``,
``rocketmq.message.Message``) matched NO released client — lazy-import hid this
since project inception; the backend had never connected to any broker. This
implementation targets the apache RocketMQ 5.x gRPC client
(``rocketmq-python-client`` 5.1.1, pure-Python, no native lib — installable on
CI without the librocketmq native-lib pain that blocked the old ctypes client).

API map (apache 5.1.1, verified against apache/rocketmq-clients python/example):
- ``ClientConfiguration(endpoints: str, credentials, namespace='', request_timeout=3)``
- ``Credentials(ak='', sk='')``
- ``Producer(config, topics=None)`` / ``producer.startup()`` / ``producer.send(msg) -> SendReceipt``
- ``SimpleConsumer(config, consumer_group, subscription=None, await_duration=20)`` /
  ``consumer.startup()`` / ``consumer.subscribe(topic)`` /
  ``consumer.receive(max_num, invisible_duration) -> list[Message] | None`` /
  ``consumer.ack(msg)``
- ``Message()`` with ``.topic`` / ``.body`` (bytes) / ``.keys`` / ``.tag`` /
  ``.add_property(k, v)``; received messages carry ``.message_id``.

Endpoints are the gRPC PROXY (port 8081), NOT the legacy NameServer (9876) —
the broker must run with ``--enable-proxy`` (see tests/integration/docker-compose.yml).
"""

from __future__ import annotations

import logging
import re
import threading
import time
from _thread import start_new_thread
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from scrapy_extension.backends._generation import (
    GenerationLeaseGate,
    GenerationRecord,
    GenerationUnavailable,
)
from scrapy_extension.backends._optional import _is_missing_optional_dependency
from scrapy_extension.backends._redaction import _redact
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    _validate_key_name,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import (
    backend_connection_error_boundary,
    configuration_error_boundary,
    control_exception_traceback_boundary,
    import_error_traceback_boundary,
    not_implemented_error_boundary,
    queue_operation_error_boundary,
)
from scrapy_extension.settings import RocketMQSettings
from scrapy_extension.settings._broker_endpoints import (
    ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
)
from scrapy_extension.settings.rocketmq import (
    validate_rocketmq_connection,
)

logger = logging.getLogger(__name__)

_ROCKETMQ_CONFIGURATION_SETTING_NAMES: frozenset[str] = frozenset(
    RocketMQSettings.model_fields
)
_ROCKETMQ_SEND_TIMEOUT_ERROR = (
    "RocketMQ send_timeout must be an integer between 0 and 300000 milliseconds."
)
_ROCKETMQ_TOPIC_PREFIX_ERROR = "RocketMQ topic_prefix is invalid."
_ROCKETMQ_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
        _ROCKETMQ_SEND_TIMEOUT_ERROR,
        _ROCKETMQ_TOPIC_PREFIX_ERROR,
        "Unsupported RocketMQ mode.",
        "Cloud mode requires access_key and secret_key.",
    }
)
_ROCKETMQ_SAFE_CONNECTION_MESSAGES: frozenset[str] = frozenset(
    {
        "rocketmq-python-client not installed.",
        "RocketMQBackend producer initialization returned None",
        "RocketMQBackend consumer initialization returned None",
        "Failed to connect to RocketMQ.",
    }
)
_ROCKETMQ_MAX_MESSAGE_SIZE_ERROR = (
    "RocketMQ message exceeds configured max_message_size."
)
_ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE = (
    "clear_queue is not supported by the RocketMQ client"
)
_ROCKETMQ_TOPIC_ALREADY_SELECTED_ERROR = (
    "RocketMQ consumer generation already selected a different queue."
)
_ROCKETMQ_RECEIVE_PUMP_ERROR = "RocketMQ receive pump failed."
_ROCKETMQ_CONNECTION_CHANGED_PUSH_ERROR = "RocketMQ connection changed during push."
_ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR = (
    "RocketMQ connection changed during operation."
)
_ROCKETMQ_DISCONNECT_ERROR = "Failed to disconnect from RocketMQ."
_ROCKETMQ_SAFE_QUEUE_MESSAGES: frozenset[str] = frozenset(
    {
        "Not connected to RocketMQ",
        "RocketMQBackend not connected: producer is None",
        "RocketMQBackend not connected: consumer is None",
        _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR,
        _ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
        _ROCKETMQ_TOPIC_ALREADY_SELECTED_ERROR,
        _ROCKETMQ_CONNECTION_CHANGED_PUSH_ERROR,
        _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
    }
)
_ROCKETMQ_QUEUE_LEN_UNSUPPORTED_MESSAGE = (
    "RocketMQ queue depth is unsupported: no broker-side depth RPC"
)

# Module-level warn-once flag for the unsupported-depth signal (Risk 1).
# RocketMQ's deferred-ack model has no broker-side depth RPC, so queue_len
# raises NotImplementedError. The first call warns once per process so
# operators know idle detection / depth backpressure will degrade
# conservatively. Tests reset this for isolation.
_queue_len_warned: bool = False

# RocketMQ 5.x documents a 10-second floor for SimpleConsumer invisible time.
# ``ChangeInvisibleDuration`` uses the same range, so an explicit nack shortens
# the retry delay to this floor rather than waiting out the normal processing
# lease. Zero-delay nack is not supported by the broker.
_MIN_INVISIBLE_DURATION = 10

# RocketMQ Proxy clamps every SimpleConsumer request to at least five seconds
# (``grpcClientConsumerMinLongPollingTimeoutMillis``). Sending a shorter SDK
# await duration also shortens the gRPC deadline; the proxy then rejects the
# request with 40018 before checking the queue because the deadline cannot cover
# its polling floor. Match that server contract so short/non-blocking interface
# requests remain consumable instead of failing deterministically.
_MIN_LONG_POLL_DURATION = 5
_ROCKETMQ_TOPIC_PREFIX_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+\Z")


class _RocketMQConnectionAttemptFenced(RuntimeError):
    """Internal signal that teardown won a private connection attempt."""


# R22-A: ceiling (seconds) for the gRPC per-RPC ``request_timeout`` derived from
# ``send_timeout``. Matches the Field ``le=300_000`` (ms) in ``RocketMQSettings``
# — both express the same 5 min ceiling in their native units. Defense-in-depth:
# even if the Field is bypassed (raw dict config, a future settings source), the
# conversion cannot push the per-RPC deadline past this and wedge the client on
# a stalled broker. Mirrors the R21 cap discipline
# (``CIRCUIT_BREAKER_MAX_RESET_TIMEOUT_S`` / ``_MAX_BACKOFF_S``).
_MAX_REQUEST_TIMEOUT_S = 300

# Consumer shutdown should interrupt an in-flight receive promptly. Do not trust
# either shutdown itself or that interrupt guarantee during teardown: a broken
# SDK must not make ``disconnect`` wait forever for cleanup or the daemon pump.
_CLIENT_SHUTDOWN_JOIN_TIMEOUT_S = 1.0
_RECEIVE_PUMP_JOIN_TIMEOUT_S = 1.0


def _validate_queue_name_argument(
    _backend: object,
    queue_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a public queue argument before its terminal error boundary."""
    _validate_key_name(queue_name, "queue_name")


def _validate_topic_prefix(topic_prefix: object) -> str:
    """Validate the physical prefix at the connection snapshot boundary."""
    if (
        type(topic_prefix) is not str
        or not topic_prefix
        or len(topic_prefix.encode("utf-8")) > 127
        or _ROCKETMQ_TOPIC_PREFIX_PATTERN.fullmatch(topic_prefix) is None
    ):
        raise ConfigurationError(
            _ROCKETMQ_TOPIC_PREFIX_ERROR,
            setting_name="topic_prefix",
        )
    return topic_prefix


def _validate_send_timeout(send_timeout: object) -> int:
    """Revalidate mutable settings before deriving an SDK timeout."""
    if type(send_timeout) is not int or send_timeout < 0 or send_timeout > 300_000:
        raise ConfigurationError(
            _ROCKETMQ_SEND_TIMEOUT_ERROR,
            setting_name="send_timeout",
        )
    return send_timeout


class _RocketMQCleanupResult:
    """Fence one daemon shutdown task's result after its join budget expires."""

    __slots__ = (
        "_accepting",
        "_completed",
        "_completed_event",
        "_error",
        "_lock",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._accepting = True
        self._completed = False
        self._error: BaseException | None = None
        self._completed_event = threading.Event()

    def publish(self, error: BaseException | None) -> None:
        """Publish completion only while the disconnect generation accepts it."""
        with self._lock:
            if self._accepting:
                self._completed = True
                self._error = error
        self._completed_event.set()

    def fence(self) -> tuple[bool, BaseException | None]:
        """Reject late publication and return the result visible at the fence."""
        with self._lock:
            self._accepting = False
            return self._completed, self._error


@dataclass(frozen=True, slots=True)
class _RocketMQOperationSnapshot:
    """Immutable operation settings fixed for one client generation."""

    topic_prefix: str
    max_message_size: int
    invisible_duration: int


@dataclass(slots=True, eq=False)
class _RocketMQGenerationHandles:
    """Clients and legacy delivery state retained for admitted operations."""

    producer: Any
    consumer: Any
    snapshot: _RocketMQOperationSnapshot
    consumer_generation: int
    legacy_message: Any = None
    legacy_delivery: tuple[Any, int, Any] | None = None


class _RocketMQAckToken:
    """Consumer-generation-scoped token for one RocketMQ delivery."""

    __slots__ = (
        "_settlement_lock",
        "_settlement_state",
        "consumer",
        "generation",
        "message",
    )

    def __init__(self, message: Any, consumer: Any, generation: int) -> None:
        self.message = message
        self.consumer = consumer
        self.generation = generation
        self._settlement_lock = threading.Lock()
        self._settlement_state = "pending"

    def _settle(self, terminal_state: str, operation: Callable[[], None]) -> bool:
        """Run one broker settlement, restoring retryability on failure."""
        with self._settlement_lock:
            if self._settlement_state != "pending":
                return False
            self._settlement_state = "settling"
            completed = False
            try:
                operation()
                completed = True
            finally:
                self._settlement_state = terminal_state if completed else "pending"
            return True


class RocketMQBackend(Backend, QueueBackend):
    """RocketMQ backend implementation (apache 5.1.1 gRPC client).

    Note: RocketMQ only supports QueueBackend operations.
    SetBackend and StorageBackend are not supported by RocketMQ. Configuring
    RocketMQ for the set/storage component is rejected at config time by
    ``resolve_backend_config`` (RocketMQ is excluded from
    ``SET_CAPABLE_BACKENDS`` / ``STORAGE_CAPABLE_BACKENDS``). If that gating is
    bypassed, instantiation fails fast via the dedicated guard classes
    ``RocketMQSetBackend`` / ``RocketMQStorageBackend`` (raise
    ``ConfigurationError`` in ``__init__``).

    The direct receive API binds one logical queue/topic to each connected
    consumer generation. To consume a different queue, callers must disconnect
    and reconnect before the first pop for the replacement generation.
    """

    _push_is_durable = True
    # Ack capability (initiative #4 — at-least-once): the apache SimpleConsumer
    # uses a deferred-ack model. ``receive`` yields messages WITHOUT acking; the
    # caller acks via the opaque token after processing. A crash before ack →
    # the broker's invisible-duration window redelivers the message (at-least-once).
    # ``supports_concurrent_ack=True`` because ack is per-message (no single-slot
    # overwrite) — correct under ``CONCURRENT_REQUESTS > 1``.
    requires_ack = True
    supports_concurrent_ack = True

    def __init__(self, config: RocketMQSettings) -> None:
        """Initialize RocketMQ backend.

        Args:
            config: Configuration for RocketMQ connection.
        """
        self.config = config
        self._producer: Any = None
        self._consumer: Any = None
        self._connection_snapshot: _RocketMQOperationSnapshot | None = None
        # Producer and consumer form one client generation.  ``connect`` must not
        # publish a second generation while the first is starting, and
        # ``disconnect`` must not detach a half-started pair underneath it.
        self._connection_lock = threading.RLock()
        # A disconnect fences SDK constructors before waiting for this lock. The
        # connect single-flight remains serialized, preserving idempotent setup.
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_epoch = 0
        self._connect_attempt_epoch: int | None = None
        self._generation_gate: GenerationLeaseGate[_RocketMQGenerationHandles] = (
            GenerationLeaseGate()
        )
        self._consumer_generation = 0
        self._subscribed_topics: set[str] = set()
        # RocketMQ Proxy mandates a blocking long poll. Keep that RPC off scheduler
        # threads and publish at most one exact generation-scoped delivery locally.
        self._receive_condition = threading.Condition()
        self._receive_buffer: deque[tuple[Any, Any, int]] = deque()
        self._receive_worker: threading.Thread | None = None
        self._receive_stop: threading.Event | None = None
        self._receive_consumer: Any = None
        self._receive_call_thread: threading.Thread | None = None
        self._deferred_receive_consumer: Any = None
        self._receive_generation = 0
        self._receive_snapshot: _RocketMQOperationSnapshot | None = None
        self._selected_topic: str | None = None
        # Pump failures are terminal for this consumer generation. Retaining only
        # a boolean for ordinary failures avoids keeping a sensitive driver graph
        # and lets every waiter construct its own fixed-text QueueError.
        self._receive_failed = False
        self._receive_control_error: BaseException | None = None
        # Private live-test hook: observe a driver failure synchronously in the
        # pump thread without retaining its graph on the backend.
        self._receive_error_observer: Callable[[BaseException], None] | None = None
        self._receive_cycle = 0
        self._receive_demand = 0
        # Legacy single-slot for the ``ack(token=None)`` fallback path. Set by
        # ``pop`` / ``pop_with_ack``; cleared when ``ack`` acks the tracked message.
        # The token path (preferred under ``CONCURRENT_REQUESTS > 1``) does not
        # depend on this slot.
        self._last_msg: Any = None
        self._last_delivery: tuple[Any, int, Any] | None = None

    def _connect_attempt_is_fenced(self) -> bool:
        with self._lifecycle_lock:
            epoch = self._connect_attempt_epoch
            return epoch is not None and epoch != self._lifecycle_epoch

    def _operation_snapshot(self) -> _RocketMQOperationSnapshot:
        """Capture and validate naming and delivery settings for a generation."""
        return _RocketMQOperationSnapshot(
            topic_prefix=_validate_topic_prefix(self.config.topic_prefix),
            max_message_size=self.config.max_message_size,
            invisible_duration=self.config.invisible_duration,
        )

    def _publish_generation_locked(self, *, allow_empty: bool = False) -> None:
        """Publish the producer/consumer pair as one opaque generation."""
        del allow_empty
        if self._producer is None and self._consumer is None:
            return
        if self._generation_gate.current is not None:
            return
        self._generation_gate.publish(
            _RocketMQGenerationHandles(
                self._producer,
                self._consumer,
                self._connection_snapshot or self._operation_snapshot(),
                self._consumer_generation,
            )
        )

    @contextmanager
    def _lease_generation(
        self, operation: str
    ) -> Iterator[GenerationRecord[_RocketMQGenerationHandles] | None]:
        """Lease a RocketMQ handle for the complete SDK operation."""
        with self._connection_lock:
            current = self._generation_gate.current
            if current is None:
                if operation == "push":
                    has_required_graph = self._producer is not None
                elif operation in {"ack", "nack", "pop"}:
                    has_required_graph = self._consumer is not None
                else:
                    has_required_graph = (
                        self._producer is not None or self._consumer is not None
                    )
                if has_required_graph:
                    self._publish_generation_locked()
                current = self._generation_gate.current
        if current is None:
            # Preserve direct-injection compatibility; the existing public
            # connected guards produce the stable QueueError for this case.
            yield None
            return
        try:
            with self._generation_gate.lease(operation) as record:
                yield record
        except GenerationUnavailable:
            raise QueueError(
                "RocketMQ backend is disconnected", operation=operation
            ) from None

    @import_error_traceback_boundary
    @backend_connection_error_boundary(
        "Failed to connect to RocketMQ.",
        "rocketmq",
        safe_messages=_ROCKETMQ_SAFE_CONNECTION_MESSAGES,
    )
    @configuration_error_boundary(
        "RocketMQ configuration is invalid.",
        _ROCKETMQ_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_ROCKETMQ_SAFE_CONFIGURATION_MESSAGES,
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
    def connect(self) -> None:
        """Establish connection to RocketMQ (gRPC proxy).

        Raises:
          BackendConnectionError: If connection / startup fails, or the optional dep
            is missing.
          ConfigurationError: If configuration is invalid.
        """
        with self._connection_lock:
            # A complete generation remains owned by this backend until an explicit
            # disconnect. Repeated (or overlapping) connects are no-ops.
            if self._producer is not None and self._consumer is not None:
                return
            if self._producer is not None or self._consumer is not None:
                residual_cleanup_failed = self._abort_partial_connect()
                if residual_cleanup_failed:
                    self._log_cleanup_diagnostic()
                # A shutdown/logger callback may have synchronously installed a
                # replacement. Do not overwrite that callback-owned generation.
                if self._producer is not None and self._consumer is not None:
                    return
            with self._lifecycle_lock:
                self._connect_attempt_epoch = self._lifecycle_epoch
            try:
                self._connect_unlocked()
            finally:
                with self._lifecycle_lock:
                    self._connect_attempt_epoch = None

    @import_error_traceback_boundary
    @backend_connection_error_boundary(
        "Failed to connect to RocketMQ.",
        "rocketmq",
        safe_messages=_ROCKETMQ_SAFE_CONNECTION_MESSAGES,
    )
    @configuration_error_boundary(
        "RocketMQ configuration is invalid.",
        _ROCKETMQ_CONFIGURATION_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_ROCKETMQ_SAFE_CONFIGURATION_MESSAGES,
        pass_through_exception_types=(BackendConnectionError, ImportError),
    )
    def _connect_unlocked(self) -> None:
        """Build one client generation while ``_connection_lock`` is held."""
        mode = self.config.mode
        namesrv_address = self.config.namesrv_address
        access_key = self.config.access_key
        secret_key = self.config.secret_key
        consumer_group = self.config.consumer_group
        send_timeout = _validate_send_timeout(self.config.send_timeout)
        tls_enabled = self.config.tls_enabled
        allow_remote_plaintext = self.config.allow_remote_plaintext
        _, namesrv_address, key_text, secret_text, tls_enabled = (
            validate_rocketmq_connection(
                mode,
                namesrv_address,
                access_key,
                secret_key,
                tls_enabled,
                allow_remote_plaintext,
            )
        )
        snapshot = self._operation_snapshot()

        try:
            from rocketmq import (
                ClientConfiguration,
                Credentials,
                Producer,
                SimpleConsumer,
            )
        except ImportError as e:
            if not _is_missing_optional_dependency(e, "rocketmq"):
                raise
            raise BackendConnectionError(
                "rocketmq-python-client not installed.", backend_type="rocketmq"
            ) from None

        producer: Any = None
        consumer: Any = None
        invariant_error: BackendConnectionError | None = None
        cleanup_diagnostic_pending = False

        def cleanup_candidates() -> None:
            nonlocal cleanup_diagnostic_pending
            cleanup_diagnostic_pending = self._shutdown_detached_clients(
                (consumer, "consumer"),
                (producer, "producer"),
                suppress_control_errors=True,
            )

        try:
            if key_text is not None and secret_text is not None:
                credentials = Credentials(_redact(key_text), _redact(secret_text))
            else:
                credentials = Credentials()
            fenced = self._connect_attempt_is_fenced()

            request_timeout = min(
                max(3, send_timeout // 1000),
                _MAX_REQUEST_TIMEOUT_S,
            )
            config_obj = ClientConfiguration(
                endpoints=namesrv_address,
                credentials=credentials,
                request_timeout=request_timeout,
            )
            fenced = fenced or self._connect_attempt_is_fenced()

            producer = Producer(config_obj, tls_enable=tls_enabled)
            fenced = fenced or self._connect_attempt_is_fenced()
            if producer is None:
                invariant_error = BackendConnectionError(
                    "RocketMQBackend producer initialization returned None",
                    backend_type="rocketmq",
                )
            else:
                producer.startup()
                fenced = fenced or self._connect_attempt_is_fenced()
                consumer = SimpleConsumer(
                    config_obj,
                    consumer_group,
                    await_duration=0,
                    tls_enable=tls_enabled,
                )
                fenced = fenced or self._connect_attempt_is_fenced()
                if consumer is None:
                    invariant_error = BackendConnectionError(
                        "RocketMQBackend consumer initialization returned None",
                        backend_type="rocketmq",
                    )
                else:
                    consumer.startup()
                    fenced = fenced or self._connect_attempt_is_fenced()

            if fenced:
                raise _RocketMQConnectionAttemptFenced()
            if invariant_error is None:
                with self._lifecycle_lock:
                    if self._connect_attempt_is_fenced():
                        raise _RocketMQConnectionAttemptFenced()
                    self._producer = producer
                    self._consumer = consumer
                    self._connection_snapshot = snapshot
                    self._consumer_generation += 1
                    self._publish_generation_locked()
        except _RocketMQConnectionAttemptFenced:
            cleanup_candidates()
            if cleanup_diagnostic_pending:
                self._log_cleanup_diagnostic()
            return
        except Exception:
            cleanup_candidates()
            startup_error = BackendConnectionError(
                "Failed to connect to RocketMQ.", backend_type="rocketmq"
            )
            if cleanup_diagnostic_pending:
                self._log_cleanup_diagnostic()
            raise startup_error from None
        except BaseException:
            cleanup_candidates()
            raise

        if invariant_error is not None:
            cleanup_candidates()
            if cleanup_diagnostic_pending:
                self._log_cleanup_diagnostic()
            raise invariant_error
        if cleanup_diagnostic_pending:
            self._log_cleanup_diagnostic()

        try:
            logger.debug("Connected to RocketMQ proxy")
        except BaseException:
            pass

    @staticmethod
    def _log_cleanup_diagnostic() -> None:
        """Emit a fixed best-effort detached-client cleanup diagnostic."""
        try:
            logger.debug("RocketMQ detached client shutdown failed.")
        except BaseException:
            pass

    def _fence_receive_pump_unlocked(self) -> threading.Thread | None:
        """Fence pump admission and discard, but never settle, local deliveries."""
        with self._receive_condition:
            worker = self._receive_worker
            if self._receive_stop is not None:
                self._receive_stop.set()
            self._receive_buffer.clear()
            self._receive_failed = False
            self._receive_control_error = None
            self._receive_consumer = None
            self._receive_generation = 0
            self._receive_snapshot = None
            self._selected_topic = None
            self._receive_demand = 0
            self._receive_condition.notify_all()
            return worker

    @staticmethod
    def _thread_definitely_unstarted(worker: threading.Thread) -> bool:
        """Return true only when the launch state proves no target ran."""
        try:
            started = getattr(worker, "_started", None)
            return started is not None and not started.is_set() and worker.ident is None
        except BaseException:
            return False

    def _finish_receive_pump_shutdown(
        self, worker: threading.Thread | None
    ) -> tuple[bool, BaseException | None]:
        """Bound the detached-worker join and retain a control failure."""
        join_error: BaseException | None = None
        try:
            if (
                worker is not None
                and worker is not threading.current_thread()
                and worker.ident is not None
            ):
                worker.join(timeout=_RECEIVE_PUMP_JOIN_TIMEOUT_S)
            worker_stopped = (
                worker is None
                or worker is threading.current_thread()
                or not worker.is_alive()
            )
        except BaseException as error:
            # Joining is cleanup. Finish the state fence before returning the
            # exact signal to the caller, rather than leaving a wedged worker
            # reference that can poison the next generation.
            join_error = error
            worker_stopped = False
        try:
            with self._receive_condition:
                # Clear admission even when the SDK left the old daemon blocked.
                # Its captured stop event and generation identity fence all late
                # results; a reconnect may therefore install a fresh pump without
                # stale publication or the old worker clearing the replacement
                # reference.
                if self._receive_worker is worker:
                    self._receive_worker = None
                    self._receive_stop = None
                elif worker is None and self._receive_worker is None:
                    # No replacement was installed while cleanup ran. Do not
                    # clear a newer generation's stop event by unconditional
                    # mirror assignment.
                    self._receive_stop = None
                self._receive_condition.notify_all()
        except BaseException as error:
            if join_error is None:
                join_error = error
            worker_stopped = False
        return worker_stopped, join_error

    def _abort_partial_connect(self) -> bool:
        """Detach and best-effort stop clients created by a failed connect."""
        retired = self._generation_gate.retire()
        producer = self._producer
        consumer = self._consumer
        self._producer = None
        self._consumer = None
        self._consumer_generation += 1
        self._subscribed_topics.clear()
        if retired is not None:
            retired.value.legacy_message = self._last_msg
            retired.value.legacy_delivery = self._last_delivery
        self._last_msg = None
        self._last_delivery = None
        self._connection_snapshot = None
        worker = self._fence_receive_pump_unlocked()
        # The helper is called while connection setup owns the lifecycle lock;
        # drain after detaching the mirrors so an admitted operation can finish
        # without re-entering that lock.
        self._generation_gate.drain(retired)
        cleanup_failed = self._shutdown_detached_clients(
            (consumer, "consumer"),
            (producer, "producer"),
            suppress_control_errors=True,
        )
        worker_stopped, _pump_control_error = self._finish_receive_pump_shutdown(worker)
        return cleanup_failed or not worker_stopped

    @control_exception_traceback_boundary
    @backend_connection_error_boundary(
        _ROCKETMQ_DISCONNECT_ERROR,
        "rocketmq",
    )
    def disconnect(self) -> None:
        """Fence receives, close clients, and join the bounded receive pump."""
        self._disconnect()

    def _disconnect(self) -> None:
        """Tear down one generation behind the terminal public error boundary."""
        # Fence private constructors before waiting for the connect single-flight.
        with self._lifecycle_lock:
            self._lifecycle_epoch += 1
        with self._connection_lock:
            # Retire admission and detach compatibility mirrors atomically. The
            # authoritative handles remain in the retired record until every
            # admitted SDK operation has returned.
            retired = self._generation_gate.retire()
            generation_handles = retired.value if retired is not None else None
            producer = (
                generation_handles.producer
                if generation_handles is not None
                else self._producer
            )
            consumer = (
                generation_handles.consumer
                if generation_handles is not None
                else self._consumer
            )
            self._producer = None
            self._consumer = None
            self._consumer_generation += 1
            self._subscribed_topics.clear()
            if retired is not None:
                retired.value.legacy_message = self._last_msg
                retired.value.legacy_delivery = self._last_delivery
            self._last_msg = None
            self._last_delivery = None
            self._connection_snapshot = None
            with self._receive_condition:
                defer_receive_shutdown = (
                    self._receive_call_thread is threading.current_thread()
                    and self._receive_consumer is consumer
                )
                if defer_receive_shutdown:
                    self._deferred_receive_consumer = consumer
            worker = self._fence_receive_pump_unlocked()

        # Do not hold connection_lock while draining an admitted producer/ack
        # operation. A bounded caller wait never releases this authoritative lease.
        # Shutdown is a generation finalizer rather than an unconditional action
        # after drain: a same-thread disconnect from producer.send/ack must not
        # shut down the client from inside its own SDK callback.
        cleanup_failed = False
        control_error: BaseException | None = None

        def finalize_clients() -> None:
            nonlocal cleanup_failed, control_error
            try:
                shutdown_clients = (
                    ((producer, "producer"),)
                    if defer_receive_shutdown
                    else ((consumer, "consumer"), (producer, "producer"))
                )
                cleanup_failed = self._shutdown_detached_clients(*shutdown_clients)
            except BaseException as error:
                # Finish the bounded pump join before restoring process-control
                # flow. This preserves KeyboardInterrupt/SystemExit without an
                # unbounded wait when receive ignored the failed shutdown.
                control_error = error

        generation_control_error = self._generation_gate.drain(
            retired, finalize_clients
        )
        worker_stopped, pump_control_error = self._finish_receive_pump_shutdown(worker)
        if generation_control_error is not None:
            # The drain wait precedes detached shutdown and pump joining. Preserve
            # that earlier primary control signal over later cleanup signals.
            raise generation_control_error
        if control_error is not None:
            raise control_error
        if pump_control_error is not None:
            raise pump_control_error
        if cleanup_failed or not worker_stopped:
            self._log_cleanup_diagnostic()
            raise BackendConnectionError(
                _ROCKETMQ_DISCONNECT_ERROR,
                backend_type="rocketmq",
            )
        # This diagnostic follows the completed disconnect state transition. A
        # misbehaving logging handler must not report the completed operation as
        # failed or resurrect an already-detached client generation.
        try:
            logger.debug("Disconnected from RocketMQ")
        except BaseException:
            pass

    @staticmethod
    def _shutdown_detached_client(closer: Any, result: _RocketMQCleanupResult) -> None:
        """Run one potentially blocking SDK shutdown in a fenced daemon task."""
        error: BaseException | None = None
        try:
            closer.shutdown()
        except BaseException as caught:
            error = caught
        result.publish(error)

    @classmethod
    def _shutdown_detached_clients(
        cls,
        *clients: tuple[Any | None, str],
        suppress_control_errors: bool = False,
    ) -> bool:
        """Bound all SDK shutdowns and retain the first ordered control error."""
        tasks: list[tuple[threading.Thread | None, _RocketMQCleanupResult]] = []
        management_errors: list[BaseException] = []
        for closer, label in clients:
            if closer is None:
                continue
            result = _RocketMQCleanupResult()
            worker = threading.Thread(
                target=cls._shutdown_detached_client,
                args=(closer, result),
                name=f"rocketmq-shutdown-{label}",
                daemon=True,
            )
            try:
                worker.start()
            except BaseException as start_error:
                # Thread.start() may raise after launching the target. Retain that
                # worker when its identity proves it started; otherwise use the
                # lower-level daemon launcher so the detached client is still
                # offered shutdown before the result fence.
                management_errors.append(start_error)
                if not cls._thread_definitely_unstarted(worker):
                    tasks.append((worker, result))
                    continue
                try:
                    start_new_thread(cls._shutdown_detached_client, (closer, result))
                except BaseException as fallback_error:
                    management_errors.append(fallback_error)
                    result.publish(fallback_error)
                    tasks.append((None, result))
                else:
                    tasks.append((None, result))
            else:
                tasks.append((worker, result))

        deadline = time.monotonic() + _CLIENT_SHUTDOWN_JOIN_TIMEOUT_S
        for task_worker, result in tasks:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                if task_worker is not None:
                    if task_worker.ident is not None:
                        task_worker.join(timeout=remaining)
                else:
                    result._completed_event.wait(timeout=remaining)
            except BaseException as wait_error:
                management_errors.append(wait_error)

        cleanup_failed = False
        primary_control_error: BaseException | None = None
        for _worker, result in tasks:
            completed, shutdown_error = result.fence()
            if not completed:
                cleanup_failed = True
            elif isinstance(shutdown_error, Exception):
                cleanup_failed = True
            elif shutdown_error is not None and primary_control_error is None:
                primary_control_error = shutdown_error

        for management_error in management_errors:
            if isinstance(management_error, Exception):
                cleanup_failed = True
            elif primary_control_error is None:
                primary_control_error = management_error

        if primary_control_error is not None:
            if not suppress_control_errors:
                raise primary_control_error
            cleanup_failed = True
        return cleanup_failed

    def is_connected(self) -> bool:
        """Check if RocketMQ is connected (both clients running).

        Returns:
          True if producer and consumer clients are initialized and running.
        """
        if self._producer is None or self._consumer is None:
            return False
        # apache clients expose ``is_running`` as a BOOL PROPERTY (not a method)
        # — True after startup(), False after shutdown().
        try:
            return bool(self._producer.is_running and self._consumer.is_running)
        except Exception:  # noqa: BLE001 - is_connected must not raise
            return False

    def ping(self) -> bool:
        """Check RocketMQ health (local-state check).

        Returns:
          True if ``is_connected`` reports both clients running.

        Note:
          Local-state check, not a broker round-trip — same caveat as the prior
          implementation (R1-P2-16). A real liveness probe would need a broker
          round-trip; the right one for the gRPC proxy is an open design question.
        """
        return self.is_connected()

    @property
    def backend_type(self) -> BackendType:
        """Return backend type."""
        return BackendType.ROCKETMQ

    def _get_topic_name(
        self,
        queue_name: str,
        snapshot: _RocketMQOperationSnapshot | None = None,
    ) -> str:
        """Get full topic name for queue.

        Args:
          queue_name: Base queue name.

        Returns:
          Full topic name.
        """
        _validate_key_name(queue_name, "queue_name")
        prefix = self.config.topic_prefix if snapshot is None else snapshot.topic_prefix
        return f"{prefix}_{queue_name}"

    @queue_operation_error_boundary(
        "push",
        "Failed to push RocketMQ message.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to queue.

        Args:
          queue_name: Name of the queue.
          item: Item to push (bytes).
          priority: Priority value (higher = more urgent).

        Raises:
          QueueError: If push fails.
        """
        _validate_key_name(queue_name, "queue_name")
        try:
            with self._lease_generation("push") as generation:
                snapshot = (
                    generation.value.snapshot
                    if generation is not None
                    else self._operation_snapshot()
                )
                # R22-C: enforce the documented client-side size cap from the
                # generation snapshot, never from mutable live settings.
                if len(item) > snapshot.max_message_size:
                    raise QueueError(
                        _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR,
                        queue_name=queue_name,
                        operation="push",
                    )
                producer = (
                    generation.value.producer
                    if generation is not None
                    else self._producer
                )
                if generation is None:
                    if producer is None or not self.is_connected():
                        raise QueueError(
                            "Not connected to RocketMQ",
                            queue_name=queue_name,
                            operation="push",
                        )
                elif producer is None:
                    error = "RocketMQBackend not connected: producer is None"
                    raise QueueError(error, queue_name=queue_name, operation="push")
                from rocketmq import Message

                topic_name = self._get_topic_name(queue_name, snapshot)
                msg = Message()
                msg.topic = topic_name
                msg.body = item
                # apache Message has no native priority field; carry it as ``keys``
                # so a priority-aware consumer could read it. RocketMQ topic
                # ordering is by queue, not priority — the priority arg is accepted
                # for interface symmetry but does not reorder within a topic.
                msg.keys = str(priority)
                producer.send(msg)
                if generation is not None and not generation.accepting:
                    raise QueueError(
                        _ROCKETMQ_CONNECTION_CHANGED_PUSH_ERROR,
                        queue_name=queue_name,
                        operation="push",
                    )
        except QueueError:
            raise
        except Exception as e:
            err = f"Failed to push to queue: {e}"
            raise QueueError(err, queue_name=queue_name, operation="push") from e

    def _start_receive_worker_locked(self) -> None:
        """Start the selected generation's pump while its condition is held."""
        worker = self._receive_worker
        if worker is not None and worker.is_alive():
            return
        consumer = self._receive_consumer
        topic_name = self._selected_topic
        stop = self._receive_stop
        generation = self._receive_generation
        snapshot = self._receive_snapshot
        if consumer is None or topic_name is None or stop is None or snapshot is None:
            return
        worker = threading.Thread(
            target=self._receive_pump,
            args=(consumer, generation, topic_name, stop, snapshot),
            name=f"rocketmq-receive-{generation}",
            daemon=True,
        )
        self._receive_worker = worker
        try:
            worker.start()
        except BaseException:
            # ``Thread.start`` may fail before assigning an identity. Roll back
            # only our candidate so disconnect never tries to join an unstarted
            # worker and a later operation may retry this still-current generation.
            if self._receive_worker is worker:
                self._receive_worker = None
            self._receive_condition.notify_all()
            raise

    def _pump_is_current_locked(
        self, consumer: Any, generation: int, stop: threading.Event
    ) -> bool:
        """Return whether a pump still owns receive admission."""
        return (
            not stop.is_set()
            and self._receive_consumer is consumer
            and self._receive_generation == generation
            and self._receive_stop is stop
        )

    def _receive_pump(
        self,
        consumer: Any,
        generation: int,
        topic_name: str,
        stop: threading.Event,
        snapshot: _RocketMQOperationSnapshot,
    ) -> None:
        """Run bounded broker long polls and publish one local delivery at a time."""
        try:
            # Subscribe is a blocking SDK operation too. The active-thread marker
            # lets a same-thread callback defer shutdown, while a cross-thread
            # disconnect may still close the consumer to bound cleanup.
            with self._receive_condition:
                self._receive_call_thread = threading.current_thread()
            try:
                consumer.subscribe(topic_name)
            finally:
                with self._receive_condition:
                    if self._receive_call_thread is threading.current_thread():
                        self._receive_call_thread = None
                    self._receive_condition.notify_all()
            with self._receive_condition:
                if not self._pump_is_current_locked(consumer, generation, stop):
                    return
                self._subscribed_topics.add(topic_name)
                self._receive_condition.notify_all()
            consumer.await_duration = _MIN_LONG_POLL_DURATION
            while True:
                with self._receive_condition:
                    while self._pump_is_current_locked(consumer, generation, stop) and (
                        self._receive_buffer or self._receive_demand == 0
                    ):
                        self._receive_condition.wait()
                    if not self._pump_is_current_locked(consumer, generation, stop):
                        return
                    self._receive_demand -= 1
                # The receive pump has its own stop event and generation fence;
                # disconnect closes a cross-thread consumer to interrupt this
                # bounded cleanup operation. The active-thread marker handles the
                # exceptional same-thread reentrant callback without closing the
                # consumer from inside receive().
                with self._receive_condition:
                    self._receive_call_thread = threading.current_thread()
                try:
                    messages = consumer.receive(1, snapshot.invisible_duration)
                finally:
                    with self._receive_condition:
                        if self._receive_call_thread is threading.current_thread():
                            self._receive_call_thread = None
                        self._receive_condition.notify_all()
                with self._receive_condition:
                    if not self._pump_is_current_locked(consumer, generation, stop):
                        return
                    self._receive_cycle += 1
                    if messages:
                        self._receive_buffer.append((messages[0], consumer, generation))
                    self._receive_condition.notify_all()
        except Exception as error:
            # Never retain a driver exception (and its potentially sensitive graph)
            # across the worker boundary. Public operations expose only fixed text.
            # Fence the private live-test observation under the same identity check as
            # failure publication: a late exception from a retired pump must not be
            # attributed to the replacement generation.
            with self._receive_condition:
                if not self._pump_is_current_locked(consumer, generation, stop):
                    return
                observer = self._receive_error_observer
                if observer is not None:
                    try:
                        observer(error)
                    except BaseException:
                        pass
                if self._pump_is_current_locked(consumer, generation, stop):
                    self._receive_failed = True
                    self._receive_cycle += 1
                    self._receive_condition.notify_all()
        except BaseException as error:
            with self._receive_condition:
                if self._pump_is_current_locked(consumer, generation, stop):
                    self._receive_control_error = error
                    self._receive_cycle += 1
                    self._receive_condition.notify_all()
        finally:
            deferred_consumer = None
            with self._receive_condition:
                if self._receive_worker is threading.current_thread():
                    self._receive_worker = None
                if self._deferred_receive_consumer is consumer:
                    deferred_consumer = self._deferred_receive_consumer
                    self._deferred_receive_consumer = None
                self._receive_condition.notify_all()
            if deferred_consumer is not None:
                try:
                    deferred_consumer.shutdown()
                except BaseException:
                    # The receive/pump result is already fenced; deferred cleanup
                    # cannot replace it or re-enter the public boundary.
                    pass

    def _receive_delivery(
        self, queue_name: str, timeout: float
    ) -> tuple[Any | None, Any, int]:
        """Take one exact delivery from the generation-scoped local buffer."""
        _validate_key_name(queue_name, "queue_name")
        with self._connection_lock:
            generation_record = self._generation_gate.current
            if generation_record is None and self._consumer is not None:
                self._publish_generation_locked()
                generation_record = self._generation_gate.current
            snapshot = (
                generation_record.value.snapshot
                if generation_record is not None
                else self._operation_snapshot()
            )
            topic_name = self._get_topic_name(queue_name, snapshot)
            if not self.is_connected():
                msg = "Not connected to RocketMQ"
                raise QueueError(msg, queue_name=queue_name, operation="pop")
            consumer = self._consumer
            generation = self._consumer_generation
            if consumer is None:
                error = "RocketMQBackend not connected: consumer is None"
                raise QueueError(error, queue_name=queue_name, operation="pop")
            with self._receive_condition:
                if self._selected_topic not in (None, topic_name):
                    raise QueueError(
                        _ROCKETMQ_TOPIC_ALREADY_SELECTED_ERROR,
                        operation="pop",
                    )
                if self._selected_topic is None:
                    self._selected_topic = topic_name
                    self._receive_consumer = consumer
                    self._receive_generation = generation
                    self._receive_snapshot = snapshot
                    self._receive_stop = threading.Event()
                    self._receive_failed = False
                    self._receive_control_error = None
                    self._receive_demand = 0
                elif (
                    self._receive_consumer is not consumer
                    or self._receive_generation != generation
                ):
                    msg = "Not connected to RocketMQ"
                    raise QueueError(msg, queue_name=queue_name, operation="pop")
                # Coalesce scheduler polls into one bounded broker demand; the
                # one-slot delivery buffer and one pump thread cannot grow with
                # repeated timeout=0 calls.
                self._receive_demand = 1
                # A prior empty broker cycle leaves the pump idle on this
                # condition. Every public pop that publishes fresh demand must
                # wake it so the request performs its corresponding broker cycle.
                self._receive_condition.notify_all()
                observed_cycle = self._receive_cycle
                if not self._receive_failed and self._receive_control_error is None:
                    self._start_receive_worker_locked()

        wait_timeout = max(0.0, timeout)
        deadline = time.monotonic() + wait_timeout
        with self._receive_condition:
            while True:
                if (
                    self._receive_consumer is not consumer
                    or self._receive_generation != generation
                ):
                    msg = "Not connected to RocketMQ"
                    raise QueueError(msg, queue_name=queue_name, operation="pop")
                if self._receive_buffer:
                    delivery = self._receive_buffer.popleft()
                    self._receive_condition.notify_all()
                    return delivery
                if self._receive_control_error is not None:
                    raise self._receive_control_error
                if self._receive_failed:
                    raise QueueError(
                        _ROCKETMQ_RECEIVE_PUMP_ERROR,
                        operation="pop",
                    )
                if wait_timeout == 0 or self._receive_cycle != observed_cycle:
                    return (None, consumer, generation)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return (None, consumer, generation)
                self._receive_condition.wait(timeout=remaining)

    def _receive_message(self, queue_name: str, timeout: float) -> Any | None:
        """Take a locally pumped message without acknowledging it."""
        message, _consumer, _generation = self._receive_delivery(queue_name, timeout)
        return message

    def _receive_generation_is_current_locked(
        self, consumer: Any, generation: int
    ) -> bool:
        """Check the delivery's gate identity before publishing compatibility state."""
        current = self._generation_gate.current
        return bool(
            current is not None
            and current.accepting
            and current.value.consumer is consumer
            and current.value.consumer_generation == generation
            and self._consumer is consumer
            and self._consumer_generation == generation
        )

    @queue_operation_error_boundary(
        "pop",
        "Failed to pop RocketMQ message.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop item from queue WITHOUT acking (deferred-ack model).

        Returns the message body; the message itself is tracked in ``_last_msg``
        for the legacy ``ack`` (``token=None``) path. Ack fires only when the
        caller explicitly invokes :meth:`ack` — a crash before ack leaves the
        message unacked → the broker's invisible-duration redelivers it
        (at-least-once).

        Under :class:`BackendScheduler`, :meth:`pop_with_ack` is the preferred path
        (per-message token, correct under ``CONCURRENT_REQUESTS > 1``).

        Args:
          queue_name: Name of the queue.
          timeout: Maximum seconds to wait on the local delivery condition. A
            zero value only inspects the local buffer; the pump independently
            owns RocketMQ Proxy's bounded long poll.

        Returns:
          Popped item, or None if queue is empty.
        """
        msg, consumer, generation = self._receive_delivery(queue_name, timeout)
        if msg is None:
            return None
        with self._connection_lock:
            if not self._receive_generation_is_current_locked(consumer, generation):
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    queue_name=queue_name,
                    operation="pop",
                )
            self._last_msg = msg
            self._last_delivery = (consumer, generation, msg)
        return self._extract_body(msg)

    @queue_operation_error_boundary(
        "pop",
        "Failed to pop RocketMQ message.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, Any | None]:
        """Pop an item together with a consumer-generation-scoped ack token.

        Does NOT ack or populate the legacy single-delivery slot — the caller acks
        only through the returned token via :meth:`ack` after processing.
        :class:`BackendScheduler` threads the opaque token through
        ``request.meta["_backend_ack_token"]``.

        Args:
          queue_name: Name of the queue.
          timeout: Maximum seconds to wait on the local delivery condition. A
            zero value only inspects the local buffer; the pump independently
            owns RocketMQ Proxy's bounded long poll.

        Returns:
          ``(body_bytes, msg_token)`` or ``(None, None)`` when empty.

        Raises:
          QueueError: If not connected or the receive fails.
        """
        msg, consumer, generation = self._receive_delivery(queue_name, timeout)
        if msg is None:
            return (None, None)
        with self._connection_lock:
            if not self._receive_generation_is_current_locked(consumer, generation):
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    queue_name=queue_name,
                    operation="pop",
                )
            token = _RocketMQAckToken(msg, consumer, generation)
        return (self._extract_body(msg), token)

    @staticmethod
    def _extract_body(msg: Any) -> bytes:
        """Extract the body bytes from a received message.

        The apache ``Message.body`` is ``bytes``; defensive coercion handles any
        dynamic typing from the client.

        Args:
          msg: The received message object.

        Returns:
          The message body as bytes.
        """
        body = getattr(msg, "body", None)
        if body is None:
            return b""
        if isinstance(body, bytes):
            return body
        if isinstance(body, (bytearray, memoryview)):
            return bytes(body)
        return str(body).encode()

    def _ack_token(self, token: _RocketMQAckToken) -> None:
        """Ack one token while retaining its admitted consumer generation."""
        with self._lease_generation("ack") as generation:
            selected_consumer = (
                generation.value.consumer
                if generation is not None and generation.value.consumer is not None
                else self._consumer
            )
            expected_generation = (
                generation.value.consumer_generation
                if generation is not None
                else self._consumer_generation
            )
            if (
                token.consumer is not selected_consumer
                or token.generation != expected_generation
            ):
                token._settle("stale", lambda: None)
                return
            target = token.message
            consumer = token.consumer

            def acknowledge() -> None:
                try:
                    consumer.ack(target)
                except Exception as e:
                    msg = f"Failed to ack RocketMQ message: {e}"
                    raise QueueError(msg, operation="ack") from e

            settled = token._settle("acked", acknowledge)
            if generation is not None and not generation.accepting:
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    operation="ack",
                )
            if settled and self._last_msg is target:
                self._last_msg = None
                self._last_delivery = None

    @queue_operation_error_boundary(
        "ack",
        "Failed to ack RocketMQ message.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
    )
    def ack(self, queue_name: str, *, token: Any | None = None) -> None:
        """Ack a popped message (deferred from :meth:`pop` / :meth:`pop_with_ack`).

        With a token: ack the specific message on the consumer generation that
        delivered it. With ``token=None`` (legacy single-pop caller): ack the
        tracked ``_last_msg``.

        Args:
          queue_name: Name of the queue (unused; kept for interface symmetry).
          token: The opaque token returned by :meth:`pop_with_ack`, or ``None`` to
            ack the last-popped message.

        Raises:
          QueueError: If the underlying ack call fails.
        """
        del queue_name
        if token is not None:
            if isinstance(token, _RocketMQAckToken):
                self._ack_token(token)
            return
        # Legacy settlement is also a blocking SDK operation. Hold the same
        # generation lease as token settlement so disconnect cannot shut down
        # the consumer underneath ack().
        with self._lease_generation("ack") as generation_record:
            if generation_record is not None and not generation_record.accepting:
                handles = generation_record.value
                target = handles.legacy_message
                delivery_state = handles.legacy_delivery
            else:
                handles = None
                target = self._last_msg
                delivery_state = self._last_delivery
            if target is None:
                return
            if delivery_state is not None:
                consumer, delivery_generation, delivery = delivery_state
                if handles is None and (
                    delivery is not target
                    or delivery_generation != self._consumer_generation
                    or consumer is not self._consumer
                ):
                    return
                target = delivery
            else:
                consumer = self._consumer if handles is None else handles.consumer
            if consumer is None:
                return
            try:
                consumer.ack(target)
            except Exception as e:
                msg = f"Failed to ack RocketMQ message: {e}"
                raise QueueError(msg, operation="ack") from e
            if generation_record is not None and not generation_record.accepting:
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    operation="ack",
                )
            # Clear the legacy slot when we acked the tracked message so a later
            # ack(token=None) is a no-op, not a re-ack.
            if self._last_msg is target:
                self._last_msg = None
                self._last_delivery = None

    def _nack_token(self, token: _RocketMQAckToken) -> None:
        """Nack one token while retaining its admitted consumer generation."""
        with self._lease_generation("nack") as generation:
            selected_consumer = (
                generation.value.consumer
                if generation is not None and generation.value.consumer is not None
                else self._consumer
            )
            expected_generation = (
                generation.value.consumer_generation
                if generation is not None
                else self._consumer_generation
            )
            if (
                token.consumer is not selected_consumer
                or token.generation != expected_generation
            ):
                token._settle("stale", lambda: None)
                return
            target = token.message
            consumer = token.consumer

            def change_invisible_duration() -> None:
                try:
                    consumer.change_invisible_duration(target, _MIN_INVISIBLE_DURATION)
                except Exception as e:
                    msg = f"Failed to nack RocketMQ message: {e}"
                    raise QueueError(msg, operation="nack") from e

            settled = token._settle("nacked", change_invisible_duration)
            if generation is not None and not generation.accepting:
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    operation="nack",
                )
            if settled and self._last_msg is target:
                self._last_msg = None
                self._last_delivery = None

    @queue_operation_error_boundary(
        "nack",
        "Failed to nack RocketMQ message.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
    )
    def nack(self, queue_name: str, *, token: Any | None = None) -> None:
        """Shorten a popped message's lease to RocketMQ's 10-second floor.

        The client has no dedicated nack call, but its
        ``change_invisible_duration`` operation can schedule prompt redelivery.
        RocketMQ rejects durations below 10 seconds, so unlike SQS this cannot
        make a message immediately visible.

        Args:
          queue_name: Name of the queue (unused; interface symmetry).
          token: The opaque token returned by :meth:`pop_with_ack`, or ``None`` to
            nack the last-popped message.

        Raises:
          QueueError: If changing the message lease fails.
        """
        del queue_name
        if token is not None:
            if isinstance(token, _RocketMQAckToken):
                self._nack_token(token)
            return
        # Legacy settlement is also a blocking SDK operation. Hold the same
        # generation lease as token settlement so disconnect cannot shut down
        # the consumer underneath change_invisible_duration().
        with self._lease_generation("nack") as generation_record:
            if generation_record is not None and not generation_record.accepting:
                handles = generation_record.value
                target = handles.legacy_message
                delivery_state = handles.legacy_delivery
            else:
                handles = None
                target = self._last_msg
                delivery_state = self._last_delivery
            if target is None:
                return
            if delivery_state is not None:
                consumer, delivery_generation, delivery = delivery_state
                if handles is None and (
                    delivery is not target
                    or delivery_generation != self._consumer_generation
                    or consumer is not self._consumer
                ):
                    return
                target = delivery
            else:
                consumer = self._consumer if handles is None else handles.consumer
            if consumer is None:
                return
            try:
                consumer.change_invisible_duration(target, _MIN_INVISIBLE_DURATION)
            except Exception as e:
                msg = f"Failed to nack RocketMQ message: {e}"
                raise QueueError(msg, operation="nack") from e
            if generation_record is not None and not generation_record.accepting:
                raise QueueError(
                    _ROCKETMQ_CONNECTION_CHANGED_OPERATION_ERROR,
                    operation="nack",
                )
            if self._last_msg is target:
                self._last_msg = None
                self._last_delivery = None

    @not_implemented_error_boundary(
        _ROCKETMQ_QUEUE_LEN_UNSUPPORTED_MESSAGE,
        validator=_validate_queue_name_argument,
    )
    def queue_len(self, queue_name: str) -> int:
        """Report that queue depth is unsupported by the RocketMQ client.

        RocketMQ's deferred-ack model has no broker-side depth RPC. Returning 0
        would falsely report an empty queue and can make Scrapy enter idle while
        work is pending. ``NotImplementedError`` lets queue monitoring ignore the
        sample, backpressure continue to pop, and pending detection stay
        conservative. A one-time warning surfaces the limitation to operators.

        Args:
          queue_name: Name of the queue.

        Raises:
          NotImplementedError: Always; the client has no broker-side depth RPC.
        """
        global _queue_len_warned
        if not _queue_len_warned:
            _queue_len_warned = True
            # The warning only describes the already-established capability
            # contract. It cannot replace the required NotImplementedError.
            try:
                logger.warning(
                    "RocketMQ queue_len() is unsupported (deferred-ack model has no "
                    "broker-side depth RPC). Pending detection will stay conservative; "
                    "monitor via pop-rate / consumer-liveness instead. This warning "
                    "fires once per process."
                )
            except BaseException:
                pass
        raise NotImplementedError(_ROCKETMQ_QUEUE_LEN_UNSUPPORTED_MESSAGE)

    @queue_operation_error_boundary(
        "clear_queue",
        "Failed to clear RocketMQ queue.",
        safe_messages=_ROCKETMQ_SAFE_QUEUE_MESSAGES,
        validator=_validate_queue_name_argument,
    )
    def clear_queue(self, queue_name: str) -> None:
        """Report that RocketMQ broker-side queue purge is unsupported.

        Args:
          queue_name: Name of the queue.

        Raises:
          QueueError: If disconnected or because the client has no purge API.
        """
        _validate_key_name(queue_name, "queue_name")
        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg, queue_name=queue_name, operation="clear_queue")
        msg = _ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE
        raise QueueError(msg, queue_name=queue_name, operation="clear_queue")


# ---------------------------------------------------------------------------
# Set / Storage — class-level guard (replaces former per-method stubs)
# ---------------------------------------------------------------------------
#
# RocketMQ is excluded from SET_CAPABLE_BACKENDS and STORAGE_CAPABLE_BACKENDS
# at the connector layer, so these classes are unreachable under normal config
# resolution (resolve_backend_config raises ConfigurationError first). They
# exist as the fail-fast surface for anyone who bypasses that gating.


def _unsupported_component_guard(
    component: str, setting_key: str
) -> ConfigurationError:
    """Build the ConfigurationError raised when RocketMQ is bound to an
    unsupported component (set/storage) via direct instantiation that bypasses
    the connector capability gating.

    Args:
        component: The unsupported component name (``"set"`` / ``"storage"``).
        setting_key: The Scrapy setting that selects the component backend.

    Returns:
        A ``ConfigurationError`` with an actionable message.
    """
    if component == "storage":
        alternatives = "redis, mongodb, elasticsearch, memcached, or dynamodb"
    else:
        alternatives = "redis, mongodb, or elasticsearch"
    msg = (
        f"RocketMQ does not support {component} operations: it is a message "
        f"queue with no native set/membership or key-value semantics. Select a "
        f"different backend via {setting_key} (e.g. {alternatives})."
    )
    return ConfigurationError(msg, setting_name=setting_key)


class RocketMQSetBackend(RocketMQBackend):
    """Guard class: RocketMQ cannot serve the ``SetBackend`` interface.

    Construction fails fast with a typed ``ConfigurationError``.
    """

    def __init__(self, config: RocketMQSettings) -> None:
        """Reject construction — RocketMQ does not support the set interface.

        Raises:
            ConfigurationError: Always.
        """
        error = _unsupported_component_guard("set", "SCRAPY_SET_BACKEND_TYPE")
        del config
        raise error


class RocketMQStorageBackend(RocketMQBackend):
    """Guard class: RocketMQ cannot serve the ``StorageBackend`` interface.

    Construction fails fast with a typed ``ConfigurationError``.
    """

    def __init__(self, config: RocketMQSettings) -> None:
        """Reject construction — RocketMQ does not support the storage interface.

        Raises:
            ConfigurationError: Always.
        """
        error = _unsupported_component_guard("storage", "SCRAPY_STORAGE_BACKEND_TYPE")
        del config
        raise error
