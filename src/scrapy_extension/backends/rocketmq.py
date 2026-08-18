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
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

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
_ROCKETMQ_SAFE_CONFIGURATION_MESSAGES: frozenset[str] = frozenset(
    {
        ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
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
_ROCKETMQ_DISCONNECT_ERROR = "Failed to disconnect from RocketMQ."
_ROCKETMQ_SAFE_QUEUE_MESSAGES: frozenset[str] = frozenset(
    {
        "Not connected to RocketMQ",
        "RocketMQBackend not connected: producer is None",
        "RocketMQBackend not connected: consumer is None",
        _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR,
        _ROCKETMQ_CLEAR_QUEUE_UNSUPPORTED_MESSAGE,
        _ROCKETMQ_TOPIC_ALREADY_SELECTED_ERROR,
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


class _RocketMQCleanupResult:
    """Fence one daemon shutdown task's result after its join budget expires."""

    __slots__ = ("_accepting", "_completed", "_error", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._accepting = True
        self._completed = False
        self._error: BaseException | None = None

    def publish(self, error: BaseException | None) -> None:
        """Publish completion only while the disconnect generation accepts it."""
        with self._lock:
            if self._accepting:
                self._completed = True
                self._error = error

    def fence(self) -> tuple[bool, BaseException | None]:
        """Reject late publication and return the result visible at the fence."""
        with self._lock:
            self._accepting = False
            return self._completed, self._error


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
        # Producer and consumer form one client generation.  ``connect`` must not
        # publish a second generation while the first is starting, and
        # ``disconnect`` must not detach a half-started pair underneath it.
        self._connection_lock = threading.RLock()
        self._consumer_generation = 0
        self._subscribed_topics: set[str] = set()
        # RocketMQ Proxy mandates a blocking long poll. Keep that RPC off scheduler
        # threads and publish at most one exact generation-scoped delivery locally.
        self._receive_condition = threading.Condition()
        self._receive_buffer: deque[tuple[Any, Any, int]] = deque()
        self._receive_worker: threading.Thread | None = None
        self._receive_stop: threading.Event | None = None
        self._receive_consumer: Any = None
        self._receive_generation = 0
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
            # disconnect.  Repeated (or overlapping) connects are therefore no-ops
            # rather than silently leaking an earlier Producer/Consumer pair.
            if self._producer is not None and self._consumer is not None:
                return
            # A failed/interrupted historical connect could leave one side assigned.
            # Retire it before beginning a fresh generation; this preserves the
            # failure cleanup contract while preventing a one-sided client leak.
            if self._producer is not None or self._consumer is not None:
                residual_cleanup_failed = self._abort_partial_connect()
                if residual_cleanup_failed:
                    self._log_cleanup_diagnostic()
            self._connect_unlocked()

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
        send_timeout = self.config.send_timeout
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

        missing_dependency = False
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
            missing_dependency = True
        if missing_dependency:
            raise BackendConnectionError(
                "rocketmq-python-client not installed.", backend_type="rocketmq"
            )

        startup_error: BackendConnectionError | None = None
        invariant_error: BackendConnectionError | None = None
        cleanup_diagnostic_pending = False
        try:
            # Credentials: empty Credentials() for no-auth (the broker fixture runs
            # with auth disabled); Credentials(ak, sk) when both are provided.
            if key_text is not None and secret_text is not None:
                credentials = Credentials(_redact(key_text), _redact(secret_text))
            else:
                credentials = Credentials()

            # ``namesrv_address`` is, in this gRPC rewrite, the PROXY endpoints
            # (``host:8081``). The field name is kept for settings-schema
            # compatibility; the value must point at the broker's gRPC proxy, NOT the
            # legacy NameServer (9876). The broker must run with ``--enable-proxy``.
            request_timeout = min(
                max(3, send_timeout // 1000),
                _MAX_REQUEST_TIMEOUT_S,
            )
            config_obj = ClientConfiguration(
                endpoints=namesrv_address,
                credentials=credentials,
                request_timeout=request_timeout,
            )

            self._producer = Producer(config_obj, tls_enable=tls_enabled)
            if self._producer is None:
                invariant_error = BackendConnectionError(
                    "RocketMQBackend producer initialization returned None",
                    backend_type="rocketmq",
                )
            else:
                self._producer.startup()

                # The client defaults await_duration to 20 seconds, so initialize it to
                # zero; each receive replaces it with the requested duration clamped to
                # RocketMQ Proxy's five-second server floor.
                self._consumer = SimpleConsumer(
                    config_obj,
                    consumer_group,
                    await_duration=0,
                    tls_enable=tls_enabled,
                )
                if self._consumer is None:
                    invariant_error = BackendConnectionError(
                        "RocketMQBackend consumer initialization returned None",
                        backend_type="rocketmq",
                    )
                else:
                    self._consumer.startup()
                    self._consumer_generation += 1
        except Exception:
            cleanup_diagnostic_pending = self._abort_partial_connect()
            startup_error = BackendConnectionError(
                "Failed to connect to RocketMQ.", backend_type="rocketmq"
            )
        except BaseException:
            # KeyboardInterrupt/SystemExit are not ``Exception`` subclasses, so the
            # arms above cannot catch them — without this arm a Ctrl+C raised after
            # ``self._producer = Producer(...)`` / ``startup()`` skips
            # ``_abort_partial_connect()``, leaking both clients (TCP sockets + bg
            # threads) and wedging the backend. Detach the partially-built clients
            # before re-raising. Mirrors mongodb.py / elasticsearch.py / kafka
            # ``except BaseException`` arms.
            self._abort_partial_connect()
            raise

        if invariant_error is not None:
            # These local contract violations use fixed text, so preserve their
            # existing public diagnostics while still raising outside any handler.
            cleanup_diagnostic_pending = self._abort_partial_connect()
            startup_error = invariant_error

        if cleanup_diagnostic_pending:
            # The startup handler has finished, so a logging extension cannot
            # recover the raw driver failure through ``sys.exc_info()``.
            self._log_cleanup_diagnostic()

        if startup_error is not None:
            # Raise outside the driver exception handler so endpoint/credential text
            # cannot survive through ``__cause__`` or ``__context__``.
            raise startup_error

        # A complete producer/consumer pair is live once the generation advances.
        # Diagnostics after that publication are strictly observational: an
        # extension logger is allowed to fail (including with a control exception)
        # without treating the published clients as an aborted private candidate.
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
            self._selected_topic = None
            self._receive_demand = 0
            self._receive_condition.notify_all()
            return worker

    def _finish_receive_pump_shutdown(self, worker: threading.Thread | None) -> bool:
        """Bound the detached-worker join and report whether it really stopped."""
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
        with self._receive_condition:
            # Clear admission even when the SDK left the old daemon blocked. Its
            # captured stop event and generation identity fence all late results;
            # a reconnect may therefore install a fresh pump without stale
            # publication or the old worker clearing the replacement reference.
            if self._receive_worker is worker:
                self._receive_worker = None
            self._receive_stop = None
            self._receive_condition.notify_all()
        return worker_stopped

    def _abort_partial_connect(self) -> bool:
        """Detach and best-effort stop clients created by a failed connect."""
        producer = self._producer
        consumer = self._consumer
        self._producer = None
        self._consumer = None
        self._consumer_generation += 1
        self._subscribed_topics.clear()
        self._last_msg = None
        self._last_delivery = None
        worker = self._fence_receive_pump_unlocked()
        cleanup_failed = self._shutdown_detached_clients(
            (consumer, "consumer"),
            (producer, "producer"),
            suppress_control_errors=True,
        )
        worker_stopped = self._finish_receive_pump_shutdown(worker)
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
        with self._connection_lock:
            # Detach first so no public pop can enter this generation while its
            # blocking receive is being interrupted and joined.
            producer = self._producer
            consumer = self._consumer
            self._producer = None
            self._consumer = None
            self._consumer_generation += 1
            self._subscribed_topics.clear()
            self._last_msg = None
            self._last_delivery = None
            worker = self._fence_receive_pump_unlocked()
            cleanup_failed = False
            control_error: BaseException | None = None
            try:
                cleanup_failed = self._shutdown_detached_clients(
                    (consumer, "consumer"), (producer, "producer")
                )
            except BaseException as error:
                # Finish the bounded pump join before restoring process-control
                # flow. This preserves KeyboardInterrupt/SystemExit without an
                # unbounded wait when receive ignored the failed shutdown.
                control_error = error
            worker_stopped = self._finish_receive_pump_shutdown(worker)
            if control_error is not None:
                raise control_error
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
        tasks: list[tuple[threading.Thread, _RocketMQCleanupResult]] = []
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
            tasks.append((worker, result))
            try:
                worker.start()
            except BaseException as start_error:
                result.publish(start_error)

        deadline = time.monotonic() + _CLIENT_SHUTDOWN_JOIN_TIMEOUT_S
        for worker, _result in tasks:
            if worker.ident is None:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            worker.join(timeout=remaining)

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

    def _get_topic_name(self, queue_name: str) -> str:
        """Get full topic name for queue.

        Args:
          queue_name: Base queue name.

        Returns:
          Full topic name.
        """
        _validate_key_name(queue_name, "queue_name")
        return f"{self.config.topic_prefix}_{queue_name}"

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
        if not self.is_connected():
            msg = "Not connected to RocketMQ"
            raise QueueError(msg, queue_name=queue_name, operation="push")
        # R22-C: enforce the documented client-side size cap (fail-fast) so an
        # operator who tightens ``max_message_size`` below ``queue_max_item_bytes``
        # gets a clear QueueError at push, not an opaque broker-side rejection. The
        # Field was previously dead config (declared, never read).
        if len(item) > self.config.max_message_size:
            msg = _ROCKETMQ_MAX_MESSAGE_SIZE_ERROR
            raise QueueError(msg, queue_name=queue_name, operation="push")

        try:
            from rocketmq import Message

            topic_name = self._get_topic_name(queue_name)
            msg = Message()
            msg.topic = topic_name
            msg.body = item
            # apache Message has no native priority field; carry it as ``keys`` so a
            # priority-aware consumer could read it. rocketmq topic ordering is by
            # queue, not priority — the priority arg is accepted for interface
            # symmetry but does not reorder within a topic.
            msg.keys = str(priority)
            if self._producer is None:
                error = "RocketMQBackend not connected: producer is None"
                raise QueueError(error, queue_name=queue_name, operation="push")
            self._producer.send(msg)
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
        if consumer is None or topic_name is None or stop is None:
            return
        worker = threading.Thread(
            target=self._receive_pump,
            args=(consumer, generation, topic_name, stop),
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
    ) -> None:
        """Run bounded broker long polls and publish one local delivery at a time."""
        try:
            consumer.subscribe(topic_name)
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
                messages = consumer.receive(1, self.config.invisible_duration)
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
            with self._receive_condition:
                if self._receive_worker is threading.current_thread():
                    self._receive_worker = None
                self._receive_condition.notify_all()

    def _receive_delivery(
        self, queue_name: str, timeout: float
    ) -> tuple[Any | None, Any, int]:
        """Take one exact delivery from the generation-scoped local buffer."""
        _validate_key_name(queue_name, "queue_name")
        topic_name = self._get_topic_name(queue_name)
        with self._connection_lock:
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
            if not isinstance(token, _RocketMQAckToken):
                return
            if (
                token.generation != self._consumer_generation
                or token.consumer is not self._consumer
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

            if token._settle("acked", acknowledge) and self._last_msg is target:
                self._last_msg = None
                self._last_delivery = None
            return
        else:
            target = self._last_msg
            if target is None:
                return
            if self._last_delivery is not None:
                consumer, generation, delivery = self._last_delivery
                if (
                    delivery is not target
                    or generation != self._consumer_generation
                    or consumer is not self._consumer
                ):
                    return
            else:
                consumer = self._consumer
            if consumer is None:
                return
        try:
            consumer.ack(target)
        except Exception as e:
            msg = f"Failed to ack RocketMQ message: {e}"
            raise QueueError(msg, operation="ack") from e
        else:
            # Clear the legacy slot when we acked the tracked message so a later
            # ack(token=None) is a no-op, not a re-ack.
            if self._last_msg is target:
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
            if not isinstance(token, _RocketMQAckToken):
                return
            if (
                token.generation != self._consumer_generation
                or token.consumer is not self._consumer
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

            if (
                token._settle("nacked", change_invisible_duration)
                and self._last_msg is target
            ):
                self._last_msg = None
                self._last_delivery = None
            return
        else:
            target = self._last_msg
            if target is None:
                return
            if self._last_delivery is not None:
                consumer, generation, delivery = self._last_delivery
                if (
                    delivery is not target
                    or generation != self._consumer_generation
                    or consumer is not self._consumer
                ):
                    return
            else:
                consumer = self._consumer
            if consumer is None:
                return
        try:
            consumer.change_invisible_duration(target, _MIN_INVISIBLE_DURATION)
        except Exception as e:
            msg = f"Failed to nack RocketMQ message: {e}"
            raise QueueError(msg, operation="nack") from e
        else:
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
