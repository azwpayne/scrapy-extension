"""MongoDB backend implementation with multi-mode support.

This module provides a MongoDB-based implementation of the backend interfaces
for distributed crawling, supporting multiple deployment modes:
- Standalone: Single MongoDB instance
- Replica Set: High availability with automatic failover
- Sharded Cluster: Horizontal scaling with mongos routers
- Atlas: MongoDB Atlas cloud service
"""

from __future__ import annotations

import logging
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import TYPE_CHECKING, Any, ClassVar, cast

from scrapy_extension.backends._optional import _is_missing_optional_dependency

# Import the distribution's top-level package before its bundled ``bson``
# namespace. When the optional extra is absent, this preserves the actionable
# ``pymongo`` classification instead of failing first with ``No module named
# 'bson'``.
try:
    from pymongo import ASCENDING, MongoClient, ReadPreference
    from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError
    from pymongo.read_concern import ReadConcern
    from pymongo.write_concern import WriteConcern
except ImportError as e:
    if not _is_missing_optional_dependency(e, "pymongo"):
        raise
    raise ImportError(
        "MongoDB backend requires 'pymongo'. Install with: pip install scrapy-extension[mongodb]"
    ) from e

try:
    from bson.binary import Binary
    from bson.decimal128 import Decimal128
    from bson.errors import BSONError
except ImportError as e:
    if not _is_missing_optional_dependency(e, "bson"):
        raise
    raise ImportError(
        "MongoDB backend requires 'pymongo'. Install with: pip install scrapy-extension[mongodb]"
    ) from e

from scrapy_extension.backends._redaction import _redact
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
    _hash_item,
    _validate_key_name,
    _validate_ttl,
    secret_value,
)
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
)
from scrapy_extension.exceptions._redaction import (
    backend_connection_error_boundary,
    configuration_error_boundary,
    queue_operation_error_boundary,
    set_operation_error_boundary,
    storage_operation_error_boundary,
)
from scrapy_extension.exceptions.base import StorageError
from scrapy_extension.settings import MongoDBMode
from scrapy_extension.settings.mongodb import (
    is_mongodb_direct_loopback_uri,
    uses_mongodb_external_auth,
    validate_mongodb_auth_source,
    validate_mongodb_authentication,
    validate_mongodb_collection_domains,
    validate_mongodb_database,
    validate_mongodb_replica_set_name,
    validate_mongodb_seed_endpoints,
    validate_mongodb_transport_security,
    validate_mongodb_uri,
    validate_mongodb_write_concern,
)

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.database import Database

    from scrapy_extension.settings import MongoDBSettings

logger = logging.getLogger(__name__)

_CAPABILITY_DOMAIN_MARKER_ID = "scrapy-extension:capability-domain:v1"
_CAPABILITY_DOMAIN_MARKER_FIELD = "scrapy_extension_capability_domain"
_MONGODB_CONNECT_SETTING_NAMES: frozenset[str] = frozenset(
    {
        "auth_mechanism",
        "auth_source",
        "allow_remote_plaintext",
        "collection_names",
        "database",
        "journal",
        "max_idle_time_ms",
        "max_pool_size",
        "min_pool_size",
        "mode",
        "mongos_routers",
        "password",
        "read_preference",
        "replica_set_members",
        "replica_set_name",
        "server_selection_timeout_ms",
        "tls_allow_invalid_certificates",
        "tls_ca_file",
        "tls_cert_file",
        "tls_enabled",
        "tls_key_file",
        "uri",
        "username",
        "w",
        "w_timeout_ms",
    }
)
_MONGODB_SAFE_CONNECT_MESSAGES: frozenset[str] = frozenset(
    {"Unsupported MongoDB mode."}
)
_MONGODB_QUEUE_PUSH_ERROR = "MongoDB queue push failed."
_MONGODB_QUEUE_POP_ERROR = "MongoDB queue pop failed."
_MONGODB_QUEUE_LENGTH_ERROR = "MongoDB queue length read failed."
_BSON_INT64_MIN = -(2**63)
_BSON_INT64_MAX = 2**63 - 1
# MongoDB's ``number`` query alias excludes booleans. Decimal128 bounds retain
# every finite BSON numeric value while excluding NaN and both infinities.
_MONGODB_MAX_FINITE_PRIORITY = Decimal128("9.999999999999999999999999999999999E+6144")
_MONGODB_MIN_FINITE_PRIORITY = Decimal128("-9.999999999999999999999999999999999E+6144")
_MONGODB_QUEUE_CLEAR_ERROR = "MongoDB queue clear failed."
_MONGODB_SET_ADD_ERROR = "MongoDB set add failed."
_MONGODB_SET_REMOVE_ERROR = "MongoDB set remove failed."
_MONGODB_SET_CONTAINS_ERROR = "MongoDB set membership check failed."
_MONGODB_SET_LENGTH_ERROR = "MongoDB set length read failed."
_MONGODB_SET_CLEAR_ERROR = "MongoDB set clear failed."
_MONGODB_STORAGE_STORE_ERROR = "MongoDB storage store failed."
_MONGODB_STORAGE_RETRIEVE_ERROR = "MongoDB storage retrieve failed."
_MONGODB_STORAGE_DELETE_ERROR = "MongoDB storage delete failed."
_MONGODB_STORAGE_EXISTS_ERROR = "MongoDB storage existence check failed."
_MONGODB_STORAGE_TTL_ERROR = "MongoDB storage TTL read failed."
_MONGODB_STORAGE_CLEAR_ERROR = "MongoDB storage clear failed."


def _validate_queue_name_argument(
    _backend: object,
    queue_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct MongoDB queue name outside its terminal boundary."""
    _validate_key_name(queue_name, "queue_name")


def _validate_queue_push_arguments(
    _backend: object,
    queue_name: str,
    _item: bytes,
    priority: float = 0.0,
) -> None:
    """Reject priorities whose negated stored form is not a finite BSON number."""
    _validate_key_name(queue_name, "queue_name")
    valid_priority = False
    if not isinstance(priority, bool):
        if isinstance(priority, int):
            stored_priority = -priority
            valid_priority = _BSON_INT64_MIN <= stored_priority <= _BSON_INT64_MAX
        elif isinstance(priority, float):
            valid_priority = math.isfinite(-priority)
    if not valid_priority:
        raise ValueError("priority must be a finite non-boolean number")


def _active_queue_filter(queue_name: str) -> dict[str, Any]:
    """Select only deliverable records; malformed records stay quarantined in place.

    The backend intentionally uses no destructive cleanup pass. Documents that do
    not match this schema remain durable in the queue collection for inspection,
    while valid records behind them remain eligible for the atomic pop. This same
    active-record policy is used by :meth:`MongoDBBackend.queue_len`; clear_queue
    deliberately remains broader so it removes active and quarantined documents.
    """
    return {
        "queue_name": {"$eq": queue_name},
        "item": {"$type": "binData"},
        "priority": {
            "$type": "number",
            "$gte": _MONGODB_MIN_FINITE_PRIORITY,
            "$lte": _MONGODB_MAX_FINITE_PRIORITY,
        },
        "created_at": {"$type": "date"},
    }


def _is_valid_queue_result(result: object, queue_name: str) -> bool:
    """Defensively verify the driver's result obeys the server query schema."""
    if not isinstance(result, Mapping):
        return False
    payload = result.get("item")
    priority = result.get("priority")
    created_at = result.get("created_at")
    if (
        result.get("queue_name") != queue_name
        or not isinstance(payload, (bytes, Binary))
        or isinstance(priority, bool)
        or not isinstance(created_at, datetime)
    ):
        return False
    if isinstance(priority, Decimal128):
        return priority.to_decimal().is_finite()
    if isinstance(priority, (int, float)):
        try:
            return math.isfinite(priority)
        except OverflowError:
            return False
    return False


def _validate_set_name_argument(
    _backend: object,
    set_name: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct MongoDB set name outside its terminal boundary."""
    _validate_key_name(set_name, "set_name")


def _validate_storage_key_argument(
    _backend: object,
    key: str,
    *_args: Any,
    **_kwargs: Any,
) -> None:
    """Validate a direct MongoDB storage key outside its terminal boundary."""
    _validate_key_name(key, "key")


def _validate_store_arguments(
    _backend: object,
    key: str,
    data: bytes,
    ttl: int | None = None,
) -> None:
    """Validate storage write arguments before a terminal error boundary."""
    del data
    _validate_key_name(key, "key")
    _validate_ttl(ttl)


def _validate_storage_prefix_argument(
    _backend: object,
    prefix: str | None = None,
) -> None:
    """Validate a non-empty clear prefix before backend implementation frames."""
    if prefix is not None:
        _validate_key_name(prefix, "prefix")


@dataclass(frozen=True)
class _MongoDBConnectionSnapshot:
    """One fully validated, repr-safe set of values for a connect attempt."""

    mode: MongoDBMode
    uri: str
    database: str
    collection_names: tuple[str, str, str]
    replica_set_name: str | None
    replica_set_members: tuple[str, ...]
    mongos_routers: tuple[str, ...]
    min_pool_size: int
    max_pool_size: int
    max_idle_time_ms: int
    wait_queue_timeout_ms: int
    server_selection_timeout_ms: int
    heartbeat_frequency_ms: int
    w: int | str
    journal: bool
    w_timeout_ms: int | None
    tls_enabled: bool
    tls_ca_file: str | None
    tls_cert_file: str | None
    tls_key_file: str | None
    tls_allow_invalid_certificates: bool
    username: str | None
    password: str | None
    auth_source: str
    auth_mechanism: str | None
    authenticated: bool
    force_direct_connection: bool
    read_preference: str


class MongoDBBackend(Backend, QueueBackend, SetBackend, StorageBackend):
    """MongoDB backend implementation with multi-mode support.

    Implements all backend interfaces using MongoDB collections:
    - Queue: Collection with priority and created_at fields
    - Set: Collection with unique index on (set_name, item_hash)
    - Storage: Collection with TTL index on expireAt

    Supports standalone, replica_set, sharded_cluster, and atlas deployment modes.

    Attributes:
        config: MongoDBSettings instance with connection parameters.
        _client: The MongoDB client instance (None until connected).
        _db: The MongoDB database instance.
    """

    _push_is_durable = True

    # Read preference mapping - defined as class constant to avoid recreating
    _READ_PREF_MAP: ClassVar[dict[str, str]] = {
        "primary": "primary",
        "secondary": "secondary",
        "nearest": "nearest",
        "primarypreferred": "primaryPreferred",
        "secondarypreferred": "secondaryPreferred",
    }

    def __init__(self, config: MongoDBSettings) -> None:
        """Initialize MongoDB backend.

        Args:
            config: Configuration for MongoDB connection.
        """
        self.config = config
        # U8: parameterize pymongo generics — the document shape is backend-defined
        # (queue docs / set docs / storage docs all carry their own keys), so
        # ``dict[str, Any]`` is the honest element type rather than a leak.
        self._client: MongoClient[dict[str, Any]] | None = None
        self._db: Database[dict[str, Any]] | None = None
        self._queue_collection: Collection[dict[str, Any]] | None = None
        self._set_collection: Collection[dict[str, Any]] | None = None
        self._storage_collection: Collection[dict[str, Any]] | None = None
        # Cache client kwargs to avoid rebuilding on reconnection
        self._client_kwargs: dict[str, Any] | None = None
        # Read preference is captured only during the guarded connection snapshot.
        # Validating mutable config in ``__init__`` would bypass that public error
        # boundary and retain raw values in constructor traceback frames.
        self._read_preference: str | None = None
        # A backend can be used directly as well as through ConnectionManager.
        # Serialize publication/retirement of its client graph so concurrent direct
        # callers cannot create two clients and lose one without closing it.
        self._connection_lock = RLock()

    @backend_connection_error_boundary(
        "Failed to connect to MongoDB.",
        "mongodb",
    )
    @configuration_error_boundary(
        "MongoDB configuration is invalid.",
        _MONGODB_CONNECT_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_MONGODB_SAFE_CONNECT_MESSAGES,
        catch_unexpected=False,
    )
    def connect(self) -> None:
        """Establish connection to MongoDB based on deployment mode.

        Creates the appropriate MongoDB client based on the configuration mode.
        Supports standalone, replica_set, sharded_cluster, and atlas modes.

        Raises:
            BackendConnectionError: If the connection cannot be established.
            ConfigurationError: If the configuration is invalid for the mode.
        """
        with self._connection_lock:
            # A published client graph is complete: the client has been pinged, its
            # collections initialized, capability domains claimed, and indexes
            # created. Re-running setup would allocate an unowned replacement client.
            if self._client is not None:
                return
            self._refresh_connection_cache()
            self._connect()

    def _refresh_connection_cache(self) -> None:
        """Rebuild configuration-derived caches for a fresh client generation."""
        self._client_kwargs = None
        self._read_preference = None

    @staticmethod
    def _validated_snapshot_int(value: object, setting_name: str, minimum: int) -> int:
        """Validate a mutable integer option before it reaches PyMongo."""
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ConfigurationError(
                f"MongoDB {setting_name} must be an integer >= {minimum}.",
                setting_name=setting_name,
            )
        return value

    @staticmethod
    def _validated_snapshot_optional_string(
        value: object, setting_name: str
    ) -> str | None:
        """Validate an optional path-like client option from mutable settings."""
        if value is None:
            return None
        if type(value) is not str or not value.strip():
            raise ConfigurationError(
                f"MongoDB {setting_name} must be a non-empty string when set.",
                setting_name=setting_name,
            )
        return value

    def _validated_read_preference(self, value: object) -> str:
        """Normalize a mutable read-preference value to PyMongo's spelling."""
        if type(value) is not str:
            raise ConfigurationError(
                "MongoDB read_preference must be a supported string.",
                setting_name="read_preference",
            )
        normalized = value.lower().replace("_", "")
        result = self._READ_PREF_MAP.get(normalized)
        if result is None:
            raise ConfigurationError(
                "MongoDB read_preference is unsupported.",
                setting_name="read_preference",
            )
        return result

    @configuration_error_boundary(
        "MongoDB configuration is invalid.",
        _MONGODB_CONNECT_SETTING_NAMES,
        preserve_static_message=True,
        safe_messages=_MONGODB_SAFE_CONNECT_MESSAGES,
    )
    def _capture_connection_snapshot(self) -> _MongoDBConnectionSnapshot:
        """Capture and validate every value consumed by one client generation.

        ``MongoDBSettings`` remains mutable for compatibility. A single immutable
        snapshot closes the interval where a configuration could otherwise pass a
        security check and then be changed before ``MongoClient`` consumes it.
        """
        mode = self.config.mode
        if not isinstance(mode, MongoDBMode):
            raise ConfigurationError("Unsupported MongoDB mode.", setting_name="mode")

        uri = validate_mongodb_uri(self.config.uri)
        database = validate_mongodb_database(self.config.database)
        collection_names = validate_mongodb_collection_domains(
            self.config.queue_collection,
            self.config.set_collection,
            self.config.storage_collection,
        )
        replica_set_name = validate_mongodb_replica_set_name(
            self.config.replica_set_name
        )
        replica_set_members = validate_mongodb_seed_endpoints(
            self.config.replica_set_members, "replica_set_members"
        )
        mongos_routers = validate_mongodb_seed_endpoints(
            self.config.mongos_routers, "mongos_routers"
        )

        min_pool_size = self._validated_snapshot_int(
            self.config.min_pool_size, "min_pool_size", 0
        )
        max_pool_size = self._validated_snapshot_int(
            self.config.max_pool_size, "max_pool_size", 1
        )
        if min_pool_size > max_pool_size:
            raise ConfigurationError(
                "MongoDB min_pool_size must be <= max_pool_size.",
                setting_name="min_pool_size",
            )
        max_idle_time_ms = self._validated_snapshot_int(
            self.config.max_idle_time_ms, "max_idle_time_ms", 0
        )
        wait_queue_timeout_ms = self._validated_snapshot_int(
            self.config.wait_queue_timeout_ms, "wait_queue_timeout_ms", 0
        )
        server_selection_timeout_ms = self._validated_snapshot_int(
            self.config.server_selection_timeout_ms, "server_selection_timeout_ms", 0
        )
        heartbeat_frequency_ms = self._validated_snapshot_int(
            self.config.heartbeat_frequency_ms, "heartbeat_frequency_ms", 0
        )
        w, w_timeout_ms = validate_mongodb_write_concern(
            self.config.w, self.config.w_timeout_ms
        )
        journal = self.config.journal
        if type(journal) is not bool:
            raise ConfigurationError(
                "MongoDB journal must be a boolean.", setting_name="journal"
            )

        tls_enabled = self.config.tls_enabled
        tls_allow_invalid_certificates = self.config.tls_allow_invalid_certificates
        allow_remote_plaintext = self.config.allow_remote_plaintext
        tls_ca_file = self._validated_snapshot_optional_string(
            self.config.tls_ca_file, "tls_ca_file"
        )
        tls_cert_file = self._validated_snapshot_optional_string(
            self.config.tls_cert_file, "tls_cert_file"
        )
        tls_key_file = self._validated_snapshot_optional_string(
            self.config.tls_key_file, "tls_key_file"
        )

        username = self.config.username
        password = secret_value(self.config.password)
        auth_source = validate_mongodb_auth_source(self.config.auth_source)
        auth_mechanism = self.config.auth_mechanism
        authenticated = validate_mongodb_authentication(
            username,
            password,
            auth_mechanism,
            auth_source,
        )
        validate_mongodb_transport_security(
            mode=mode,
            uri=uri,
            replica_set_members=replica_set_members,
            mongos_routers=mongos_routers,
            tls_enabled=tls_enabled,
            tls_allow_invalid_certificates=tls_allow_invalid_certificates,
            username=username,
            password=password,
            auth_mechanism=auth_mechanism,
            auth_source=auth_source,
            allow_remote_plaintext=allow_remote_plaintext,
            tls_ca_file=tls_ca_file,
            tls_cert_file=tls_cert_file,
            tls_key_file=tls_key_file,
        )

        return _MongoDBConnectionSnapshot(
            mode=mode,
            uri=uri,
            database=database,
            collection_names=collection_names,
            replica_set_name=replica_set_name,
            replica_set_members=replica_set_members,
            mongos_routers=mongos_routers,
            min_pool_size=min_pool_size,
            max_pool_size=max_pool_size,
            max_idle_time_ms=max_idle_time_ms,
            wait_queue_timeout_ms=wait_queue_timeout_ms,
            server_selection_timeout_ms=server_selection_timeout_ms,
            heartbeat_frequency_ms=heartbeat_frequency_ms,
            w=w,
            journal=journal,
            w_timeout_ms=w_timeout_ms,
            tls_enabled=tls_enabled,
            tls_ca_file=tls_ca_file,
            tls_cert_file=tls_cert_file,
            tls_key_file=tls_key_file,
            tls_allow_invalid_certificates=tls_allow_invalid_certificates,
            username=username,
            password=cast(str | None, _redact(password)),
            auth_source=auth_source,
            auth_mechanism=cast(str | None, auth_mechanism),
            authenticated=authenticated,
            force_direct_connection=(
                authenticated
                and not tls_enabled
                and mode is MongoDBMode.STANDALONE
                and is_mongodb_direct_loopback_uri(uri)
            ),
            read_preference=self._validated_read_preference(
                self.config.read_preference
            ),
        )

    def _connect(self) -> None:
        """Connect one fresh client generation while ``_connection_lock`` is held."""
        snapshot = self._capture_connection_snapshot()
        self._read_preference = snapshot.read_preference
        marker_options = self._marker_collection_options(
            snapshot.mode,
            journal=snapshot.journal,
            w_timeout_ms=snapshot.w_timeout_ms,
        )
        startup_error: BackendConnectionError | None = None
        cleanup_diagnostic_pending = False
        try:
            if snapshot.mode == MongoDBMode.STANDALONE:
                self._connect_standalone(snapshot, marker_options)
            elif snapshot.mode == MongoDBMode.REPLICA_SET:
                self._connect_replica_set(snapshot, marker_options)
            elif snapshot.mode == MongoDBMode.SHARDED_CLUSTER:
                self._connect_sharded_cluster(snapshot, marker_options)
            else:
                self._connect_atlas(snapshot, marker_options)
        except ConnectionFailure:
            cleanup_diagnostic_pending = self._discard_client(
                suppress_process_control=True
            )
            startup_error = BackendConnectionError(
                f"Failed to connect to MongoDB ({snapshot.mode.value}).",
                backend_type="mongodb",
            )
        except BackendConnectionError:
            cleanup_diagnostic_pending = self._discard_client(
                suppress_process_control=True
            )
            # Marker/setup helpers may retain a driver cause. The public startup
            # boundary must not re-expose that exception graph.
            startup_error = BackendConnectionError(
                f"Failed to connect to MongoDB ({snapshot.mode.value}).",
                backend_type="mongodb",
            )
        except ConfigurationError:
            self._discard_client(suppress_process_control=True)
            raise
        except Exception:
            cleanup_diagnostic_pending = self._discard_client(
                suppress_process_control=True
            )
            # Unexpected driver/plugin errors are not safe public diagnostics.
            startup_error = BackendConnectionError(
                f"Failed to connect to MongoDB ({snapshot.mode.value}).",
                backend_type="mongodb",
            )
        except BaseException:
            self._discard_client(suppress_process_control=True)
            raise

        if cleanup_diagnostic_pending:
            # The driver failure is no longer active.  Reporting the independent
            # close failure here prevents custom handlers from seeing it through
            # ``sys.exc_info()``.
            self._log_cleanup_diagnostic()

        if startup_error is not None:
            # Raise outside the driver exception handler so endpoint/credential text
            # cannot survive through ``__cause__`` or ``__context__``.
            raise startup_error

        # The complete client graph is now published.  Success diagnostics must
        # not turn that completed generation into a failed connection attempt.
        try:
            logger.debug("Connected to MongoDB in %s mode", snapshot.mode.value)
        except BaseException:
            pass

    @staticmethod
    def _log_cleanup_diagnostic() -> None:
        """Emit a fixed best-effort MongoDB cleanup diagnostic."""
        try:
            logger.debug("Failed to close MongoDB client")
        except BaseException:
            pass

    def _discard_client(self, *, suppress_process_control: bool = False) -> bool:
        """Clear all handles and best-effort close the current client.

        Returns whether a close failure was suppressed. Callers converting a
        startup failure to a fixed public error emit the resulting diagnostic only
        after their outer exception handler has completed.
        """
        close_failed = False
        with self._connection_lock:
            client = self._client
            self._client = None
            self._db = None
            self._queue_collection = None
            self._set_collection = None
            self._storage_collection = None
            if client is not None:
                try:
                    client.close()
                except Exception:
                    close_failed = True
                except BaseException:
                    if not suppress_process_control:
                        raise
                    close_failed = True
        return close_failed

    def _build_client_kwargs(
        self, snapshot: _MongoDBConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build common MongoDB client kwargs.

        Returns:
            Dictionary of client configuration options.
        """
        if snapshot is None and self._client_kwargs is not None:
            return self._client_kwargs.copy()
        if snapshot is None:
            snapshot = self._capture_connection_snapshot()
        self._read_preference = snapshot.read_preference

        kwargs: dict[str, Any] = {
            "minPoolSize": snapshot.min_pool_size,
            "maxPoolSize": snapshot.max_pool_size,
            "maxIdleTimeMS": snapshot.max_idle_time_ms,
            "waitQueueTimeoutMS": snapshot.wait_queue_timeout_ms,
            "serverSelectionTimeoutMS": snapshot.server_selection_timeout_ms,
            "heartbeatFrequencyMS": snapshot.heartbeat_frequency_ms,
        }

        kwargs.update(self._write_concern_kwargs(snapshot))
        kwargs.update(self._tls_kwargs(snapshot))
        kwargs.update(self._auth_kwargs(snapshot))
        if snapshot.force_direct_connection:
            # The cleartext loopback exception is safe only when the driver cannot
            # discover and follow a replica topology to remote endpoints.
            kwargs["directConnection"] = True

        # Add read preference
        kwargs["readPreference"] = snapshot.read_preference

        # Cache for future use
        self._client_kwargs = kwargs.copy()
        return kwargs

    def _write_concern_kwargs(
        self, snapshot: _MongoDBConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build write-concern kwargs from one validated connection snapshot."""
        if snapshot is None:
            snapshot = self._capture_connection_snapshot()
        kwargs: dict[str, Any] = {"w": snapshot.w, "journal": snapshot.journal}
        if snapshot.w_timeout_ms is not None:
            kwargs["wtimeoutMS"] = snapshot.w_timeout_ms
        return kwargs

    def _validated_write_concern(self) -> tuple[int | str, int | None]:
        """Revalidate the acknowledged-write policy against mutable settings."""
        return validate_mongodb_write_concern(
            self.config.w,
            self.config.w_timeout_ms,
        )

    def _tls_kwargs(
        self, snapshot: _MongoDBConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build TLS kwargs from one validated connection snapshot."""
        if snapshot is None:
            snapshot = self._capture_connection_snapshot()
        kwargs: dict[str, Any] = {}
        if not (snapshot.tls_enabled or snapshot.mode is MongoDBMode.ATLAS):
            return kwargs
        kwargs["tls"] = True
        if snapshot.tls_ca_file:
            kwargs["tlsCAFile"] = snapshot.tls_ca_file
        if snapshot.tls_cert_file:
            kwargs["tlsCertificateKeyFile"] = snapshot.tls_cert_file
        if snapshot.tls_key_file and not snapshot.tls_cert_file:
            kwargs["tlsCertificateKeyFile"] = snapshot.tls_key_file
        kwargs["tlsAllowInvalidCertificates"] = snapshot.tls_allow_invalid_certificates
        return kwargs

    def _auth_kwargs(
        self, snapshot: _MongoDBConnectionSnapshot | None = None
    ) -> dict[str, Any]:
        """Build authentication kwargs from one validated connection snapshot."""
        if snapshot is None:
            snapshot = self._capture_connection_snapshot()
        kwargs: dict[str, Any] = {}
        if not snapshot.authenticated:
            return kwargs
        if snapshot.username is not None:
            kwargs["username"] = snapshot.username
        if snapshot.password is not None:
            kwargs["password"] = snapshot.password
        if snapshot.auth_mechanism:
            kwargs["authMechanism"] = snapshot.auth_mechanism
        if uses_mongodb_external_auth(snapshot.auth_mechanism):
            kwargs["authSource"] = "$external"
        else:
            kwargs["authSource"] = snapshot.auth_source
        return kwargs

    def _compute_read_preference(self) -> str | None:
        """Compute read preference string for MongoDB.

        Returns:
            Read preference string or None for default.
        """
        read_preference = getattr(self.config, "read_preference", None)
        if read_preference is None:
            return None
        return self._validated_read_preference(read_preference)

    def _get_read_preference(self) -> str | None:
        """Get cached read preference string for MongoDB.

        Returns:
            Read preference string or None for default.
        """
        return self._read_preference

    def _initialize_collections(
        self,
        database: str,
        collection_names: tuple[str, str, str],
        marker_options: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize database and create indexes."""
        if self._client is None:
            msg = "MongoDB client not initialized"
            raise BackendConnectionError(msg, backend_type="mongodb")
        # Initialize database and collections
        queue_collection, set_collection, storage_collection = collection_names
        self._db = self._client[database]
        self._queue_collection = self._db[queue_collection]
        self._set_collection = self._db[set_collection]
        self._storage_collection = self._db[storage_collection]

        if marker_options is None:
            _, w_timeout_ms = self._validated_write_concern()
            marker_options = self._marker_collection_options(
                self.config.mode,
                journal=self.config.journal,
                w_timeout_ms=w_timeout_ms,
            )
        self._claim_collection_domains(marker_options)

        # Create indexes
        self._create_indexes()

    def _claim_collection_domains(
        self,
        marker_options: Mapping[str, Any],
    ) -> None:
        """Claim each physical collection for exactly one capability domain."""
        if (
            self._queue_collection is None
            or self._set_collection is None
            or self._storage_collection is None
        ):
            msg = "Collections not initialized: cannot claim capability domains"
            raise BackendConnectionError(msg, backend_type="mongodb")
        claims = (
            (self._queue_collection, "queue"),
            (self._set_collection, "set"),
            (self._storage_collection, "storage"),
        )
        for collection, domain in claims:
            marker_collection = collection.with_options(**marker_options)
            self._claim_collection_domain(marker_collection, domain)

    @staticmethod
    def _marker_collection_options(
        mode: MongoDBMode,
        *,
        journal: bool | None,
        w_timeout_ms: int | None,
    ) -> dict[str, Any]:
        """Build an isolated durability policy for capability-domain markers."""
        options: dict[str, Any] = {"read_preference": ReadPreference.PRIMARY}
        if mode == MongoDBMode.STANDALONE:
            return options

        write_concern_kwargs: dict[str, Any] = {"w": "majority"}
        if journal is not None:
            write_concern_kwargs["j"] = journal
        if w_timeout_ms is not None:
            write_concern_kwargs["wtimeout"] = w_timeout_ms
        options["read_concern"] = ReadConcern("majority")
        options["write_concern"] = WriteConcern(**write_concern_kwargs)
        return options

    @staticmethod
    def _claim_collection_domain(
        collection: Collection[dict[str, Any]],
        domain: str,
    ) -> None:
        """Atomically install or confirm one collection's domain marker."""
        exists, existing_domain = MongoDBBackend._read_collection_domain_marker(
            collection
        )
        if exists:
            MongoDBBackend._require_matching_collection_domain(existing_domain, domain)
            return

        marker = {
            "_id": _CAPABILITY_DOMAIN_MARKER_ID,
            # Domain-dependent data is deliberately below an array boundary. A
            # sharded collection cannot use a multikey index as its shard-key index,
            # so a valid shard key either sees identical fixed/missing values for all
            # contenders (routing them together) or rejects this insert fail-closed.
            # This preserves the fixed-_id mutex even when _id is not the shard key.
            _CAPABILITY_DOMAIN_MARKER_FIELD: [{"domain": domain}],
        }
        try:
            collection.insert_one(marker)
        except DuplicateKeyError as conflict:
            exists, existing_domain = MongoDBBackend._read_collection_domain_marker(
                collection
            )
            if not exists:
                raise BackendConnectionError(
                    "Failed to install the MongoDB capability-domain marker.",
                    backend_type="mongodb",
                ) from conflict
            MongoDBBackend._require_matching_collection_domain(existing_domain, domain)

    @staticmethod
    def _read_collection_domain_marker(
        collection: Collection[dict[str, Any]],
    ) -> tuple[bool, object]:
        """Read at most two markers so a poisoned sharded state fails closed."""
        markers = list(
            collection.find(
                {"_id": _CAPABILITY_DOMAIN_MARKER_ID},
                {_CAPABILITY_DOMAIN_MARKER_FIELD: 1},
            ).limit(2)
        )
        if len(markers) > 1:
            raise ConfigurationError(
                "A MongoDB physical collection has conflicting domain markers.",
                setting_name="collection_names",
            )
        marker = markers[0] if markers else None
        # A real PyMongo Collection configured by this backend returns ``dict`` or
        # ``None``. Treat a non-mapping test double as inconclusive/absent: the
        # subsequent insert is still authoritative and a real existing marker
        # forces DuplicateKey plus a second primary read before acceptance.
        if marker is None or not isinstance(marker, Mapping):
            return False, None
        return True, marker.get(_CAPABILITY_DOMAIN_MARKER_FIELD)

    @staticmethod
    def _require_matching_collection_domain(
        existing_domain: object,
        requested_domain: str,
    ) -> None:
        """Accept only the exact one-element ownership envelope."""
        if (
            type(existing_domain) is list
            and len(existing_domain) == 1
            and type(existing_domain[0]) is dict
            and set(existing_domain[0]) == {"domain"}
            and type(existing_domain[0]["domain"]) is str
            and existing_domain[0]["domain"] == requested_domain
        ):
            return
        raise ConfigurationError(
            (
                "A MongoDB physical collection is already claimed by another or "
                "malformed scrapy-extension capability domain."
            ),
            setting_name="collection_names",
        )

    def _connect_standalone(
        self,
        snapshot: _MongoDBConnectionSnapshot,
        marker_options: Mapping[str, Any],
    ) -> None:
        """Connect to standalone MongoDB instance."""
        kwargs = self._build_client_kwargs(snapshot)
        self._client = MongoClient(snapshot.uri, **kwargs)
        self._client.admin.command("ping")
        self._initialize_collections(
            snapshot.database, snapshot.collection_names, marker_options
        )

    def _connect_replica_set(
        self,
        snapshot: _MongoDBConnectionSnapshot,
        marker_options: Mapping[str, Any],
    ) -> None:
        """Connect to MongoDB replica set.

        Uses replica_set_name if provided, otherwise uses URI.
        """
        kwargs = self._build_client_kwargs(snapshot)

        # The seed list has already been parsed as host[:port] values. Keep the
        # generated URI authority-only: database selection and replica topology
        # are explicit client operations/kwargs, never string interpolation.
        if snapshot.replica_set_members:
            uri = f"mongodb://{','.join(snapshot.replica_set_members)}/"
        else:
            uri = snapshot.uri

        if snapshot.replica_set_name:
            kwargs["replicaSet"] = snapshot.replica_set_name

        self._client = MongoClient(uri, **kwargs)
        self._client.admin.command("ping")
        self._initialize_collections(
            snapshot.database, snapshot.collection_names, marker_options
        )

    def _connect_sharded_cluster(
        self,
        snapshot: _MongoDBConnectionSnapshot,
        marker_options: Mapping[str, Any],
    ) -> None:
        """Connect to MongoDB sharded cluster.

        Connects via mongos routers.
        """
        kwargs = self._build_client_kwargs(snapshot)

        if snapshot.mongos_routers:
            # Use validated mongos routers as connection points without a path/query.
            routers = ",".join(snapshot.mongos_routers)
            uri = f"mongodb://{routers}/"
            self._client = MongoClient(uri, **kwargs)
        else:
            # Fall back to provided URI
            self._client = MongoClient(snapshot.uri, **kwargs)

        self._client.admin.command("ping")
        self._initialize_collections(
            snapshot.database, snapshot.collection_names, marker_options
        )

    def _connect_atlas(
        self,
        snapshot: _MongoDBConnectionSnapshot,
        marker_options: Mapping[str, Any],
    ) -> None:
        """Connect to MongoDB Atlas.

        Uses standard Atlas connection string with TLS enabled.
        """
        kwargs = self._build_client_kwargs(snapshot)

        self._client = MongoClient(snapshot.uri, **kwargs)
        self._client.admin.command("ping")
        self._initialize_collections(
            snapshot.database, snapshot.collection_names, marker_options
        )

    def _create_indexes(self) -> None:
        """Create necessary indexes for collections.

        Raises:
            BackendConnectionError: If collections are not initialized.
        """
        if (
            self._queue_collection is None
            or self._set_collection is None
            or self._storage_collection is None
        ):
            msg = "Collections not initialized: call _initialize_collections() first"
            raise BackendConnectionError(msg, backend_type="mongodb")
        # Queue indexes
        self._queue_collection.create_index(
            [
                ("queue_name", ASCENDING),
                ("priority", ASCENDING),
                ("created_at", ASCENDING),
            ]
        )

        # Set indexes
        self._set_collection.create_index(
            [("set_name", ASCENDING), ("item_hash", ASCENDING)],
            unique=True,
        )

        # Storage indexes
        self._storage_collection.create_index("key", unique=True)
        self._storage_collection.create_index(
            "expireAt",
            expireAfterSeconds=0,
        )

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._discard_client():
            self._log_cleanup_diagnostic()

    def is_connected(self) -> bool:
        """Check if MongoDB is connected.

        Returns:
            True if connected and responding to ping.
        """
        try:
            if self._client is None:
                return False
            self._client.admin.command("ping")
        except Exception:
            return False
        else:
            return True

    def ping(self) -> bool:
        """Check MongoDB health.

        Returns:
            True if MongoDB responds to ping.
        """
        return self.is_connected()

    @property
    def backend_type(self) -> BackendType:
        """Return backend type.

        Returns:
            BackendType.MONGODB
        """
        return BackendType.MONGODB

    def _assert_connected(self) -> None:
        """Verify all collections are initialized.

        Raises:
            BackendConnectionError: If not connected.
        """
        if (
            self._queue_collection is None
            or self._set_collection is None
            or self._storage_collection is None
        ):
            msg = "Not connected: call connect() first"
            raise BackendConnectionError(msg, backend_type="mongodb")

    # QueueBackend implementation
    @queue_operation_error_boundary(
        "push",
        _MONGODB_QUEUE_PUSH_ERROR,
        validator=_validate_queue_push_arguments,
        handled_exception_types=(QueueError, BackendConnectionError),
    )
    def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
        """Push item to priority queue.

        Args:
            queue_name: Name of the queue.
            item: Item to push (bytes).
            priority: Priority value (higher = more urgent).

        Raises:
            QueueError: If the push operation fails.
            ValueError: If queue_name contains invalid characters.
        """
        _validate_key_name(queue_name, "queue_name")
        self._assert_connected()
        queue_collection = self._queue_collection
        if queue_collection is None:
            msg = "MongoDBBackend not connected: queue collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        doc = {
            "queue_name": queue_name,
            "item": item,
            "priority": -priority,  # Negated for DESC sort
            "created_at": datetime.now(tz=timezone.utc),
        }
        try:
            queue_collection.insert_one(doc)
        except (PyMongoError, BSONError, OverflowError) as e:
            # The public boundary rebuilds this after the document and encoder
            # traceback have unwound, retaining only its static operation metadata.
            raise QueueError(_MONGODB_QUEUE_PUSH_ERROR, operation="push") from e

    @queue_operation_error_boundary(
        "pop",
        _MONGODB_QUEUE_POP_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(QueueError, BackendConnectionError),
    )
    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
        """Pop highest priority item from queue.

        Args:
            queue_name: Name of the queue.
            timeout: Seconds to wait (unused for MongoDB, blocking not supported).

        Returns:
            The popped item, or None if queue is empty.

        Raises:
            QueueError: If the pop operation fails.
            ValueError: If queue_name contains invalid characters.
        """
        _validate_key_name(queue_name, "queue_name")
        self._assert_connected()
        queue_collection = self._queue_collection
        if queue_collection is None:
            msg = "MongoDBBackend not connected: queue collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            # The strict predicate is evaluated by MongoDB before its atomic delete.
            # Consequently poison documents are never selected or removed, valid
            # records behind them remain live, and concurrent callers still have one
            # server-side linearization point without client-side CAS retries.
            result = queue_collection.find_one_and_delete(
                _active_queue_filter(queue_name),
                sort=[("priority", ASCENDING), ("created_at", ASCENDING)],
            )
        except (PyMongoError, BSONError) as e:
            msg = "MongoDB atomic queue pop failed."
            raise QueueError(msg, operation="pop") from e
        if result is None:
            return None
        if not _is_valid_queue_result(result, queue_name):
            # A conforming server cannot return a non-matching document. Treat a
            # driver/plugin contract violation as a static backend failure rather
            # than indexing malformed data or exposing its diagnostics.
            raise QueueError(
                "MongoDB returned a malformed queue record.", operation="pop"
            )
        payload = result["item"]
        return bytes(payload)

    @queue_operation_error_boundary(
        "pop",
        _MONGODB_QUEUE_POP_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(QueueError, BackendConnectionError),
    )
    def pop_with_ack(
        self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, None]:
        """Pop atomically without retaining the base-class operation frame."""
        return (self.pop(queue_name, timeout), None)

    @queue_operation_error_boundary(
        "queue_len",
        _MONGODB_QUEUE_LENGTH_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(QueueError, BackendConnectionError),
    )
    def queue_len(self, queue_name: str) -> int:
        """Get queue length.

        Uses count_documents with limit to avoid O(n) full collection scans.
        The limit (100000) provides an upper bound; for queues exceeding this
        threshold, the returned value indicates "at least N" rather than exact count.

        Args:
            queue_name: Name of the queue.

        Returns:
            Number of items in the queue (capped at 100000).

        Raises:
            QueueError: If the count request fails.
        """
        _validate_key_name(queue_name, "queue_name")
        self._assert_connected()
        queue_collection = self._queue_collection
        if queue_collection is None:
            msg = "MongoDBBackend not connected: queue collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            return queue_collection.count_documents(
                _active_queue_filter(queue_name), limit=100000
            )
        except PyMongoError as e:
            msg = f"Failed to get queue length for {queue_name}: {e}"
            raise QueueError(msg, queue_name=queue_name, operation="queue_len") from e

    @queue_operation_error_boundary(
        "clear_queue",
        _MONGODB_QUEUE_CLEAR_ERROR,
        validator=_validate_queue_name_argument,
        handled_exception_types=(QueueError, BackendConnectionError),
    )
    def clear_queue(self, queue_name: str) -> None:
        """Clear all items from queue.

        Args:
            queue_name: Name of the queue.

        Raises:
            ValueError: If queue_name contains invalid characters.
            QueueError: If the delete request fails.
        """
        _validate_key_name(queue_name, "queue_name")
        self._assert_connected()
        queue_collection = self._queue_collection
        if queue_collection is None:
            msg = "MongoDBBackend not connected: queue collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            queue_collection.delete_many({"queue_name": queue_name})
        except PyMongoError as e:
            msg = f"Failed to clear queue {queue_name}: {e}"
            raise QueueError(msg, queue_name=queue_name, operation="clear_queue") from e

    # SetBackend implementation
    @set_operation_error_boundary(
        _MONGODB_SET_ADD_ERROR,
        "mongodb",
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
        self._assert_connected()
        set_collection = self._set_collection
        if set_collection is None:
            msg = "MongoDBBackend not connected: set collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        doc = {
            "set_name": set_name,
            "item_hash": _hash_item(item),
            "item": item,
            "created_at": datetime.now(tz=timezone.utc),
        }
        try:
            set_collection.insert_one(doc)
        except DuplicateKeyError:
            return False
        except PyMongoError as e:
            # R-dupe-1 (option b): wrap operational PyMongoError so BackendDupeFilter's
            # graceful-degradation arm catches it (degrade to not-seen) instead of
            # crashing the crawl. DuplicateKeyError (the "already existed" signal)
            # stays first so it still returns False.
            raise BackendConnectionError(
                f"MongoDB set add failed for {set_name!r}: {e}", backend_type="mongodb"
            ) from e
        else:
            return True

    @set_operation_error_boundary(
        _MONGODB_SET_REMOVE_ERROR,
        "mongodb",
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
        self._assert_connected()
        set_collection = self._set_collection
        if set_collection is None:
            msg = "MongoDBBackend not connected: set collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = set_collection.delete_one(
                {
                    "set_name": set_name,
                    "item_hash": _hash_item(item),
                }
            )
        except PyMongoError as e:
            raise BackendConnectionError(
                f"MongoDB set remove failed for {set_name!r}: {e}",
                backend_type="mongodb",
            ) from e
        return result.deleted_count > 0

    @set_operation_error_boundary(
        _MONGODB_SET_CONTAINS_ERROR,
        "mongodb",
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
        self._assert_connected()
        set_collection = self._set_collection
        if set_collection is None:
            msg = "MongoDBBackend not connected: set collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = set_collection.find_one(
                {
                    "set_name": set_name,
                    "item_hash": _hash_item(item),
                }
            )
        except PyMongoError as e:
            raise BackendConnectionError(
                f"MongoDB set contains failed for {set_name!r}: {e}",
                backend_type="mongodb",
            ) from e
        return result is not None

    @set_operation_error_boundary(
        _MONGODB_SET_LENGTH_ERROR,
        "mongodb",
        validator=_validate_set_name_argument,
    )
    def set_len(self, set_name: str) -> int:
        """Get set size.

        Uses count_documents with limit to avoid O(n) full collection scans.
        The limit (100000) provides an upper bound; for sets exceeding this
        threshold, the returned value indicates "at least N" rather than exact count.

        Args:
            set_name: Name of the set.

        Returns:
            Number of items in the set (capped at 100000).
        """
        _validate_key_name(set_name, "set_name")
        self._assert_connected()
        set_collection = self._set_collection
        if set_collection is None:
            msg = "MongoDBBackend not connected: set collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            return set_collection.count_documents({"set_name": set_name}, limit=100000)
        except PyMongoError as e:
            raise BackendConnectionError(
                f"MongoDB set length failed for {set_name!r}: {e}",
                backend_type="mongodb",
            ) from e

    @set_operation_error_boundary(
        _MONGODB_SET_CLEAR_ERROR,
        "mongodb",
        validator=_validate_set_name_argument,
    )
    def clear_set(self, set_name: str) -> None:
        """Clear all items from set.

        Args:
            set_name: Name of the set.

        Raises:
            ValueError: If set_name contains invalid characters.
            BackendConnectionError: If not connected, or if the delete fails at the
                MongoDB layer (parity with add R-dupe-1 #38 + redis clear_set #71).
        """
        _validate_key_name(set_name, "set_name")
        self._assert_connected()
        set_collection = self._set_collection
        if set_collection is None:
            msg = "MongoDBBackend not connected: set collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            set_collection.delete_many({"set_name": set_name})
        except PyMongoError as e:
            # R-rclears-mongo: wrap operational PyMongoError (parity with add
            # R-dupe-1 #38 + redis clear_set #71) so BackendDupeFilter's
            # graceful-degradation arm can fire; a raw leak crashes callers
            # expecting BackendError.
            raise BackendConnectionError(
                f"MongoDB set clear failed for {set_name!r}: {e}",
                backend_type="mongodb",
            ) from e

    # StorageBackend implementation
    @storage_operation_error_boundary(
        "store",
        _MONGODB_STORAGE_STORE_ERROR,
        "mongodb",
        validator=_validate_store_arguments,
    )
    def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
        """Store data with key.

        Args:
            key: Storage key.
            data: Data to store (bytes).
            ttl: Optional time-to-live in seconds.

        Raises:
            BackendConnectionError: If not connected.
            ValueError: If key contains invalid characters.
            StorageError: On PyMongoError (was previously unwrapped, leaking
                ``pymongo.errors.PyMongoError`` to callers expecting
                ``except BackendError``).
        """
        _validate_key_name(key, "key")
        _validate_ttl(ttl)
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        doc: dict[str, Any] = {
            "key": key,
            "data": data,
        }
        if ttl is not None:
            doc["expireAt"] = datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)

        try:
            storage_collection.replace_one(
                {"key": key},
                doc,
                upsert=True,
            )
        except PyMongoError as e:
            msg = f"Failed to store key {key!r} in MongoDB: {e}"
            raise StorageError(msg, operation="store", key=key) from e

    @staticmethod
    def _remaining_storage_ttl(document: dict[str, Any]) -> float | None:
        """Return seconds until ``document`` expires, or None without a TTL."""
        raw_expiry = document.get("expireAt")
        if raw_expiry is None:
            return None

        expire_at = cast(datetime, raw_expiry)
        # BSON datetimes are UTC, but PyMongo returns them without tzinfo unless
        # the client opts into tz-aware decoding.
        if expire_at.tzinfo is None:
            expire_at = expire_at.replace(tzinfo=timezone.utc)
        return (expire_at - datetime.now(tz=timezone.utc)).total_seconds()

    def _lazy_reap_expired_storage(self, document: dict[str, Any], key: str) -> bool:
        """Conditionally reap an expired snapshot and report whether it expired."""
        remaining = self._remaining_storage_ttl(document)
        if remaining is None or remaining > 0:
            return False

        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        reap_failed = False
        try:
            # The read snapshot may be stale: a concurrent store() can replace the
            # same key before this delete runs. Matching the observed expireAt makes
            # the cleanup a CAS, so a fresh replacement is not removed.
            storage_collection.delete_one(
                {"key": key, "expireAt": document["expireAt"]}
            )
        except PyMongoError:
            # The expired snapshot is already logically absent and the caught
            # ordinary delete failure is best-effort.  Keep diagnostics from changing
            # that result; direct control exceptions from ``delete_one`` are not
            # caught by this PyMongoError arm.
            reap_failed = True
        if reap_failed:
            # The cleanup handler has completed, so a custom logger cannot read the
            # raw PyMongo failure from ``sys.exc_info()``.
            try:
                logger.warning("Failed to reap expired MongoDB storage key")
            except BaseException:
                pass
        return True

    @storage_operation_error_boundary(
        "retrieve",
        _MONGODB_STORAGE_RETRIEVE_ERROR,
        "mongodb",
        validator=_validate_storage_key_argument,
    )
    def retrieve(self, key: str) -> bytes | None:
        """Retrieve current data by key.

        Args:
            key: Storage key.

        Returns:
            Stored data, or None if not found or expired.

        Raises:
            BackendConnectionError: If not connected.
            StorageError: On PyMongoError (was previously unwrapped).
        """
        _validate_key_name(key, "key")
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = storage_collection.find_one({"key": key})
            if result and self._lazy_reap_expired_storage(result, key):
                return None
        except PyMongoError as e:
            msg = f"Failed to retrieve key {key!r} from MongoDB: {e}"
            raise StorageError(msg, operation="retrieve", key=key) from e
        if result:
            # storage doc stores ``data`` as bytes; cast narrows the Any from pymongo.
            return cast(bytes, result.get("data"))
        return None

    @storage_operation_error_boundary(
        "delete",
        _MONGODB_STORAGE_DELETE_ERROR,
        "mongodb",
        validator=_validate_storage_key_argument,
    )
    def delete(self, key: str) -> bool:
        """Delete data by key.

        Args:
            key: Storage key.

        Returns:
            True if deleted, False if didn't exist.

        Raises:
            BackendConnectionError: If not connected.
            StorageError: On PyMongoError (was previously unwrapped).
        """
        _validate_key_name(key, "key")
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = storage_collection.delete_one({"key": key})
        except PyMongoError as e:
            msg = f"Failed to delete key {key!r} in MongoDB: {e}"
            raise StorageError(msg, operation="delete", key=key) from e
        return result.deleted_count > 0

    @storage_operation_error_boundary(
        "exists",
        _MONGODB_STORAGE_EXISTS_ERROR,
        "mongodb",
        validator=_validate_storage_key_argument,
    )
    def exists(self, key: str) -> bool:
        """Check if key exists and has not expired.

        Args:
            key: Storage key.

        Returns:
            True if key exists and is current.

        Raises:
            BackendConnectionError: If not connected.
            StorageError: On PyMongoError (was previously unwrapped).
        """
        _validate_key_name(key, "key")
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = storage_collection.find_one(
                {"key": key}, {"_id": 1, "expireAt": 1}
            )
            if result and self._lazy_reap_expired_storage(result, key):
                return False
        except PyMongoError as e:
            msg = f"Failed to check existence of key {key!r} in MongoDB: {e}"
            raise StorageError(msg, operation="exists", key=key) from e
        return result is not None

    @storage_operation_error_boundary(
        "ttl",
        _MONGODB_STORAGE_TTL_ERROR,
        "mongodb",
        validator=_validate_storage_key_argument,
    )
    def ttl(self, key: str) -> int | None:
        """Get remaining time-to-live.

        Args:
            key: Storage key.

        Returns:
            Non-negative seconds remaining, or None if absent, permanent, or expired.

        Raises:
            BackendConnectionError: If not connected.
            StorageError: On PyMongoError (was previously unwrapped).
        """
        _validate_key_name(key, "key")
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        try:
            result = storage_collection.find_one({"key": key}, {"expireAt": 1})
        except PyMongoError as e:
            msg = f"Failed to read TTL of key {key!r} in MongoDB: {e}"
            raise StorageError(msg, operation="ttl", key=key) from e
        if result is None:
            return None
        remaining = self._remaining_storage_ttl(result)
        if remaining is None:
            return None
        if remaining <= 0:
            self._lazy_reap_expired_storage(result, key)
            return None
        return max(0, int(remaining))

    @storage_operation_error_boundary(
        "clear_storage",
        _MONGODB_STORAGE_CLEAR_ERROR,
        "mongodb",
        validator=_validate_storage_prefix_argument,
    )
    def clear_storage(self, prefix: str | None = None) -> None:
        """Clear all stored data, optionally filtered by prefix.

        Args:
            prefix: If provided, only clear keys starting with this prefix.
                   If None, clear all storage data.

        Raises:
            BackendConnectionError: If not connected.
            StorageError: On PyMongoError (was previously unwrapped).
        """
        if prefix is not None:
            _validate_key_name(prefix, "prefix")
        self._assert_connected()
        storage_collection = self._storage_collection
        if storage_collection is None:
            msg = "MongoDBBackend not connected: storage collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        if prefix:
            pattern = re.escape(prefix)
            try:
                storage_collection.delete_many({"key": {"$regex": f"^{pattern}"}})
            except PyMongoError as e:
                msg = f"Failed to clear MongoDB storage (prefix={prefix!r}): {e}"
                raise StorageError(msg, operation="clear_storage", key=None) from e
        else:
            try:
                storage_collection.delete_many(
                    {"_id": {"$ne": _CAPABILITY_DOMAIN_MARKER_ID}}
                )
            except PyMongoError as e:
                msg = f"Failed to clear MongoDB storage: {e}"
                raise StorageError(msg, operation="clear_storage", key=None) from e
