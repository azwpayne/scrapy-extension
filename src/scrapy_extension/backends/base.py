"""Base backend definitions and abstract interfaces.

This module defines the abstract base classes and interfaces that all
backend implementations must follow.
"""

from __future__ import annotations

__all__ = [
  "Backend",
  "BackendType",
  "JSONSerializer",
  "QueueBackend",
  "Serializer",
  "SetBackend",
  "StorageBackend",
]

import base64
import binascii
import hashlib
import json
import math
import re
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Protocol

from pydantic import SecretStr

#: Legacy bytes marker retained for reading payloads written before the escaped
#: codec. New writes use ``_CODEC_TAG`` and escape marker-shaped user dicts.
_BYTES_TAG = "__b64__"

# New bytes markers are escaped before JSON encoding so caller-owned dictionaries
# can use any key/value shape without being retyped during deserialize.
_CODEC_TAG = "__scrapy_extension_json_type__"
_CODEC_DATA = "data"
_CODEC_BYTES = "bytes"
_CODEC_DICT = "dict"


def _json_default(obj: object) -> object:
  """JSON default handler for types Scrapy request dicts commonly contain.

  Handles the types that appear in real-world ``request.meta``:
  - ``datetime`` / ``date`` → ISO 8601 string (round-trips via ``datetime.fromisoformat``)
  - ``bytes`` / ``bytearray`` → tagged base64 marker (the recursive codec
    handles these before this fallback in normal ``JSONSerializer`` use)
  - ``Decimal`` → ``str`` (preserves exact decimal representation, avoids float drift)
  - ``UUID`` → ``str`` (canonical hex form)
  - ``set`` / ``frozenset`` → ``list`` (JSON has no set type; order undefined)
  - ``Enum`` → ``.value`` (preserves the enum's declared value, not the member)
  - ``pathlib.Path`` → ``str`` (preserves the path representation)

  Everything else raises ``TypeError`` — surfacing the caller's bug rather
  than silently ``str()``-ing it (which produced ``"b'x'"`` for bytes and
  lost the original value).

  Args:
      obj: The non-JSON-native object to convert.

  Returns:
      A JSON-native representation (str, list, int, etc.).

  Raises:
      TypeError: If the object's type isn't handled.
  """
  if isinstance(obj, (datetime, date)):
    return obj.isoformat()
  if isinstance(obj, (bytes, bytearray)):
    return {
      _CODEC_TAG: _CODEC_BYTES,
      _CODEC_DATA: base64.b64encode(bytes(obj)).decode("ascii"),
    }
  if isinstance(obj, Decimal):
    return str(obj)
  if isinstance(obj, uuid.UUID):
    return str(obj)
  if isinstance(obj, (set, frozenset)):
    return list(obj)
  if isinstance(obj, Enum):
    return obj.value
  if isinstance(obj, Path):
    return str(obj)
  type_name = type(obj).__name__
  if type_name in {"SecretStr", "SecretBytes"}:
    # pydantic secrets in request.meta (#31) — emit the underlying value via
    # the duck-typed accessor (no pydantic import; the type-name check mirrors
    # exceptions/base._is_secret_value). Deserialize yields a plain str —
    # SecretStr is not reconstructable from JSON without a pydantic hook; the
    # goal here is "does not crash the push path".
    get_secret_value = getattr(obj, "get_secret_value", None)
    return get_secret_value() if callable(get_secret_value) else str(obj)
  raise TypeError(
    f"Object of type {type_name} is not JSON serializable. "
    f"Pre-serialize {type_name} instances before pushing to the queue, "
    f"or extend scrapy_extension.backends.base._json_default."
  )


def _decode_bytes_tag(obj: object) -> object:
  """Decode the legacy ``{"__b64__": ...}`` bytes marker when valid.

  New writes use the escaped recursive codec. It wraps a caller-owned dict with
  this exact shape, so new serialize → deserialize round trips cannot collide;
  the legacy decoder remains solely for rolling-upgrade compatibility.

  Args:
      obj: A decoded JSON value.

  Returns:
      ``bytes`` for a tagged marker dict; the original dict otherwise.
  """
  if isinstance(obj, dict) and len(obj) == 1:
    value = obj.get(_BYTES_TAG)
    if isinstance(value, str):
      try:
        return base64.b64decode(value, validate=True)
      except (binascii.Error, ValueError):
        # A legitimately-stored dict shaped like {"__b64__": "<non-base64>"}
        # (a spider's own meta key, or a truncated/corrupt value) must NOT
        # crash the entire request deserialize (#31). Fall through: the dict
        # is returned unchanged so the value surfaces as a plain str instead
        # of dropping the whole pop.
        pass
  return obj


def _looks_like_codec_marker(obj: dict[object, object]) -> bool:
  """Whether a caller-owned dict would collide with a supported wire marker."""
  if len(obj) == 1 and isinstance(obj.get(_BYTES_TAG), str):
    return True
  return (
    len(obj) == 2
    and obj.get(_CODEC_TAG) in {_CODEC_BYTES, _CODEC_DICT}
    and _CODEC_DATA in obj
  )


def _encode_json_value(obj: object) -> object:
  """Recursively encode bytes while escaping marker-shaped caller dictionaries."""
  if isinstance(obj, (bytes, bytearray)):
    return {
      _CODEC_TAG: _CODEC_BYTES,
      _CODEC_DATA: base64.b64encode(bytes(obj)).decode("ascii"),
    }
  if isinstance(obj, dict):
    for key in obj:
      if not isinstance(key, str):
        raise TypeError(
          "JSON object keys must be strings, "
          f"got {type(key).__name__}"
        )
    encoded = {key: _encode_json_value(value) for key, value in obj.items()}
    if _looks_like_codec_marker(encoded):
      return {
        _CODEC_TAG: _CODEC_DICT,
        _CODEC_DATA: list(encoded.items()),
      }
    return encoded
  if isinstance(obj, (list, tuple)):
    return [_encode_json_value(value) for value in obj]
  if isinstance(obj, float) and not math.isfinite(obj):
    raise ValueError(f"JSON numbers must be finite, got {obj!r}")
  if obj is None or isinstance(obj, (str, int, float, bool)):
    return obj
  return _encode_json_value(_json_default(obj))


def _decode_json_value(obj: object) -> object:
  """Decode current markers and legacy bytes tags without dict collisions."""
  if isinstance(obj, list):
    return [_decode_json_value(value) for value in obj]
  if not isinstance(obj, dict):
    return obj

  if (
    len(obj) == 2
    and obj.get(_CODEC_TAG) == _CODEC_DICT
    and isinstance(obj.get(_CODEC_DATA), list)
  ):
    items = obj[_CODEC_DATA]
    if all(
      isinstance(pair, list)
      and len(pair) == 2
      and isinstance(pair[0], str)
      for pair in items
    ):
      decoded: dict[str, object] = {}
      for key, value in items:
        if key in decoded:
          raise ValueError(f"Duplicate escaped JSON object key: {key!r}")
        decoded[key] = _decode_json_value(value)
      return decoded

  if (
    len(obj) == 2
    and obj.get(_CODEC_TAG) == _CODEC_BYTES
    and isinstance(obj.get(_CODEC_DATA), str)
  ):
    try:
      return base64.b64decode(obj[_CODEC_DATA], validate=True)
    except (binascii.Error, ValueError):
      pass

  legacy = _decode_bytes_tag(obj)
  if legacy is not obj:
    return legacy
  return {key: _decode_json_value(value) for key, value in obj.items()}


def _reject_non_finite_json_constant(value: str) -> object:
  """Reject Python's non-standard NaN/Infinity JSON extensions."""
  raise ValueError(f"JSON numbers must be finite, got {value}")


def _json_object_from_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
  """Build a JSON object while rejecting ambiguous duplicate member names."""
  result: dict[str, object] = {}
  for key, value in pairs:
    if key in result:
      raise ValueError(f"Duplicate JSON object key: {key!r}")
    result[key] = value
  return result


def secret_value(s: SecretStr | str | None) -> str | None:
  """Extract the raw string from a SecretStr (or pass through plain str).

  Defensive against plain ``str`` values that bypass pydantic validation
  (e.g., ``config.password = "x"`` after construction, which doesn't
  coerce to SecretStr unless ``validate_assignment=True``).

  Args:
      s: A SecretStr, plain str, or None.

  Returns:
      The secret's raw string value, or None.
  """
  if s is None:
    return None
  if isinstance(s, SecretStr):
    return s.get_secret_value()
  return s


class Serializer(Protocol):
  """Protocol for serializers.

  Any class implementing this protocol can be used for serializing
  and deserializing data for backend storage.
  """

  def serialize(self, obj: object) -> bytes:
    """Serialize an object to bytes.

    Args:
        obj: The object to serialize.

    Returns:
        The serialized bytes.
    """
    ...

  def deserialize(self, data: bytes) -> object:
    """Deserialize bytes to an object.

    Args:
        data: The bytes to deserialize.

    Returns:
        The deserialized object.
    """
    ...


class JSONSerializer:
  """JSON serializer implementation.

  Uses Python's json module for serialization. Suitable for
  serializing basic Python types and simple objects.
  """

  def serialize(self, obj: object) -> bytes:
    """Serialize an object to JSON bytes.

    Uses an escaped recursive codec plus ``_json_default`` for common
    non-JSON-native types found in Scrapy request dicts (datetime → ISO,
    bytes → tagged base64 marker).
    Truly unexpected types raise TypeError with a clear message — no silent
    ``str()`` coercion.

    Args:
        obj: The object to serialize.

    Returns:
        JSON-encoded bytes.

    Raises:
        TypeError: If the object contains types not handled by _json_default.
    """
    return json.dumps(
      _encode_json_value(obj),
      default=_json_default,
      allow_nan=False,
    ).encode("utf-8")

  def deserialize(self, data: bytes) -> object:
    """Deserialize JSON bytes to an object.

    Reverses escaped current bytes markers and legacy ``{"__b64__": ...}``
    markers. Marker-shaped caller dictionaries remain dictionaries.

    Args:
        data: The JSON bytes to deserialize.

    Returns:
        The deserialized object.
    """
    return _decode_json_value(
      json.loads(
        data.decode("utf-8"),
        parse_constant=_reject_non_finite_json_constant,
        object_pairs_hook=_json_object_from_pairs,
      )
    )


# Shared utilities for backends

KEY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]+$")


def _validate_key_name(name: str, field_name: str = "name") -> None:
    """Validate key/queue/set/index name to prevent injection.

    Args:
        name: The name to validate.
        field_name: Field name for error messages.

    Raises:
        ValueError: If name contains invalid characters.
    """
    if not name or not KEY_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid {field_name}: {name!r}. "
            f"Only alphanumeric, dots, underscores, hyphens, and colons allowed."
        )


def _validate_ttl(ttl: int | None) -> None:
    """Validate the shared StorageBackend TTL input contract.

    ``None`` is the permanent-value sentinel. Concrete TTLs are positive
    integers; zero, negatives, floats, and bools otherwise diverge across
    Redis, MongoDB, ElasticSearch, DynamoDB, and Memcached.
    """
    if ttl is None:
        return
    if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl <= 0:
        raise ValueError(f"ttl must be a positive integer or None, got {ttl!r}")


def _hash_item(item: bytes) -> str:
    """Generate SHA256 hash for item.

    Args:
        item: Item to hash.

    Returns:
        SHA256 hex digest.
    """
    return hashlib.sha256(item).hexdigest()


def _get_mode_text(mode: object) -> str:
    """Get a displayable string for a mode enum value.

    Args:
        mode: The mode enum value.

    Returns:
        A string representation of the mode.
    """
    try:
        return str(mode)
    except (TypeError, ValueError):
        return getattr(mode, "value", repr(mode))


class BackendType(str, Enum):
  """Supported backend types for distributed crawling.

  Attributes:
      REDIS: Redis backend for distributed crawling.
      MONGODB: MongoDB backend for distributed crawling.
      KAFKA: Kafka backend for distributed crawling.
      RABBITMQ: RabbitMQ backend for distributed crawling.
      ELASTICSEARCH: ElasticSearch backend for distributed crawling.
      ROCKETMQ: RocketMQ backend for distributed crawling.
      PULSAR: Pulsar backend for distributed crawling (queue-only).
      MEMCACHED: Memcached backend (StorageBackend — KV with TTL).
      SQS: Amazon SQS backend (queue-only MQ).
      DYNAMODB: DynamoDB backend (StorageBackend — NoSQL KV).
  """

  REDIS = "redis"
  MONGODB = "mongodb"
  KAFKA = "kafka"
  RABBITMQ = "rabbitmq"
  ELASTICSEARCH = "elasticsearch"
  ROCKETMQ = "rocketmq"
  PULSAR = "pulsar"
  MEMCACHED = "memcached"
  SQS = "sqs"
  DYNAMODB = "dynamodb"

  @classmethod
  def _missing_(cls, value: object) -> BackendType | None:
    """Reject unknown values with a descriptive error.

        Round-14 R14-B note: USER-FACING backend-type validation is routed
        through ``Settings._validate_backend_type`` (a ``field_validator``),
        which accepts ANY registry-known 3rd-party string AND raises
        ``ConfigurationError`` (the project's config-error family) for unknown
        values — never pydantic ``ValidationError``. This ``_missing_`` hook
        is a DEFENSIVE backstop for direct ``BackendType(x)`` calls that
        bypass the settings layer (e.g. internal code paths). It keeps the
        pre-R14-B ``ValueError`` so enum semantics remain conventional for
        low-level callers; operators hitting this path through ``Settings``
        see ``ConfigurationError`` instead (see
        ``settings/base.py::_validate_backend_type``).

        Args:
            value: The value that did not match any member.

        Raises:
            ValueError: Always — ``_missing_`` must return ``None`` or a
                member; we choose to raise for fail-fast UX.
        """
    valid = ", ".join(repr(m.value) for m in cls)
    msg = (
      f"{value!r} is not a valid {cls.__name__}. "
      f"Valid values: {valid}."
    )
    raise ValueError(msg)


class Backend(ABC):
  """Abstract base class for all backends.

  All backend implementations must inherit from this class and
  implement the abstract methods for connection management.
  """

  @abstractmethod
  def connect(self) -> None:
    """Establish connection to the backend.

    This method should create any necessary connections and
    prepare the backend for use.

    Raises:
        ConnectionError: If the connection cannot be established.
    """

  @abstractmethod
  def disconnect(self) -> None:
    """Close connection to the backend.

    This method should cleanly close all connections and
    release any resources.
    """

  @abstractmethod
  def is_connected(self) -> bool:
    """Check if the backend is connected.

    Returns:
        True if connected, False otherwise.
    """

  @abstractmethod
  def ping(self) -> bool:
    """Check backend health.

    Returns:
        True if the backend is healthy and responsive.
    """

  @property
  @abstractmethod
  def backend_type(self) -> BackendType | str:
    """Return the backend type.

    Round-5 R5-1: widened to ``BackendType | str`` so 3rd-party backends
    (registered via entry-points) can return a plain registry-key string
    instead of a bundled ``BackendType`` member. Bundled backends still
    return their canonical ``BackendType`` member — additive, no behavior
    change for the 10 bundled backends.

    Returns:
        The BackendType enum value (bundled) or registry-key string
        (3rd-party) for this backend.
    """


@dataclass(frozen=True, slots=True)
class _QueuePushReceipt:
  """Internal proof returned by the exact backend push operation.

  ``worker_crash_durable`` means the accepted item survives loss of the
  current crawler process.  It deliberately says nothing about a storage
  cluster or broker losing its own durable state.
  """

  worker_crash_durable: bool


class _DurablePushRequired(Exception):
  """Internal policy rejection that must not count as a backend failure."""


class QueueBackend(ABC):
  """Interface for queue operations.

  Backends that support queue operations must implement this interface.

  Ack-capability contract (round-2):

  - ``requires_ack``: True when ``pop`` yields a message that the caller
    MUST subsequently acknowledge via :meth:`ack` (else the message is
    redelivered). False for atomic-pop backends (Redis, MongoDB,
    ElasticSearch) — their pop removes the item in one step, so ack/nack
    are no-ops and the scheduler's ack wiring is inert. RocketMQ is
    deferred-ack (``requires_ack=True``): its gRPC ``receive`` yields a
    message the caller must ``ack`` before the invisible-duration window
    elapses (at-least-once redelivery), so it overrides ``pop_with_ack``
    / ``ack`` rather than inheriting the atomic defaults.
  - ``supports_concurrent_ack``: True when ack is safe under
    ``CONCURRENT_REQUESTS > 1`` (i.e. the backend tracks per-message ack
    state). **As of 2026-07-10 every bundled backend sets this True** —
    atomic-pop backends (Redis/MongoDB/ES) because ack is a no-op, and all
    five MQ backends (Kafka/RabbitMQ/RocketMQ/SQS/Pulsar) because each
    tracks a per-message token (in-flight set / ReceiptHandle / MessageId).
    A 3rd-party backend that can only hold a single ack slot may set False;
    the scheduler's ``from_settings`` gate then raises
    ``ConfigurationError`` for ``requires_ack and not
    supports_concurrent_ack`` under ``CONCURRENT_REQUESTS > 1`` unless the
    explicit ``SCRAPY_ACK_UNSAFE_CONCURRENT_REQUESTS`` opt-out is set. (The
    gate is unreachable for the 10 bundled backends — it remains a defensive
    backstop for a hypothetical single-slot 3rd-party backend.)

  Defaults (``requires_ack=False``, ``supports_concurrent_ack=True``) keep
  atomic-pop backends untouched and are the safe baseline for any new
  QueueBackend that does not override them.
  """

  requires_ack: bool = False
  """True if pop yields a message needing explicit :meth:`ack` (MQ backends)."""

  supports_concurrent_ack: bool = True
  """True if ack is correct under ``CONCURRENT_REQUESTS > 1`` (real in-flight set)."""

  _push_is_durable: ClassVar[bool] = False
  """Whether this backend invariably crosses a worker-crash durable boundary."""

  def _push_with_durability(
    self,
    queue_name: str,
    item: bytes,
    priority: float = 0.0,
    *,
    require_durable: bool = False,
  ) -> _QueuePushReceipt:
    """Push once and return operation-bound worker-crash durability.

    This concrete, package-private extension keeps pre-existing third-party
    ``QueueBackend`` implementations source compatible.  Their inherited
    default is fail-closed: ordinary pushes still delegate to the stable
    public :meth:`push`, while a durable-required transfer is rejected before
    the backend can mutate any process-local queue.
    """
    durable = self._push_is_durable is True
    if require_durable and not durable:
      raise _DurablePushRequired
    self.push(queue_name, item, priority)
    return _QueuePushReceipt(worker_crash_durable=durable)

  @abstractmethod
  def push(self, queue_name: str, item: bytes, priority: float = 0.0) -> None:
    """Push an item to a queue.

    Args:
        queue_name: The name of the queue.
        item: The item to push (serialized bytes).
        priority: Priority of the item (higher = more urgent).

    Raises:
        QueueError: If the push operation fails.
    """

  @abstractmethod
  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Pop an item from a queue.

    Args:
        queue_name: The name of the queue.
        timeout: Seconds to wait for an item (0 = non-blocking).

    Returns:
        The popped item, or None if the queue is empty.

    Raises:
        QueueError: If the pop operation fails.
    """

  @abstractmethod
  def queue_len(self, queue_name: str) -> int:
    """Get the number of items in a queue.

    Args:
        queue_name: The name of the queue.

    Returns:
        The number of items in the queue.
    """

  @abstractmethod
  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from a queue.

    Args:
        queue_name: The name of the queue.
    """

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, Any | None]:
    """Pop an item together with an opaque ack token.

    For atomic-pop backends (Redis, MongoDB, ElasticSearch) the default
    implementation returns ``(self.pop(queue_name, timeout), None)`` — there
    is no separate ack step, so the token is ``None``.

    Message-queue backends (Kafka, RabbitMQ) override to return a
    backend-specific token that the scheduler carries in
    ``request.meta["_backend_ack_token"]`` and hands back to
    :meth:`ack` / :meth:`nack` so the *specific* message that was popped
    is acked — not merely the last-popped one. This is what makes ack
    correct under ``CONCURRENT_REQUESTS > 1`` (N pops before any ack no
    longer overwrite a single slot).

    Args:
        queue_name: The name of the queue.
        timeout: Seconds to wait for an item (0 = non-blocking).

    Returns:
        A ``(item, token)`` tuple. ``item`` is ``None`` when the queue is
        empty; ``token`` is backend-specific (``None`` for atomic-pop
        backends, opaque to callers).

    Raises:
        QueueError: If the pop operation fails.
    """
    return (self.pop(queue_name, timeout), None)

  def ack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Acknowledge a popped message for ``queue_name``.

    Atomic backends (Redis, MongoDB, ElasticSearch) implement this as a
    no-op: their pop is already atomic, so there is no "unacked" state to
    transition. Deferred-ack backends (Kafka, RabbitMQ, RocketMQ, Pulsar,
    SQS) override to commit the offset / basic_ack / consumer-ack the
    delivery.

    When ``token`` is provided (the scheduler always provides it for
    message-queue backends), the override acks the *specific* message
    identified by that token — correct under ``CONCURRENT_REQUESTS > 1``.
    When ``token`` is ``None`` (atomic backends, or legacy single-pop
    callers), overrides fall back to acking the last-popped message.

    The default no-op makes ack() safe to call from the scheduler even
    when the backend doesn't need it.

    Args:
        queue_name: The name of the queue whose message should be
            acknowledged.
        token: Opaque ack token returned by :meth:`pop_with_ack`. When
            ``None``, overrides ack the last-popped message (legacy).
    """
    del queue_name, token

  def nack(self, queue_name: str, *, token: Any | None = None) -> None:
    """Negatively acknowledge a popped message for ``queue_name``.

    Atomic backends implement this as a no-op. Message-queue backends
    override to requeue / re-deliver the message for another consumer.

    Args:
        queue_name: The name of the queue whose message should be
            negatively acknowledged.
        token: Opaque ack token returned by :meth:`pop_with_ack`. When
            ``None``, overrides nack the last-popped message (legacy).
    """
    del queue_name, token


class SetBackend(ABC):
  """Interface for set operations.

  Backends that support set operations must implement this interface.
  """

  @abstractmethod
  def add(self, set_name: str, item: bytes) -> bool:
    """Add an item to a set.

    Args:
        set_name: The name of the set.
        item: The item to add (serialized bytes).

    Returns:
        True if the item was added, False if it already existed.
    """

  @abstractmethod
  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove an item from a set.

    Args:
        set_name: The name of the set.
        item: The item to remove.

    Returns:
        True if the item was removed, False if it didn't exist.
    """

  @abstractmethod
  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if an item is in a set.

    Args:
        set_name: The name of the set.
        item: The item to check.

    Returns:
        True if the item exists in the set.
    """

  @abstractmethod
  def set_len(self, set_name: str) -> int:
    """Get the number of items in a set.

    Args:
        set_name: The name of the set.

    Returns:
        The number of items in the set.
    """

  @abstractmethod
  def clear_set(self, set_name: str) -> None:
    """Clear all items from a set.

    Args:
        set_name: The name of the set.
    """


class StorageBackend(ABC):
  """Interface for storage operations.

  Backends that support storage operations must implement this interface.
  """

  @abstractmethod
  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with a key.

    Args:
        key: The storage key.
        data: The data to store (bytes).
        ttl: Optional time-to-live in seconds.
    """

  @abstractmethod
  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: The storage key.

    Returns:
        The stored data, or None if not found.
    """

  @abstractmethod
  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: The storage key.

    Returns:
        True if the key was deleted, False if it didn't exist.
    """

  @abstractmethod
  def exists(self, key: str) -> bool:
    """Check if a key exists.

    Args:
        key: The storage key.

    Returns:
        True if the key exists.
    """

  @abstractmethod
  def ttl(self, key: str) -> int | None:
    """Get the remaining time-to-live for a key.

    Args:
        key: The storage key.

    Returns:
        Non-negative seconds remaining, or None when the key is missing,
        has no TTL, or has already expired. A backend that cannot inspect
        remaining TTL (for example Memcached) may also return None for a live
        expiring key; callers must treat None as "no observable live TTL".
    """

  @abstractmethod
  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix. If None,
            clear all storage data.
    """
