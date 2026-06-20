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
import hashlib
import json
import re
import uuid
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Protocol

from pydantic import SecretStr


def _json_default(obj: object) -> object:
  """JSON default handler for types Scrapy request dicts commonly contain.

  Handles the types that appear in real-world ``request.meta``:
  - ``datetime`` / ``date`` → ISO 8601 string (round-trips via ``datetime.fromisoformat``)
  - ``bytes`` / ``bytearray`` → base64-encoded ASCII string
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
    return base64.b64encode(bytes(obj)).decode("ascii")
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
  raise TypeError(
    f"Object of type {type_name} is not JSON serializable. "
    f"Pre-serialize {type_name} instances before pushing to the queue, "
    f"or extend scrapy_extension.backends.base._json_default."
  )


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

    Uses ``_json_default`` to handle common non-JSON-native types found in
    Scrapy request dicts (datetime → ISO, bytes → base64). Truly unexpected
    types raise TypeError with a clear message — no silent ``str()`` coercion.

    Args:
        obj: The object to serialize.

    Returns:
        JSON-encoded bytes.

    Raises:
        TypeError: If the object contains types not handled by _json_default.
    """
    return json.dumps(obj, default=_json_default).encode("utf-8")

  def deserialize(self, data: bytes) -> object:
    """Deserialize JSON bytes to an object.

    Args:
        data: The JSON bytes to deserialize.

    Returns:
        The deserialized object.
    """
    return json.loads(data.decode("utf-8"))


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

  @classmethod
  def _missing_(cls, value: object) -> BackendType | None:
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
  def backend_type(self) -> BackendType:
    """Return the backend type.

    Returns:
        The BackendType enum value for this backend.
    """


class QueueBackend(ABC):
  """Interface for queue operations.

  Backends that support queue operations must implement this interface.
  """

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

  def ack(self, queue_name: str) -> None:
    """Acknowledge the last-popped message for ``queue_name``.

    Atomic backends (Redis, MongoDB, ElasticSearch, RocketMQ) implement
    this as a no-op: their pop is already atomic, so there is no
    "unacked" state to transition. Message-queue backends (Kafka,
    RabbitMQ) override to commit the offset / basic_ack the delivery.

    The default no-op makes ack() safe to call from the scheduler even
    when the backend doesn't need it.

    Args:
        queue_name: The name of the queue whose last message should be
            acknowledged.
    """
    del queue_name

  def nack(self, queue_name: str) -> None:
    """Negatively acknowledge the last-popped message for ``queue_name``.

    Atomic backends implement this as a no-op. Message-queue backends
    override to requeue / re-deliver the message for another consumer.

    Args:
        queue_name: The name of the queue whose last message should be
            negatively acknowledged.
    """
    del queue_name


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
        Seconds remaining, None if no TTL, or -1 if expired.
    """

  @abstractmethod
  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix. If None,
            clear all storage data.
    """
