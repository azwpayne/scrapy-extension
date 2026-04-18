"""Base backend definitions and abstract interfaces.

This module defines the abstract base classes and interfaces that all
backend implementations must follow.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Protocol


class Serializer(Protocol):
  """Protocol for serializers.

  Any class implementing this protocol can be used for serializing
  and deserializing data for backend storage.
  """

  def serialize(self, obj: Any) -> bytes:
    """Serialize an object to bytes.

    Args:
        obj: The object to serialize.

    Returns:
        The serialized bytes.
    """
    ...

  def deserialize(self, data: bytes) -> Any:
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

  def serialize(self, obj: Any) -> bytes:
    """Serialize an object to JSON bytes.

    Args:
        obj: The object to serialize.

    Returns:
        JSON-encoded bytes.
    """
    return json.dumps(obj, default=str).encode("utf-8")

  def deserialize(self, data: bytes) -> Any:
    """Deserialize JSON bytes to an object.

    Args:
        data: The JSON bytes to deserialize.

    Returns:
        The deserialized object.
    """
    return json.loads(data.decode("utf-8"))


class BackendType(str, Enum):
  """Supported backend types for distributed crawling.

  Attributes:
      REDIS: Redis backend for distributed crawling.
      MONGODB: MongoDB backend for distributed crawling.
      KAFKA: Kafka backend for distributed crawling.
      RABBITMQ: RabbitMQ backend for distributed crawling.
      ELASTICSEARCH: ElasticSearch backend for distributed crawling.
      ROCKETMQ: RocketMQ backend for distributed crawling.
  """

  REDIS = "redis"
  MONGODB = "mongodb"
  KAFKA = "kafka"
  RABBITMQ = "rabbitmq"
  ELASTICSEARCH = "elasticsearch"
  ROCKETMQ = "rocketmq"


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
        prefix:
        - If provided, only clear keys starting with this prefix.
        - If None, clear all storage data.
    """
