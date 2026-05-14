"""ElasticSearch backend implementation."""

from __future__ import annotations

import base64
import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

try:
    from elasticsearch import Elasticsearch, NotFoundError, RequestError, TransportError
except ImportError as e:
    raise ImportError(
        "ElasticSearch backend requires 'elasticsearch'. Install with: pip install scrapy-extension[elasticsearch]"
    ) from e

from scrapy_extension.backends.base import (
  Backend,
  BackendType,
  QueueBackend,
  SetBackend,
  StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError
from scrapy_extension.settings.elasticsearch import ElasticSearchMode

if TYPE_CHECKING:
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

logger = logging.getLogger(__name__)

# Key name validation pattern
_KEY_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]+$")


def _validate_key_name(name: str, field_name: str = "name") -> None:
    """Validate key/queue/index name to prevent injection.

    Args:
        name: The name to validate.
        field_name: Field name for error messages.

    Raises:
        ValueError: If name contains invalid characters.
    """
    if not name or not _KEY_NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid {field_name}: {name!r}. "
            f"Only alphanumeric, dots, underscores, hyphens, and colons allowed."
        )


def _b64encode(data: bytes) -> str:
  return base64.b64encode(data).decode("ascii")


def _b64decode(data: str) -> bytes:
  return base64.b64decode(data.encode("ascii"))


class ElasticSearchBackend(Backend, QueueBackend, SetBackend, StorageBackend):
  """ElasticSearch backend: Queue (sorted docs), Set (unique _id), Storage (key-value with TTL)."""

  def __init__(self, config: ElasticSearchSettings) -> None:
    """Initialize ElasticSearch backend.

    Args:
        config: Configuration for ElasticSearch connection.
    """
    self.config = config
    self._client: Elasticsearch | None = None

  def _build_kwargs(self) -> dict[str, Any]:
    """Build common ElasticSearch client kwargs.

    Returns:
        Dictionary of client configuration options.
    """
    kwargs: dict[str, Any] = {
      "request_timeout": self.config.request_timeout,
      "max_retries": self.config.max_retries,
      "retry_on_timeout": self.config.retry_on_timeout,
    }
    if self.config.api_key:
      kwargs["api_key"] = self.config.api_key
    elif self.config.username and self.config.password:
      kwargs["basic_auth"] = (self.config.username, self.config.password)
    return kwargs

  def connect(self) -> None:
    """Establish connection to ElasticSearch.

    Raises:
        BackendConnectionError: If the connection cannot be established.
    """
    try:
      kwargs = self._build_kwargs()
      if self.config.mode == ElasticSearchMode.CLOUD:
        if not self.config.cloud_id:
          msg = "Cloud mode requires 'cloud_id'"
          raise BackendConnectionError(msg, backend_type="elasticsearch")
        kwargs["cloud_id"] = self.config.cloud_id
      else:
        kwargs["hosts"] = self.config.hosts
        kwargs["verify_certs"] = self.config.verify_certs
        if self.config.ca_certs:
          kwargs["ca_certs"] = self.config.ca_certs
      self._client = Elasticsearch(**kwargs)
      self._client.ping()
      self._ensure_indices()
      logger.debug("Connected to ElasticSearch in %s mode", self.config.mode.value)
    except TransportError as e:
      msg = f"Failed to connect to ElasticSearch ({self.config.mode.value}): {e}"
      raise BackendConnectionError(msg, backend_type="elasticsearch") from e

  def _ensure_indices(self) -> None:
    """Create indices if they don't exist."""
    assert self._client is not None
    for name in (
      self.config.queue_index,
      self.config.set_index,
      self.config.storage_index,
    ):
      if not self._client.indices.exists(index=name):
        self._client.indices.create(index=name)

  def disconnect(self) -> None:
    """Close ElasticSearch connection."""
    if self._client:
      self._client.close()
      self._client = None

  def is_connected(self) -> bool:
    """Check if ElasticSearch is connected.

    Returns:
        True if connected and responding to ping.
    """
    try:
      return self._client is not None and self._client.ping()
    except TransportError:
      return False

  def ping(self) -> bool:
    """Check ElasticSearch health.

    Returns:
        True if ElasticSearch responds to ping.
    """
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
    """
    if self._client is None:
      self.connect()
    return self._client

  # ---- Queue ----

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
    doc = {
      "queue_name": queue_name,
      "item": _b64encode(item),
      "priority": -priority,
      "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
      self.client.index(index=self.config.queue_index, document=doc)
    except TransportError as e:
      raise QueueError(str(e), queue_name=queue_name, operation="push") from e

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:  # noqa: ARG002
    """Pop highest priority item from queue.

    Args:
        queue_name: Name of the queue.
        timeout: Seconds to wait (unused for ElasticSearch, blocking not supported).

    Returns:
        The popped item, or None if queue is empty.

    Raises:
        QueueError: If the pop operation fails.
        ValueError: If queue_name contains invalid characters.
    """
    _validate_key_name(queue_name, "queue_name")
    try:
      resp = self.client.search(
        index=self.config.queue_index,
        query={"term": {"queue_name": queue_name}},
        sort=[{"priority": "asc"}, {"created_at": "asc"}],
        size=1,
      )
      hits = resp.get("hits", {}).get("hits", [])
      if not hits:
        return None
      doc = hits[0]
      self.client.delete(index=self.config.queue_index, id=doc["_id"])
    except NotFoundError:
      return None
    except TransportError as e:
      raise QueueError(str(e), queue_name=queue_name, operation="pop") from e
    else:
      return _b64decode(doc["_source"]["item"])

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
      return self._count(self.config.queue_index, "queue_name", queue_name)
    except TransportError as e:
      raise QueueError(str(e), queue_name=queue_name, operation="queue_len") from e

  def clear_queue(self, queue_name: str) -> None:
    """Clear all items from queue.

    Args:
        queue_name: Name of the queue.
    """
    self._delete_by_term(self.config.queue_index, "queue_name", queue_name)

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
      self.client.index(
        index=self.config.set_index, id=doc_id, document=doc, op_type="create"
      )
    except RequestError as e:
      if "version_conflict" in str(e).lower():
        return False
      raise
    except TransportError:
      return False
    else:
      return True

  def remove(self, set_name: str, item: bytes) -> bool:
    """Remove item from set.

    Args:
        set_name: Name of the set.
        item: Item to remove.

    Returns:
        True if removed, False if didn't exist.
    """
    return self._delete_by_id(self.config.set_index, self._set_doc_id(set_name, item))

  def contains(self, set_name: str, item: bytes) -> bool:
    """Check if item is in set.

    Args:
        set_name: Name of the set.
        item: Item to check.

    Returns:
        True if item exists in the set.
    """
    try:
      response = self.client.exists(
        index=self.config.set_index, id=self._set_doc_id(set_name, item)
      )
      return bool(response)
    except TransportError:
      return False

  def set_len(self, set_name: str) -> int:
    """Get set size.

    Args:
        set_name: Name of the set.

    Returns:
        Number of items in the set.
    """
    return self._count(self.config.set_index, "set_name", set_name)

  def clear_set(self, set_name: str) -> None:
    """Clear all items from set.

    Args:
        set_name: Name of the set.
    """
    self._delete_by_term(self.config.set_index, "set_name", set_name)

  # ---- Storage ----

  def store(self, key: str, data: bytes, ttl: int | None = None) -> None:
    """Store data with key.

    Args:
        key: Storage key.
        data: Data to store (bytes).
        ttl: Optional time-to-live in seconds.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    doc: dict[str, Any] = {"key": key, "data": _b64encode(data)}
    if ttl is not None:
      doc["expireAt"] = (
        datetime.now(tz=timezone.utc) + timedelta(seconds=ttl)
      ).isoformat()
    self.client.index(index=self.config.storage_index, id=key, document=doc)

  def retrieve(self, key: str) -> bytes | None:
    """Retrieve data by key.

    Args:
        key: Storage key.

    Returns:
        Stored data, or None if not found.
    """
    try:
      resp = self.client.get(index=self.config.storage_index, id=key)
    except NotFoundError:
      return None
    except TransportError:
      return None
    else:
      return _b64decode(resp["_source"]["data"])

  def delete(self, key: str) -> bool:
    """Delete data by key.

    Args:
        key: Storage key.

    Returns:
        True if deleted, False if didn't exist.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    return self._delete_by_id(self.config.storage_index, key)

  def exists(self, key: str) -> bool:
    """Check if key exists.

    Args:
        key: Storage key.

    Returns:
        True if key exists.

    Raises:
        ValueError: If key contains invalid characters.
    """
    _validate_key_name(key, "key")
    try:
      response = self.client.exists(index=self.config.storage_index, id=key)
      return bool(response)
    except TransportError:
      return False

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining, None if no TTL, -1 if expired.
    """
    try:
      resp = self.client.get(index=self.config.storage_index, id=key)
    except NotFoundError:
      return -1
    except TransportError:
      return None
    else:
      expire_str = resp["_source"].get("expireAt")
      if not expire_str:
        return None
      remaining = (
        datetime.fromisoformat(expire_str) - datetime.now(tz=timezone.utc)
      ).total_seconds()
      return -1 if remaining <= 0 else max(0, int(remaining))

  def clear_storage(self, prefix: str | None = None) -> None:
    """Clear all stored data, optionally filtered by prefix.

    Args:
        prefix: If provided, only clear keys starting with this prefix.
               If None, clear all storage data.
    """
    query = {"prefix": {"key": prefix}} if prefix else {"match_all": {}}
    self._delete_by_query(self.config.storage_index, query)

  # ---- Shared helpers ----

  def _count(self, index: str, field: str, value: str) -> int:
    """Count documents matching a term query.

    Args:
        index: Index name.
        field: Field to match.
        value: Value to match.

    Returns:
        Number of matching documents.
    """
    try:
      resp = self.client.count(index=index, query={"term": {field: value}})
    except TransportError:
      return 0
    else:
      return resp.get("count", 0)

  def _delete_by_id(self, index: str, doc_id: str) -> bool:
    """Delete document by ID.

    Args:
        index: Index name.
        doc_id: Document ID.

    Returns:
        True if deleted, False if didn't exist.
    """
    try:
      self.client.delete(index=index, id=doc_id)
    except NotFoundError:
      return False
    except TransportError:
      return False
    else:
      return True

  def _delete_by_term(self, index: str, field: str, value: str) -> None:
    """Delete all documents matching a term query.

    Args:
        index: Index name.
        field: Field to match.
        value: Value to match.
    """
    self._delete_by_query(index, {"term": {field: value}})

  def _delete_by_query(self, index: str, query: dict) -> None:
    """Delete all documents matching a query.

    Args:
        index: Index name.
        query: Query dict.
    """
    try:
      self.client.delete_by_query(index=index, query=query)
    except TransportError as e:
      logger.warning("Failed to delete from %s: %s", index, e)
