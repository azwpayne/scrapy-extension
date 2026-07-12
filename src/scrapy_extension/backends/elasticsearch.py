"""ElasticSearch backend implementation."""

from __future__ import annotations

import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

try:
    from elasticsearch import (
        ConflictError,
        Elasticsearch,
        NotFoundError,
        RequestError,
        TransportError,
    )
except ImportError as e:
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
    secret_value,
)
from scrapy_extension.exceptions import BackendConnectionError, QueueError, StorageError
from scrapy_extension.settings.elasticsearch import ElasticSearchMode

if TYPE_CHECKING:
  from scrapy_extension.settings.elasticsearch import ElasticSearchSettings

logger = logging.getLogger(__name__)


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
      kwargs["api_key"] = _redact(secret_value(self.config.api_key))
    elif self.config.username and self.config.password:
      kwargs["basic_auth"] = (
        self.config.username,
        _redact(secret_value(self.config.password)),
      )
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
    if self._client is None:
      msg = "ElasticSearchBackend not connected: client is None"
      raise BackendConnectionError(msg, backend_type="elasticsearch")
    for name in (
      self.config.queue_index,
      self.config.set_index,
      self.config.storage_index,
    ):
      try:
        self._client.indices.create(index=name)
      except RequestError as e:
        # HTTP 400 resource_already_exists_exception = idempotent success
        # (index created by a prior connect or a peer worker). Anything else
        # is a real config error — re-raise so it surfaces.
        if "resource_already_exists" not in str(e).lower():
          raise

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

    Raises:
        BackendConnectionError: If the client cannot be initialized.
    """
    if self._client is None:
      self.connect()
    if self._client is None:
      msg = "ElasticSearchBackend not connected: client is None after connect()"
      raise BackendConnectionError(msg, backend_type="elasticsearch")
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
      # No ``refresh`` on write — read-your-writes is enforced at the READ
      # side (pop/count call ``indices.refresh`` first), which amortizes one
      # forced refresh per read instead of ~1s per push. The per-push
      # ``refresh="wait_for"`` was a ~250x perf regression (1010ms/push vs
      # 4ms without — see bench_es_push_refresh.py); ES does not batch
      # ``wait_for`` across consecutive pushes, so each one pays the full
      # refresh-interval wait.
      self.client.index(index=self.config.queue_index, document=doc)
    except TransportError as e:
      raise QueueError(str(e), queue_name=queue_name, operation="push") from e

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
    for _attempt in range(max_attempts):
      try:
        # Force one refresh before searching so recent pushes AND deletes
        # from prior pops are visible. Forced ``indices.refresh`` is ms-scale
        # (just flushes the indexing buffer to a segment) — far cheaper than
        # the per-push ``refresh="wait_for"`` it replaces (which blocked ~1s
        # per push). Amortized: N fast pushes + 1 refresh per read.
        self.client.indices.refresh(index=self.config.queue_index)
        resp = self.client.search(
          index=self.config.queue_index,
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
        )
        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
          return None
        doc = hits[0]
        try:
          # No ``refresh`` on delete — the NEXT pop's pre-search refresh
          # (above) flushes this delete, so the search won't re-find the doc.
          self.client.delete(
            index=self.config.queue_index,
            id=doc["_id"],
            if_seq_no=doc["_seq_no"],
            if_primary_term=doc["_primary_term"],
          )
        except ConflictError:
          # Lost the race to another worker — retry to find the next item.
          continue
        return _b64decode(doc["_source"]["item"])
      except NotFoundError:
        return None
      except TransportError as e:
        raise QueueError(str(e), queue_name=queue_name, operation="pop") from e
    return None

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
      # No ``refresh`` on write — ``contains`` is by-id (immediately
      # consistent); ``set_len`` refreshes in ``_count``. Same amortized
      # read-refresh rationale as push.
      self.client.index(
        index=self.config.set_index,
        id=doc_id,
        document=doc,
        op_type="create",
      )
    except ConflictError:
      return False
    except RequestError as e:
      if "version_conflict" in str(e).lower():
        return False
      raise
    except TransportError as e:
      # R-dupe-1 (option b): wrap transient TransportError so BackendDupeFilter's
      # graceful-degradation arm catches it (degrade to not-seen) instead of
      # crashing the crawl. ConflictError + version-conflict RequestError (the
      # "already existed" signals) stay first. Non-conflict RequestError still
      # re-raises (contract error, not transient). Supersedes R31-A1.
      raise BackendConnectionError(
        f"ElasticSearch set add failed for {set_name!r}: {e}",
        backend_type="elasticsearch",
      ) from e
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
    response = self.client.exists(
      index=self.config.set_index, id=self._set_doc_id(set_name, item)
    )
    return bool(response)

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
    try:
      self.client.index(index=self.config.storage_index, id=key, document=doc)
    except TransportError as e:
      msg = f"Failed to store key {key!r} in ElasticSearch: {e}"
      raise StorageError(msg, operation="store", key=key) from e

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
    except TransportError as e:
      msg = f"Failed to retrieve key {key!r} from ElasticSearch: {e}"
      raise StorageError(msg, operation="retrieve", key=key) from e
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
    try:
      return self._delete_by_id(self.config.storage_index, key)
    except TransportError as e:
      msg = f"Failed to delete key {key!r} from ElasticSearch: {e}"
      raise StorageError(msg, operation="delete", key=key) from e

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
    response = self.client.exists(index=self.config.storage_index, id=key)
    return bool(response)

  def ttl(self, key: str) -> int | None:
    """Get remaining time-to-live.

    Args:
        key: Storage key.

    Returns:
        Seconds remaining, None if no TTL or key is absent, -1 if expired.

    Note:
        A missing key returns None (not -1) so callers can distinguish
        "doesn't exist" from "expired" — matching the R5 contract fix on
        Redis and MongoDB. Pre-R48 this conflated the two via -1.
    """
    try:
      resp = self.client.get(index=self.config.storage_index, id=key)
    except NotFoundError:
      return None
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
      # Forced refresh so just-written docs (push/add don't refresh) are
      # searchable — same amortized-read-refresh rationale as pop.
      self.client.indices.refresh(index=index)
      # ``.keyword`` subfield — see pop's term-query note. ``queue_name`` /
      # ``set_name`` are dynamically mapped as ``text``; count must match the
      # exact (unanalyzed) value via the keyword subfield.
      resp = self.client.count(
        index=index, query={"term": {f"{field}.keyword": value}}
      )
    except TransportError:
      return 0
    else:
      return cast(int, resp.get("count", 0))

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
    return True

  def _delete_by_term(self, index: str, field: str, value: str) -> None:
    """Delete all documents matching a term query.

    Args:
        index: Index name.
        field: Field to match.
        value: Value to match.
    """
    # ``.keyword`` subfield — same exact-match rationale as ``_count``.
    self._delete_by_query(index, {"term": {f"{field}.keyword": value}})

  def _delete_by_query(self, index: str, query: dict[str, Any]) -> None:
    """Delete all documents matching a query.

    Args:
        index: Index name.
        query: Query dict.
    """
    try:
      self.client.delete_by_query(index=index, query=query)
    except TransportError as e:
      logger.warning("Failed to delete from %s: %s", index, e)
