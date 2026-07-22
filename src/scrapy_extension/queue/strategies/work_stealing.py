"""Work-stealing queue strategy — pop-side load balancing across workers (subsystem ②).

In multi-worker Scrapy deployments, push-side fairness (RoundRobin) doesn't
help when arrival is uneven — workers that finish early sit idle while peers
have backlog. This strategy implements **pop-side load balancing**: each
worker owns a stable, backend-portable physical queue; ``push`` writes to
the local worker's queue; ``pop`` checks own first, then **steals** from peer
queues (round-robin, short timeout each) when own is empty.

Worker IDs are explicit in v1 (``worker_id`` + ``peer_ids`` config). A future
v2 may use a StorageBackend heartbeat registry for dynamic peer discovery.

``pop`` semantics:
1. Non-blocking check of own queue (``timeout=0``). Hit → return.
2. Round-robin steal across ``peer_ids``. Each probe is capped by
   ``steal_timeout`` and the caller's remaining total timeout; ``timeout=0``
   keeps every probe non-blocking. Hit → return + advance ``_steal_idx`` past
   the stolen peer.
3. If all empty and budget remains: one blocking pop on the own queue using
   only that remaining budget.

All payload state lives backend-side. The in-process steal cursor is only a
fairness hint, so resetting it on restart cannot lose or duplicate a message;
``snapshot`` remains ``None`` and ``restore`` remains a no-op.
"""

from __future__ import annotations

__all__ = [
  "DEFAULT_STEAL_TIMEOUT",
  "MAX_STEAL_PEERS",
  "WorkStealingQueueStrategy",
]

import logging
import threading
import time
import uuid
from typing import TYPE_CHECKING

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.queue.strategies._names import (
  ensure_fanout_backend_supported,
  physical_strategy_queue_name,
)
from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  normalize_queue_timeout,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

#: Default per-peer steal-pop timeout (50ms — cheap probe).
DEFAULT_STEAL_TIMEOUT: float = 0.05
#: Hard cap on peer fan-out (every pop/depth query performs per-peer RPCs).
MAX_STEAL_PEERS: int = 256


class WorkStealingQueueStrategy(QueueStrategy):
  """Pop-side load balancing via cross-queue stealing.

  Attributes:
      _worker_id: Own worker identifier (own queue suffix).
      _peer_ids: Peer worker IDs to steal from when own queue is empty.
      _steal_timeout: Per-peer pop timeout during the steal phase.
      _steal_idx: Round-robin cursor into ``_peer_ids``.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    *,
    worker_id: str | None = None,
    peer_ids: tuple[str, ...] = (),
    steal_timeout: float = DEFAULT_STEAL_TIMEOUT,
  ) -> None:
    """Initialize the work-stealing strategy.

    Args:
        connection_manager: Connection manager providing the QueueBackend.
        worker_id: Own worker identifier. ``None`` → auto-generated UUID4 hex
            (one fresh ID per strategy instance).
        peer_ids: Peer worker IDs to steal from. Empty tuple = no stealing
            (strategy degenerates to per-worker queue with blocking fallback).
        steal_timeout: Per-peer pop timeout during the steal phase (default
            50ms). Keeps steal probes cheap; raise for slow backends.

    Raises:
        ValueError: If worker/peer IDs are invalid, the peer count is too high,
            or ``steal_timeout`` is not finite and non-negative.
    """
    super().__init__(connection_manager)
    ensure_fanout_backend_supported(connection_manager, strategy="work_stealing")
    try:
      normalized_steal_timeout = normalize_queue_timeout(steal_timeout)
    except ValueError as e:
      raise ValueError(
        f"steal_timeout must be finite and >= 0, got {steal_timeout!r}"
      ) from e
    if worker_id is not None and (not isinstance(worker_id, str) or not worker_id):
      raise ValueError(
        f"worker_id must be a non-empty string or None, got {worker_id!r}"
      )
    if worker_id is not None:
      _validate_key_name(worker_id, "worker_id")
    self._worker_id = worker_id if worker_id is not None else uuid.uuid4().hex
    if worker_id is None:
      # #31: a restart without a stable SCRAPY_QUEUE_WORKER_ID generates a NEW
      # id → new own-queue name → the previous own-queue's items are stranded
      # (queue_len reports 0, no cleanup). Warn so operators set a sticky id.
      logger.warning(
        "WorkStealingQueueStrategy auto-generated worker_id %r. A restart "
        "without a stable SCRAPY_QUEUE_WORKER_ID strands the previous "
        "own-queue's items. Set SCRAPY_QUEUE_WORKER_ID for production "
        "multi-worker deployments.",
        self._worker_id,
      )
    if isinstance(peer_ids, (str, bytes)):
      raise ValueError("peer_ids must be an iterable of non-empty strings")
    normalized_peers: list[str] = []
    seen_peers = {self._worker_id}
    for peer in peer_ids:
      if not isinstance(peer, str) or not peer:
        raise ValueError(f"peer_ids must contain only non-empty strings, got {peer!r}")
      _validate_key_name(peer, "peer_id")
      if peer in seen_peers:
        continue
      seen_peers.add(peer)
      normalized_peers.append(peer)
      if len(normalized_peers) > MAX_STEAL_PEERS:
        raise ValueError(f"peer_ids must contain at most {MAX_STEAL_PEERS} peers")
    self._peer_ids = tuple(normalized_peers)
    self._steal_timeout = normalized_steal_timeout
    self._steal_idx = 0
    self._steal_lock = threading.Lock()

  def _own_queue(self, queue_name: str) -> str:
    return self._worker_queue(queue_name, self._worker_id)

  def _worker_queue(self, queue_name: str, worker_id: str) -> str:
    """Return the stable, backlog-compatible name for one worker's queue."""
    return physical_strategy_queue_name(
      self._connection_manager,
      queue_name=queue_name,
      namespace="worker",
      discriminator=worker_id,
      legacy_name=f"{queue_name}:{worker_id}",
    )

  @staticmethod
  def _remaining_timeout(deadline: float | None) -> float:
    if deadline is None:
      return 0.0
    return max(0.0, deadline - time.monotonic())

  def is_push_durable(self, *, delay: float, source: str) -> bool:
    """Report that each worker queue is backed by durable queue storage."""
    del delay, source
    return True

  def push(
    self,
    queue_name: str,
    item: bytes,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> None:
    """Push to the local worker's own queue.

    Args:
        queue_name: The logical queue name.
        item: Serialized item bytes.
        priority: Priority passed through to the backend.
        delay: Ignored (work-stealing is not a delay queue).
        source: Ignored (work-stealing routes by worker_id, not source).
    """
    del delay, source
    self._connection_manager.get_queue_backend().push(
      self._own_queue(queue_name), item, priority
    )

  def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
    """Own queue first; steal from peers round-robin; final blocking fallback.

    Args:
        queue_name: The logical queue name.
        timeout: Seconds to block on own queue if both own and all peers empty.

    Returns:
        The next item (own or stolen), or None if everything empty.
    """
    timeout = normalize_queue_timeout(timeout)
    qb = self._connection_manager.get_queue_backend()
    own = self._own_queue(queue_name)
    deadline = time.monotonic() + timeout if timeout > 0 else None

    # 1. Non-blocking check of own queue.
    item = qb.pop(own, 0.0)
    if item is not None:
      return item

    # 2. Round-robin steal from peers.
    n_peers = len(self._peer_ids)
    if n_peers:
      with self._steal_lock:
        for i in range(n_peers):
          idx = (self._steal_idx + i) % n_peers
          peer = self._peer_ids[idx]
          remaining = self._remaining_timeout(deadline)
          peer_timeout = min(self._steal_timeout, remaining)
          item = qb.pop(self._worker_queue(queue_name, peer), peer_timeout)
          if item is not None:
            # Advance cursor PAST the peer we stole from, so the next steal
            # round starts at the next peer (fair round-robin).
            self._steal_idx = (idx + 1) % n_peers
            return item
      # All peers empty — fall through.

    # 3. Blocking fallback on own queue honoring caller's wait contract.
    remaining = self._remaining_timeout(deadline)
    if remaining > 0:
      return qb.pop(own, remaining)
    return None

  def pop_with_ack(
    self, queue_name: str, timeout: float = 0.0
  ) -> tuple[bytes | None, object | None]:
    """Own queue first, steal from peers, then blocking fallback -- via
    ``pop_with_ack`` so the MQ per-message ack token threads through (#28).
    Mirrors ``pop`` but returns ``(data, token)``.
    """
    timeout = normalize_queue_timeout(timeout)
    qb = self._connection_manager.get_queue_backend()
    own = self._own_queue(queue_name)
    deadline = time.monotonic() + timeout if timeout > 0 else None
    data, token = self._pop_backend_instance_with_ack(qb, own, 0.0)
    if data is not None:
      return (data, token)
    n_peers = len(self._peer_ids)
    if n_peers:
      with self._steal_lock:
        for i in range(n_peers):
          idx = (self._steal_idx + i) % n_peers
          peer = self._peer_ids[idx]
          remaining = self._remaining_timeout(deadline)
          peer_timeout = min(self._steal_timeout, remaining)
          data, token = self._pop_backend_instance_with_ack(
            qb,
            self._worker_queue(queue_name, peer),
            peer_timeout,
          )
          if data is not None:
            self._steal_idx = (idx + 1) % n_peers
            return (data, token)
    remaining = self._remaining_timeout(deadline)
    if remaining > 0:
      return self._pop_backend_instance_with_ack(qb, own, remaining)
    return (None, None)

  def queue_len(self, queue_name: str) -> int:
    """Return backlog across every queue this worker is able to consume."""
    qb = self._connection_manager.get_queue_backend()
    return sum(
      qb.queue_len(self._worker_queue(queue_name, worker_id))
      for worker_id in (self._worker_id, *self._peer_ids)
    )

  def clear(self, queue_name: str) -> None:
    """Clear own queue only (peer queues belong to other workers)."""
    self._connection_manager.get_queue_backend().clear_queue(
      self._own_queue(queue_name)
    )
