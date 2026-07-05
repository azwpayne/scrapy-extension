"""Work-stealing queue strategy — pop-side load balancing across workers (subsystem ②).

In multi-worker Scrapy deployments, push-side fairness (RoundRobin) doesn't
help when arrival is uneven — workers that finish early sit idle while peers
have backlog. This strategy implements **pop-side load balancing**: each
worker owns a primary queue ``<queue_name>:<worker_id>``; ``push`` writes to
the local worker's queue; ``pop`` checks own first, then **steals** from peer
queues (round-robin, short timeout each) when own is empty.

Worker IDs are explicit in v1 (``worker_id`` + ``peer_ids`` config). A future
v2 may use a StorageBackend heartbeat registry for dynamic peer discovery.

``pop`` semantics:
1. Non-blocking check of own queue (``timeout=0``). Hit → return.
2. Round-robin steal across ``peer_ids``, each with ``steal_timeout`` (default
   50ms — cheap enough to probe many peers without blocking long). Hit →
   return + advance ``_steal_idx`` past the stolen peer.
3. If all empty AND caller ``timeout > 0``: one blocking ``pop(own, timeout)``
   honoring the caller's wait contract.

All state lives backend-side (no in-process holding); ``snapshot`` returns
``None`` and ``restore`` is a no-op (ABC defaults).
"""

from __future__ import annotations

__all__ = ["DEFAULT_STEAL_TIMEOUT", "WorkStealingQueueStrategy"]

import uuid
from typing import TYPE_CHECKING

from scrapy_extension.queue.strategies.base import QueueStrategy

if TYPE_CHECKING:
  from scrapy_extension.backends.connectors import ConnectionManager

#: Default per-peer steal-pop timeout (50ms — cheap probe).
DEFAULT_STEAL_TIMEOUT: float = 0.05


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
        ValueError: If ``steal_timeout < 0``.
    """
    super().__init__(connection_manager)
    if steal_timeout < 0:
      raise ValueError(f"steal_timeout must be >= 0, got {steal_timeout}")
    self._worker_id = worker_id if worker_id is not None else uuid.uuid4().hex
    self._peer_ids = tuple(peer_ids)
    self._steal_timeout = steal_timeout
    self._steal_idx = 0

  def _own_queue(self, queue_name: str) -> str:
    return f"{queue_name}:{self._worker_id}"

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
    qb = self._connection_manager.get_queue_backend()
    own = self._own_queue(queue_name)

    # 1. Non-blocking check of own queue.
    item = qb.pop(own, 0.0)
    if item is not None:
      return item

    # 2. Round-robin steal from peers.
    n_peers = len(self._peer_ids)
    if n_peers:
      for i in range(n_peers):
        idx = (self._steal_idx + i) % n_peers
        peer = self._peer_ids[idx]
        item = qb.pop(f"{queue_name}:{peer}", self._steal_timeout)
        if item is not None:
          # Advance cursor PAST the peer we stole from, so the next steal
          # round starts at the next peer (fair round-robin).
          self._steal_idx = (idx + 1) % n_peers
          return item
      # All peers empty — fall through.

    # 3. Blocking fallback on own queue honoring caller's wait contract.
    if timeout > 0:
      return qb.pop(own, timeout)
    return None

  def queue_len(self, queue_name: str) -> int:
    """Own queue length only (peer queues belong to other workers)."""
    return self._connection_manager.get_queue_backend().queue_len(
      self._own_queue(queue_name)
    )

  def clear(self, queue_name: str) -> None:
    """Clear own queue only (peer queues belong to other workers)."""
    self._connection_manager.get_queue_backend().clear_queue(
      self._own_queue(queue_name)
    )
