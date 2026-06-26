"""Observability hook interface (Unit F — Tier-2).

Defines :class:`Monitor` — a structural protocol of no-op hook methods the
queue / dupefilter / pipeline components call at the seam points that matter
for operability (push, pop, dedup hit/miss, queue depth, store, error). A
:class:`NullMonitor` is the safe default so components emit telemetry without
crashing when no crawler / stats collector is wired.

This mirrors the strategy-ABC + factory pattern already used for
:class:`~scrapy_extension.dupefilter.filters.base.MembershipFilter` and
:class:`~scrapy_extension.queue.strategies.base.QueueStrategy` — a pluggable
interface with a no-op default that preserves prior behavior exactly.
"""

from __future__ import annotations

__all__ = [
  "DEFAULT_BACKPRESSURE_THRESHOLD",
  "Monitor",
  "NullMonitor",
]

#: Default depth at which ``on_queue_depth`` flips ``queue/backpressure`` on.
#:
#: Deliberately finite and modest. Operators alerted on ``queue/backpressure``
#: get a signal roughly when a worker has a minute of pending work at typical
#: pop rates; tune via ``ScrapyStatsMonitor(backpressure_threshold=...)``. The
#: architect's #1 operability gap was "no backpressure signal" — this default
#: makes the signal default-on without throttling (action is a later tier).
DEFAULT_BACKPRESSURE_THRESHOLD = 1_000


class Monitor:
  """Structural base class for observability hooks.

  Subclass and override the hooks you care about. The default
  implementation of every hook is a no-op, so a bare ``Monitor()``
  (or :class:`NullMonitor`) is always safe to call from any component.

  Why a concrete base class (not ``typing.Protocol``):

  - ``Protocol`` with ``runtime_checkable`` would let us duck-type, but
    the components hold a ``Monitor`` instance, not a class — they need a
    real object whose hook methods exist and are no-ops by default.
  - A base class gives us that default behavior, plus ``isinstance`` works
    for the "is this the null default?" checks tests rely on.

  Hooks (all no-ops by default):

  - ``on_push(queue_name, priority)`` — after a successful queue push.
  - ``on_pop(queue_name)`` — after a successful queue pop.
  - ``on_dedup_hit(key)`` — request fingerprint was already seen.
  - ``on_dedup_miss(key)`` — request fingerprint was newly recorded.
  - ``on_queue_depth(queue_name, depth)`` — current pending depth (gauge).
  - ``on_store(key)`` — after a successful storage write (pipeline lane).
  - ``on_filter_full()`` — membership filter at capacity; caller degrades.
  - ``on_error(operation, error)`` — an operation raised; record per-op.
  """

  def on_push(self, queue_name: str, priority: float) -> None:
    """Record a successful queue push.

    Args:
        queue_name: The queue the item was pushed to.
        priority: The push priority (higher = more urgent).
    """

  def on_pop(self, queue_name: str) -> None:
    """Record a successful queue pop.

    Args:
        queue_name: The queue the item was popped from.
    """

  def on_dedup_hit(self, key: str) -> None:
    """Record a dedup hit (request already seen).

    Args:
        key: The request fingerprint that was already present.
    """

  def on_dedup_miss(self, key: str) -> None:
    """Record a dedup miss (request newly recorded).

    Args:
        key: The request fingerprint that was newly added.
    """

  def on_queue_depth(self, queue_name: str, depth: int) -> None:
    """Record the current queue depth (a gauge, not a counter).

    Args:
        queue_name: The queue whose depth was sampled.
        depth: The number of pending items at sample time.
    """

  def on_store(self, key: str) -> None:
    """Record a successful storage write.

    Emitted by the item pipeline (another lane); defined here so the
    protocol is complete and the pipeline can drop in unchanged.

    Args:
        key: The storage key that was written.
    """

  def on_filter_full(self) -> None:
    """Record that the membership filter reported it is at capacity.

    Emitted by the dupefilter when a bounded-capacity filter (cuckoo)
    raises :class:`~scrapy_extension.dupefilter.filters.base.FilterFull` and
    the dupefilter degrades by treating the overflow request as not-seen.
    Lets a stats monitor count ``dupefilter/filter_full`` occurrences via the
    monitor contract — without the dupefilter reaching into its private
    stats attribute.
    """

  def on_error(self, operation: str, error: BaseException) -> None:
    """Record an operation error.

    Args:
        operation: The operation name (e.g. ``"push"``, ``"pop"``).
        error: The exception that was raised.
    """


class NullMonitor(Monitor):
  """No-op monitor — the safe default.

  Inherits every no-op hook from :class:`Monitor`. Exists as a named
  sentinel so components can distinguish "no monitor wired" from "a real
  monitor that happens to record nothing" via ``isinstance(m, NullMonitor)``.
  """
