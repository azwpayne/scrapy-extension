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
  "DEFAULT_POP_RATE_WINDOW_S",
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

#: Default rolling window (seconds) over which ``on_pop_rate`` reports rate.
#:
#: 60s matches the architect's "calls/sec over a 1m window" contract (U2):
#: long enough to smooth per-second jitter, short enough that a stalled
#: consumer surfaces as a falling-edge within a minute. ``BackendQueue``
#: evicts timestamps older than this from its rolling counter on every pop;
#: the value is also passed as ``window_s`` to ``on_pop_rate`` so a monitor
#: can record it under a window-tagged stat key (``queue/pop_rate_1m``).
DEFAULT_POP_RATE_WINDOW_S = 60.0


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
  - ``on_pop(queue_name)`` — per pop ATTEMPT (R14-D semantics fix).
    Emitted by ``BackendQueue.pop`` on every call — including empty pops —
    because the consumer-liveness signal ("is the worker popping at all?")
    is independent of whether an item was returned. The matching stat is
    ``queue/pop_attempt_count`` (renamed from ``queue/pop_count`` in R14-D so
    the key matches behavior).
  - ``on_dedup_hit(key)`` — request fingerprint was already seen.
  - ``on_dedup_miss(key)`` — request fingerprint was newly recorded.
  - ``on_queue_depth(queue_name, depth)`` — current pending depth (gauge).
  - ``on_store(key)`` — after a successful storage write (pipeline lane).
  - ``on_filter_full()`` — membership filter at capacity; caller degrades.
  - ``on_pop_rate(window_s, rate)`` — rolling pop rate (U2 operability).
    Emitted by ``BackendQueue.pop`` on a sampling cadence (NOT every pop);
    ``rate`` is pops per second over the trailing ``window_s`` window.
  - ``on_filter_saturation(used, capacity)`` — membership-filter fill ratio
    (U2 operability). Emitted by ``BackendDupeFilter.request_seen`` when the
    underlying filter exposes a ``saturation`` property (cuckoo + bloom), and
    by ``MemoryMembershipFilter`` directly at LRU-eviction time. Lets operators
    see a filter APPROACHING full (e.g. >0.9) before the ``on_filter_full``
    overflow signal fires.
  - ``on_error(operation, error)`` — an operation raised; record per-op.
    Wired (R14-D) at the ``BackendQueue`` push-except and deserialize-fail
    arms so serialization failures surface as ``errors/push`` /
    ``errors/pop`` instead of being dead observability.
  - ``on_connect(backend_type)`` — a backend connection was established.
    Wired (R14-D) from ``ConnectionManager.connect`` on the success path.
  - ``on_disconnect(backend_type, reason)`` — a backend was disconnected.
    Wired (R14-D) from ``ConnectionManager.close``; ``reason`` is the Scrapy
    close reason (may be ``None`` in non-engine teardown paths).
  - ``on_retry(backend_type, attempt)`` — a connection retry fired.
    Wired (R14-D) from ``ConnectionManager.connect`` before each backoff
    sleep; ``attempt`` is 1-based (1 = first retry).
  """

  def on_push(self, queue_name: str, priority: float) -> None:
    """Record a successful queue push.

    Args:
        queue_name: The queue the item was pushed to.
        priority: The push priority (higher = more urgent).
    """

  def on_pop(self, queue_name: str) -> None:
    """Record a pop ATTEMPT (R14-D semantics fix — per attempt, not per success).

    Emitted by :meth:`BackendQueue.pop
    <scrapy_extension.queue.queue.BackendQueue.pop>` on EVERY pop call —
    including empty pops. The consumer-liveness signal ("is the worker
    popping at all?") is independent of whether an item was returned, so the
    matching stat is ``queue/pop_attempt_count`` (renamed from
    ``queue/pop_count`` in R14-D so the key matches the per-attempt
    behavior). For per-success push accounting see :meth:`on_push`.

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

  def on_pop_rate(self, window_s: float, rate: float) -> None:
    """Record the rolling queue pop rate (U2 operability signal).

    Emitted by :meth:`BackendQueue.pop
    <scrapy_extension.queue.queue.BackendQueue.pop>` on a sampling cadence
    (NOT every pop — derived alongside the depth sample to keep the hot path
    cheap). ``rate`` is pops per second over the trailing ``window_s``
    seconds. The default window is :data:`DEFAULT_POP_RATE_WINDOW_S` (60s).

    Why a rate, not a counter: ``queue/pop_count`` already counts pops; the
    operability question is "is the consumer alive *lately*?" — a rolling
    rate answers that without forcing the operator to do wall-clock math
    against a monotonic counter. A stalled crawler shows up as a falling-edge
    to ~0 within one window.

    Args:
        window_s: The trailing window the rate was computed over (seconds).
        rate: Pops per second over that window.
    """

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    """Record membership-filter saturation (U2 operability signal).

    Emitted by :meth:`BackendDupeFilter.request_seen
    <scrapy_extension.dupefilter.dupefilter.BackendDupeFilter.request_seen>`
    after each add when the underlying filter exposes a ``saturation``
    property (currently only the cuckoo filter — set/memory/bloom do not
    surface capacity and stay silent). This is the APPROACHING-full signal:
    it rises through 0.9 before :meth:`on_filter_full` ever fires, giving
    operators a leading indicator to raise ``SCRAPY_DEDUP_CUCKOO_CAPACITY``
    before the filter overflows and degrades to passthrough.

    Args:
        used: Number of items currently recorded in the filter.
        capacity: Filter capacity in items, or ``None`` if the filter is
            unbounded (in which case saturation is reported as ``0.0`` —
            an unbounded filter cannot be saturated).
    """

  def on_error(self, operation: str, error: BaseException) -> None:
    """Record an operation error.

    Wired (R14-D) at the ``BackendQueue`` push-except and deserialize-fail
    arms so serialization failures surface as ``errors/push`` /
    ``errors/pop`` instead of being dead observability (the hook previously
    had zero call sites).

    Args:
        operation: The operation name (e.g. ``"push"``, ``"pop"``).
        error: The exception that was raised.
    """

  def on_connect(self, backend_type: str) -> None:
    """Record a successful backend connection (R14-D connection-lifecycle hook).

    Wired from :meth:`ConnectionManager.connect
    <scrapy_extension.backends.connectors.ConnectionManager.connect>` on the
    success path. ``backend_type`` is the registry-key string (e.g.
    ``"redis"``, ``"mongodb"``). Default no-op so existing subclasses and
    :class:`NullMonitor` keep working unchanged.

    Args:
        backend_type: The backend-type registry string that connected.
    """

  def on_disconnect(self, backend_type: str, reason: str | None) -> None:
    """Record a backend disconnect (R14-D connection-lifecycle hook).

    Wired from :meth:`ConnectionManager.close
    <scrapy_extension.backends.connectors.ConnectionManager.close>` when the
    last holder releases the shared manager. ``reason`` is the Scrapy
    engine close reason (may be ``None`` in non-engine teardown paths — e.g.
    orphan-eviction under registry pressure). Default no-op so existing
    subclasses and :class:`NullMonitor` keep working unchanged.

    Args:
        backend_type: The backend-type registry string that disconnected.
        reason: Scrapy engine close reason (or ``None``).
    """

  def on_retry(self, backend_type: str, attempt: int) -> None:
    """Record a connection retry (R14-D connection-lifecycle hook).

    Wired from :meth:`ConnectionManager.connect
    <scrapy_extension.backends.connectors.ConnectionManager.connect>` before
    each exponential-backoff sleep. ``attempt`` is 1-based (1 = first retry,
    i.e. the second overall attempt). Default no-op so existing subclasses
    and :class:`NullMonitor` keep working unchanged.

    Args:
        backend_type: The backend-type registry string being retried.
        attempt: 1-based retry index (1 = first retry).
    """


class NullMonitor(Monitor):
  """No-op monitor — the safe default.

  Inherits every no-op hook from :class:`Monitor`. Exists as a named
  sentinel so components can distinguish "no monitor wired" from "a real
  monitor that happens to record nothing" via ``isinstance(m, NullMonitor)``.
  """
