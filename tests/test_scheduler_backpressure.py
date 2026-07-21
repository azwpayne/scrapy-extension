"""Unit BP-2: scheduler depth-driven backpressure gate.

Round-4 (B1): ``next_request`` slows consumption when queue depth reaches the
``pause_at`` threshold; while paused it makes one bounded progress pop every
two polls so a sole consumer can drain to ``resume_at`` (hysteresis). Depth
source is ``len(self._queue)`` (fresh, same source ``has_pending_requests``
trusts). Default-off when ``pause_at is None`` → byte-identical behavior to
the pre-fix path.

Mock-queue only — no real backend. Mirrors the pattern in
``test_scheduler_envelope.py``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from scrapy import Request, Spider

from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import (
  BackendConnectionError,
  QueueError,
  SerializationError,
)
from scrapy_extension.schedule.scheduler import BackendScheduler


def _stats_counter() -> tuple[dict[str, int], Any]:
  """Minimal stats-collector-like object so we can assert inc_value counts.

  Returns ``(counts_dict, stats_instance)`` — assert via the dict.
  """
  counts: dict[str, int] = {}

  class _Stats:
    def inc_value(self, key: str, count: int = 1, **_: Any) -> None:
      counts[key] = counts.get(key, 0) + count

    def get_value(self, key: str, default: int = 0) -> int:
      return counts.get(key, default)

  return counts, _Stats()


class _FakeSpider(Spider):
  name = "foo"

  def __init__(self) -> None:
    # Bypass Scrapy's Spider.__init__ (needs crawler context for type-checking
    # only). We set just what the scheduler reads.
    self.crawler = None  # type: ignore[assignment]


class _LenControllableQueue:
  """Mock queue whose ``__len__`` returns a settable value and whose ``pop``
  is a Mock.

  ``len(queue)`` is the depth source the gate reads (same source
  ``has_pending_requests`` trusts via ``len(self)``).
  """

  def __init__(self, depth: int = 0, pop_value: Request | None = None) -> None:
    self._depth = depth
    self.pop = MagicMock(name="pop", return_value=pop_value)
    self.push = MagicMock(name="push")
    self.ack = MagicMock(name="ack")
    self.nack = MagicMock(name="nack")
    self.close = MagicMock(name="close")

  def __len__(self) -> int:
    return self._depth

  def set_depth(self, depth: int) -> None:
    self._depth = depth


class _SelfDrainingQueue(_LenControllableQueue):
  """Queue whose only depth change comes from successful ``pop`` calls."""

  def __init__(self, depth: int) -> None:
    super().__init__(depth=depth)
    self.pop = MagicMock(name="pop", side_effect=self._pop)

  def _pop(self, timeout: float = 0.0) -> Request | None:
    del timeout
    if self._depth <= 0:
      return None
    request = Request(f"https://example.com/{self._depth}")
    self._depth -= 1
    return request


class TestBackpressureDefaultOff:
  """Test 1: pause_at=None → current behavior pinned (pop is called)."""

  def test_pop_called_when_pause_at_unset(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    _counts, stats = _stats_counter()
    scheduler = BackendScheduler(connection_manager=manager, stats=stats)
    req = Request("https://example.com/a")
    queue = _LenControllableQueue(depth=10, pop_value=req)
    scheduler._queue = queue  # type: ignore[assignment]

    result = scheduler.next_request()

    assert result is req
    queue.pop.assert_called_once()


class TestBackpressurePause:
  """Test 2: pause_at=10, len=10 → return None, pop NOT called, stat bumped."""

  def test_pause_returns_none_and_skips_pop(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    queue = _LenControllableQueue(depth=10, pop_value=Request("https://example.com/a"))
    scheduler._queue = queue  # type: ignore[assignment]

    result = scheduler.next_request()

    assert result is None
    queue.pop.assert_not_called()
    assert counts.get("scheduler/backpressure_pause") == 1


class TestBackpressureHysteresis:
  """Test 3: a paused sole consumer makes bounded progress to resume_at."""

  def test_paused_consumer_drains_to_resume_threshold_without_external_help(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    queue = _SelfDrainingQueue(depth=10)
    scheduler._queue = queue  # type: ignore[assignment]

    # The scheduler is the only consumer. Ten polls must deterministically
    # alternate five pauses with five progress pops, draining 10 -> 5 without
    # any test-side set_depth() escape hatch.
    results = [scheduler.next_request() for _ in range(10)]

    assert sum(request is not None for request in results) == 5
    assert len(queue) == 5
    assert queue.pop.call_count == 5
    assert scheduler._backpressure_paused is True
    assert counts.get("scheduler/backpressure_pause") == 1
    assert counts.get("scheduler/backpressure_resume") is None

    # The next bounded poll observes depth == resume_at, exits hysteresis, and
    # returns to the normal pop path.
    result = scheduler.next_request()

    assert result is not None
    assert len(queue) == 4
    assert queue.pop.call_count == 6
    assert scheduler._backpressure_paused is False
    assert counts.get("scheduler/backpressure_resume") == 1


class TestBackpressureFlapDefaultResume:
  """Test 4: pause_at=10 only (resume_at defaults to pause_at).

  With resume_at == pause_at, the pause and resume thresholds coincide. The
  first crossing still emits one paused poll; once depth reaches the shared
  threshold, the next poll resumes the normal pop path.
  """

  def test_resume_at_defaults_to_pause_at(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
    )
    # Internal: resume_at defaults to pause_at when None.
    assert scheduler._resume_at == 10

    req = Request("https://example.com/a")
    queue = _LenControllableQueue(depth=11, pop_value=req)
    scheduler._queue = queue  # type: ignore[assignment]

    # 1. depth=11 (> resume_at=10) → pause, stay paused, return None.
    assert scheduler.next_request() is None
    assert counts.get("scheduler/backpressure_pause") == 1
    queue.pop.assert_not_called()

    # 2. depth=10 (== resume_at) → resume, pops the request.
    queue.set_depth(10)
    result = scheduler.next_request()
    assert result is req
    assert counts.get("scheduler/backpressure_resume") == 1
    queue.pop.assert_called_once()


class TestBackpressureStatNames:
  """Test 5: only the two documented stat keys are mutated by the gate."""

  def test_only_documented_stat_keys_mutated(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    queue = _LenControllableQueue(depth=10)
    scheduler._queue = queue  # type: ignore[assignment]

    scheduler.next_request()  # pause
    queue.set_depth(5)
    scheduler.next_request()  # resume

    # Exactly the two stat keys, nothing else mutated by the gate path
    # (pop NOT called on the pause call; resume call pops with pop_value=None
    # so no dequeued stat is bumped either).
    assert set(counts.keys()) == {
      "scheduler/backpressure_pause",
      "scheduler/backpressure_resume",
    }


class TestBackpressureOpenResets:
  """Test 6: open(spider) resets both per-spider gate state fields."""

  def test_open_resets_paused_state(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    scheduler = BackendScheduler(
      connection_manager=manager,
      backpressure_pause_at=10,
    )
    # Manually set the flag True (simulating a prior paused state / re-open).
    scheduler._backpressure_paused = True
    scheduler._backpressure_probe_due = True

    scheduler.open(_FakeSpider())

    assert scheduler._backpressure_paused is False
    assert scheduler._backpressure_probe_due is False


class TestBackpressureLenErrorDegradesToPop:
  """Tests 8-9: when ``len(self._queue)`` raises (QueueError OR
  NotImplementedError), the gate can't read depth → it degrades to pop (no
  crash, no stall, flag not stuck). The NotImplementedError path is the
  RocketMQ ``queue_len`` contract (``rocketmq.py`` raises NotImplementedError);
  without the gate's inner try, ``next_request`` would crash on
  RocketMQ + backpressure."""

  def test_queue_error_from_len_degrades_to_pop(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    _counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
    )
    queue = MagicMock(name="BackendQueue")
    queue.__len__ = MagicMock(side_effect=QueueError("len unavailable"))
    queue.pop = MagicMock(return_value=None)
    scheduler._queue = queue

    result = scheduler.next_request()
    assert result is None  # pop returned None
    # Gate skipped (depth unreadable) → pop WAS called (degrade), not None'd.
    queue.pop.assert_called_once_with(timeout=0)
    # Flag not stuck True (gate skipped before any assignment).
    assert scheduler._backpressure_paused is False

  def test_not_implemented_from_len_degrades_to_pop(self) -> None:
    """RocketMQ queue_len raises NotImplementedError; gate must degrade, not crash."""
    manager = MagicMock(name="ConnectionManager")
    _counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
    )
    queue = MagicMock(name="BackendQueue")
    queue.__len__ = MagicMock(side_effect=NotImplementedError("rocketmq queue_len"))
    queue.pop = MagicMock(return_value=None)
    scheduler._queue = queue

    result = scheduler.next_request()  # must NOT raise NotImplementedError
    assert result is None
    queue.pop.assert_called_once_with(timeout=0)
    assert scheduler._backpressure_paused is False


@pytest.mark.parametrize(
  "transient_error",
  [
    CircuitBreakerOpenError("redis-queue"),
    BackendConnectionError("redis reconnect exhausted"),
  ],
)
def test_next_request_degrades_during_transient_backend_outage(
  transient_error: Exception,
) -> None:
  """Circuit rejection or failed reconnect is an empty poll, not a crash."""
  manager = MagicMock(name="ConnectionManager")
  scheduler = BackendScheduler(connection_manager=manager)
  queue = MagicMock(name="BackendQueue")
  queue.pop.side_effect = transient_error
  scheduler._queue = queue

  assert scheduler.next_request() is None
  queue.pop.assert_called_once_with(timeout=0)


@pytest.mark.parametrize(
  "transient_error",
  [
    CircuitBreakerOpenError("redis-queue"),
    BackendConnectionError("redis reconnect exhausted"),
  ],
)
def test_has_pending_requests_stays_conservative_during_transient_outage(
  transient_error: Exception,
) -> None:
  """An unavailable depth source must never make Scrapy declare idle."""
  manager = MagicMock(name="ConnectionManager")
  scheduler = BackendScheduler(connection_manager=manager)
  queue = MagicMock(name="BackendQueue")
  queue.__len__.side_effect = transient_error
  scheduler._queue = queue

  assert scheduler.has_pending_requests() is True


class TestBackpressureStatsNoneAndFallthrough:
  """G8-G10: close stat-None + armed-but-below-threshold branches.

  Characterization tests — pin that the gate works without a stats collector
  and that an armed gate below threshold falls through to pop.
  See docs/superpowers/specs/2026-07-02-scheduler-branch-closure-design.md.
  """

  def test_G8_pause_without_stats(self) -> None:
    """G8: pause_at=10, depth=10, stats=None → return None, paused flag set.

    Covers the stats-None sub-branch of the pause arm (683->685) — the
    ``if self.stats:`` guard before the pause-stat bump must skip cleanly.
    """
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=None,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    queue = _LenControllableQueue(depth=10)
    scheduler._queue = queue  # type: ignore[assignment]

    result = scheduler.next_request()

    assert result is None
    queue.pop.assert_not_called()
    assert scheduler._backpressure_paused is True  # paused despite no stats

  def test_G9_resume_without_stats(self) -> None:
    """G9: stats=None; pause then drain-to-resume → second call pops, flag cleared.

    Covers the stats-None sub-branch of the resume arm (688->692).
    """
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=None,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    req = Request("https://example.com/a")
    queue = _LenControllableQueue(depth=10, pop_value=req)
    scheduler._queue = queue  # type: ignore[assignment]

    # 1. depth=10 → pause (no stat, stats=None), return None.
    assert scheduler.next_request() is None
    assert scheduler._backpressure_paused is True

    # 2. depth=5 (== resume_at) → resume, pop returns req (no stat, stats=None).
    queue.set_depth(5)
    result = scheduler.next_request()
    assert result is req
    assert scheduler._backpressure_paused is False

  def test_G10_gate_armed_below_threshold_pops(self) -> None:
    """G10: pause_at set, depth below threshold, never paused → pop proceeds.

    Covers the fall-through branch (685->692): gate is armed (pause_at is not
    None) but depth never reached pause_at, so ``_backpressure_paused`` stays
    False and control flows straight to pop.
    """
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      backpressure_pause_at=10,
      backpressure_resume_at=5,
    )
    req = Request("https://example.com/a")
    queue = _LenControllableQueue(depth=5, pop_value=req)  # below pause_at
    scheduler._queue = queue  # type: ignore[assignment]

    result = scheduler.next_request()

    assert result is req
    queue.pop.assert_called_once_with(timeout=0)
    assert scheduler._backpressure_paused is False  # never paused
    # No pause/resume stat bumped — gate didn't trigger.
    assert "scheduler/backpressure_pause" not in counts
    assert "scheduler/backpressure_resume" not in counts


class TestEnqueueDedupReservation:
  """A failed queue push must not permanently commit a dedup reservation."""

  @pytest.mark.parametrize(
    "push_error",
    [QueueError("temporary queue outage"), SerializationError("temporary encoding error")],
  )
  def test_push_failure_rolls_back_new_fingerprint_for_healthy_retry(
    self,
    push_error: Exception,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = [push_error, None]
    scheduler._queue = queue
    request = Request("https://example.com/retry")

    assert scheduler.enqueue_request(request) is False
    assert len(membership_filter) == 0

    assert scheduler.enqueue_request(request) is True
    assert len(membership_filter) == 1
    assert queue.push.call_count == 2

  def test_degraded_dedup_miss_does_not_roll_back_uncreated_reservation(
    self,
    mocker,
  ) -> None:
    """An open circuit admits a request but creates no fingerprint to undo."""
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.Mock(name="membership_filter")
    membership_filter.add.side_effect = CircuitBreakerOpenError("redis-set")
    membership_filter.saturation = None
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue

    request = Request("https://example.com/no-reservation")
    assert scheduler.enqueue_request(request) is False
    membership_filter.remove.assert_not_called()

  def test_reservation_precedes_push_and_filters_reentrant_duplicate(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    request = Request("https://example.com/concurrent")
    nested_results: list[bool] = []

    def push(_request: Request, *, priority: float = 0.0) -> None:
      del _request, priority
      nested_results.append(scheduler.enqueue_request(request.replace()))

    queue.push.side_effect = push

    assert scheduler.enqueue_request(request) is True
    assert nested_results == [False]
    assert queue.push.call_count == 1
    assert len(membership_filter) == 1

  def test_custom_dupefilter_without_forget_records_rollback_error(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(spec=["request_seen", "log"])
    dupefilter.request_seen.return_value = False
    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.com/custom")) is False
    assert counts.get("scheduler/dupefilter_rollback_error") == 1
    assert counts.get("scheduler/queue_error") == 1


if __name__ == "__main__":
  pytest.main([__file__, "-q", "--tb=short", "-p", "no:randomly"])
