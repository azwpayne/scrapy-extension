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
from unittest.mock import MagicMock, create_autospec

import pytest
from scrapy import Request, Spider

from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import (
  BackendConnectionError,
  QueueError,
  SerializationError,
)
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.base import QueueStrategy, _PreparedQueuePush
from scrapy_extension.schedule import scheduler as scheduler_module
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


def _durable_queue_mock(name: str = "BackendQueue") -> MagicMock:
  """Model the bundled queue's private receipt while retaining a push spy."""
  queue = MagicMock(spec=BackendQueue, name=name)

  def push_with_durability(
    request: Request,
    priority: float = 0.0,
  ) -> bool:
    queue.push(request, priority=priority)
    return True

  queue._push_with_durability.side_effect = push_with_durability
  return queue


def _durable_strategy_mock(name: str = "DurableQueueStrategy") -> MagicMock:
  """Model a custom strategy that implements the private receipt protocol."""
  strategy = MagicMock(spec=QueueStrategy, name=name)

  def prepare(
    queue_name: str,
    *,
    priority: float = 0.0,
    delay: float = 0.0,
    source: str = "default",
  ) -> _PreparedQueuePush:
    def commit(item: bytes, require_durable: bool) -> bool:
      del require_durable
      strategy.push(
        queue_name,
        item,
        priority=priority,
        delay=delay,
        source=source,
      )
      return True

    return _PreparedQueuePush(backend_route=True, _commit=commit)

  strategy._prepare_push.side_effect = prepare
  return strategy


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


class _SameRequestReentrantMonitor(Monitor):
  """Re-enter one miss with the exact Request whose push has not run yet."""

  def __init__(self) -> None:
    self._dupefilter: BackendDupeFilter | None = None
    self._request: Request | None = None
    self._reentered = False
    self.nested_results: list[bool] = []

  def bind(self, dupefilter: BackendDupeFilter, request: Request) -> None:
    self._dupefilter = dupefilter
    self._request = request

  def on_dedup_miss(self, key: str) -> None:
    del key
    if self._reentered:
      return
    self._reentered = True
    if self._dupefilter is None or self._request is None:
      raise RuntimeError("reentrant monitor is not bound")
    self.nested_results.append(
      self._dupefilter.request_seen(self._request)
    )


class _SameRequestSchedulerMonitor(Monitor):
  """Re-enter the complete scheduler while its fingerprint is provisional."""

  def __init__(self) -> None:
    self._scheduler: BackendScheduler | None = None
    self._request: Request | None = None
    self._reentered = False
    self.nested_results: list[bool] = []

  def bind(self, scheduler: BackendScheduler, request: Request) -> None:
    self._scheduler = scheduler
    self._request = request

  def on_dedup_miss(self, key: str) -> None:
    del key
    if self._reentered:
      return
    self._reentered = True
    if self._scheduler is None or self._request is None:
      raise RuntimeError("scheduler monitor is not bound")
    self.nested_results.append(
      self._scheduler.enqueue_request(self._request)
    )


class _BackendErrorReentrantMonitor(Monitor):
  """Re-enter the same request while an outage miss is being observed."""

  def __init__(self) -> None:
    self._dupefilter: BackendDupeFilter | None = None
    self._request: Request | None = None
    self._reentered = False
    self.nested_results: list[bool] = []

  def bind(self, dupefilter: BackendDupeFilter, request: Request) -> None:
    self._dupefilter = dupefilter
    self._request = request

  def on_error(self, operation: str, error: BaseException) -> None:
    del operation, error
    if self._reentered:
      return
    self._reentered = True
    if self._dupefilter is None or self._request is None:
      raise RuntimeError("backend-error monitor is not bound")
    self.nested_results.append(
      self._dupefilter.request_seen(self._request)
    )


class _SchedulerStop(BaseException):
  """Process-control sentinel used to verify scheduler receipt ownership."""


class _OneShotSchedulerStopMonitor(Monitor):
  def __init__(self, signal: BaseException) -> None:
    self._signal = signal
    self._raised = False

  def on_dedup_miss(self, key: str) -> None:
    del key
    if not self._raised:
      self._raised = True
      raise self._signal


class _LegacyRequestSeenOverride(BackendDupeFilter):
  """Model a pre-extension subclass overriding only Scrapy's stable hook."""

  def __init__(self, *args: Any, seen: bool, **kwargs: Any) -> None:
    super().__init__(*args, **kwargs)
    self._seen = seen
    self.calls = 0

  def request_seen(self, request: Request) -> bool:
    del request
    self.calls += 1
    return self._seen


class _HandoffInterruptingDupeFilter(BackendDupeFilter):
  """Interrupt after the base method publishes but before the caller receives."""

  def __init__(
    self,
    *args: Any,
    signal: BaseException,
    **kwargs: Any,
  ) -> None:
    super().__init__(*args, **kwargs)
    self._signal = signal
    self._raised = False

  def request_seen_with_reservation(
    self,
    request: Request,
    owner: object | None = None,
  ) -> Any:
    decision = super().request_seen_with_reservation(request, owner)
    if not self._raised:
      self._raised = True
      raise self._signal
    return decision


class _SerializationAfterReservationDupeFilter(BackendDupeFilter):
  """Fail after publishing owner intent but before returning its receipt."""

  def request_seen_with_reservation(
    self,
    request: Request,
    owner: object | None = None,
  ) -> Any:
    super().request_seen_with_reservation(request, owner)
    raise SerializationError("decision serialization failed")


class _CommitInterruptingDupeFilter(BackendDupeFilter):
  """Interrupt finalization after a durable queue push."""

  def __init__(self, *args: Any, signal: BaseException, **kwargs: Any) -> None:
    super().__init__(*args, **kwargs)
    self._signal = signal

  def commit_reservation(self, reservation: object) -> None:
    del reservation
    raise self._signal


class _IntentCleanupInterruptingDupeFilter(BackendDupeFilter):
  """Interrupt the first silent owner cleanup before delegating on retry."""

  def __init__(self, *args: Any, signal: BaseException, **kwargs: Any) -> None:
    super().__init__(*args, **kwargs)
    self._signal = signal
    self.cleanup_calls = 0

  def rollback_reservation_intent(self, owner: object) -> None:
    self.cleanup_calls += 1
    if self.cleanup_calls == 1:
      raise self._signal
    super().rollback_reservation_intent(owner)


class _CustomAtomicDecision:
  def __init__(
    self,
    *,
    seen: bool,
    reservation: object | None,
    provisional: bool,
  ) -> None:
    self.seen = seen
    self.reservation = reservation
    self.observational = provisional


class _ExplicitAtomicDupeFilter:
  """Independent structural implementation of the transactional extension."""

  def __init__(self) -> None:
    self.receipt = object()
    self.atomic_calls = 0
    self.legacy_calls = 0
    self.commits: list[object] = []
    self.rollbacks: list[object] = []
    self.intent_rollbacks: list[object] = []

  def request_seen(self, request: Request) -> bool:
    del request
    self.legacy_calls += 1
    return True

  def request_seen_with_reservation(
    self,
    request: Request,
    owner: object,
  ) -> _CustomAtomicDecision:
    del request, owner
    self.atomic_calls += 1
    return _CustomAtomicDecision(
      seen=False,
      reservation=self.receipt,
      provisional=False,
    )

  def commit_reservation(self, reservation: object) -> None:
    self.commits.append(reservation)

  def rollback_reservation(self, reservation: object) -> None:
    self.rollbacks.append(reservation)

  def rollback_reservation_intent(self, owner: object) -> None:
    self.intent_rollbacks.append(owner)

  def log(self, request: Request, spider: Spider) -> None:
    del request, spider


class _CommitFailingExplicitAtomicDupeFilter(_ExplicitAtomicDupeFilter):
  def commit_reservation(self, reservation: object) -> None:
    del reservation
    raise QueueError("commit bookkeeping unavailable")


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
    queue = _durable_queue_mock()
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
    queue = _durable_queue_mock()
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
  queue = _durable_queue_mock()
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
  queue = _durable_queue_mock()
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
    queue = _durable_queue_mock()
    queue.push.side_effect = [push_error, None]
    scheduler._queue = queue
    request = Request("https://example.com/retry")

    assert scheduler.enqueue_request(request) is False
    assert len(membership_filter) == 0

    assert scheduler.enqueue_request(request) is True
    assert len(membership_filter) == 1
    assert queue.push.call_count == 2

  def test_same_request_monitor_reentry_cannot_erase_rollback_receipt(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    monitor = _SameRequestReentrantMonitor()
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=monitor,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = [QueueError("queue unavailable"), None]
    scheduler._queue = queue
    request = Request("https://example.com/reentrant-reservation")
    monitor.bind(dupefilter, request)

    assert scheduler.enqueue_request(request) is False
    assert monitor.nested_results == [True]
    assert len(membership_filter) == 0

    retry = Request(request.url)
    assert scheduler.enqueue_request(retry) is True
    assert len(membership_filter) == 1
    assert queue.push.call_count == 2

  def test_observational_monitor_reentry_never_settles_source(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    monitor = _SameRequestSchedulerMonitor()
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=monitor,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue
    request = Request(
      "https://example.com/provisional-source",
      meta={"_backend_ack_token": "source-token"},
    )
    monitor.bind(scheduler, request)

    assert scheduler.enqueue_request(request) is False
    assert monitor.nested_results == [False]
    queue.ack.assert_not_called()
    assert request.meta["_backend_ack_token"] == "source-token"
    assert len(membership_filter) == 0

  def test_cross_instance_duplicate_source_gets_durable_handoff(
    self,
    mocker,
  ) -> None:
    shared_membership = MemoryMembershipFilter(maxsize=None)
    manager_a = MagicMock(name="ConnectionManagerA")
    dupefilter_a = BackendDupeFilter(
      connection_manager=manager_a,
      membership_filter=shared_membership,
    )
    owner_request = Request("https://example.com/cross-worker")
    owner_decision = dupefilter_a.request_seen_with_reservation(owner_request)
    assert owner_decision.reservation is not None

    manager_b = MagicMock(name="ConnectionManagerB")
    backend_b = manager_b.get_queue_backend.return_value
    strategy_b = _durable_strategy_mock()
    spider = mocker.Mock(name="Spider")
    spider.crawler.stats = None
    queue_b = BackendQueue(
      connection_manager=manager_b,
      queue_name="cross-worker-queue",
      spider=spider,
      queue_strategy=strategy_b,
    )
    dupefilter_b = BackendDupeFilter(
      connection_manager=manager_b,
      membership_filter=shared_membership,
    )
    scheduler_b = BackendScheduler(
      connection_manager=manager_b,
      dupefilter=dupefilter_b,
    )
    scheduler_b._queue = queue_b
    competing = Request(
      owner_request.url,
      meta={"_backend_ack_token": "source-b"},
    )

    assert scheduler_b.enqueue_request(competing) is True

    strategy_b.push.assert_called_once()
    backend_b.ack.assert_called_once_with(
      "cross-worker-queue",
      token="source-b",
    )
    assert "_backend_ack_token" not in competing.meta

    # Worker A owned only a local intent. Its later failure cannot erase the
    # marker worker B published after crossing its durable strategy boundary.
    dupefilter_a.rollback_reservation(owner_decision.reservation)
    assert len(shared_membership) == 1

  def test_cross_instance_plain_request_marker_survives_other_intent_rollback(
    self,
  ) -> None:
    shared_membership = MemoryMembershipFilter(maxsize=None)
    manager_a = MagicMock(name="ConnectionManagerA")
    dupefilter_a = BackendDupeFilter(
      connection_manager=manager_a,
      membership_filter=shared_membership,
    )
    request = Request("https://example.com/cross-worker-plain")
    owner_decision = dupefilter_a.request_seen_with_reservation(request)
    assert owner_decision.reservation is not None
    assert len(shared_membership) == 0

    manager_b = MagicMock(name="ConnectionManagerB")
    dupefilter_b = BackendDupeFilter(
      connection_manager=manager_b,
      membership_filter=shared_membership,
    )
    scheduler_b = BackendScheduler(
      connection_manager=manager_b,
      dupefilter=dupefilter_b,
    )
    queue_b = _durable_queue_mock("BackendQueueB")
    scheduler_b._queue = queue_b

    assert scheduler_b.enqueue_request(request.replace()) is True
    queue_b.push.assert_called_once()
    assert len(shared_membership) == 1

    dupefilter_a.rollback_reservation(owner_decision.reservation)
    assert len(shared_membership) == 1

  def test_unreturned_intent_never_creates_cross_worker_ghost_marker(
    self,
  ) -> None:
    shared_membership = MemoryMembershipFilter(maxsize=None)
    manager_a = MagicMock(name="ConnectionManagerA")
    dupefilter_a = BackendDupeFilter(
      connection_manager=manager_a,
      membership_filter=shared_membership,
    )
    request = Request("https://example.com/cross-worker-crash")

    abandoned = dupefilter_a.request_seen_with_reservation(request)
    assert abandoned.reservation is not None
    assert len(shared_membership) == 0

    manager_b = MagicMock(name="ConnectionManagerB")
    dupefilter_b = BackendDupeFilter(
      connection_manager=manager_b,
      membership_filter=shared_membership,
    )
    scheduler_b = BackendScheduler(
      connection_manager=manager_b,
      dupefilter=dupefilter_b,
    )
    queue_b = _durable_queue_mock("BackendQueueB")
    scheduler_b._queue = queue_b

    assert scheduler_b.enqueue_request(request.replace()) is True
    assert len(shared_membership) == 1

  def test_volatile_strategy_uses_local_shadow_not_persistent_marker(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    strategy = MagicMock(name="VolatileQueueStrategy")
    strategy.is_push_durable.return_value = False
    spider = mocker.Mock(name="Spider")
    spider.crawler.stats = None
    queue = BackendQueue(
      connection_manager=manager,
      queue_name="volatile-queue",
      spider=spider,
      queue_strategy=strategy,
    )
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    scheduler._queue = queue
    request = Request("https://example.com/volatile")

    assert scheduler.enqueue_request(request) is True
    assert len(membership) == 0
    assert len(dupefilter._volatile_fingerprints) == 1

    assert scheduler.enqueue_request(request.replace()) is False
    strategy.push.assert_called_once()

  @pytest.mark.parametrize("public_result", [False, True, None])
  def test_custom_queue_return_value_does_not_redefine_push_durability(
    self,
    public_result,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="CustomQueue")
    queue.push.return_value = public_result
    scheduler._queue = queue

    assert scheduler.enqueue_request(
      Request("https://example.com/custom-queue-return")
    ) is True
    assert len(membership) == 0
    assert len(dupefilter._volatile_fingerprints) == 1

  def test_duplicate_deferred_child_completes_after_durable_handoff(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    backend = manager.get_queue_backend.return_value
    strategy = _durable_strategy_mock()
    spider = mocker.Mock(name="Spider")
    spider.crawler.stats = None
    queue = BackendQueue(
      connection_manager=manager,
      queue_name="deferred-child-queue",
      spider=spider,
      queue_strategy=strategy,
    )
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    scheduler._queue = queue
    original = Request("https://example.com/deferred-duplicate")
    initial = dupefilter.request_seen_with_reservation(original)
    assert initial.reservation is not None
    dupefilter.commit_reservation(initial.reservation)
    group = scheduler_module._DeferredReplacementAckGroup(
      scheduler,
      "source-token",
    )
    child = group.new_child()
    assert child is not None
    group.seal()
    replacement = original.replace(
      meta={"_backend_ack_token": child},
    )

    assert scheduler.enqueue_request(replacement) is True

    strategy.push.assert_called_once()
    backend.ack.assert_called_once_with(
      "deferred-child-queue",
      token="source-token",
    )
    assert group._pending == set()
    assert group._terminal is True
    assert "_backend_ack_token" not in replacement.meta

  def test_monitor_process_control_compensates_before_scheduler_retry(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    signal = _SchedulerStop()
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=_OneShotSchedulerStopMonitor(signal),
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = [QueueError("queue unavailable"), None]
    scheduler._queue = queue
    request = Request("https://example.com/monitor-stop")

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(request)
    assert raised.value is signal
    assert queue.push.call_count == 1
    assert len(membership_filter) == 0

    assert scheduler.enqueue_request(request.replace()) is True
    assert queue.push.call_count == 2
    assert len(membership_filter) == 1

  def test_push_process_control_rolls_back_without_masking_signal(self) -> None:
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
    queue = _durable_queue_mock()
    signal = _SchedulerStop()
    queue.push.side_effect = [signal, None]
    scheduler._queue = queue
    request = Request("https://example.com/push-stop")

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(request)
    assert raised.value is signal
    assert len(membership_filter) == 0

    assert scheduler.enqueue_request(request.replace()) is True
    assert queue.push.call_count == 2
    assert len(membership_filter) == 1

  def test_interrupted_receipt_handoff_rolls_back_by_owner_intent(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    signal = _SchedulerStop()
    dupefilter = _HandoffInterruptingDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      signal=signal,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue
    request = Request("https://example.com/handoff-stop")

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(request)
    assert raised.value is signal
    queue.push.assert_not_called()
    assert len(membership_filter) == 0

    assert scheduler.enqueue_request(request.replace()) is True
    queue.push.assert_called_once()
    assert len(membership_filter) == 1

  def test_serialization_error_after_intent_publication_cleans_owner_maps(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    dupefilter = _SerializationAfterReservationDupeFilter(
      connection_manager=manager,
      membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue

    for attempt in range(3):
      request = Request(f"https://example.com/intent-error/{attempt}")
      assert scheduler.enqueue_request(request) is False
      assert dupefilter._active_reservations == {}
      assert dupefilter._reservations_by_owner == {}

    queue.push.assert_not_called()

  def test_durable_push_commit_interruption_cleans_by_owner_intent(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    strategy = _durable_strategy_mock()
    queue = BackendQueue(
      connection_manager=manager,
      queue_name="commit-interruption",
      queue_strategy=strategy,
    )
    signal = _SchedulerStop()
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = _CommitInterruptingDupeFilter(
      connection_manager=manager,
      membership_filter=membership,
      signal=signal,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    scheduler._queue = queue

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(Request("https://example.com/commit-stop"))

    assert raised.value is signal
    strategy.push.assert_called_once()
    assert len(membership) == 0
    assert dupefilter._active_reservations == {}
    assert dupefilter._reservations_by_owner == {}

  def test_class_level_queue_push_monkeypatch_is_not_bypassed(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    strategy = _durable_strategy_mock()
    queue = BackendQueue(
      connection_manager=manager,
      queue_name="class-patched-push",
      queue_strategy=strategy,
    )
    membership = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    scheduler._queue = queue
    patched_push = mocker.patch.object(
      BackendQueue,
      "push",
      side_effect=QueueError("patched public push failed"),
    )
    request = Request("https://example.com/class-patched-push")

    assert scheduler.enqueue_request(request) is False

    patched_push.assert_called_once_with(request, priority=0)
    strategy.push.assert_not_called()
    assert len(membership) == 0
    assert dupefilter._active_reservations == {}
    assert dupefilter._reservations_by_owner == {}

  def test_process_control_uses_silent_intent_cleanup_and_preserves_signal(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.MagicMock(spec=MembershipFilter)
    membership_filter.__contains__.return_value = False
    membership_filter.add.return_value = True
    membership_filter.saturation = None
    original = _SchedulerStop()
    cleanup = _SchedulerStop()
    monitor = _OneShotSchedulerStopMonitor(cleanup)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=monitor,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = [original, None]
    scheduler._queue = queue
    request = Request("https://example.com/cleanup-stop")

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(request)
    assert raised.value is original
    assert queue.push.call_count == 1
    membership_filter.remove.assert_not_called()
    assert monitor._raised is False

    monitor._raised = True
    assert scheduler.enqueue_request(request.replace()) is True
    assert queue.push.call_count == 2

  def test_secondary_cleanup_signal_cannot_replace_primary_or_leak_receipt(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    original = _SchedulerStop()
    cleanup = _SchedulerStop()
    dupefilter = _IntentCleanupInterruptingDupeFilter(
      connection_manager=manager,
      membership_filter=MemoryMembershipFilter(maxsize=None),
      signal=cleanup,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = original
    scheduler._queue = queue

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(Request("https://example.com/double-stop"))

    assert raised.value is original
    assert dupefilter.cleanup_calls == 2
    assert dupefilter._active_reservations == {}
    assert dupefilter._reservations_by_owner == {}

  def test_cleanup_process_control_after_queue_error_propagates(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.MagicMock(spec=MembershipFilter)
    membership_filter.__contains__.return_value = False
    membership_filter.add.return_value = True
    membership_filter.saturation = None
    cleanup = _SchedulerStop()
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=_OneShotSchedulerStopMonitor(cleanup),
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue

    with pytest.raises(_SchedulerStop) as raised:
      scheduler.enqueue_request(Request("https://example.com/cleanup-primary"))

    assert raised.value is cleanup
    membership_filter.remove.assert_not_called()

  def test_failed_push_does_not_mutate_membership_before_retry(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.MagicMock(spec=MembershipFilter)
    membership_filter.__contains__.return_value = False
    membership_filter.add.return_value = True
    membership_filter.saturation = None
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = [QueueError("queue unavailable"), None]
    scheduler._queue = queue
    request = Request("https://example.com/rollback-retry")

    assert scheduler.enqueue_request(request) is False
    membership_filter.add.assert_not_called()
    membership_filter.remove.assert_not_called()

    assert scheduler.enqueue_request(request.replace()) is True
    assert membership_filter.__contains__.call_count == 2
    assert membership_filter.add.call_count == 1
    membership_filter.remove.assert_not_called()
    assert queue.push.call_count == 2

  def test_degraded_monitor_reentry_cannot_create_hidden_reservation(
    self,
    mocker,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.MagicMock(spec=MembershipFilter)
    membership_filter.__contains__.side_effect = [
      BackendConnectionError("backend unavailable", backend_type="redis"),
      False,
    ]
    membership_filter.add.return_value = True
    membership_filter.saturation = None
    monitor = _BackendErrorReentrantMonitor()
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
      monitor=monitor,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = [QueueError("queue unavailable"), None]
    scheduler._queue = queue
    request = Request("https://example.com/degraded-reentry")
    monitor.bind(dupefilter, request)

    assert scheduler.enqueue_request(request) is False
    assert monitor.nested_results == [True]
    membership_filter.remove.assert_not_called()

    assert scheduler.enqueue_request(request.replace()) is True
    assert queue.push.call_count == 2
    assert membership_filter.__contains__.call_count == 2
    assert membership_filter.add.call_count == 1

  def test_inherited_atomic_extension_does_not_bypass_seen_override(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    dupefilter = _LegacyRequestSeenOverride(
      connection_manager=manager,
      membership_filter=membership_filter,
      seen=True,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.com/custom")) is False
    assert dupefilter.calls == 1
    queue.push.assert_not_called()

  def test_seen_override_can_allow_base_filter_duplicate(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    request = Request("https://example.com/custom-allow")
    dupefilter = _LegacyRequestSeenOverride(
      connection_manager=manager,
      membership_filter=membership_filter,
      seen=False,
    )
    membership_filter.add(dupefilter.request_fingerprint(request).encode())
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue

    assert scheduler.enqueue_request(request) is True
    assert dupefilter.calls == 1
    queue.push.assert_called_once()

  def test_autospec_dupefilter_uses_stable_scrapy_hook(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    dupefilter = create_autospec(BackendDupeFilter, instance=True)
    dupefilter.request_seen.return_value = False
    dupefilter.consume_reservation.return_value = False
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue
    request = Request("https://example.com/autospec")

    assert scheduler.enqueue_request(request) is True
    dupefilter.request_seen.assert_called_once_with(request)
    dupefilter.request_seen_with_reservation.assert_not_called()
    queue.push.assert_called_once_with(request, priority=0)

  def test_instance_seen_override_is_not_bypassed(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    stable_hook = MagicMock(return_value=True)
    dupefilter.request_seen = stable_hook  # type: ignore[method-assign]
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue
    request = Request("https://example.com/instance-override")

    assert scheduler.enqueue_request(request) is False
    stable_hook.assert_called_once_with(request)
    queue.push.assert_not_called()

  def test_class_level_seen_monkeypatch_is_not_bypassed(self, mocker) -> None:
    manager = MagicMock(name="ConnectionManager")
    stable_hook = mocker.patch.object(
      BackendDupeFilter,
      "request_seen",
      return_value=False,
    )
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=MemoryMembershipFilter(maxsize=None),
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue
    request = Request("https://example.com/class-override")

    assert scheduler.enqueue_request(request) is True
    stable_hook.assert_called_once_with(request)
    queue.push.assert_called_once_with(request, priority=0)

  def test_explicit_independent_atomic_protocol_is_honored(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    dupefilter = _ExplicitAtomicDupeFilter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.com/atomic")) is True
    assert dupefilter.atomic_calls == 1
    assert dupefilter.legacy_calls == 0
    assert dupefilter.commits == [dupefilter.receipt]
    assert not dupefilter.rollbacks
    assert not dupefilter.intent_rollbacks

  def test_post_push_commit_error_does_not_reclassify_durable_enqueue(
    self,
  ) -> None:
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = _CommitFailingExplicitAtomicDupeFilter()
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
      stats=stats,
    )
    queue = _durable_queue_mock()
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.com/commit")) is True

    queue.push.assert_called_once()
    assert not dupefilter.rollbacks
    assert not dupefilter.intent_rollbacks
    assert counts.get("scheduler/dupefilter_commit_error") == 1
    assert counts.get("scheduler/enqueued") == 1

  def test_degraded_dedup_miss_does_not_roll_back_uncreated_reservation(
    self,
    mocker,
  ) -> None:
    """An open circuit admits intent without mutating membership on failure."""
    manager = MagicMock(name="ConnectionManager")
    membership_filter = mocker.MagicMock(spec=MembershipFilter)
    membership_filter.__contains__.side_effect = CircuitBreakerOpenError(
      "redis-set"
    )
    membership_filter.saturation = None
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
    )
    queue = _durable_queue_mock()
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue

    request = Request("https://example.com/no-reservation")
    assert scheduler.enqueue_request(request) is False
    membership_filter.add.assert_not_called()
    membership_filter.remove.assert_not_called()

  def test_committed_replacement_ack_failure_keeps_dedup_reservation(
    self,
    mocker,
  ) -> None:
    """A committed replacement stays accepted while its source redelivers."""
    manager = MagicMock(name="ConnectionManager")
    backend = manager.get_queue_backend.return_value
    backend.ack.side_effect = QueueError("source ack failed")
    strategy = _durable_strategy_mock("QueueStrategy")
    membership_filter = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=manager,
      membership_filter=membership_filter,
    )
    counts, stats = _stats_counter()
    spider = mocker.Mock(name="Spider")
    spider.crawler.stats = stats
    queue = BackendQueue(
      connection_manager=manager,
      queue_name="test_queue",
      spider=spider,
      queue_strategy=strategy,
    )
    scheduler = BackendScheduler(
      connection_manager=manager,
      dupefilter=dupefilter,
      stats=stats,
    )
    scheduler._queue = queue

    request = Request(
      "https://example.com/replacement",
      meta={"_backend_ack_token": "old-token"},
    )
    assert scheduler.enqueue_request(request) is True

    strategy.push.assert_called_once()
    assert len(membership_filter) == 1
    assert counts.get("scheduler/ack_error") == 1
    assert counts.get("scheduler/queue_error") is None
    assert request.meta["_backend_ack_token"] == "old-token"

  def test_reentrant_push_can_complete_two_at_least_once_attempts(self) -> None:
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
    queue = _durable_queue_mock()
    scheduler._queue = queue
    request = Request("https://example.com/concurrent")
    nested_results: list[bool] = []
    reentered = False

    def push(_request: Request, *, priority: float = 0.0) -> None:
      nonlocal reentered
      del _request, priority
      if reentered:
        return
      reentered = True
      nested_results.append(scheduler.enqueue_request(request.replace()))

    queue.push.side_effect = push

    assert scheduler.enqueue_request(request) is True
    assert nested_results == [True]
    assert queue.push.call_count == 2
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
    queue = _durable_queue_mock()
    queue.push.side_effect = QueueError("queue unavailable")
    scheduler._queue = queue

    assert scheduler.enqueue_request(Request("https://example.com/custom")) is False
    assert counts.get("scheduler/dupefilter_rollback_error") == 1
    assert counts.get("scheduler/queue_error") == 1


if __name__ == "__main__":
  pytest.main([__file__, "-q", "--tb=short", "-p", "no:randomly"])
