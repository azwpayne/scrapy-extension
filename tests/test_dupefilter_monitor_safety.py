"""Telemetry failures must not change duplicate-filter control flow."""

from __future__ import annotations

import threading
from typing import Any

import pytest
from pytest_mock import MockerFixture
from scrapy.http import Request

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import BackendConnectionError
from scrapy_extension.monitor.base import Monitor


class _SelectiveRaisingMonitor(Monitor):
  """Monitor double that fails only the selected telemetry hooks."""

  def __init__(self, *hooks: str) -> None:
    self._hooks = set(hooks)

  def _raise_for(self, hook: str) -> None:
    if hook in self._hooks:
      raise RuntimeError(f"{hook} telemetry unavailable")

  def fail(self, hook: str) -> None:
    """Make a later invocation of ``hook`` fail."""
    self._hooks.add(hook)

  def on_dedup_hit(self, key: str) -> None:
    del key
    self._raise_for("on_dedup_hit")

  def on_dedup_miss(self, key: str) -> None:
    del key
    self._raise_for("on_dedup_miss")

  def on_filter_full(self) -> None:
    self._raise_for("on_filter_full")

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    del used, capacity
    self._raise_for("on_filter_saturation")

  def on_error(self, operation: str, error: BaseException) -> None:
    del operation, error
    self._raise_for("on_error")


class _LifecycleLockProbeMonitor(Monitor):
  """Probe whether a callback runs after the dupefilter releases its lock."""

  def __init__(self, hook: str) -> None:
    self._hook = hook
    self._lock: Any = None
    self.completed_during_hook: list[bool] = []
    self._threads: list[threading.Thread] = []

  def bind(self, lock: Any) -> None:
    self._lock = lock

  def _probe(self, hook: str) -> None:
    if hook != self._hook:
      return
    completed = threading.Event()
    started = threading.Event()

    def acquire_lifecycle_lock() -> None:
      started.set()
      with self._lock:
        completed.set()

    thread = threading.Thread(target=acquire_lifecycle_lock, daemon=True)
    self._threads.append(thread)
    thread.start()
    worker_started = started.wait(2.0)
    self.completed_during_hook.append(
      worker_started and completed.wait(2.0)
    )

  def on_dedup_miss(self, key: str) -> None:
    del key
    self._probe("on_dedup_miss")

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    del used, capacity
    self._probe("on_filter_saturation")

  def join(self) -> None:
    for thread in self._threads:
      thread.join(timeout=1)


class _AttributeRaisingMonitor(Monitor):
  """Fail while resolving a hook, before its method body can run."""

  def __getattribute__(self, name: str) -> Any:
    if name == "on_dedup_miss":
      raise RuntimeError("monitor hook descriptor unavailable")
    return super().__getattribute__(name)


class _MonitorStop(BaseException):
  """Process-control sentinel that telemetry isolation must not swallow."""


class _ProcessControlMonitor(Monitor):
  def on_dedup_miss(self, key: str) -> None:
    del key
    raise _MonitorStop

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    del used, capacity
    raise _MonitorStop


class _RecordingMonitor(Monitor):
  """Capture the observable order and cadence of dedup telemetry."""

  def __init__(self) -> None:
    self.events: list[str] = []

  def on_dedup_hit(self, key: str) -> None:
    del key
    self.events.append("hit")

  def on_dedup_miss(self, key: str) -> None:
    del key
    self.events.append("miss")

  def on_filter_saturation(self, used: int, capacity: int | None) -> None:
    del used, capacity
    self.events.append("saturation")


class _FifoProbeMonitor(Monitor):
  """Block the first callback and detect overlap or completion reordering."""

  def __init__(self) -> None:
    self.first_entered = threading.Event()
    self.release_first = threading.Event()
    self._lock = threading.Lock()
    self.call_order: list[str] = []
    self.completion_order: list[str] = []
    self.active = 0
    self.max_active = 0

  def on_dedup_miss(self, key: str) -> None:
    with self._lock:
      first = not self.call_order
      self.call_order.append(key)
      self.active += 1
      self.max_active = max(self.max_active, self.active)
    if first:
      self.first_entered.set()
      self.release_first.wait(5.0)
    with self._lock:
      self.completion_order.append(key)
      self.active -= 1


class _NoSaturationCapacityPoisonFilter(MembershipFilter):
  """Expose no saturation and fail if an irrelevant capacity is inspected."""

  def __init__(self) -> None:
    self._items: set[bytes] = set()

  @property
  def saturation(self) -> None:
    return None

  @property
  def capacity(self) -> int:
    raise RuntimeError("capacity must remain lazy without saturation")

  def add(self, item: bytes) -> bool:
    before = len(self._items)
    self._items.add(item)
    return len(self._items) != before

  def __contains__(self, item: bytes) -> bool:
    return item in self._items

  def __len__(self) -> int:
    return len(self._items)

  def clear(self) -> None:
    self._items.clear()

def test_miss_monitor_failure_preserves_new_reservation(
  mock_connection_manager: Any,
) -> None:
  membership = MemoryMembershipFilter(maxsize=10)
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor("on_dedup_miss"),
  )
  request = Request("https://example.test/new")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True
  assert dupefilter.request_seen(request) is True


def test_hit_monitor_failure_preserves_duplicate_result(
  mock_connection_manager: Any,
) -> None:
  membership = MemoryMembershipFilter(maxsize=10)
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor("on_dedup_hit"),
  )
  request = Request("https://example.test/duplicate")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.request_seen(request) is True


def test_retry_allowance_monitor_failure_preserves_one_shot_miss(
  mock_connection_manager: Any,
) -> None:
  membership = BloomMembershipFilter(capacity=1_000, error_rate=0.01)
  monitor = _SelectiveRaisingMonitor()
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=monitor,
  )
  request = Request("https://example.test/retry")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True
  dupefilter.forget(request)
  monitor.fail("on_dedup_miss")
  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True
  assert dupefilter.request_seen(request) is True


@pytest.mark.parametrize("hook", ["on_filter_full", "on_dedup_miss"])
def test_filter_full_monitor_failure_preserves_degraded_miss(
  mock_connection_manager: Any,
  mocker: MockerFixture,
  hook: str,
) -> None:
  membership = mocker.MagicMock(spec=MembershipFilter)
  membership.add.side_effect = FilterFull("capacity exhausted")
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor(hook),
  )
  request = Request(f"https://example.test/full/{hook}")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is False


@pytest.mark.parametrize("hook", ["on_error", "on_dedup_miss"])
def test_backend_error_monitor_failure_preserves_degraded_miss(
  mock_connection_manager: Any,
  mocker: MockerFixture,
  hook: str,
) -> None:
  membership = mocker.MagicMock(spec=MembershipFilter)
  membership.add.side_effect = BackendConnectionError(
    "backend unavailable", backend_type="redis"
  )
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor(hook),
  )
  request = Request(f"https://example.test/backend/{hook}")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is False


def test_saturation_monitor_failure_preserves_new_reservation(
  mock_connection_manager: Any,
) -> None:
  membership = CuckooMembershipFilter(capacity=100, error_rate=0.01)
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor("on_filter_saturation"),
  )
  request = Request("https://example.test/saturation")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True
  assert dupefilter.request_seen(request) is True


def test_memory_saturation_monitor_failure_does_not_reject_insert(
  mock_connection_manager: Any,
) -> None:
  membership = MemoryMembershipFilter(maxsize=1)
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=_SelectiveRaisingMonitor("on_filter_saturation"),
  )
  request = Request("https://example.test/memory-saturation")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True
  assert len(membership) == 1

  replacement = Request("https://example.test/memory-eviction")
  assert dupefilter.request_seen(replacement) is False
  assert dupefilter.consume_reservation(replacement) is True
  assert len(membership) == 1


def test_standalone_memory_monitor_failure_does_not_reject_insert() -> None:
  membership = MemoryMembershipFilter(maxsize=1)
  membership.set_monitor(_SelectiveRaisingMonitor("on_filter_saturation"))

  assert membership.add(b"new") is True
  assert b"new" in membership


def test_dupefilter_diagnostic_failure_does_not_reject_reservation(
  mock_connection_manager: Any,
  mocker: MockerFixture,
) -> None:
  mocker.patch(
    "scrapy_extension.dupefilter.dupefilter.logger.debug",
    side_effect=RuntimeError("logging unavailable"),
  )
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=10),
    monitor=_SelectiveRaisingMonitor("on_dedup_miss"),
  )
  request = Request("https://example.test/diagnostic-failure")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True


def test_standalone_memory_diagnostic_failure_does_not_reject_insert(
  mocker: MockerFixture,
) -> None:
  mocker.patch(
    "scrapy_extension.dupefilter.filters.memory_filter.logger.debug",
    side_effect=RuntimeError("logging unavailable"),
  )
  membership = MemoryMembershipFilter(maxsize=1)
  membership.set_monitor(_SelectiveRaisingMonitor("on_filter_saturation"))

  assert membership.add(b"new") is True
  assert b"new" in membership


def test_memory_saturation_preserves_insert_only_cadence_and_order(
  mock_connection_manager: Any,
) -> None:
  monitor = _RecordingMonitor()
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=1),
    monitor=monitor,
  )
  request = Request("https://example.test/memory-cadence")

  assert dupefilter.request_seen(request) is False
  assert monitor.events == ["saturation", "miss"]

  monitor.events.clear()
  assert dupefilter.request_seen(request) is True
  assert monitor.events == ["hit"]


def test_capacity_is_not_read_when_custom_filter_has_no_saturation(
  mock_connection_manager: Any,
) -> None:
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=_NoSaturationCapacityPoisonFilter(),
  )
  request = Request("https://example.test/lazy-capacity")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True


def test_concurrent_monitor_dispatch_is_fifo_non_overlapping_and_non_waiting(
  mock_connection_manager: Any,
) -> None:
  monitor = _FifoProbeMonitor()
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=10),
    monitor=monitor,
  )
  first_request = Request("https://example.test/fifo/first")
  second_request = Request("https://example.test/fifo/second")
  first_key = dupefilter.request_fingerprint(first_request)
  second_key = dupefilter.request_fingerprint(second_request)
  results: dict[str, bool] = {}
  second_done = threading.Event()

  first_thread = threading.Thread(
    target=lambda: results.__setitem__(
      "first", dupefilter.request_seen(first_request)
    ),
    daemon=True,
  )

  def submit_second() -> None:
    results["second"] = dupefilter.request_seen(second_request)
    second_done.set()

  second_thread = threading.Thread(target=submit_second, daemon=True)
  first_thread.start()
  try:
    assert monitor.first_entered.wait(2.0)
    second_thread.start()
    # A peer request must not wait for the elected monitor drainer. Its event
    # remains in the shared FIFO and is delivered by that drainer in order.
    assert second_done.wait(2.0)
  finally:
    monitor.release_first.set()
  first_thread.join(timeout=2.0)
  second_thread.join(timeout=2.0)

  assert not first_thread.is_alive()
  assert not second_thread.is_alive()
  assert results == {"first": False, "second": False}
  assert monitor.call_order == [first_key, second_key]
  assert monitor.completion_order == [first_key, second_key]
  assert monitor.max_active == 1


def test_blocked_monitor_backlog_is_bounded_by_complete_event_batches(
  mock_connection_manager: Any,
) -> None:
  monitor = _FifoProbeMonitor()
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=BloomMembershipFilter(capacity=1_000, error_rate=0.01),
    monitor=monitor,
  )
  # The first miss is active while its saturation event remains queued. One
  # more two-event [miss, saturation] batch fits exactly; the next has only one
  # slot available and must therefore be dropped whole.
  dupefilter._monitor_event_limit = 4
  requests = [
    Request(f"https://example.test/bounded-fifo/{index}")
    for index in range(3)
  ]
  keys = [dupefilter.request_fingerprint(request) for request in requests]

  first_thread = threading.Thread(
    target=dupefilter.request_seen,
    args=(requests[0],),
    daemon=True,
  )
  first_thread.start()
  try:
    assert monitor.first_entered.wait(2.0)
    assert dupefilter.request_seen(requests[1]) is False
    assert dupefilter.request_seen(requests[2]) is False
    assert len(dupefilter._monitor_events) == 3
    assert dupefilter._monitor_overflow_warned is True
  finally:
    monitor.release_first.set()
  first_thread.join(timeout=2.0)

  assert not first_thread.is_alive()
  assert monitor.call_order == keys[:2]
  assert monitor.completion_order == keys[:2]
  assert monitor.max_active == 1


def test_hook_attribute_failure_is_isolated_after_reservation(
  mock_connection_manager: Any,
) -> None:
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=10),
    monitor=_AttributeRaisingMonitor(),
  )
  request = Request("https://example.test/descriptor")

  assert dupefilter.request_seen(request) is False
  assert dupefilter.consume_reservation(request) is True


@pytest.mark.parametrize(
  ("hook", "maxsize"),
  [
    ("on_dedup_miss", 10),
    ("on_filter_saturation", 1),
  ],
)
def test_monitor_hooks_run_after_lifecycle_lock_release(
  mock_connection_manager: Any,
  hook: str,
  maxsize: int,
) -> None:
  monitor = _LifecycleLockProbeMonitor(hook)
  membership = MemoryMembershipFilter(maxsize=maxsize)
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=membership,
    monitor=monitor,
  )
  monitor.bind(dupefilter._lifecycle_lock)

  assert dupefilter.request_seen(Request(f"https://example.test/lock/{hook}")) is False
  monitor.join()
  assert monitor.completed_during_hook == [True]


def test_dupefilter_does_not_swallow_process_control_exception(
  mock_connection_manager: Any,
) -> None:
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=10),
    monitor=_ProcessControlMonitor(),
  )
  request = Request("https://example.test/process-control")

  with pytest.raises(_MonitorStop):
    dupefilter.request_seen(request)
  assert dupefilter.consume_reservation(request) is True
  assert dupefilter._monitor_drain_token is None


def test_process_control_before_drain_releases_election(
  mock_connection_manager: Any,
  mocker: MockerFixture,
) -> None:
  dupefilter = BackendDupeFilter(
    connection_manager=mock_connection_manager,
    membership_filter=MemoryMembershipFilter(maxsize=10),
  )
  real_drain = dupefilter._drain_monitor_events
  drain = mocker.patch.object(
    dupefilter,
    "_drain_monitor_events",
    side_effect=_MonitorStop,
  )
  first = Request("https://example.test/pre-drain-stop")

  with pytest.raises(_MonitorStop):
    dupefilter.request_seen(first)
  assert dupefilter.consume_reservation(first) is True
  assert dupefilter._monitor_drain_token is None
  assert len(dupefilter._monitor_events) == 1

  # A later decision can win a fresh election and drain both the retained
  # event and its own; the aborted owner never poisons the FIFO permanently.
  drain.side_effect = real_drain
  second = Request("https://example.test/post-drain-stop")
  assert dupefilter.request_seen(second) is False
  assert dupefilter._monitor_drain_token is None
  assert not dupefilter._monitor_events


def test_memory_filter_does_not_swallow_process_control_exception() -> None:
  membership = MemoryMembershipFilter(maxsize=1)
  membership.set_monitor(_ProcessControlMonitor())

  with pytest.raises(_MonitorStop):
    membership.add(b"process-control")
  assert b"process-control" in membership
