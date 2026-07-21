"""Tests for WorkStealingQueueStrategy — pop-side load balancing across workers.

Covers:
- push routes to own queue ``<name>:<worker_id>``
- pop returns from own queue when non-empty (no steal attempt)
- pop steals from peer when own empty
- pop tries peers round-robin (steal_idx advances)
- pop returns None when all peers empty + no timeout
- pop blocking fallback on own queue when timeout > 0 and all peers empty
- custom worker_id; default worker_id is auto-generated UUID (unique per instance)
- queue_len covers own + peers; clear remains scoped to own queue
- snapshot/restore ABC defaults (None / no-op)
- config validation: finite timeout and bounded, normalized peers
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy
from scrapy_extension.schedule.scheduler import BackendScheduler


def _strategy(
  *,
  worker_id: str | None = "w1",
  peer_ids: tuple[str, ...] = ("w2", "w3"),
  steal_timeout: float = 0.05,
) -> tuple[WorkStealingQueueStrategy, MagicMock]:
  cm = MagicMock(name="ConnectionManager")
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  return (
    WorkStealingQueueStrategy(
      cm,
      worker_id=worker_id,
      peer_ids=peer_ids,
      steal_timeout=steal_timeout,
    ),
    qb,
  )


@pytest.mark.parametrize("backend_type", ["kafka", "rocketmq"])
def test_multi_topic_backends_are_rejected_before_worker_scanning(backend_type):
  """Topic-switching consumers cannot isolate own and peer deliveries."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = backend_type

  with pytest.raises(ConfigurationError, match="passthrough"):
    WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=("w2",))

  cm.get_queue_backend.assert_not_called()


def test_pop_with_ack_threads_backend_token_from_own_queue():
  """#28: pop_with_ack checks the own queue via backend.pop_with_ack and
  returns (data, token) so MQ backends paired with the work-stealing strategy
  keep per-message ack correlation (previously pop() returned data only)."""
  s, qb = _strategy()
  qb.pop_with_ack.return_value = (b"item", "TOKEN-OWN")
  data, token = s.pop_with_ack("q", 0.0)
  assert data == b"item"
  assert token == "TOKEN-OWN"
  # Own queue first (no peer steal needed since own was non-empty).
  qb.pop_with_ack.assert_called_once_with(s._own_queue("q"), 0.0)


# ---------------------------------------------------------------------------
# push — own queue only
# ---------------------------------------------------------------------------


def test_push_routes_to_own_queue():
  s, qb = _strategy(worker_id="alice")
  s.push("q", b"x", priority=0.5)
  qb.push.assert_called_once_with(s._own_queue("q"), b"x", 0.5)


def test_derived_worker_names_are_backend_portable_and_distinct():
  """Logical names and worker IDs cannot inject Kafka-invalid separators."""
  from scrapy_extension.backends.base import _validate_key_name
  from scrapy_extension.backends.kafka import _validate_topic_name

  first, _ = _strategy(worker_id="region:a")
  second, _ = _strategy(worker_id="region.a")
  first_name = first._own_queue("jobs:tenant-a")
  second_name = second._own_queue("jobs:tenant-a")

  assert first_name != second_name
  for name in (first_name, second_name):
    _validate_key_name(name, "queue_name")
    _validate_topic_name(name)


def test_push_ignores_delay_and_source():
  s, qb = _strategy(worker_id="alice")
  s.push("q", b"x", delay=10.0, source="ignored")
  qb.push.assert_called_once_with(s._own_queue("q"), b"x", 0.0)


# ---------------------------------------------------------------------------
# pop — own first, then steal
# ---------------------------------------------------------------------------


def test_pop_returns_from_own_queue_no_steal():
  """Own queue has an item → no steal attempt is made."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.side_effect = [b"own-item"]
  item = s.pop("q")
  assert item == b"own-item"
  # Only one pop call (own); no peer steals.
  assert qb.pop.call_count == 1
  assert qb.pop.call_args_list[0].args == (s._own_queue("q"), 0.0)


def test_pop_steals_from_first_peer_with_item():
  """Own empty, first peer has item → steal from peer."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.side_effect = [None, b"stolen"]  # own empty, w2 has item
  item = s.pop("q")
  assert item == b"stolen"
  assert qb.pop.call_count == 2
  assert qb.pop.call_args_list[0].args == (s._own_queue("q"), 0.0)
  assert qb.pop.call_args_list[1].args == (s._worker_queue("q", "w2"), 0.0)


def test_zero_timeout_never_accumulates_blocking_peer_probes():
  """QueueBackend's timeout=0 contract remains non-blocking across all peers."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"), steal_timeout=4.0)
  qb.pop.return_value = None

  assert s.pop("q", timeout=0.0) is None
  assert [call.args[1] for call in qb.pop.call_args_list] == [0.0, 0.0, 0.0]


def test_compatible_backend_keeps_published_worker_queue_name():
  """A rolling upgrade reads and writes the existing ``q:worker`` queue in place."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = b"legacy"
  cm.get_queue_backend.return_value = qb
  s = WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=("w2",))

  assert s._own_queue("q") == "q:w1"
  assert s.pop("q") == b"legacy"
  qb.pop.assert_called_once_with("q:w1", 0.0)


def test_pop_with_ack_from_published_peer_queue_preserves_token():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  qb.pop_with_ack.side_effect = [
    (None, None),
    (b"legacy-peer", "legacy-token"),
  ]
  cm.get_queue_backend.return_value = qb
  s = WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=("w2",))

  assert s.pop_with_ack("q") == (b"legacy-peer", "legacy-token")
  assert [call.args[0] for call in qb.pop_with_ack.call_args_list] == [
    "q:w1",
    "q:w2",
  ]


def test_pop_with_ack_binds_stolen_token_to_physical_peer_queue():
  """A stolen delivery must settle against the peer queue that issued it."""
  from scrapy_extension.backends.base import QueueBackend
  from scrapy_extension.queue.strategies.base import _BoundQueueAckToken

  class _StealBackend(QueueBackend):
    def __init__(self) -> None:
      self.pop_calls: list[str] = []
      self.ack_calls: list[tuple[str, object]] = []

    def push(
      self, queue_name: str, item: bytes, priority: float = 0.0
    ) -> None:
      del queue_name, item, priority

    def pop(self, queue_name: str, timeout: float = 0.0) -> bytes | None:
      del queue_name, timeout
      return None

    def pop_with_ack(
      self, queue_name: str, timeout: float = 0.0
    ) -> tuple[bytes | None, object | None]:
      del timeout
      self.pop_calls.append(queue_name)
      if queue_name == "q:w2":
        return b"stolen", "peer-token"
      return None, None

    def ack(self, queue_name: str, *, token: object | None = None) -> None:
      self.ack_calls.append((queue_name, token))

    def queue_len(self, queue_name: str) -> int:
      del queue_name
      return 0

    def clear_queue(self, queue_name: str) -> None:
      del queue_name

  backend = _StealBackend()
  manager = MagicMock(name="ConnectionManager")
  manager.backend_type = "redis"
  manager.get_queue_backend.return_value = backend
  strategy = WorkStealingQueueStrategy(manager, worker_id="w1", peer_ids=("w2",))

  data, token = strategy.pop_with_ack("q")

  assert data == b"stolen"
  assert isinstance(token, _BoundQueueAckToken)
  assert token.backend is backend
  assert token.queue_name == "q:w2"
  token.ack()
  assert backend.ack_calls == [("q:w2", "peer-token")]


def test_pop_never_probes_legacy_worker_name_on_sqs():
  """Colon-based legacy names were invalid on SQS and must not be resolved."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "sqs"
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  s = WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=("w2",))

  assert s.pop("q") is None
  assert [call.args[0] for call in qb.pop.call_args_list] == [
    s._own_queue("q"),
    s._worker_queue("q", "w2"),
  ]


def test_rabbitmq_uses_portable_name_only_when_legacy_name_cannot_exist():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "rabbitmq"
  cm.get_queue_backend.return_value = MagicMock()
  s = WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=())
  overlong_queue = "q" * 253

  physical = s._own_queue(overlong_queue)

  assert physical != f"{overlong_queue}:w1"
  assert len(physical.encode()) <= 255


def test_pop_skips_empty_peer_steals_from_next():
  """Own + w2 empty, w3 has item."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.side_effect = [None, None, b"from-w3"]
  item = s.pop("q")
  assert item == b"from-w3"
  assert qb.pop.call_count == 3


def test_pop_round_robin_advances_steal_idx():
  """Two consecutive steals start at different peers (round-robin)."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  # First pop: own empty, w2 has item → steal_idx advances past w2.
  qb.pop.side_effect = [None, b"a"]
  assert s.pop("q") == b"a"
  # Second pop: own empty. Round-robin → next steal starts at w3 (idx=1).
  qb.pop.side_effect = [None, b"b"]
  assert s.pop("q") == b"b"
  # Second steal attempt was on w3, not w2.
  last_peer_call = qb.pop.call_args_list[-1].args
  assert last_peer_call == (s._worker_queue("q", "w3"), 0.0)


def test_pop_returns_none_when_all_empty_no_timeout():
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.return_value = None
  assert s.pop("q") is None
  # 1 own (non-blocking) + 2 peers = 3 pops total.
  assert qb.pop.call_count == 3


def test_pop_blocking_fallback_on_own_when_timeout():
  """All empty + timeout > 0 → final blocking pop on own queue."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.side_effect = [None, None, None, b"arrived"]
  item = s.pop("q", timeout=5.0)
  assert item == b"arrived"
  # 3 non-blocking (own + 2 peers) + 1 blocking (own, full timeout).
  assert qb.pop.call_count == 4
  final_queue, final_timeout = qb.pop.call_args_list[3].args
  assert final_queue == s._own_queue("q")
  assert 0 < final_timeout <= 5.0


def test_peer_probes_and_fallback_share_one_total_timeout_budget(mocker):
  now = [100.0]
  mocker.patch(
    "scrapy_extension.queue.strategies.work_stealing.time.monotonic",
    side_effect=lambda: now[0],
  )
  s, qb = _strategy(
    worker_id="w1",
    peer_ids=("w2", "w3"),
    steal_timeout=4.0,
  )

  def consume_timeout(_queue_name, timeout):
    now[0] += timeout
    return None

  qb.pop.side_effect = consume_timeout

  assert s.pop("q", timeout=5.0) is None
  assert sum(call.args[1] for call in qb.pop.call_args_list) == pytest.approx(5.0)
  assert [call.args[1] for call in qb.pop.call_args_list] == [0.0, 4.0, 1.0]


def test_no_peer_ids_skips_steal_phase():
  """peer_ids=() → no steal attempts; goes straight to blocking fallback."""
  s, qb = _strategy(worker_id="solo", peer_ids=())
  qb.pop.side_effect = [None, b"x"]
  item = s.pop("q", timeout=2.0)
  assert item == b"x"
  assert qb.pop.call_count == 2  # own non-blocking + own blocking


# ---------------------------------------------------------------------------
# worker_id defaults
# ---------------------------------------------------------------------------


def test_default_worker_id_is_unique_per_instance():
  cm = MagicMock()
  cm.get_queue_backend.return_value = MagicMock()
  s1 = WorkStealingQueueStrategy(cm)
  s2 = WorkStealingQueueStrategy(cm)
  assert s1._worker_id != s2._worker_id
  assert len(s1._worker_id) > 0


# ---------------------------------------------------------------------------
# queue_len, clear — own queue only
# ---------------------------------------------------------------------------


def test_queue_len_reflects_complete_steal_topology():
  """Peer backlog must keep Scrapy's idle detector alive for a stealing worker."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.queue_len.side_effect = [0, 20, 22]

  assert s.queue_len("q") == 42
  assert [call.args[0] for call in qb.queue_len.call_args_list] == [
    s._own_queue("q"),
    s._worker_queue("q", "w2"),
    s._worker_queue("q", "w3"),
  ]


def test_peer_backlog_keeps_scheduler_pending():
  """Regression: own=0/peer>0 must not let Scrapy close before a steal pop."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2",))
  qb.queue_len.side_effect = [0, 1]

  class _StrategyDepthQueue:
    def __len__(self):
      return s.queue_len("q")

  scheduler = BackendScheduler(connection_manager=MagicMock())
  scheduler._queue = _StrategyDepthQueue()  # type: ignore[assignment]

  assert scheduler.has_pending_requests() is True


def test_concurrent_steal_scans_do_not_race_the_round_robin_cursor():
  s, qb = _strategy(worker_id="w1", peer_ids=("w2",), steal_timeout=0.0)
  peer_queue = s._worker_queue("q", "w2")
  first_peer_entered = threading.Event()
  second_own_entered = threading.Event()
  release_first_peer = threading.Event()
  state_lock = threading.Lock()
  own_calls = 0
  peer_calls = 0

  def pop(queue_name, _timeout):
    nonlocal own_calls, peer_calls
    if queue_name == s._own_queue("q"):
      with state_lock:
        own_calls += 1
        if own_calls == 2:
          second_own_entered.set()
      return None
    assert queue_name == peer_queue
    with state_lock:
      peer_calls += 1
      current_peer_call = peer_calls
    if current_peer_call == 1:
      first_peer_entered.set()
      assert release_first_peer.wait(timeout=2.0)
    return None

  qb.pop.side_effect = pop
  with ThreadPoolExecutor(max_workers=2) as pool:
    first = pool.submit(s.pop, "q")
    assert first_peer_entered.wait(timeout=2.0)
    second = pool.submit(s.pop, "q")
    assert second_own_entered.wait(timeout=2.0)
    with state_lock:
      assert peer_calls == 1
    release_first_peer.set()
    assert first.result(timeout=2.0) is None
    assert second.result(timeout=2.0) is None


def test_peer_ids_are_deduplicated_and_exclude_self():
  s, _ = _strategy(worker_id="w1", peer_ids=("w1", "w2", "w2", "w3"))
  assert s._peer_ids == ("w2", "w3")


def test_clear_clears_own_queue_only():
  s, qb = _strategy(worker_id="w1", peer_ids=("w2",))
  s.clear("q")
  qb.clear_queue.assert_called_once_with(s._own_queue("q"))


def test_queue_len_and_clear_use_one_published_name_per_worker():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  qb.queue_len.side_effect = [3, 7]
  cm.get_queue_backend.return_value = qb
  s = WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=("w2",))

  assert s.queue_len("q") == 10
  assert [call.args[0] for call in qb.queue_len.call_args_list] == [
    "q:w1",
    "q:w2",
  ]

  s.clear("q")
  qb.clear_queue.assert_called_once_with("q:w1")


# ---------------------------------------------------------------------------
# snapshot / restore — ABC defaults
# ---------------------------------------------------------------------------


def test_snapshot_returns_none():
  s, _ = _strategy()
  assert s.snapshot() is None


def test_restore_is_noop():
  s, _ = _strategy()
  s.restore(b"anything")
  s.restore(None)


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_negative_steal_timeout_raises():
  cm = MagicMock()
  with pytest.raises(ValueError, match="steal_timeout must be finite and >= 0"):
    WorkStealingQueueStrategy(cm, steal_timeout=-0.1)


@pytest.mark.parametrize("steal_timeout", [True, "1", float("nan"), float("inf")])
def test_invalid_steal_timeout_raises(steal_timeout):
  cm = MagicMock()
  with pytest.raises(ValueError, match="steal_timeout must be finite and >= 0"):
    WorkStealingQueueStrategy(cm, steal_timeout=steal_timeout)


@pytest.mark.parametrize("worker_id", ["", "bad id", "bad/id", 1])
def test_invalid_worker_id_raises(worker_id):
  with pytest.raises(ValueError, match="worker_id"):
    _strategy(worker_id=worker_id)


@pytest.mark.parametrize("peer_ids", [("",), ("bad id",), ("bad/id",), (1,), "w2"])
def test_invalid_peer_ids_raise(peer_ids):
  cm = MagicMock()
  with pytest.raises(ValueError, match="peer"):
    WorkStealingQueueStrategy(cm, worker_id="w1", peer_ids=peer_ids)


def test_peer_count_is_bounded():
  cm = MagicMock()
  peers = tuple(f"w{i}" for i in range(257))
  with pytest.raises(ValueError, match="at most 256"):
    WorkStealingQueueStrategy(cm, worker_id="owner", peer_ids=peers)
