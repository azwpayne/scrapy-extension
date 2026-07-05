"""Tests for WorkStealingQueueStrategy — pop-side load balancing across workers.

Covers:
- push routes to own queue ``<name>:<worker_id>``
- pop returns from own queue when non-empty (no steal attempt)
- pop steals from peer when own empty
- pop tries peers round-robin (steal_idx advances)
- pop returns None when all peers empty + no timeout
- pop blocking fallback on own queue when timeout > 0 and all peers empty
- custom worker_id; default worker_id is auto-generated UUID (unique per instance)
- queue_len / clear operate on own queue only
- snapshot/restore ABC defaults (None / no-op)
- config validation: steal_timeout >= 0
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scrapy_extension.queue.strategies.work_stealing import WorkStealingQueueStrategy


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


# ---------------------------------------------------------------------------
# push — own queue only
# ---------------------------------------------------------------------------


def test_push_routes_to_own_queue():
  s, qb = _strategy(worker_id="alice")
  s.push("q", b"x", priority=0.5)
  qb.push.assert_called_once_with("q:alice", b"x", 0.5)


def test_push_ignores_delay_and_source():
  s, qb = _strategy(worker_id="alice")
  s.push("q", b"x", delay=10.0, source="ignored")
  qb.push.assert_called_once_with("q:alice", b"x", 0.0)


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
  assert qb.pop.call_args_list[0].args == ("q:w1", 0.0)


def test_pop_steals_from_first_peer_with_item():
  """Own empty, first peer has item → steal from peer."""
  s, qb = _strategy(worker_id="w1", peer_ids=("w2", "w3"))
  qb.pop.side_effect = [None, b"stolen"]  # own empty, w2 has item
  item = s.pop("q")
  assert item == b"stolen"
  assert qb.pop.call_count == 2
  assert qb.pop.call_args_list[0].args == ("q:w1", 0.0)
  assert qb.pop.call_args_list[1].args == ("q:w2", 0.05)


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
  assert last_peer_call == ("q:w3", 0.05)


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
  assert qb.pop.call_args_list[3].args == ("q:w1", 5.0)


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


def test_queue_len_reflects_own_queue_only():
  s, qb = _strategy(worker_id="w1")
  qb.queue_len.return_value = 42
  assert s.queue_len("q") == 42
  qb.queue_len.assert_called_once_with("q:w1")


def test_clear_clears_own_queue_only():
  s, qb = _strategy(worker_id="w1", peer_ids=("w2",))
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
  with pytest.raises(ValueError, match="steal_timeout must be >= 0"):
    WorkStealingQueueStrategy(cm, steal_timeout=-0.1)
