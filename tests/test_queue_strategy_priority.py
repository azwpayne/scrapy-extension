"""Tests for PriorityQueueStrategy — strategy-layer priority via N physical buckets.

Covers:
- Arbitrary signed Scrapy priority → level mapping
- Saturation at the configured high/low buckets
- Pop scans p0..p(N-1), returns first non-empty
- Pop falls through to blocking wait on level 0 when all empty + timeout > 0
- queue_len sums all levels
- clear clears all levels
- snapshot/restore ABC defaults (None / no-op — state is backend-side)
- Config validation: levels >= 1
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.priority import PriorityQueueStrategy


def _strategy(levels: int = 3) -> tuple[PriorityQueueStrategy, MagicMock]:
  """Build a strategy with a mocked ConnectionManager + QueueBackend."""
  cm = MagicMock(name="ConnectionManager")
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  return PriorityQueueStrategy(cm, levels=levels), qb


@pytest.mark.parametrize("backend_type", ["kafka", "rocketmq"])
def test_multi_topic_backends_are_rejected_before_bucket_scanning(backend_type):
  """A single consumer cannot preserve bucket isolation across topic switches."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = backend_type

  with pytest.raises(ConfigurationError, match="passthrough"):
    PriorityQueueStrategy(cm)

  cm.get_queue_backend.assert_not_called()


def test_pop_with_ack_threads_backend_token_from_first_non_empty_bucket():
  """#28: pop_with_ack scans buckets via backend.pop_with_ack and returns
  (data, token) so MQ backends paired with the priority strategy keep
  per-message ack correlation (previously pop() returned data only → the
  scheduler saw token=None → silent at-least-once hazard under MQ)."""
  s, qb = _strategy(levels=3)
  qb.pop_with_ack.return_value = (b"item", "TOKEN-P0")
  data, token = s.pop_with_ack("q", 0.0)
  assert data == b"item"
  assert token == "TOKEN-P0"
  # First non-empty bucket (p0, highest priority) — does not scan further.
  qb.pop_with_ack.assert_called_once_with(s._bucket_queue("q", 0), 0.0)


def test_pop_with_ack_binds_token_to_physical_bucket_and_issuer():
  """Terminal handling must preserve both backend incarnation and bucket."""
  from scrapy_extension.backends.base import QueueBackend
  from scrapy_extension.queue.strategies.base import _BoundQueueAckToken

  class _TokenBackend(QueueBackend):
    def __init__(self) -> None:
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
      return b"item", f"token:{queue_name}"

    def ack(self, queue_name: str, *, token: object | None = None) -> None:
      self.ack_calls.append((queue_name, token))

    def queue_len(self, queue_name: str) -> int:
      del queue_name
      return 0

    def clear_queue(self, queue_name: str) -> None:
      del queue_name

  backend = _TokenBackend()
  manager = MagicMock(name="ConnectionManager")
  manager.backend_type = "rabbitmq"
  manager.get_queue_backend.return_value = backend
  strategy = PriorityQueueStrategy(manager, levels=3)

  data, token = strategy.pop_with_ack("q")

  assert data == b"item"
  assert isinstance(token, _BoundQueueAckToken)
  assert token.backend is backend
  assert token.queue_name == "q:p0"
  token.ack()
  assert backend.ack_calls == [("q:p0", "token:q:p0")]


def test_pop_with_ack_uses_one_blocking_fallback_after_empty_scan():
  s, qb = _strategy(levels=3)
  qb.pop_with_ack.side_effect = [
    (None, None),
    (None, None),
    (None, None),
    (b"arrived", "token"),
  ]

  assert s.pop_with_ack("q", timeout=2.5) == (b"arrived", "token")
  assert qb.pop_with_ack.call_count == 4
  assert qb.pop_with_ack.call_args_list[-1].args == (
    s._bucket_queue("q", 0),
    2.5,
  )


# ---------------------------------------------------------------------------
# push — priority → level mapping
# ---------------------------------------------------------------------------


def test_push_highest_priority_goes_to_level_zero():
  """priority=1.0 → level 0 (popped first)."""
  s, qb = _strategy(levels=3)
  s.push("q", b"urgent", priority=1.0)
  qb.push.assert_called_once_with(s._bucket_queue("q", 0), b"urgent")


def test_push_default_priority_goes_to_middle_level():
  """Scrapy's default priority 0 maps to the neutral middle bucket."""
  s, qb = _strategy(levels=3)
  s.push("q", b"bulk", priority=0.0)
  qb.push.assert_called_once_with(s._bucket_queue("q", 1), b"bulk")


def test_push_mid_priority_goes_to_middle_level():
  """priority=0.5 → level 1 (middle of 3)."""
  s, qb = _strategy(levels=3)
  s.push("q", b"normal", priority=0.5)
  qb.push.assert_called_once_with(s._bucket_queue("q", 1), b"normal")


def test_push_priority_above_one_clamps_to_level_zero():
  """priority > 1.0 clamps to 1.0 → level 0."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x", priority=99.0)
  qb.push.assert_called_once_with(s._bucket_queue("q", 0), b"x")


def test_push_negative_priority_clamps_to_last_level():
  """priority < 0.0 clamps to 0.0 → level N-1."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x", priority=-5.0)
  qb.push.assert_called_once_with(s._bucket_queue("q", 2), b"x")


def test_omitted_priority_goes_to_middle_level():
  """Caller omitting priority uses Scrapy's neutral default 0."""
  s, qb = _strategy(levels=3)
  s.push("q", b"x")
  qb.push.assert_called_once_with(s._bucket_queue("q", 1), b"x")


def test_push_two_levels():
  """levels=2 splits at priority=0.5."""
  s, qb = _strategy(levels=2)
  s.push("q", b"high", priority=0.9)  # → level 0
  s.push("q", b"low", priority=0.1)  # → level 1
  assert qb.push.call_args_list[0].args == (s._bucket_queue("q", 0), b"high")
  assert qb.push.call_args_list[1].args == (s._bucket_queue("q", 1), b"low")


def test_scrapy_integer_priorities_are_centered_and_monotonic():
  """Scrapy priorities are arbitrary signed integers, with zero as default."""
  s, _ = _strategy(levels=3)

  assert s._level_for(10) == 0
  assert s._level_for(0) == 1
  assert s._level_for(-10) == 2
  assert [s._level_for(value) for value in (100, 1, 0, -1, -100)] == [
    0,
    0,
    1,
    2,
    2,
  ]


def test_derived_bucket_names_are_backend_portable_and_collision_resistant():
  """Strategy-created names must not introduce Kafka-invalid ``:`` separators."""
  from scrapy_extension.backends.base import _validate_key_name
  from scrapy_extension.backends.kafka import _validate_topic_name

  s, _ = _strategy(levels=3)
  names = {s._bucket_queue("jobs:tenant-a", level) for level in range(3)}

  assert len(names) == 3
  for name in names:
    _validate_key_name(name, "queue_name")
    _validate_topic_name(name)


# ---------------------------------------------------------------------------
# pop — scan high → low, blocking fallback
# ---------------------------------------------------------------------------


def test_pop_returns_from_highest_non_empty_level():
  """p0 empty, p1 has item → returns p1's item after scanning p0."""
  s, qb = _strategy(levels=3)
  qb.pop.side_effect = [None, b"from-p1", None]
  item = s.pop("q")
  assert item == b"from-p1"
  # Scanned p0 (None), then p1 (hit) — stopped, didn't check p2.
  assert qb.pop.call_count == 2
  assert qb.pop.call_args_list[0].args == (s._bucket_queue("q", 0), 0.0)
  assert qb.pop.call_args_list[1].args == (s._bucket_queue("q", 1), 0.0)


def test_compatible_backend_keeps_published_bucket_name():
  """A rolling upgrade reads and writes the existing ``q:pN`` queue in place."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  cm.get_queue_backend.return_value = qb
  qb.pop.return_value = b"legacy"
  s = PriorityQueueStrategy(cm, levels=3)

  assert s._bucket_queue("q", 0) == "q:p0"
  assert s.pop("q") == b"legacy"
  qb.pop.assert_called_once_with("q:p0", 0.0)


def test_pop_with_ack_from_published_bucket_preserves_token():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  qb.pop_with_ack.return_value = (b"legacy", "legacy-token")
  cm.get_queue_backend.return_value = qb
  s = PriorityQueueStrategy(cm, levels=1)

  assert s.pop_with_ack("q") == (b"legacy", "legacy-token")
  qb.pop_with_ack.assert_called_once_with("q:p0", 0.0)


def test_pop_never_probes_legacy_bucket_name_on_sqs():
  """Colon-based legacy names were invalid on SQS and must not be resolved."""
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "sqs"
  qb = MagicMock(name="QueueBackend")
  qb.pop.return_value = None
  cm.get_queue_backend.return_value = qb
  s = PriorityQueueStrategy(cm, levels=2)

  assert s.pop("q") is None
  assert [call.args[0] for call in qb.pop.call_args_list] == [
    s._bucket_queue("q", 0),
    s._bucket_queue("q", 1),
  ]


def test_rabbitmq_uses_portable_name_only_when_legacy_name_cannot_exist():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "rabbitmq"
  cm.get_queue_backend.return_value = MagicMock()
  s = PriorityQueueStrategy(cm, levels=1)
  overlong_queue = "q" * 253

  physical = s._bucket_queue(overlong_queue, 0)

  assert physical != f"{overlong_queue}:p0"
  assert len(physical.encode()) <= 255


def test_pop_returns_none_when_all_levels_empty_no_timeout():
  """All levels empty, timeout=0 → scans all levels, returns None."""
  s, qb = _strategy(levels=3)
  qb.pop.return_value = None
  assert s.pop("q") is None
  assert qb.pop.call_count == 3  # scanned all 3 levels non-blocking


def test_pop_blocking_fallback_on_level_zero_when_timeout():
  """All empty + timeout > 0 → non-blocking scan + 1 blocking pop on p0."""
  s, qb = _strategy(levels=3)
  qb.pop.side_effect = [None, None, None, b"arrived"]
  item = s.pop("q", timeout=5.0)
  assert item == b"arrived"
  # 3 non-blocking scans (all None) + 1 blocking pop on p0 with full timeout.
  assert qb.pop.call_count == 4
  assert qb.pop.call_args_list[3].args == (s._bucket_queue("q", 0), 5.0)


def test_pop_skip_blocking_when_scan_finds_item():
  """If the non-blocking scan finds an item, NO blocking pop follows."""
  s, qb = _strategy(levels=2)
  qb.pop.side_effect = [b"fast"]
  s.pop("q", timeout=10.0)
  assert qb.pop.call_count == 1  # found in p0, no blocking fallback


# ---------------------------------------------------------------------------
# queue_len, clear
# ---------------------------------------------------------------------------


def test_queue_len_sums_all_levels():
  s, qb = _strategy(levels=3)
  qb.queue_len.side_effect = [10, 20, 30]
  assert s.queue_len("q") == 60
  assert qb.queue_len.call_args_list[0].args == (s._bucket_queue("q", 0),)
  assert qb.queue_len.call_args_list[2].args == (s._bucket_queue("q", 2),)


def test_queue_len_and_clear_use_one_published_bucket_on_compatible_backend():
  cm = MagicMock(name="ConnectionManager")
  cm.backend_type = "redis"
  qb = MagicMock(name="QueueBackend")
  qb.queue_len.return_value = 5
  cm.get_queue_backend.return_value = qb
  s = PriorityQueueStrategy(cm, levels=1)

  assert s.queue_len("q") == 5
  s.clear("q")
  expected = ["q:p0"]
  assert [call.args[0] for call in qb.queue_len.call_args_list] == expected
  assert [call.args[0] for call in qb.clear_queue.call_args_list] == expected


def test_clear_clears_all_levels():
  s, qb = _strategy(levels=3)
  s.clear("q")
  assert qb.clear_queue.call_count == 3
  cleared = [c.args[0] for c in qb.clear_queue.call_args_list]
  assert cleared == [s._bucket_queue("q", level) for level in range(3)]


# ---------------------------------------------------------------------------
# snapshot / restore — ABC defaults (no in-process state)
# ---------------------------------------------------------------------------


def test_snapshot_returns_none():
  s, _ = _strategy()
  assert s.snapshot() is None


def test_restore_is_noop():
  s, _ = _strategy()
  s.restore(b"anything")  # no crash
  s.restore(None)


# ---------------------------------------------------------------------------
# config validation
# ---------------------------------------------------------------------------


def test_levels_zero_raises_value_error():
  with pytest.raises(ValueError, match="levels must be an integer"):
    _strategy(levels=0)


def test_levels_negative_raises_value_error():
  with pytest.raises(ValueError, match="levels must be an integer"):
    _strategy(levels=-1)


@pytest.mark.parametrize("levels", [True, 1.5, 257])
def test_levels_rejects_non_integer_and_unbounded_fanout(levels):
  with pytest.raises(ValueError, match="levels must be an integer"):
    _strategy(levels=levels)


def test_levels_one_routes_everything_to_p0():
  """Edge: levels=1 collapses to a single bucket — degenerate but valid."""
  s, qb = _strategy(levels=1)
  s.push("q", b"x", priority=1.0)
  s.push("q", b"y", priority=0.0)
  assert all(
    c.args == (s._bucket_queue("q", 0), expected)
    for c, expected in zip(qb.push.call_args_list, [b"x", b"y"], strict=True)
  )


@pytest.mark.parametrize("priority", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_priority_is_rejected(priority: float):
  s, qb = _strategy(levels=3)

  with pytest.raises(ValueError, match="priority must be finite"):
    s.push("q", b"item", priority=priority)

  qb.push.assert_not_called()
