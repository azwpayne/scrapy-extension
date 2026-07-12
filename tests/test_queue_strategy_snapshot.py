"""Tests for QueueStrategy snapshot/restore (initiative #3) + BackendQueue wiring.

Covers:
- Delay snapshot serializes the held heap (versioned JSON, seq excluded)
- Delay restore round-trips, skips corrupt/unknown-format/malformed entries
- Restored past-ready items drain on the next pop
- ABC defaults (passthrough snapshot -> None, restore no-op)
- BackendQueue.close() persists BEFORE strategy.close() clears the heap
- BackendQueue.__init__ restores on construction
- Storage-incapable backends skip gracefully (no crash)
"""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy
from scrapy_extension.queue.strategies.passthrough import PassthroughQueueStrategy

_SNAPSHOT_KEY = "queue:snapshot:q"


def _delay(*, clock_value: float = 100.0, default_delay: float = 0.0) -> DelayQueueStrategy:
  """DelayQueueStrategy with a frozen clock (deterministic ready_at)."""
  return DelayQueueStrategy(
    MagicMock(name="ConnectionManager"),
    default_delay=default_delay,
    clock=lambda: clock_value,
  )


def _strip_seq(heap: list) -> list:
  """Compare heaps modulo the seq tie-breaker (re-sequenced on restore)."""
  return sorted((ready_at, item, priority) for ready_at, _s, item, priority in heap)


# ---------------------------------------------------------------------------
# Delay snapshot
# ---------------------------------------------------------------------------


def test_delay_snapshot_empty_returns_none():
  """An empty held heap snapshots to None (nothing to persist)."""
  assert _delay().snapshot() is None


def test_delay_snapshot_serializes_held_items():
  """A non-empty heap serializes to a versioned JSON bytes blob."""
  strategy = _delay(clock_value=100.0)
  strategy.push("q", b"item-1", delay=10.0, priority=2.0)
  strategy.push("q", b"item-2", delay=20.0, priority=1.0)

  blob = strategy.snapshot()
  assert blob is not None
  data = json.loads(blob.decode("utf-8"))
  assert data["version"] == 1
  assert data["strategy"] == "delay"
  assert len(data["items"]) == 2
  by_ready = {item["ready_at"]: item for item in data["items"]}
  assert set(by_ready) == {110.0, 120.0}
  assert base64.b64decode(by_ready[110.0]["item_b64"]) == b"item-1"
  assert base64.b64decode(by_ready[120.0]["item_b64"]) == b"item-2"
  assert by_ready[110.0]["priority"] == 2.0
  assert by_ready[120.0]["priority"] == 1.0


def test_delay_snapshot_excludes_seq():
  """The seq tie-breaker is NOT persisted (re-sequenced fresh on restore)."""
  strategy = _delay()
  strategy.push("q", b"x", delay=5.0)

  data = json.loads(strategy.snapshot().decode("utf-8"))
  assert "seq" not in data["items"][0]


# ---------------------------------------------------------------------------
# Delay restore
# ---------------------------------------------------------------------------


def test_delay_restore_round_trip_preserves_state():
  """snapshot -> restore on a fresh strategy reproduces the heap (modulo seq)."""
  src = _delay(clock_value=100.0)
  src.push("q", b"a", delay=10.0, priority=1.0)
  src.push("q", b"b", delay=20.0, priority=2.0)

  dst = _delay(clock_value=100.0)
  dst.restore(src.snapshot())

  assert _strip_seq(dst._holding) == [(110.0, b"a", 1.0), (120.0, b"b", 2.0)]


def test_delay_restore_none_is_noop():
  """restore(None) and restore(b'') are silent no-ops."""
  strategy = _delay()
  strategy.restore(None)
  strategy.restore(b"")
  assert strategy._holding == []


def test_delay_restore_corrupt_json_skipped():
  """restore of non-JSON bytes logs + skips (no crash, heap stays empty)."""
  strategy = _delay()
  strategy.restore(b"\x00 not json \x00")
  assert strategy._holding == []


def test_delay_restore_unknown_format_skipped():
  """restore of valid JSON with the wrong strategy/version is skipped."""
  strategy = _delay()
  blob = json.dumps({"version": 99, "strategy": "other", "items": []}).encode()
  strategy.restore(blob)
  assert strategy._holding == []


def test_delay_restore_skips_malformed_entries():
  """A snapshot with one good + one bad entry recovers the good one only."""
  strategy = _delay()
  good = {
    "ready_at": 110.0,
    "item_b64": base64.b64encode(b"ok").decode(),
    "priority": 1.0,
  }
  bad = {"ready_at": "not-a-float", "item_b64": "!!!"}
  blob = json.dumps({"version": 1, "strategy": "delay", "items": [good, bad]}).encode()

  strategy.restore(blob)

  assert len(strategy._holding) == 1
  assert strategy._holding[0][2] == b"ok"  # item bytes recovered


def test_delay_restore_past_ready_drains_on_pop():
  """Restored items with past ready_at drain into the live queue on the next pop."""
  src = _delay(clock_value=100.0)
  src.push("q", b"due", delay=10.0)  # ready_at = 110
  blob = src.snapshot()

  dst = _delay(clock_value=200.0)  # clock now past ready_at -> item is due
  dst.restore(blob)

  dst.pop("q")  # drain fires

  qb = dst._connection_manager.get_queue_backend()
  qb.push.assert_called_once_with("q", b"due", 0.0)


# ---------------------------------------------------------------------------
# ABC defaults
# ---------------------------------------------------------------------------


def test_passthrough_snapshot_returns_none_default():
  """The ABC default snapshot() returns None (passthrough has no state)."""
  assert PassthroughQueueStrategy(MagicMock()).snapshot() is None


def test_abc_restore_default_is_noop():
  """The ABC default restore() accepts any state without crashing."""
  strategy = PassthroughQueueStrategy(MagicMock())
  strategy.restore(b"anything")
  strategy.restore(None)


# ---------------------------------------------------------------------------
# BackendQueue wiring (close persists, init restores, storage-incapable skips)
# ---------------------------------------------------------------------------


def _storage_mock(retrieve_return=None):
  storage = MagicMock(name="StorageBackend")
  storage.retrieve.return_value = retrieve_return
  return storage


def _wired_cm(storage=None, queue_backend=None):
  cm = MagicMock(name="ConnectionManager")
  cm.get_storage_backend.return_value = storage if storage is not None else _storage_mock()
  cm.get_queue_backend.return_value = queue_backend or MagicMock(name="QueueBackend")
  return cm


def test_backends_queue_close_persists_snapshot_before_clearing():
  """close() snapshots the held heap THEN strategy.close() clears it.

  Regression guard: the snapshot must capture state BEFORE Delay.close()
  clears _holding, or the persisted blob is empty.
  """
  storage = _storage_mock()
  cm = _wired_cm(storage=storage)
  strategy = _delay(clock_value=100.0)
  strategy.push("q", b"x", delay=10.0)
  bq = BackendQueue(
    connection_manager=cm, queue_name="q", queue_strategy=strategy, monitor=MagicMock()
  )

  bq.close()

  storage.store.assert_called_once()
  args = storage.store.call_args.args
  assert args[0] == _SNAPSHOT_KEY
  assert json.loads(args[1].decode())["items"][0]["item_b64"]


def test_backends_queue_init_restores_snapshot():
  """BackendQueue.__init__ retrieves the snapshot + strategy.restore runs."""
  src = _delay(clock_value=100.0)
  src.push("q", b"recovered", delay=10.0)
  blob = src.snapshot()

  storage = _storage_mock(retrieve_return=blob)
  cm = _wired_cm(storage=storage)
  strategy = _delay(clock_value=100.0)

  BackendQueue(
    connection_manager=cm, queue_name="q", queue_strategy=strategy, monitor=MagicMock()
  )

  assert len(strategy._holding) == 1
  assert strategy._holding[0][2] == b"recovered"
  storage.retrieve.assert_called_once_with(_SNAPSHOT_KEY)


def test_backends_queue_close_skips_when_strategy_has_no_state():
  """Passthrough (snapshot -> None) skips the storage.store call entirely."""
  storage = _storage_mock()
  cm = _wired_cm(storage=storage)
  bq = BackendQueue(connection_manager=cm, queue_name="q", monitor=MagicMock())

  bq.close()

  storage.store.assert_not_called()


def test_backends_queue_storage_incapable_skips_cleanly():
  """A storage-incapable backend (NotImplementedError) skips snapshot, no crash."""
  cm = MagicMock(name="ConnectionManager")
  cm.get_storage_backend.side_effect = NotImplementedError("no storage")
  cm.get_queue_backend.return_value = MagicMock()
  strategy = _delay(clock_value=100.0)
  strategy.push("q", b"x", delay=10.0)

  bq = BackendQueue(
    connection_manager=cm, queue_name="q", queue_strategy=strategy, monitor=MagicMock()
  )
  bq.close()  # _restore_snapshot (init) + _persist_snapshot (close) both skip


def test_backends_queue_init_skips_when_cm_has_no_storage_attr():
  """A connection manager without ``get_storage_backend`` skips snapshot, no crash.

  Regression: test stubs (e.g. ``_NullConnectionManager``) may not expose the
  storage interface at all. ``getattr(..., None)`` must short-circuit rather
  than ``AttributeError``.
  """

  class _NoStorageCM:
    pass

  strategy = _delay(clock_value=100.0)
  strategy.push("q", b"x", delay=10.0)
  # init + close both touch the (absent) storage interface — must not raise:
  bq = BackendQueue(
    connection_manager=_NoStorageCM(),  # type: ignore[arg-type]
    queue_name="q",
    queue_strategy=strategy,
    monitor=MagicMock(),
  )
  bq.close()


def test_backends_queue_restore_skips_non_bytes_state():
  """A non-bytes retrieve result (e.g. a mock) is skipped, not passed to restore.

  Regression: an auto-mocked connection manager's ``retrieve`` returns a Mock,
  not bytes — ``isinstance(state, (bytes, bytearray))`` must guard before
  ``strategy.restore()`` or ``json.loads`` raises TypeError.
  """
  cm = MagicMock(name="ConnectionManager")
  # retrieve returns a Mock (not None, not bytes) — simulating an auto-mock CM:
  cm.get_storage_backend.return_value.retrieve.return_value = MagicMock(name="not-bytes")
  cm.get_queue_backend.return_value = MagicMock()
  strategy = _delay(clock_value=100.0)

  BackendQueue(
    connection_manager=cm, queue_name="q", queue_strategy=strategy, monitor=MagicMock()
  )
  # No crash, and strategy.restore was never handed the non-bytes value (held heap empty):
  assert strategy._holding == []


def test_backends_queue_init_restore_crash_does_not_break_startup():
  """Regression: a strategy.restore() that raises (buggy third-party strategy,
  or a snapshot from an incompatible version) must NOT crash BackendQueue.__init__.

  _restore_snapshot's docstring promises 'never crashes startup'; pre-fix the
  restore() call was the one operation in the method NOT wrapped in try/except
  (snapshot/get_storage/retrieve/store all were). The bundled strategies
  (delay/time_wheel/ring_buffer) already catch their own decode errors, so the
  live risk is a third-party strategy with an un-hardened restore().
  """
  storage = _storage_mock(retrieve_return=b'{"some":"prior"}')
  cm = _wired_cm(storage=storage)
  strategy = _delay(clock_value=100.0)

  def _raise(_state):
    raise RuntimeError("third-party restore bug")

  strategy.restore = _raise  # type: ignore[assignment]

  # Must NOT raise — the restore() crash is logged + swallowed.
  BackendQueue(
    connection_manager=cm, queue_name="q", queue_strategy=strategy, monitor=MagicMock()
  )


# ---------------------------------------------------------------------------
# Spider-scoped snapshot key (initiative #16)
# ---------------------------------------------------------------------------


def _make_queue_for_key(spider=None, queue_name="jobs"):
  """Minimal BackendQueue for snapshot-key unit tests.

  ``connection_manager`` is a spec-empty Mock so the queue can be constructed
  without touching a real backend; only ``_snapshot_key()`` is exercised (a
  pure derivation over ``self._spider`` / ``self.queue_name``).
  """
  return BackendQueue(
    connection_manager=MagicMock(spec=[]),
    queue_name=queue_name,
    spider=spider,
  )


def test_snapshot_key_includes_spider_name():
  """Two queues with the same queue_name but different spiders MUST produce
  different snapshot keys (initiative #16: cross-spider snapshot isolation).

  Regression: prior to #16 the key was ``<prefix><queue_name>`` only, so two
  spiders sharing a storage backend (multi-spider in one process, or
  multi-worker with shared Redis/Mongo/ES) overwrote each other's strategy
  snapshot on close — and on restart the survivor restored the wrong spider's
  Delay heap.
  """
  spider_a = SimpleNamespace(name="spiderA")
  spider_b = SimpleNamespace(name="spiderB")

  key_a = _make_queue_for_key(spider=spider_a, queue_name="jobs")._snapshot_key()
  key_b = _make_queue_for_key(spider=spider_b, queue_name="jobs")._snapshot_key()

  assert key_a != key_b
  assert "spiderA" in key_a
  assert "spiderB" in key_b
  assert key_a.endswith(":jobs")
  assert key_b.endswith(":jobs")


def test_snapshot_key_without_spider_preserves_legacy_shape():
  """A queue constructed without a spider keeps the pre-#16 key
  ``queue:snapshot:<queue_name>`` — backward-compat for the no-spider
  construction path used by the rest of this test module and by test stubs.
  """
  key = _make_queue_for_key(spider=None, queue_name="jobs")._snapshot_key()
  assert key == "queue:snapshot:jobs"


def test_snapshot_key_spider_without_name_attr_falls_back():
  """A spider-like object without a ``name`` attribute falls back to the
  queue_name-only key rather than raising ``AttributeError``.

  Mirrors the defensive ``getattr`` chaining already used at
  ``queue.py:561`` (``getattr(self._spider, "crawler", None)``).
  """
  key = _make_queue_for_key(spider=SimpleNamespace(), queue_name="jobs")._snapshot_key()
  assert key == "queue:snapshot:jobs"
