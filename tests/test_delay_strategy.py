"""Tests for DelayQueueStrategy (subsystem ②)."""

from __future__ import annotations

import json
import logging
import threading

import pytest

from scrapy_extension.queue.strategies.delay import (
  DEFAULT_DELAY_MAX_HELD,
  DelayQueueStrategy,
)


def _clock(now: list[float]):
  """Return a clock callable backed by a mutable single-element list."""
  return lambda: now[0]


class TestDelayQueueStrategy:
  def test_push_holds_until_ready(self, mock_connection_manager) -> None:
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, clock=_clock(now)
    )
    strat.push("q", b"x")
    # Held — not yet in the live queue.
    assert len(strat._holding) == 1
    mock_connection_manager.get_queue_backend().push.assert_not_called()

  def test_pop_with_ack_threads_mq_token(self, mock_connection_manager, mocker) -> None:
    # delay.pop_with_ack must delegate to _pop_backend_with_ack (which threads
    # the MQ per-message ack token) -- pre-fix the inherited base default
    # dropped it and silently fell back to atomic pop() (at-most-once for MQ
    # backends).
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=0.0, clock=_clock(now)
    )
    deleg = mocker.patch.object(
      strat, "_pop_backend_with_ack", return_value=(b"item", "delay-token")
    )
    data, token = strat.pop_with_ack("q")
    assert (data, token) == (b"item", "delay-token")
    deleg.assert_called_once_with("q", 0.0)

  def test_close_with_held_items_warns_and_clears(
    self, mock_connection_manager, caplog
  ) -> None:
    """close() must emit a WARNING naming the held-item count, then clear."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, clock=_clock(now)
    )
    strat.push("q", b"a")
    strat.push("q", b"b")
    strat.push("q", b"c")
    assert len(strat._holding) == 3

    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.close()

    assert len(strat._holding) == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "DelayQueueStrategy" in msg
    assert "3" in msg  # held-item count

  def test_close_empty_is_quiet(self, mock_connection_manager, caplog) -> None:
    """close() with an empty holding list must emit NO warning."""
    strat = DelayQueueStrategy(mock_connection_manager)
    assert len(strat._holding) == 0

    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.close()

    assert len(strat._holding) == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []

  @pytest.mark.parametrize(
    "default_delay",
    [True, -0.01, -1.0, float("nan"), float("inf"), float("-inf")],
  )
  def test_invalid_delay_raises(
    self, mock_connection_manager, default_delay: float
  ) -> None:
    with pytest.raises(ValueError, match="default_delay"):
      DelayQueueStrategy(mock_connection_manager, default_delay=default_delay)

  @pytest.mark.parametrize("delay", [float("nan"), float("inf"), float("-inf")])
  def test_non_finite_push_delay_is_rejected(
    self, mock_connection_manager, delay: float
  ) -> None:
    strat = DelayQueueStrategy(mock_connection_manager)

    with pytest.raises(ValueError, match="delay"):
      strat.push("q", b"x", delay=delay)

    assert strat._holding == []
    mock_connection_manager.get_queue_backend().push.assert_not_called()

  @pytest.mark.parametrize("delay", [True, -1.0])
  def test_bool_and_negative_push_delay_are_rejected(
    self, mock_connection_manager, delay: float
  ) -> None:
    strat = DelayQueueStrategy(mock_connection_manager, default_delay=1.0)

    with pytest.raises(ValueError, match="delay"):
      strat.push("q", b"x", delay=delay)

    assert strat._holding == []
    mock_connection_manager.get_queue_backend.assert_not_called()

  def test_bool_max_held_is_rejected(self, mock_connection_manager) -> None:
    with pytest.raises(ValueError, match="max_held"):
      DelayQueueStrategy(mock_connection_manager, max_held=True)

  @pytest.mark.parametrize("priority", [float("nan"), float("inf"), float("-inf")])
  def test_non_finite_priority_is_rejected(
    self, mock_connection_manager, priority: float
  ) -> None:
    strat = DelayQueueStrategy(mock_connection_manager, default_delay=1.0)

    with pytest.raises(ValueError, match="priority"):
      strat.push("q", b"x", priority=priority)

    assert strat._holding == []

  @pytest.mark.parametrize("clock_value", [float("nan"), float("inf")])
  def test_non_finite_clock_cannot_create_held_item(
    self, mock_connection_manager, clock_value: float
  ) -> None:
    strat = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=1.0,
      clock=lambda: clock_value,
    )

    with pytest.raises(ValueError, match="clock"):
      strat.push("q", b"x")

    assert strat._holding == []

  def test_default_max_held_threshold(self, mock_connection_manager) -> None:
    """Constructor ships a 100k default soft cap on the holding heap (SPEC U5)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    assert strat._max_held == DEFAULT_DELAY_MAX_HELD == 100_000

  def test_soft_cap_warns_once_when_exceeded(
    self, mock_connection_manager, caplog
  ) -> None:
    """Holding >max_held items fires ONE warning; further pushes stay quiet."""
    import scrapy_extension.queue.strategies.delay as mod

    mod._over_cap_warned = False  # reset module-level flag for a clean slate
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=3, clock=_clock(now)
    )
    # Fill up to the cap — no warning yet.
    strat.push("q", b"a")
    strat.push("q", b"b")
    strat.push("q", b"c")
    assert len(strat._holding) == 3

    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.push("q", b"d")  # exceeds cap → warn
      strat.push("q", b"e")  # still over → no second warning (warn-once)
      strat.push("q", b"f")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    # Exactly one warning (the soft-cap one); close() would add another but is not called here.
    cap_warnings = [
      w
      for w in warnings
      if "max_held" in w.getMessage() or "delay" in w.getMessage().lower()
    ]
    assert len(cap_warnings) == 1
    msg = cap_warnings[0].getMessage()
    # Warn points at the unbounded-growth risk + distributed-delay roadmap (U10).
    assert "max_held" in msg or "holding" in msg

  def test_soft_cap_does_not_block_push(self, mock_connection_manager, caplog) -> None:
    """The cap is a SOFT cap (warn-only) — push still succeeds past the cap."""
    import scrapy_extension.queue.strategies.delay as mod

    mod._over_cap_warned = False
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=1, clock=_clock(now)
    )
    strat.push("q", b"a")
    strat.push("q", b"b")  # over cap
    strat.push("q", b"c")  # further over
    # Nothing dropped: soft cap warns but never refuses items.
    assert len(strat._holding) == 3

  def test_explicit_max_held_zero_disables_warning(
    self, mock_connection_manager, caplog
  ) -> None:
    """max_held<=0 disables the soft-cap warning (explicit opt-out for advanced users)."""
    import scrapy_extension.queue.strategies.delay as mod

    mod._over_cap_warned = False
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, max_held=0, clock=_clock(now)
    )
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      for i in range(10):
        strat.push("q", str(i).encode())
    cap_warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING
      and ("max_held" in r.getMessage() or "holding" in r.getMessage())
    ]
    assert cap_warnings == []

  @pytest.mark.parametrize("max_held", [0, -1])
  def test_invalid_max_held_disables(
    self, mock_connection_manager, max_held: int
  ) -> None:
    """Non-positive max_held is accepted (= disabled), per opt-out contract."""
    strat = DelayQueueStrategy(mock_connection_manager, max_held=max_held)
    assert strat._max_held == max_held

  # ----- R14-F: priority must survive the delay drain -----

  def test_drain_retains_explicit_priority(self, mock_connection_manager) -> None:
    """R14-F HIGH: a delayed item pushed with ``priority=`` must re-enter the
    live queue at that priority when drained — not silently land at 0.

    Regression guard for the dropped-on-drain priority-inversion bug: the
    holding heap tuple gains a priority slot, and ``_drain_ready`` re-passes
    it to ``qb.push``. Before the fix, ``qb.push(queue_name, item)`` was
    called with no ``priority=`` so every delayed item drained at priority 0.
    """
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=10.0, clock=_clock(now)
    )
    strat.push("q", b"x", priority=10.0)
    assert len(strat._holding) == 1
    # Advance clock past ready_at and pop → triggers _drain_ready.
    now[0] = 111.0
    strat.pop("q")
    # Backend push must carry the original priority, not the default 0.
    mock_connection_manager.get_queue_backend().push.assert_called_once()
    call = mock_connection_manager.get_queue_backend().push.call_args
    assert call.args[0] == "q"
    assert call.args[1] == b"x"
    # Either positional (priority as 3rd arg) or keyword — assert it's 10.0.
    priority_arg = (
      call.args[2] if len(call.args) > 2 else call.kwargs.get("priority", 0.0)
    )
    assert priority_arg == 10.0, (
      f"delayed item drained at priority {priority_arg} instead of 10.0 "
      "(silent priority inversion — R14-F HIGH regression)"
    )

  def test_drain_retains_priority_kwarg_form(self, mock_connection_manager) -> None:
    """R14-F: priority is re-passed as a keyword to the backend push (robust
    to either positional or kwarg call shape; pins the explicit ``priority=``
    pass-through so a future refactor doesn't silently drop it again)."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=5.0, clock=_clock(now)
    )
    strat.push("q", b"y", priority=7.5)
    now[0] = 200.0  # well past ready_at
    strat.pop("q")
    call = mock_connection_manager.get_queue_backend().push.call_args
    # Keyword form preferred (matches the existing live-push call shape).
    assert (
      call.kwargs.get("priority", call.args[2] if len(call.args) > 2 else 0.0) == 7.5
    )

  def test_drain_priority_defaults_to_zero_when_unspecified(
    self, mock_connection_manager
  ) -> None:
    """R14-F backward-compat: a delayed item pushed with no explicit priority
    still drains at priority 0 (the pre-fix default). Existing delay callers
    that never set ``priority=`` must not observe any behavior change."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager, default_delay=1.0, clock=_clock(now)
    )
    strat.push("q", b"z")  # no priority= → default 0.0
    now[0] = 102.0
    strat.pop("q")
    call = mock_connection_manager.get_queue_backend().push.call_args
    priority_arg = (
      call.args[2] if len(call.args) > 2 else call.kwargs.get("priority", 0.0)
    )
    assert priority_arg == 0.0

  def test_failed_drain_keeps_due_item_for_retry(self, mock_connection_manager) -> None:
    """A transient live-queue push failure must not discard a due item."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=1.0,
      clock=_clock(now),
    )
    strat.push("q", b"retry-me")
    now[0] = 101.0
    backend = mock_connection_manager.get_queue_backend()
    backend.push.side_effect = [RuntimeError("temporary"), None]

    with pytest.raises(RuntimeError, match="temporary"):
      strat.pop("q")
    assert len(strat._holding) == 1

    strat.pop("q")
    assert len(strat._holding) == 0
    assert backend.push.call_count == 2

  def test_concurrent_drains_do_not_duplicate_due_item(
    self, mock_connection_manager
  ) -> None:
    """Only one pop may transfer a given due item to the live queue."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=1.0,
      clock=_clock(now),
    )
    strat.push("q", b"once")
    now[0] = 101.0

    clock_calls = 0
    clock_calls_lock = threading.Lock()
    second_drain_observed = threading.Event()

    def concurrent_clock() -> float:
      nonlocal clock_calls
      with clock_calls_lock:
        clock_calls += 1
        if clock_calls == 2:
          second_drain_observed.set()
      return now[0]

    strat._clock = concurrent_clock
    backend = mock_connection_manager.get_queue_backend()
    first_push_entered = threading.Event()
    release_first_push = threading.Event()

    def blocking_push(*_args) -> None:
      first_push_entered.set()
      if not release_first_push.wait(timeout=2.0):
        raise AssertionError("first due-item transfer was not released")

    backend.push.side_effect = blocking_push
    errors: list[BaseException] = []

    def pop_once() -> None:
      try:
        strat.pop("q")
      except BaseException as exc:  # noqa: BLE001 - capture thread failures
        errors.append(exc)

    first = threading.Thread(target=pop_once, daemon=True)
    second = threading.Thread(target=pop_once, daemon=True)
    first.start()
    assert first_push_entered.wait(timeout=2.0)
    second.start()

    try:
      assert not second_drain_observed.wait(timeout=0.2), (
        "a second drain observed the same heap head while its transfer was in flight"
      )
    finally:
      release_first_push.set()
      first.join(timeout=2.0)
      second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert backend.push.call_count == 1
    assert strat._holding == []

  def test_snapshot_cannot_duplicate_item_during_live_transfer(
    self, mock_connection_manager
  ) -> None:
    """A snapshot observes one side of the held-to-live commit, never both."""
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=1.0,
      clock=_clock(now),
    )
    strat.push("q", b"moving")
    now[0] = 101.0
    backend = mock_connection_manager.get_queue_backend()
    transfer_entered = threading.Event()
    release_transfer = threading.Event()
    snapshot_done = threading.Event()
    snapshots: list[bytes | None] = []
    errors: list[BaseException] = []

    def blocking_push(*_args) -> None:
      transfer_entered.set()
      if not release_transfer.wait(timeout=2.0):
        raise AssertionError("due-item transfer was not released")

    backend.push.side_effect = blocking_push

    def pop_once() -> None:
      try:
        strat.pop("q")
      except BaseException as exc:  # noqa: BLE001 - capture thread failures
        errors.append(exc)

    def take_snapshot() -> None:
      try:
        snapshots.append(strat.snapshot())
      except BaseException as exc:  # noqa: BLE001 - capture thread failures
        errors.append(exc)
      finally:
        snapshot_done.set()

    pop_thread = threading.Thread(target=pop_once, daemon=True)
    snapshot_thread = threading.Thread(target=take_snapshot, daemon=True)
    pop_thread.start()
    assert transfer_entered.wait(timeout=2.0)
    snapshot_thread.start()

    try:
      assert not snapshot_done.wait(timeout=0.2)
    finally:
      release_transfer.set()
      pop_thread.join(timeout=2.0)
      snapshot_thread.join(timeout=2.0)

    assert not pop_thread.is_alive()
    assert not snapshot_thread.is_alive()
    assert errors == []
    assert snapshots == [None]


class TestSnapshotRestore:
  """Snapshot/restore (initiative #3): restore must be robust to every
  corrupt/malformed snapshot shape — it logs + skips, never crashes the spider.

  Covers the full ``restore()`` defensive surface (delay.py lines 272-325):
  empty state, corrupt JSON, unknown format, items-not-a-list, non-dict entry,
  malformed entry fields, zero-recovered.
  """

  def test_snapshot_empty_returns_none(self, mock_connection_manager) -> None:
    """An empty holding heap snapshots to None (no state to persist)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    assert strat.snapshot() is None

  def test_snapshot_roundtrip_restores_held_items(
    self, mock_connection_manager
  ) -> None:
    """Happy path: snapshot a strategy with held items, restore into a fresh
    strategy → the held items are recovered (covers the ``if recovered`` info
    branch + the well-formed-entry path)."""
    strat = DelayQueueStrategy(mock_connection_manager, default_delay=10.0)
    strat.push("q", b"item-a", priority=1.0)
    strat.push("q", b"item-b", priority=2.0)
    assert len(strat._holding) == 2
    state = strat.snapshot()
    assert state is not None

    fresh = DelayQueueStrategy(mock_connection_manager)
    fresh.restore(state)
    assert len(fresh._holding) == 2
    recovered_items = {entry[2] for entry in fresh._holding}
    assert recovered_items == {b"item-a", b"item-b"}

  def test_restore_atomically_replaces_existing_state_and_is_idempotent(
    self, mock_connection_manager
  ) -> None:
    source = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=10.0,
      clock=lambda: 100.0,
      wall_clock=lambda: 1_000.0,
    )
    source.push("q", b"restored")
    state = source.snapshot()

    target = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=10.0,
      clock=lambda: 200.0,
      wall_clock=lambda: 1_000.0,
    )
    target.push("q", b"pre-existing")

    target.restore(state)
    target.restore(state)

    assert [entry[2] for entry in target._holding] == [b"restored"]

  def test_restore_v1_due_items_preserves_original_deadline_order(
    self, mock_connection_manager
  ) -> None:
    state = json.dumps(
      {
        "version": 1,
        "strategy": "delay",
        # A legacy heap array is not globally sorted beyond its root.
        "items": [
          {"ready_at": 10.0, "item_b64": "Zmlyc3Q=", "priority": 0.0},
          {"ready_at": 30.0, "item_b64": "dGhpcmQ=", "priority": 0.0},
          {"ready_at": 20.0, "item_b64": "c2Vjb25k", "priority": 0.0},
        ],
      }
    ).encode()
    strat = DelayQueueStrategy(mock_connection_manager, clock=lambda: 100.0)

    strat.restore(state)
    strat.pop("q")

    backend = mock_connection_manager.get_queue_backend.return_value
    pushed = [call.args[1] for call in backend.push.call_args_list]
    assert pushed == [b"first", b"second", b"third"]

  def test_restore_none_is_noop(self, mock_connection_manager) -> None:
    """restore(None) and restore(b'') return without touching the heap."""
    strat = DelayQueueStrategy(mock_connection_manager)
    strat.restore(None)
    strat.restore(b"")
    assert len(strat._holding) == 0

  def test_restore_corrupt_json_warns_and_starts_clean(
    self, mock_connection_manager, caplog
  ) -> None:
    """Non-JSON / non-UTF-8 bytes → warning + clean start (no crash)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(b"\xff\xfe not valid json")
    assert any("corrupt snapshot" in r.message for r in caplog.records)
    assert len(strat._holding) == 0

  def test_restore_unknown_format_warns(self, mock_connection_manager, caplog) -> None:
    """Valid JSON but wrong strategy/version → unknown-format warning."""
    strat = DelayQueueStrategy(mock_connection_manager)
    bogus = json.dumps({"version": 99, "strategy": "other", "items": []}).encode()
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(bogus)
    assert any("unknown snapshot format" in r.message for r in caplog.records)

  def test_restore_items_not_a_list_warns(
    self, mock_connection_manager, caplog
  ) -> None:
    """``items`` present but not a list → warning + clean start."""
    strat = DelayQueueStrategy(mock_connection_manager)
    bogus = json.dumps(
      {"version": 1, "strategy": "delay", "items": "not-a-list"}
    ).encode()
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(bogus)
    assert any("'items' not a list" in r.message for r in caplog.records)

  def test_restore_non_dict_entry_skipped(self, mock_connection_manager) -> None:
    """A non-dict entry in ``items`` is silently skipped (the ``continue``)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    items = ["not-a-dict", 42, None]  # all non-dict → all skipped
    bogus = json.dumps({"version": 1, "strategy": "delay", "items": items}).encode()
    strat.restore(bogus)
    assert len(strat._holding) == 0

  def test_restore_malformed_entry_skipped_with_warning(
    self, mock_connection_manager, caplog
  ) -> None:
    """An entry missing required keys / wrong types → warning + skip."""
    strat = DelayQueueStrategy(mock_connection_manager)
    items = [
      {"ready_at": 1.0},  # missing item_b64 + priority
      {"ready_at": "not-a-float", "item_b64": "Yg==", "priority": 1.0},
    ]
    bogus = json.dumps({"version": 1, "strategy": "delay", "items": items}).encode()
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(bogus)
    assert any("malformed entry" in r.message for r in caplog.records)
    assert len(strat._holding) == 0  # both entries malformed → none recovered

  def test_restore_skips_non_finite_priority(
    self, mock_connection_manager, caplog
  ) -> None:
    strat = DelayQueueStrategy(
      mock_connection_manager,
      clock=lambda: 10.0,
      wall_clock=lambda: 100.0,
    )
    state = json.dumps(
      {
        "version": 2,
        "strategy": "delay",
        "snapshot_wall_time": 100.0,
        "items": [
          {
            "remaining": 1.0,
            "item_b64": "eA==",
            "priority": "nan",
          }
        ],
      }
    ).encode()

    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(state)

    assert strat._holding == []
    assert any("malformed entry" in r.message for r in caplog.records)

  def test_restore_zero_recovered_no_info_log(
    self, mock_connection_manager, caplog
  ) -> None:
    """When 0 items recover (all entries skipped), the recovery info-log is
    NOT emitted (covers the ``if recovered`` False branch)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    items = ["non-dict-entry"]  # skipped → recovered stays 0
    bogus = json.dumps({"version": 1, "strategy": "delay", "items": items}).encode()
    with caplog.at_level(
      logging.INFO, logger="scrapy_extension.queue.strategies.delay"
    ):
      strat.restore(bogus)
    assert not any("recovered" in r.message for r in caplog.records)

  def test_push_with_delay_emits_on_delay_depth(
    self, mock_connection_manager, mocker
  ) -> None:
    """Risk 3: a held item emits on_delay_depth(held_count) for operability."""
    from scrapy_extension.monitor.base import Monitor

    monitor = mocker.Mock(spec=Monitor)
    now = [100.0]
    strat = DelayQueueStrategy(
      mock_connection_manager,
      default_delay=10.0,
      clock=_clock(now),
      monitor=monitor,
    )
    strat.push("q", b"x")
    # Held item → on_delay_depth fires with the held count.
    monitor.on_delay_depth.assert_called_once_with(1)
