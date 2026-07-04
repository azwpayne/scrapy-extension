"""Tests for DelayQueueStrategy (subsystem ②)."""

from __future__ import annotations

import json
import logging

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

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
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

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.close()

    assert len(strat._holding) == 0
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []

  @pytest.mark.parametrize("default_delay", [-0.01, -1.0])
  def test_invalid_delay_raises(self, mock_connection_manager, default_delay: float) -> None:
    with pytest.raises(ValueError, match="default_delay"):
      DelayQueueStrategy(mock_connection_manager, default_delay=default_delay)

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

    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.push("q", b"d")  # exceeds cap → warn
      strat.push("q", b"e")  # still over → no second warning (warn-once)
      strat.push("q", b"f")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    # Exactly one warning (the soft-cap one); close() would add another but is not called here.
    cap_warnings = [
      w for w in warnings if "max_held" in w.getMessage() or "delay" in w.getMessage().lower()
    ]
    assert len(cap_warnings) == 1
    msg = cap_warnings[0].getMessage()
    # Warn points at the unbounded-growth risk + distributed-delay roadmap (U10).
    assert "max_held" in msg or "holding" in msg

  def test_soft_cap_does_not_block_push(
    self, mock_connection_manager, caplog
  ) -> None:
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
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      for i in range(10):
        strat.push("q", str(i).encode())
    cap_warnings = [
      r for r in caplog.records
      if r.levelno == logging.WARNING and ("max_held" in r.getMessage() or "holding" in r.getMessage())
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
    priority_arg = call.args[2] if len(call.args) > 2 else call.kwargs.get("priority", 0.0)
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
    assert call.kwargs.get("priority", call.args[2] if len(call.args) > 2 else 0.0) == 7.5

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
    priority_arg = call.args[2] if len(call.args) > 2 else call.kwargs.get("priority", 0.0)
    assert priority_arg == 0.0


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
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.restore(b"\xff\xfe not valid json")
    assert any("corrupt snapshot" in r.message for r in caplog.records)
    assert len(strat._holding) == 0

  def test_restore_unknown_format_warns(
    self, mock_connection_manager, caplog
  ) -> None:
    """Valid JSON but wrong strategy/version → unknown-format warning."""
    strat = DelayQueueStrategy(mock_connection_manager)
    bogus = json.dumps({"version": 99, "strategy": "other", "items": []}).encode()
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
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
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.restore(bogus)
    assert any("'items' not a list" in r.message for r in caplog.records)

  def test_restore_non_dict_entry_skipped(
    self, mock_connection_manager
  ) -> None:
    """A non-dict entry in ``items`` is silently skipped (the ``continue``)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    items = ["not-a-dict", 42, None]  # all non-dict → all skipped
    bogus = json.dumps(
      {"version": 1, "strategy": "delay", "items": items}
    ).encode()
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
    bogus = json.dumps(
      {"version": 1, "strategy": "delay", "items": items}
    ).encode()
    with caplog.at_level(logging.WARNING, logger="scrapy_extension.queue.strategies.delay"):
      strat.restore(bogus)
    assert any("malformed entry" in r.message for r in caplog.records)
    assert len(strat._holding) == 0  # both entries malformed → none recovered

  def test_restore_zero_recovered_no_info_log(
    self, mock_connection_manager, caplog
  ) -> None:
    """When 0 items recover (all entries skipped), the recovery info-log is
    NOT emitted (covers the ``if recovered`` False branch)."""
    strat = DelayQueueStrategy(mock_connection_manager)
    items = ["non-dict-entry"]  # skipped → recovered stays 0
    bogus = json.dumps(
      {"version": 1, "strategy": "delay", "items": items}
    ).encode()
    with caplog.at_level(logging.INFO, logger="scrapy_extension.queue.strategies.delay"):
      strat.restore(bogus)
    assert not any("recovered" in r.message for r in caplog.records)
