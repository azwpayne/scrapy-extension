# R132 TASKS — concurrent-disconnect handle races

> Spec: [R132-concurrent-disconnect-handle-races-SPEC.md](R132-concurrent-disconnect-handle-races-SPEC.md)
> Plan: [R132-concurrent-disconnect-handle-races-PLAN.md](R132-concurrent-disconnect-handle-races-PLAN.md)

## Task 1 — rocketmq subscription-generation race (Finding A)

- [ ] RED: `test_subscribe_in_flight_reconnect_does_not_poison_subscribed_topics`
  in `tests/test_rocketmq_resilience.py` (subscribe side effect calls
  `backend.disconnect()`; expect `QueueError` matching "reconnected" AND
  `_subscribed_topics` stays empty). Confirmed failing on current code.
- [ ] RED: `test_subscribe_records_topic_when_generation_stable` (happy path
  still records the topic) — should PASS both before and after; guards against
  over-blocking.
- [ ] GREEN: `_ensure_subscribed` identity re-check under `_connection_lock`
  after `subscribe()` returns; typed `QueueError` on mismatch; set-add inside
  the lock; no network I/O under the lock.
- [ ] Focused: `uv run --frozen pytest tests/test_rocketmq_resilience.py tests/test_rocketmq_backend.py -q`

## Task 2 — mongodb local handle capture (Finding B)

- [ ] RED: PropertyMock tests in `tests/test_mongodb_backend.py` for pop
  (`_queue_collection`), add (`_set_collection`), store
  (`_storage_collection`): guard read returns the stub, use read returns None
  → must NOT surface `AttributeError`. Confirmed failing on current code.
- [ ] GREEN: capture `collection = self._<x>_collection` before each None
  guard at the 16 op-method sites; use the local everywhere in the method
  body; message text byte-identical; setup paths under `_connection_lock`
  untouched.
- [ ] Focused: `uv run --frozen pytest tests/test_mongodb_backend.py -q`

## Task 3 — Gate + ship (both findings)

- [ ] `ruff check src tests conftest.py`
- [ ] `ruff format --check src tests conftest.py`
- [ ] `uv run --frozen pytest`
- [ ] `mypy --strict src`
- [ ] Atomic commit per finding (messages in PLAN Phase 4)
- [ ] LEDGER.md R132 rows; EXECUTION-INDEX updated if convention requires
- [ ] Push `HEAD:main` (ff); do NOT touch primary dirty tree
- [ ] Memory round entry + MEMORY.md
