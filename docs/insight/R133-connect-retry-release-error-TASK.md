# R133 TASKS — connect-retry release-error preservation

> Spec: [R133-connect-retry-release-error-SPEC.md](R133-connect-retry-release-error-SPEC.md)
> Plan: [R133-connect-retry-release-error-PLAN.md](R133-connect-retry-release-error-PLAN.md)

## Task 1 — R133-A retry-loop release error

- [ ] RED: `test_connect_with_retries_preserves_release_error_when_close_wins`
  in `tests/test_connection_manager.py` — retry-loop path, release reason in
  message, no "Failed to connect after". Confirmed failing on current code.
- [ ] GREEN: bare `raise` on `_retired` inside `except Exception`; remove the
  post-except retired check/break; telemetry/backoff untouched.
- [ ] Focused: `uv run --frozen pytest tests/test_connection_manager.py -q`

## Task 2 — R66 closure (documentation only)

- [ ] LEDGER row: R66 queue.py codec-set unhashable — REFUTED (single
  production call site sits inside the broad poison catch; identical
  observable behavior). No code change.

## Task 3 — Landed-WIP scan (5 dims, opus finders + adversarial verify)

- [ ] Surfaces: queue/queue.py (snapshot v3/tombstone), connectors.py
  (breaker policy resolution), kafka.py (lock order), delay.py,
  time_wheel.py (has_item token settlement)
- [ ] Dismissed list respected (overflow-latch COMPLETE, volatile-LRU,
  bloom/cuckoo, dupefilter/ES/throttle-family clean, R66 codec-set now
  REFUTED)
- [ ] Confirmed findings: hand-verify → ship or queue for R134+

## Task 4 — Gate + ship

- [ ] `ruff check` / `ruff format --check src tests conftest.py` /
  `uv run --frozen pytest` / `mypy --strict src`
- [ ] Atomic commit(s); LEDGER rows; push HEAD:main
- [ ] Memory round entry + MEMORY.md index
