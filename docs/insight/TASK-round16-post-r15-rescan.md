# TASK — Round 16: Post-Round-15 Re-scan Closeout

> **Companion.** [`SPEC-round16-post-r15-rescan.md`](./SPEC-round16-post-r15-rescan.md), [`PLAN-round16-post-r15-rescan.md`](./PLAN-round16-post-r15-rescan.md). `/goal`-shaped units (executed inline via fanned-out Claude subagents — omc/`/goal` bridge is dead in this env). Harness tasks track live status.

**Status legend:** ⏳ pending · 🔄 in-progress · ✅ done · ⏸ deferred

| ID | Title | Sev | Files (owner scope) | Effort | Depends | Harness | Status |
|---|---|:-:|---|:-:|:-:|:-:|:-:|
| **R16-A** | kafka + rocketmq `connect()` `except BaseException` arm (cleanup on Ctrl+C mid-construction) | 🔴MED | `backends/{kafka,rocketmq}.py` + tests | S | — | #8 | ⏳ |
| **R16-B** | README:527 kafka `clear_queue` `NotImplementedError`→`QueueError` | 🟠MED | `README.md` | S | — | #9 | ⏳ |
| **R16-C** | conftest `push_is_durable` knob: translate `_DurablePushRequired`→`QueueError` (faithful) | 🟢LOW | `tests/conftest.py` | S | — | #10 | ⏳ |
| **R16-D** | dynamodb clear_storage TOCTOU comment correction (best-effort, not hard boundary) | 🟢LOW | `backends/dynamodb.py` | S | — | #11 | ⏳ |
| **R16-E** | docs/operability: runbook stuck-crawl + STABILITY 5 settings + pulsar/rabbitmq/memcached `from None`→`from e` | 🟢LOW | `docs/runbook.md`, `.github/STABILITY.md`, `backends/{pulsar,rabbitmq,memcached}.py` + tests | M | — | #12 | ⏳ |
| **R16-GATE** | ruff + mypy --strict + pytest + coverage≥95% | 🔧 | — | S | A–E | #13 | ⏳ |
| **R16-MERGE** | ff-merge worktree→main, push, delete branch (main-only) | 🔧 | git | S | GATE | #14 | ⏳ |
| ⏸ R16-F | deferred: memcached clear_storage parity; dupefilter.clear() graceful-degradation; U2 test timeout guards | 🟢LOW | — | M | — | — | ⏸ |

## Acceptance snapshots (per unit)

- **R16-A:** new test — KeyboardInterrupt raised between producer-assignment and admin-assignment → `_abort_partial_connect()` (kafka) / discard (rocketmq) ran, `is_connected()` is False, no leaked handle. Parity with `test_elasticsearch_connect_cleanup.py`.
- **R16-B:** README:527 reads `QueueError`; grep `NotImplementedError` in README returns 0 for kafka clear.
- **R16-C:** setting `manager.push_is_durable = False` + `require_durable=True` raises `QueueError` (not `_DurablePushRequired`); all ~14 consumers still green (default durable=True).
- **R16-D:** dynamodb.py:922-925 comment states the epoch fence is best-effort (a mid-clear retirement can close the client before the in-flight batch_write); no behavior change.
- **R16-E:** runbook presents `queue/pop_rate_1m` + `dupefilter/filter_saturation` as landed primary signals; STABILITY lists the 5 settings; pulsar/rabbitmq/memcached connect raises preserve `__cause__` (test asserts).

## Constraints
Atomic commit per unit · merge to main (main-only) · Claude Code only.
