# R133 SPEC — connect-retry release-error preservation (+ landed-WIP scan, R66 closure)

> Plan: [R133-connect-retry-release-error-PLAN.md](R133-connect-retry-release-error-PLAN.md)
> Tasks: [R133-connect-retry-release-error-TASK.md](R133-connect-retry-release-error-TASK.md)

## Context

R133 (2026-08-15 fire). Round number taken from LEDGER tail (R132 latest).
The user's 14-file dirty tree landed earlier today as 7 atomic commits
(`3c34be5..1606d0c`, pushed), unblocking the two queued dirty-blocked
findings. Both were re-located and re-verified at HEAD `1606d0c` before any
TDD, per the R88 lesson.

## Finding A (ships) — retry loop swallows the typed release error

`ConnectionManager._connect_with_retries`
(src/scrapy_extension/backends/connectors.py:2043-2096 at HEAD):

- `_attempt_connection()` raises a typed, actionable release error when a
  concurrent `close()` wins the race ("backend discarded", or "Cannot connect
  a released ConnectionManager" — both `BackendConnectionError`).
- The non-retryable tuple at :2047 (`ConfigurationError, ValidationError,
  ImportError`) does not include it, so the broad `except Exception` at :2052
  catches it; the post-except retired check (:2071-2074) then `break`s with
  `failed_attempt = True`.
- The loop tail (:2091-2096) raises the GENERIC
  `"Failed to connect after {total_attempts} attempts"` — discarding the
  specific release reason AND misreporting the count (`total_attempts` is the
  configured max `retry_attempts + 1`, not the number actually run).

The underlying contract is real and unit-tested for `_attempt_connection`
directly (`tests/test_connection_manager.py` close-wins test), but the
retry-loop integration path re-wraps and loses it. User impact: a caller that
closes a manager while a connect is in flight gets a misleading generic
failure instead of the actionable released-manager error, with a wrong
attempt count.

**Fix (validated GREEN in R71 before the dirty-tree revert)**: re-raise the
current exception (`raise`, bare) from inside the `except Exception` block
when `_retired` is observed under `self._lock`, and remove the post-except
retired-check/break. Bare `raise` needs no survivor variable (the `as e`
target is cleared at except-suite exit — post-except `raise e` would be
`UnboundLocalError`).

## Finding B (closed as REFUTED) — R66 queue.py unhashable codec set

R66 queued "queue.py:784 `_BODY_CODEC` set→tuple (HIGH)" as the R64 sibling
(base.py `_looks_like_codec_marker` set→tuple). Re-verified at HEAD:
`_decode_body` at queue.py:790-796 uses `codec not in {None,
_BODY_CODEC_BASE64_V1}` with JSON-sourced `codec`, so an unhashable codec
value raises raw `TypeError` instead of the intended `SerializationError`.
**However** `_decode_body` has exactly one production call site (queue.py:719)
and it sits inside the broad poison handler (`except Exception` →
`deserialization_failed`, :722): both the intended `SerializationError` and
the raw `TypeError` terminate the poison message identically and surface the
same uniform `SerializationError(_QUEUE_POP_MONITOR_FAILURE)`. No
user-visible difference → structural pattern with a downstream safety net
(R88 lesson). **REFUTED, do not fix.** Recorded in LEDGER to close the queue.

## Scan target (fresh material)

The landed WIP (944 lines) has never been scanned by any R-round. Five-dim
scan over: `queue/queue.py` (v3 snapshot keys, legacy retirement, empty-state
tombstone), `backends/connectors.py` (breaker Scrapy-settings resolution),
`backends/kafka.py` (connection-before-delivery lock order),
`queue/strategies/delay.py` + `time_wheel.py` (`has_item` accepting
`(None, token)`). Findings from this scan ship this round or queue for R134+.

## Scan results (confirmed by adversarial verify, hand-verified)

**Finding B (HIGH; ships) — priority/work_stealing blocking-wait rescan drops
Kafka tombstone tokens.** The landed rescan-after-block hunks (35f1097
priority, 8b0ea6d work_stealing) gate every `pop_with_ack` arm on
`data is not None`, silently discarding a `(None, token)` tombstone delivery;
pre-change these arms were unconditional passthroughs to
`BackendQueue._pop`'s settlement. The offset stays in `_in_flight` forever,
pinning the Kafka commit watermark → unbounded duplicate redelivery on every
restart/rebalance. Same defect class 3c34be5 fixed for delay/time_wheel in
the same landed batch. Fix: token-aware guards
(`data is not None or token is not None`) on every `pop_with_ack` arm in both
strategies, matching the delay/time_wheel predicate and throttle's
unconditional propagation.

**Finding C (MEDIUM; ships) — transient snapshot-restore failure permanently
deletes the legacy checkpoint.** In `_restore_snapshot` (queue.py:1506-1521),
a transient failure reading the empty-state tombstone makes the queue start
clean WITHOUT reading the legacy checkpoint; the next clean close then
retires (deletes) the legacy key unconditionally (queue.py:1431-1435) —
silent permanent loss of unprocessed delayed items, contradicting the
module's own invariant ("unprocessed entries cannot disappear with the only
checkpoint"). Scope: spiderless direct `BackendQueue` construction with a
colon-free queue name and no snapshot owner, during a storage blip. Fix:
(a) tombstone-read failure falls through to the legacy restore (errs toward
the invariant-tolerated duplicate direction); (b) legacy-read failure starts
clean but sets a one-shot `_defer_legacy_retirement` flag that makes the next
`_persist_snapshot` skip the legacy delete, so a later restart can still
recover the checkpoint.

## Non-goals

- No behavior change beyond the R133-A re-raise path; no signature changes.
- No fix for the refuted R66 (documented only).

## Acceptance

- RED test reproduces the generic-error swallow through the retry loop;
  GREEN asserts the typed release error surfaces.
- Full gate green: `ruff check`, `ruff format --check src tests conftest.py`,
  `uv run --frozen pytest`, `mypy --strict src`.
- Atomic commits; LEDGER rows for R133-A (LANDED) and R66 closure (REFUTED);
  memory round entry.
