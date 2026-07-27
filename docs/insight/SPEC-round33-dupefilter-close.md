# Round 33 — SPEC/PLAN/TASK: dupefilter _close_locked — REFUTED (false positive)

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_fdc25d8e-2d6`. Base: `main` @ `3ff27e7`.

## Outcome: REFUTED — no code change shipped

R33 scanned 5 dims (r32-diff-regression, scheduler-deep, dupefilter-deep,
storage-strategy-concurrency, connectors-lifecycle-deep). 1 finding surfaced
(`dupefilter-close-locked-manager-leak`); the subagent verifier confirmed it
(is_real=true, MED). **On TDD, the test suite REFUTED it** — the proposed
"mirror R26-G/R20-B" fix broke 4 existing tests that encode the dupefilter's
**intentional close-retry design**, which scheduler/pipeline do NOT have.

This round is a docs-only outcome (like R24): the loop's adversarial-verification
step worked — the second verifier (the actual test suite) caught the subagent
verifier's error.

## The finding and why it's wrong

**Finding (REFUTED):** `dupefilter/dupefilter.py:738-769` `_close_locked` gates
`connection_manager.close()` (and the `_closed/_closing` terminal reset) on
`self._filter_released` (set only on `filter.close()` success). Framed as "the
lone holdout of the pre-R26-G close shape" — a Ctrl+C during filter.close() leaks
the manager + bricks the dupefilter.

**Why the framing is wrong:** the dupefilter's `_close_locked` is NOT a missed
R26-G fix — it is a **deliberately more-sophisticated retryable-close design**.
The `_filter_released` gate is the mechanism that makes close() **retriable**:

- `test_failed_filter_close_can_be_retried` (test_dupefilter.py:358) — a regular
  `RuntimeError` on `filter.close()` must defer manager release so a retry
  (`close("retry")`) can re-close the filter (call_count==2) and then release the
  manager (called once). R26-G's "close-in-finally" would close the manager on
  the first failure → no retry possible.
- `test_failed_manager_close_can_be_retried_without_reclosing_filter` (:379) — a
  manager.close() failure must be retriable WITHOUT re-closing the already-released
  filter.
- `test_open_primary_signal_survives_cleanup_and_diagnostic_failures` (:335) — an
  open() failure must NOT release the manager (the dupefilter never opened; the
  caller owns the manager). R26-G's unconditional finally-close would violate this.

The subagent verifier did not read these retry tests; it pattern-matched "gate on
success = R26-G bug." The 4 failing tests on GREEN were the disconfirmation.

## Residual concern (accepted, not worth fixing)

The narrow case the finding ID'd — a `BaseException` (Ctrl+C) during `filter.close()`
defers the manager release, and if no retry happens (process terminating) the fd is
leaked until OS cleanup. This is a **theoretical, low-impact** cost of the retry
design: the BaseException window is the trivial filter.close() body, and the
consequence (fd leak) is reclaimed by the OS on process exit. A fix narrow enough
to preserve the retry design (release-in-finally ONLY for BaseException, not regular
Exception) would add complexity for negligible benefit and risk new bugs. **Accept
the residual; do NOT re-flag.**

## DO-NOT-RE-FLAG additions after R33

- **dupefilter `_close_locked` is intentionally retryable** — the
  `_filter_released` gate is the retry mechanism, NOT a missed R26-G fix. Do NOT
  re-flag the manager-close-on-filter-success gate. (Subagent verifier false positive.)
- scheduler-deep / storage-strategy-concurrency / connectors-lifecycle-deep produced
  no other confirmed findings this round.
- **SCHED-EXC-CATCH-1** deferred a 4th time (persistent 429 on its verifier) —
  carry to R34; consider inline-verify if cap-blocked again.

## Process note

This is the value of the TDD gate inside the loop: the test suite is a second
adversarial verifier that catches subagent-verifier errors. R33 shipped no code
(net-zero pytest count) but consumed a real false positive that would otherwise
recur in future scans. The diff-regression + retry-design tension is now documented.
