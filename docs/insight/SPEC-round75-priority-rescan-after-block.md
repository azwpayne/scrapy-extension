# SPEC — R75: PriorityQueueStrategy.pop/pop_with_ack miss lower-bucket arrivals after the p0 blocking wait

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R84 fire (2026-08-11). No fresh scan — ships the R82-queued finding #3 (confirmed real, surgical, clean-file). R82's 4-dim scan produced it; R82 shipped #1, R83 shipped #2, this fire ships #3. One R82-confirmed finding remains queued (spider_mixin.py:182, low).

## Context and audit evidence

`PriorityQueueStrategy.pop` (queue/strategies/priority.py:192-215) and `pop_with_ack` (217-238) scan levels high-priority-first, then fall through to a single blocking wait on the highest-priority bucket `p0`:

```python
        timeout = normalize_queue_timeout(timeout)
        qb = self._connection_manager.get_queue_backend()
        for level in range(self._levels):
            item = qb.pop(self._bucket_queue(queue_name, level), 0.0)
            if item is not None:
                return item
        if timeout > 0:
            return qb.pop(self._bucket_queue(queue_name, 0), timeout)   # block on p0 ONLY
        return None
```

`pop_with_ack` (227-237) has the identical shape via `_pop_backend_instance_with_ack`.

**The gap:** when the non-blocking scan finds every level empty and `timeout > 0`, the method performs ONE blocking `pop(p0, timeout)`. When that blocking pop **times out and returns None**, the method returns None **without re-checking p1..p(N-1)**. So an item that became ready in any lower-priority bucket *during the wait* is missed — returned only on the next `pop` call.

The method docstring (196-198) claims "one blocking `pop(p0, timeout)` follows so the caller's wait contract is honored" — but that claim is false precisely when a lower-bucket arrival occurs during the wait. The base `QueueStrategy.pop` contract is "Pop the next ready item … or None if empty"; returning None while p1..p(N-1) hold ready items violates it.

### Concrete failure scenario

`levels=3` (p0, p1, p2), all buckets empty at `t=0`; caller invokes `pop("q", timeout=10)`. At `t=2` a producer pushes item `X` to p2. The non-blocking scan at `t=0` returns nothing; the blocking `pop(p0, 10)` times out at `t=10` and returns None; `pop` returns None while p2 holds the ready item `X` (X is returned only on the NEXT `pop` call). Inputs: `pop(timeout>0)` on an empty queue + a lower-priority-bucket arrival during the wait → wrong output None.

**Reachability:** the public `BackendQueue.pop(timeout>0)` passes `timeout` straight through to the strategy (`_pop_with_ack` → `strategy.pop_with_ack`, queue.py), so any external caller using a positive timeout reaches this path. The bundled scheduler always pops with `timeout=0` (scheduler.py:2377), so the gap is **latent** there — but it is a real correctness defect of the priority strategy's own contract, reachable via the public API and by any future caller with `timeout>0`.

`priority.py` is clean (NOT in the dirty tree).

## Goal

Honor `QueueStrategy.pop`'s "next ready item or None if empty" contract: after the blocking wait on p0 times out empty, re-scan `p0..p(N-1)` non-blocking before returning None, so a lower-priority arrival during the wait is returned (highest-priority-first) on the same call.

## Specification

In both `pop` and `pop_with_ack`, replace the single blocking `return qb.pop(p0, timeout)` with: block on p0; on a non-None result return it; **on None, re-scan p0..p(N-1) non-blocking and return the first non-empty level** (else None).

`pop` (replace L213-214):
```python
        if timeout > 0:
            item = qb.pop(self._bucket_queue(queue_name, 0), timeout)
            if item is not None:
                return item
            # A lower-priority bucket may have received an item during the
            # blocking wait on p0; re-scan all levels non-blocking so the
            # caller's "next ready item or None if empty" contract holds.
            for level in range(self._levels):
                candidate = qb.pop(self._bucket_queue(queue_name, level), 0.0)
                if candidate is not None:
                    return candidate
        return None
```

`pop_with_ack` (replace L232-237): mirror with `_pop_backend_instance_with_ack` — block on p0; on non-None data return `(data, token)`; on None re-scan `p0..p(N-1)` non-blocking via `_pop_backend_instance_with_ack(qb, bucket(level), 0.0)` returning the first non-None.

Semantics:
- **Non-blocking scan finds an item** → returned unchanged.
- **All empty + timeout≤0** → None unchanged.
- **All empty + timeout>0, blocking p0 returns an item** → returned immediately (no re-scan). Existing `test_pop_with_ack_uses_one_blocking_fallback_after_empty_scan` stays byte-identical (call_count unchanged).
- **All empty + timeout>0, blocking p0 times out (None)** → re-scan `p0..p(N-1)` non-blocking; return the highest-priority ready item if any (including a lower-bucket arrival during the wait), else None. **(The fix.)**

The re-scan is non-blocking (timeout=0.0 each), so it adds at most N quick round-trips only on the timeout path — no extra blocking, no multiplied wait.

## Plan and independently verifiable tasks

- **R75-1 (RED, pop):** `test_pop_rechecks_lower_buckets_after_blocking_p0_timeout` — `_strategy(levels=3)`, `qb.pop.side_effect=[None,None,None,None,None,b"X"]` (scan p0/p1/p2 empty → block p0 None → re-scan p0 None, p1 b"X"). Assert `s.pop("q", timeout=2.5) == b"X"` and `qb.pop.call_count == 6`. Fails before fix (returns None, call_count 4).
- **R75-2 (RED, pop_with_ack):** `test_pop_with_ack_rechecks_lower_buckets_after_blocking_p0_timeout` — `qb.pop_with_ack.side_effect=[(None,None)]*4 + [(None,None), (b"X","TOK")]`; assert `s.pop_with_ack("q", timeout=2.5) == (b"X","TOK")`, call_count 6. Fails before fix.
- **R75-3 (GREEN):** apply the block-then-re-scan to both `pop` and `pop_with_ack`.
- **R75-4 (no-regression):** `test_pop_with_ack_uses_one_blocking_fallback_after_empty_scan` (blocking p0 succeeds → no re-scan, call_count 4) stays green. Existing scan/push/queue_len tests untouched.
- **R75-5 (GATE):** `uv run ruff check .` + `uv run ruff format --check src tests conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension` all green (sandbox-off, default uv cache). Format-check IS enforced (R64).
- **R75-6 (ship):** atomic commit + `git push origin HEAD:main` (ff) + verify CI green.

## Acceptance criteria

- After a p0 blocking-timeout, a lower-bucket arrival is returned on the same call (R75-1, R75-2 pass).
- A successful blocking p0 pop still returns immediately with no re-scan (R75-4 / existing test green, call_count unchanged).
- All existing priority-strategy tests stay green; full suite = R83's 5111 + new tests.
- ruff check + ruff format --check + mypy --strict clean; CI on `main` green.

## Confidence / risk

- **Confidence:** high — source-verified no-re-scan in both methods; the contract violation is explicit (docstring claims "wait contract honored" but it isn't on lower-bucket arrival); R82's opus verifier constructed the concrete scenario and could not refute; latent-only under the bundled scheduler (timeout=0) but real via the public API.
- **Scope-risk:** narrow — the timeout>0 fallback branch in two methods; no signature change; the successful-blocking-pop path is byte-identical (re-scan runs only on None).
- **Constraint:** touch only `priority.py` (clean — NOT in the dirty tree) + new tests.
- **Rejected:** block on each level in priority order (multiply the timeout across N levels) | violates the caller's single-timeout budget and the existing "one blocking pop" design intent; the re-scan-after-timeout is strictly better (one blocking wait + cheap non-blocking re-scan).
- **Rejected:** block on the lowest-priority bucket instead of p0 | inverts priority (a high-priority arrival during the wait would be deprioritized); p0 is correct, the gap is only the missing re-scan.
- **Directive:** a multi-bucket queue strategy's blocking fallback must re-scan ALL buckets non-blocking after the single blocking wait times out — an item can arrive in any bucket during the wait. Single-bucket-block-then-return is correct ONLY for a 1-level strategy. The same shape (block-then-re-scan) applies to any future multi-bucket strategy (work-stealing, sharded).
- **Not-tested:** real broker round-trip (mock seam); behavior under concurrent producers pushing to multiple buckets simultaneously during the wait (the highest-priority-first re-scan handles it correctly by construction).
