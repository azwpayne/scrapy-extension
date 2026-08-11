# SPEC — R77: WorkStealing pop/pop_with_ack miss peer arrivals after the own-queue blocking wait (R84 sibling)

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R86 fire (2026-08-11), 4-dim scan (ndiff + request-serde + dupefilter-internals + scheduler-internals). 3 confirmed (scheduler EMPTY); this is the top (ndiff). The other 2 (Decimal NaN serde bypass, volatile-shadow LRU-on-read) queue for future fires.

## Context and audit evidence

`WorkStealingQueueStrategy.pop` (queue/strategies/work_stealing.py:211-253) and `pop_with_ack` (255-288) check own queue, steal round-robin from peers, then fall back to a single blocking wait on **own**:

```python
        # 3. Blocking fallback on own queue honoring caller's wait contract.
        remaining = self._remaining_timeout(deadline)
        if remaining > 0:
            return qb.pop(own, remaining)          # L252 — block on OWN only
        return None
```

`pop_with_ack` (L285-288) is identical via `_pop_backend_instance_with_ack`.

**The gap (the exact R84 defect shape):** when own + all peers are empty and `timeout > 0`, the method performs ONE blocking `pop(own, remaining)`. When that blocking pop **times out returning None**, the method returns None **without re-checking own or any peer**. So an item that a peer worker pushed into one of this worker's peer queues *during the wait* — the precise cross-worker routing event work-stealing exists to handle — is missed until the next `pop` call. This violates `QueueStrategy.pop`'s contract ("the next ready item or None if empty"): `queue_len` (L290-296) sums own+peers and reports 1 while `pop` returned None → the operator sees an idle worker with a non-empty backlog.

**ndiff evidence (why this is a fresh sibling, not a re-report):** R84 (`35f1097`) fixed ONLY `PriorityQueueStrategy`; its commit Directive explicitly named this sibling — *"a multi-bucket queue strategy's blocking fallback must re-scan ALL buckets non-blocking after the single blocking wait times out … Applies to any future multi-bucket strategy (work-stealing, sharded)"* — but `work_stealing.py` was never touched (`git show 35f1097 --stat` = priority.py + 2 tests only). This is the named-deferred mirror.

**Reachability:** the public `BackendQueue.pop(timeout>0)` seam (queue.py:568 → `_pop_with_ack` → `strategy.pop_with_ack`, queue.py:763) passes `timeout` straight through. Latent under the bundled scheduler (pops `timeout=0`, scheduler.py:2377) — identical reachability profile to R84. No exception; symptom is a transient stalled consumer (the peer item is picked up on the next pop's steal phase, so it's a liveness/contract violation, not permanent loss).

`work_stealing.py` is clean (NOT in the dirty tree).

## Goal

Honor `QueueStrategy.pop`'s "next ready item or None if empty" contract for the work-stealing strategy: after the blocking wait on own times out empty, re-scan own + all peers non-blocking before returning None, so a peer arrival during the wait is returned on the same call.

## Specification

Mirror R84's priority.py fix. In both `pop` and `pop_with_ack`, replace the single blocking `return qb.pop(own, remaining)` with: block on own; on non-None return it; **on None, re-scan own + all peers non-blocking, return the first non-empty, else None.**

`pop` (replace L250-253):
```python
        remaining = self._remaining_timeout(deadline)
        if remaining > 0:
            item = qb.pop(own, remaining)
            if item is not None:
                return item
            # A peer queue may have received an item during the blocking wait
            # on own; re-scan own + all peers non-blocking so the caller's
            # "next ready item or None if empty" contract holds (R84 sibling).
            item = qb.pop(own, 0.0)
            if item is not None:
                return item
            for peer in self._peer_ids:
                candidate = qb.pop(self._worker_queue(queue_name, peer), 0.0)
                if candidate is not None:
                    return candidate
        return None
```

`pop_with_ack` (replace L285-288): mirror with `_pop_backend_instance_with_ack` — block on own; on non-None data return `(data, token)`; on None re-scan own + peers non-blocking, returning the first non-None as `(data, token)`, else `(None, None)`.

Semantics:
- **Own non-block / steal finds an item** → returned unchanged.
- **All empty + timeout≤0** → None unchanged.
- **All empty + timeout>0, blocking own returns an item** → returned immediately (no re-scan). Byte-identical to today.
- **All empty + timeout>0, blocking own times out (None)** → re-scan own + peers non-blocking; return the first ready item if any (including a peer arrival during the wait), else None. **(The fix.)**

The re-scan iterates `self._peer_ids` directly (non-blocking, timeout=0.0 each) and does NOT advance `_steal_idx` — the fairness cursor is advanced only in the main steal round; a re-scan hit is a bonus catch and the next call's steal round continues from the unchanged cursor (a minor fairness wobble, not a correctness issue). The re-scan adds at most N+1 quick non-blocking round-trips, only on the timeout path.

## Plan and independently verifiable tasks

- **R77-1 (RED, pop):** `test_pop_rechecks_peers_after_blocking_own_timeout` — `_strategy(worker_id="w1", peer_ids=("w2","w3"))`, `qb.pop.side_effect=[None,None,None,None,None,b"X"]` (own empty → steal w2/w3 empty → block own None → rescan own None, rescan w2 b"X"). Assert `s.pop("q", timeout=2.5) == b"X"`, `call_count == 6`, `call_args_list[5].args == (_worker_queue("q","w2"), 0.0)`. Fails before fix (returns None, call_count 4).
- **R77-2 (RED, pop_with_ack):** mirror with `qb.pop_with_ack.side_effect=[(None,None)]*4 + [(None,None),(b"X","TOK")]`; assert `(b"X","TOK")`, call_count 6.
- **R77-3 (GREEN):** apply block-then-re-scan to both methods.
- **R77-4 (no-regression):** `test_zero_timeout_never_accumulates_blocking_peer_probes` (timeout=0 → no block → no re-scan, 3 calls) stays green. Existing own/steal/push tests untouched.
- **R77-5 (GATE):** `uv run ruff check .` + `uv run ruff format --check src tests conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension` all green. Format-check IS enforced (R64).
- **R77-6 (ship):** atomic commit + `git push origin HEAD:main` (ff) + verify CI green.

## Acceptance criteria

- After an own-queue blocking-timeout, a peer arrival is returned on the same call (R77-1, R77-2 pass).
- A successful blocking own pop still returns immediately with no re-scan (R77-4 / existing tests green).
- All existing work-stealing tests stay green; full suite = R85's 5116 + new tests.
- ruff check + ruff format --check + mypy --strict clean; CI on `main` green.

## Confidence / risk

- **Confidence:** high — source-verified (L252/L287 return timed-out None directly, no re-scan); R84's commit Directive explicitly named this sibling; R86's opus verifier constructed the concrete scenario and could not refute.
- **Scope-risk:** narrow — the timeout>0 fallback branch in two methods; no signature change; successful-blocking path byte-identical.
- **Constraint:** touch only `work_stealing.py` (clean — NOT in the dirty tree) + new tests.
- **Rejected:** advance `_steal_idx` on a re-scan steal | adds lock contention + complexity for a fairness nuance; the re-scan is a bonus catch, the main steal round owns fairness. Correctness (returning the item) does not require it.
- **Rejected:** re-scan under `_steal_lock` | the re-scan pops are non-blocking and independent; the lock serializes the main steal round against concurrent stealers, not against a single caller's timeout re-scan.
- **Directive:** (reaffirms R84) a multi-bucket/multi-source queue strategy's blocking fallback MUST re-scan ALL sources non-blocking after the single blocking wait times out. priority.py (R84) and work_stealing.py (this) now both do; any future sharded/multi-source strategy must too.
- **Not-tested:** real broker round-trip (mock seam); concurrent peer arrivals to multiple peer queues during the wait (first-peer-wins re-scan handles it by construction).
