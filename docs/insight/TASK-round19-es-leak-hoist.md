# TASK — Round 19: ES pop() ApiError leak + R18-B hoist regression + phantom pop_rate docs

> Spec: [SPEC-round19-es-leak-hoist.md](SPEC-round19-es-leak-hoist.md)
> Plan:  [PLAN-round19-es-leak-hoist.md](PLAN-round19-es-leak-hoist.md)
> Constraints: one atomic commit per unit · all → main · Claude-only.

## Unit A — ES pop() broaden to (ApiError, TransportError) (MED, contract)
**File:** `src/scrapy_extension/backends/elasticsearch.py:326`; test `tests/test_elasticsearch_backend.py` (or sibling). `ApiError` imported at :16.
1. [RED] Test: a pop() whose `self.client.search()` (or `indices.refresh()`) raises a non-NotFound,
   non-Conflict `ApiError` subclass (e.g. `AuthenticationError`, or a bare `ApiError` constructed for
   the test) must surface as `QueueError` (queue_name + operation="pop"). Run → RED (currently raw).
2. [GREEN] Change line 326 `except TransportError as e:` → `except (ApiError, TransportError) as e:`.
   Keep `except NotFoundError: return None` (324) first; the inner `except ConflictError: continue`
   is unaffected. Do NOT touch `add()` :417 (R-dupe-1 deliberate narrowing).
3. Run gate; commit `fix(elasticsearch): wrap non-NotFound ApiError subclasses in pop() as QueueError (R19-A, contract parity)`.

## Unit B — pulsar R18-B hoist before try (LOW, R18 regression)
**File:** `src/scrapy_extension/backends/pulsar.py:422`; test `tests/test_pulsar_backend.py`.
1. [RED] Test: configure auth_token (so `pulsar.AuthenticationToken(...)` at :419 runs); patch
   `pulsar.AuthenticationToken` to raise `KeyboardInterrupt`; `pytest.raises(KeyboardInterrupt)` on
   `connect()`. Run → RED (current code raises `UnboundLocalError` from the arm referencing the
   unbound `client`).
2. [GREEN] Move `client: Any = None` from line 422 (inside try, after kwargs) to immediately after
   `snapshot = self._capture_connection_snapshot()` (line 402), before `try:` (403). No other change.
3. Run gate; commit `fix(pulsar): hoist client before try so a BaseException during kwargs-setup cannot mask the original interrupt (R19-B, R18-B regression)`.

## Unit C — phantom queue/pop_rate → queue/pop_rate_1m (LOW, docs, 7 sites)
**Files:** `docs/runbook.md:585,593`; `src/scrapy_extension/settings/base.py:292`;
`src/scrapy_extension/schedule/scheduler.py:595,1046`; `src/scrapy_extension/monitor/stats.py:127`;
`src/scrapy_extension/queue/queue.py:127`. Ref emitter `monitor/stats.py:226-227`.
1. At each site replace the bare ``queue/pop_rate`` → ``queue/pop_rate_1m`` (the default-window emitted
   key). Where the surrounding text describes the window param, add "(window-tagged: ``_1m`` at the
   default 60s, ``_{N}s`` when ``SCRAPY_MONITOR_POP_RATE_WINDOW_S`` is overridden)".
2. Verify `grep -rn 'queue/pop_rate\`' docs/ src/` shows only the corrected ``queue/pop_rate_1m`` /
   ``queue/pop_rate_{N}s`` forms (no bare ``queue/pop_rate`` left as a literal key).
3. Commit `docs(monitor): cite the real queue/pop_rate_1m window-tagged stat across runbook + 4 source docstrings (R19-C)`.

## Definition of done
- [ ] ruff clean · mypy --strict 0 issues · pytest ≥3765 passed (unsandboxed) · coverage ≥95%
- [ ] 3 atomic commits on `worktree-round19-es-leak-hoist`
- [ ] ff-merged to `main`, pushed, worktree branch deleted
- [ ] memory updated (R19 close-out note in `deep-insight-2026-07-23-ultracode.md`)
