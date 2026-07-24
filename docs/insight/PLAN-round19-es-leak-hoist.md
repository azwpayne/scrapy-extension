# PLAN — Round 19: ES pop() ApiError leak + R18-B hoist regression + phantom pop_rate docs

> Spec: [SPEC-round19-es-leak-hoist.md](SPEC-round19-es-leak-hoist.md)
> Workflow: worktree `round19-es-leak-hoist` → execute A→C (atomic commits) → gate → ff-merge to `main` → delete branch.

## Design notes
- **A**: ES exception hierarchy — `ApiError` is the base for HTTP-response errors (NotFoundError,
  ConflictError, RequestError, AuthenticationError, AuthorizationError, ServerError); `TransportError`
  is a transport-level sibling, NOT a subclass. So both must be listed. `NotFoundError` arm stays first
  (more specific). The inner `except ConflictError: continue` (delete try) is unaffected (stricter
  subclass, caught earlier). `ApiError` is already imported (elasticsearch.py:16).
- **B**: mirror rabbitmq `_open_prepared_channel` (hoist `channel` at :679 BEFORE `try:` at :680).
  Move `client: Any = None` from pulsar.py:422 to right after `snapshot = self._capture_connection_snapshot()`
  (:402), before `try:` (:403). One-line move; no behavior change on the success/Exception paths; the
  arm's identity guard + `_suppress_pulsar_errors` close are unchanged.
- **C**: 7 sites. Replace bare ``queue/pop_rate`` → ``queue/pop_rate_1m``; where the text describes the
  window param, add "(window-tagged: ``_1m`` at the default 60s, ``_{N}s`` when
  ``SCRAPY_MONITOR_POP_RATE_WINDOW_S`` is overridden)". Each site is a docstring/comment — verify the
  emitted key in `monitor/stats.py:226-227`.

## Phases
1. **TDD RED (A + B)** — see TASK for the exact tests.
2. **Implement (GREEN)** — A (broaden except), B (move hoist), C (7 doc edits).
3. **Gate** — `uv run ruff check src/ tests/`; `uv run mypy --strict src/`; `uv run pytest -q` (sandbox OFF).
4. **Merge to main** — ff-only, push, `worktree remove --force` + `branch -d` (all sandbox-off).

## Fan-out (Claude-only)
3 small, mechanical, precisely-specified units → main-loop sequential (proven anti-thrash; avoids
git/pytest races). Ultracode 10-agent scan was the insight fan-out.

## Risk notes
- **A**: confirm no caller relies on the raw `ApiError` propagating (the queue/scheduler contract is
  QueueError on operational failure — wrapping is the documented behavior; `add()`'s deliberate
  R-dupe-1 narrowing is a DIFFERENT site, untouched).
- **B**: the test must simulate a BaseException in the kwargs-setup window (patch
  `pulsar.AuthenticationToken` to raise KeyboardInterrupt) and assert `KeyboardInterrupt` propagates
  (not `UnboundLocalError`).
