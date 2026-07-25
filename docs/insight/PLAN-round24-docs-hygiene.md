# Round 24 — PLAN: docs-hygiene edits

> Spec: [SPEC-round24-docs-hygiene.md](./SPEC-round24-docs-hygiene.md).
> No code changes — the gate is ruff + mypy + `git diff --stat` (confirm
> docs-only); pytest is unaffected by `.md` edits and is skipped unless a
> `.py` is touched. Claude-Code-only.

## R24-A — `docs/migration-guide.md` R23-B/C/D timeout caps (MED)

Add a paragraph in the **Configuration Changes** section (after the
`ValidationError`/`ConfigurationError` explanation, ~L405) noting that the
socket/request timeout settings now reject non-finite values and are capped at
86400s, so a pre-R23 deployment carrying `SCRAPY_REDIS_SOCKET_TIMEOUT=inf` (or a
huge finite value) or `SCRAPY_ES_REQUEST_TIMEOUT=inf` will now fail at config
load. Cross-reference R23-B/C. Also note the R23-D RabbitMQ heartbeat cap is
covered by the generic range-failure note (no separate bullet — the refuted
finding showed it's already surfaced via the Field description + CHANGELOG).

## R24-B — `docs/runbook.md:586` pop_rate_window cap (LOW)

Append the 86400s (24h) cap to the `SCRAPY_MONITOR_POP_RATE_WINDOW_S` row in
the monitor-knobs table, with the one-line rationale (R23-E: unbounded window
was a soft-OOM foot-gun — pop-timestamp deque grows without eviction).

## R24-C — `docs/runbook.md` RocketMQ send_timeout + max_message_size (LOW)

Extend the per-item-byte-cap section (L553-560) — or add a short RocketMQ
note — documenting:
- `SCRAPY_ROCKETMQ_SEND_TIMEOUT` capped at 300000ms (5min) so a stray-zero typo
  cannot wedge the gRPC per-RPC deadline (R22-A).
- `SCRAPY_ROCKETMQ_MAX_MESSAGE_SIZE` (default 1 MiB) enforced at push time —
  items exceeding it raise `QueueError(operation="push")` client-side rather
  than surfacing as an opaque broker error (R22-C); note `=0` rejects every
  non-empty push (the Field allows 0).

## Gate

`uv run ruff check` + `uv run mypy --strict src/scrapy_extension` (confirm no
stray `.py` edit) + `git diff --stat` (confirm only `.md` changed). pytest
skipped — no `.py` touched, so the 3787-test suite cannot regress.

## Ship

ff-merge `worktree-round24` → `main` → push → delete branch (main-only). No
code-reviewer fan-out needed (docs-only; each finding was already adversarially
verified by the scan's Verify phase). Memory record.
