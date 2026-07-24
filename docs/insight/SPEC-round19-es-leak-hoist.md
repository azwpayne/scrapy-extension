# SPEC — Round 19: ES pop() ApiError leak + R18-B hoist regression + phantom pop_rate docs

> Back-nav: [Round 18 SPEC](SPEC-round18-parity-tocou.md) · [Iterative hardening](ITERATIVE-HARDENING-2026-07-21.md)
> Scan: ultracode `wf_eed6f2d8-e63` (6-dim find + adversarial verify, 10 agents, ~1.7M tokens)
> Tree: `main aed2cdc` (R18 head)

## Context & frontier state

The connect()-BaseException cluster is CLOSED (all 9 backends). Round 19 pivoted to fresh
frontiers (raw-exception leaks at hot-paths, api-contract uniformity, docs drift) + an
**r18-diff-regression** dimension.

**Result: 4 raw → 3 confirmed, 0 refuted** (1 thrash on test-quality verify). One real MED
contract violation + the adversarial dimension caught **another regression in my own R18-B
code** + a phantom-stat-key cluster R18-A missed.

## Problem statement

### A — ElasticSearch `pop()` leaks non-NotFound `ApiError` subclasses raw — MED
`pop()`'s outer except (elasticsearch.py:324-327) catches `NotFoundError` (→ return None) and
`TransportError` (→ QueueError) — but NOT the broad `ApiError`. **15 sibling ES hot-path
methods** catch `(ApiError, TransportError)` (push:256, queue_len, clear_queue, store, retrieve,
delete, etc.); `pop()` is the sole outlier. A non-NotFound, non-Conflict `ApiError` subclass
(`AuthenticationError`/`AuthorizationError` on token-expiry/permission-revoke, `RequestError` on
malformed query, `ServerError` on 5xx) raised by `indices.refresh()` (290), `search()` (291), or
`delete()` (314) escapes raw — violating the docstring's "QueueError: If the pop operation fails"
promise (line 278) and breaking caller error-handling (the queue contract is QueueError on
operational failure). The verifier confirmed `add()` at :417 is the **documented R-dupe-1
intentional narrowing** (PR #38, option b — graceful-degradation) — a deliberate case, NOT a
defect; do not "fix" it.

### B — R18-B pulsar hoist sits inside the try, after kwargs — LOW (R18 regression, my bug)
My R18-B `except BaseException` arm (pulsar.py:446-461) references local `client` (line 458), but
I hoisted `client: Any = None` at line 422 — **inside the try (starts 403) and AFTER the kwargs
block (404-421)**. That kwargs block calls `pulsar.AuthenticationToken(snapshot.auth_token)` at
:419-421 (a real C++-backed constructor). A Ctrl+C during kwargs-setup (before the hoist runs)
→ the arm evaluates `client is not None` against an unbound local → `UnboundLocalError`, which
**masks the original KeyboardInterrupt** (the arm's `raise` never runs). The sibling rabbitmq
`_open_prepared_channel` correctly hoists `channel` BEFORE the try (679 before 680) — pulsar
should mirror it. The R18 test only exercised the post-hoist window, so it missed this.

### C — phantom `queue/pop_rate` stat key (7 sites) — LOW (docs)
R18-A fixed `dupefilter/filtered` → `dupefilter/hit_count`, but the `queue/pop_rate` phantom
survived. The real emitted key is `queue/pop_rate_1m` (window-tagged: `_1m` at the default 60s,
`_{N}s` for a custom `SCRAPY_MONITOR_POP_RATE_WINDOW_S`) — `monitor/stats.py:226-227`. The bare
`queue/pop_rate` is documented at **7 sites**: `docs/runbook.md:585,593`,
`settings/base.py:292`, `schedule/scheduler.py:595,1046`, `monitor/stats.py:127`,
`queue/queue.py:127`. An operator wiring an alert to the literal bare key sees an empty series
and misdiagnoses a stuck crawl. (Verifier correctly expanded scope from the finder's 3 to all 7.)

## Non-goals (DO-NOT-RE-FLAG — accumulated)
- bloom/cuckoo (never-FN). · all connect() `from None` (secret-redaction). · pulsar `_RedactedStr`.
  · dynamodb `clear_storage` TOCTOU (documented). · `_push_is_durable` pin. · connect()-BaseException
  cluster (all 9 — CLOSED). · ES `add()` :417 TransportError-only catch = R-dupe-1 deliberate narrowing
  (PR #38, option b — do NOT broaden). · R17 arms (rabbitmq `_open_prepared_channel`, memcached connect,
  kafka null-first). · R18 arms (pulsar connect BaseException, rabbitmq publish-window identity guard) —
  EXCEPT the pulsar hoist-placement (finding B critiques the implementation).

## Units (3)
| ID | Sev | R18-reg | Surface | Fix |
|----|-----|---------|---------|-----|
| A | MED | — | elasticsearch.py:326 | `except TransportError` → `except (ApiError, TransportError)` (keep NotFoundError arm first) |
| B | LOW | ✅ | pulsar.py:422 | move `client: Any = None` hoist to BEFORE `try:` (after :402) |
| C | LOW | — | 7 sites (runbook×2, base.py, scheduler×2, stats.py, queue.py) | bare `queue/pop_rate` → `queue/pop_rate_1m` (+ window-tag note) |

## Success criteria
- ruff clean; mypy --strict 0 issues; pytest ≥ 3765 passed / 46 skipped (unsandboxed); coverage ≥ 95%.
- Each unit: ONE atomic commit; TDD (RED before GREEN) for A + B; C is docs.
- All merged to `main`; only `main` remains. Claude-only.
