# SPEC — Round 18: connect()-BaseException final parity + R17-B TOCTOU + docs drift

> Back-nav: [Round 17 SPEC](SPEC-round17-baseexception-cleanup.md) · [Iterative hardening](ITERATIVE-HARDENING-2026-07-21.md)
> Scan: ultracode `wf_e2a77a92-9dd` (6-dim find + adversarial verify, 11 agents, ~2.0M tokens)
> Tree: `main c16179b` (R17 head)

## Context & frontier state

R15→R16→R17 closed the durability, connect()-BaseException, and abort-null-ordering
clusters. Round 18 re-scanned 6 dimensions (incl. a dedicated **R17-diff-regression**
dimension) on the post-R17 tree.

**Result: 5 raw → 5 confirmed, 0 refuted.** Deep diminishing-returns — but the
adversarial R17-diff-regression dimension **caught a genuine regression in R17-B**
(my own code), and the error-lifecycle dimension found the **last** connect()-capable
backend still missing the BaseException arm. Plus two docs-drift findings.

## Problem statement

### A — runbook cites a phantom dupefilter stat key — MED
`docs/runbook.md:602` (the "Diagnose a stuck crawl" table) and `:630` (ack-error
differential prose) direct operators to read `dupefilter/filtered`. **No code emits
that key** (`grep -rn 'dupefilter/filtered' src/` → 0). The real counters are
`dupefilter/hit_count` (`monitor/stats.py:165`, per-duplicate) + `dupefilter/miss_count`
(`stats.py:170`, per-newly-seen). Every other table row is a literal key that exists, so
`dupefilter/filtered` is presented-as-literal but is phantom — an operator graphs it
during an incident, sees it flat-zero, and concludes dedup is idle. (Pre-existing drift,
surfaced now because the monitor-stats dimension was added.)

### B — pulsar `connect()` is the last backend missing the BaseException arm — LOW
`pulsar.py:430` builds `client = pulsar.Client(...)` (the C++ binding starts background
IO/service threads in the ctor), bumps generation (431), publishes `self._client = client`
(432). Only `except Exception` (441). A Ctrl+C in the 430→432 window escapes without
closing the client; it was never published so `disconnect()` can't reach it → the C++ bg
threads + lazy broker FD leak to interpreter shutdown. **All 8 other connect()-capable
backends** (redis/mongodb/es/kafka/rocketmq/dynamodb/memcached/rabbitmq) now carry the arm;
pulsar is the lone holdout. Resource leak, not wedge (`is_connected()` stays truthful —
client never published).

### C — R17-B `published`-flag TOCTOU closes a just-published live session — LOW (R17 regression)
My R17-B `except BaseException` arm (`rabbitmq.py:549`) guards on `if not published and
candidate is not None`. But `published = True` (524) runs **after** `_publish_handles_locked`
(519-523) — which installs the candidate as `self._connection`/`self._channel` as a
**side-effect before returning** (277-278). A Ctrl+C in the ~1-bytecode window between the
call return and `published = True` reaches the arm with `published` still False while the
candidate *is* the live session → the arm closes it (violating the arm's own "close ONLY
when not published" invariant). Two independent dimensions flagged this (#3 r17-diff-regression
+ #5 race-correctness — same defect). **Found by me shipping too fast in R17-B; the fix is
an identity guard on actual state instead of a lagging flag.**

### D — CHANGELOG Kafka `clear_queue` bullet stale — LOW
`.github/CHANGELOG.md:115` says Kafka `clear_queue()` "is explicitly unsupported… fails
before admin I/O" without naming the exception. Commit `6e228da` (R15) changed it to
`QueueError` (kafka.py:1377; parity w/ pulsar/rocketmq). A sibling bullet (:103) uses
identical "explicitly unsupported" phrasing but names `NotImplementedError` (for
pulsar/rocketmq `queue_len`) — an operator pattern-matches and writes
`except NotImplementedError` around Kafka clear_queue, missing the real `QueueError`.
README:527 + migration-guide:449 were corrected; CHANGELOG is the lone stale surface.

## Non-goals (DO-NOT-RE-FLAG — accumulated)
- bloom/cuckoo filters (never-FN). · pulsar/rabbitmq/memcached `connect()` `from None`
  (DELIBERATE secret-redaction). · pulsar `_RedactedStr`. · dynamodb `clear_storage`
  TOCTOU (documented best-effort). · `_push_is_durable` class-flag pin. · R17 just-shipped
  arms (rabbitmq `_open_prepared_channel` BaseException, memcached connect BaseException,
  kafka `_abort_partial_connect` null-first) — **except** the R17-B publish-window guard,
  which finding C critiques (in-scope: critique the implementation, not re-flag the closed fix).
- sqs has 0 BaseException arms but was NOT flagged — sqs `connect()` uses a boto3 client
  whose construction does not start unmanaged background threads the way pulsar/pika do;
  leaving as-is unless a future scan flags it with a concrete leak.

## Units (4)
| ID | Sev | R17-reg | Surface | Fix |
|----|-----|---------|---------|-----|
| A | MED | — | runbook.md:602,630 | `dupefilter/filtered` → `dupefilter/hit_count` (+ note `miss_count`) |
| B | LOW | — | pulsar.py:403-444 | `except BaseException` arm: close client iff `self._client is not client` (hoist `client=None`) |
| C | LOW | ✅ | rabbitmq.py:549 | guard `self._connection is not candidate.connection` (identity, not the lagging `published` flag) |
| D | LOW | — | CHANGELOG.md:115 | Kafka clear_queue bullet → name `QueueError` |

## Success criteria
- ruff clean; mypy --strict 0 issues; pytest ≥ 3763 passed / 46 skipped (unsandboxed — e2e probe needs sandbox off); coverage ≥ 95%.
- Each unit: ONE atomic commit; TDD (RED before GREEN) for B + C; A + D are docs.
- All merged to `main`; only `main` remains. Claude-only.
