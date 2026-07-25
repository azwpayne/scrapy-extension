# Round 24 — SPEC: docs-hygiene round (code surface clean)

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_70adc440-bef` (6-dim find + adversarial verify;
> 10 agents, 0 errors, ~2.1M tokens, ~41 min). Base: `main` @ `c19c090`
> (post-R23).

## Headline

**5 of 6 finder dimensions returned EMPTY — the code surface is clean this
round.** No new code defects. The high-ROI r23-diff-regression dimension
(R23's own lesson: "never drop it even when EMPTY N rounds") confirmed R23
introduced **no regressions**. Both NEW fresh-eyes dimensions
(retry/idempotency, None/Optional-deref) found nothing confirmed. Resource-leak
continuation (auditing mongodb/kafka/rocketmq/es/sqs connect→publish windows)
found nothing — the publish-window BaseException cluster is closed. Input-
validation continuation found nothing — the `Field(ge=0)` accepts-inf class is
exhausted (confirms R23's frontier note).

R24 is therefore a **docs-hygiene round**: the only confirmed findings are
documentation gaps for behavior changes R22/R23 shipped — operators upgrading
with the now-rejected values hit errors with no doc explanation.

## Scan result

**4 raw → 3 confirmed, 1 refuted.** Per-dimension: r23-regression EMPTY,
resource-leak-continuation EMPTY, retry-idempotency EMPTY, none-deref EMPTY,
input-validation-continuation EMPTY, **docs-drift 4 → 3 confirmed / 1 refuted**.

## Ship set (3 docs units — no code changes)

| ID | Sev | Surface | Gap (one line) |
|----|-----|---------|----------------|
| **A** | MED | `docs/migration-guide.md:150` | Migration guide documents the Redis pop() *arg* finite-rejection but never warns that R23-B/C added **config-time rejection** of non-finite / >86400s `SCRAPY_REDIS_SOCKET_TIMEOUT` / `SOCKET_CONNECT_TIMEOUT` and `SCRAPY_ES_REQUEST_TIMEOUT`. An upgrader carrying `=inf` (the exact foot-gun R23-B/C closed) gets a `ValidationError` with no migration signal. |
| **B** | LOW | `docs/runbook.md:586` | Runbook documents `SCRAPY_MONITOR_POP_RATE_WINDOW_S` default + window-tagging but omits the **86400s (24h) cap** R23-E enforces. |
| **C** | LOW | `docs/runbook.md:293` | Runbook's only RocketMQ row covers the invisibility lease but omits the R22-A `send_timeout` 5min cap and the R22-C `max_message_size` push-time `QueueError` fail-fast. |

## Refuted (1) — guardrail/completeness held

- `docs/migration-guide.md:525` RabbitMQ `heartbeat` cap (R23-D): already
  documented in `.github/CHANGELOG.md:528-531` (R23-F), in the Field
  description (surfaced in the `ValidationError` the operator actually hits),
  and covered generically by the migration-guide's "range failures raise
  `pydantic.ValidationError`" note. No previously-valid value breaks (pre-R23
  >65535 crashed with opaque `struct.error`; post-R23 it's a clean
  `ValidationError`). Completeness nitpick, not a defect.

## Deferred / out of scope

- Nothing. All 3 confirmed findings ship this round.

## DO-NOT-RE-FLAG additions after R24

- The R22/R23 cap behavior is now documented in runbook + migration-guide +
  CHANGELOG (the three operator-facing surfaces). A future docs-drift scan
  should not re-flag these specific caps as "undocumented."

## Frontier observation (for the memory record)

R24 is the **first round with zero code defects** — the hardening frontier has
thinned to the point where 6 dimensions (incl. 2 new fresh-eyes) surface only
docs gaps. The cron's per-fire code-defect ROI is dropping. This is a signal
for the user (not an action for this round): the 19-fires/day cadence may be
higher than the remaining defect-discovery rate justifies. The codebase is
approaching a "no more low-hanging defects" steady state; future rounds will
increasingly be docs-hygiene or turn up empty unless a genuinely new attack
surface emerges.
