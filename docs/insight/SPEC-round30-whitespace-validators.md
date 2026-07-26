# Round 30 — SPEC: whitespace-validator gaps (rabbitmq + redis)

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_1bbbbd07-1e4` — **PARTIAL: hit the 5h usage cap
> (429) mid-run** (resets 2026-07-27 07:50:18). 6/9 agents errored; 2 scanners
> (`r29-diff-regression`, `utils`) never ran; 3 scanners (pulsar/rabbitmq/redis)
> completed and produced candidates, but all verifiers 429'd. **`confirmed: []`
> is a FALSE-EMPTY** — the candidates were never verified by subagents. This
> round's findings were verified INLINE by the main loop (opus, Claude-only — the
> established fallback when subagents 429/thrash; see memory R25 "reviewer skipped
> (429 5h cap)" + 2026-07-13 direct-scan pivot). Base: `main` @ `62f1bf5`.

## Headline

**3 candidates → 3 confirmed real (inline-verified), all the same defect class:
a name/required-field validator uses truthiness (`not field`) which lets a
whitespace-only value (`" "`, `"\t\n"`) bypass the missing-field check** — the
exact pattern R29-D closed for mongodb `replica_set_name`. These are the
rabbitmq + redis instances of that pattern that R29's "rotate to under-audited
backends" surfaced (R29 memory noted pulsar/rabbitmq/redis as unmined).

The 429-blocked dimensions (utils, full r29-diff-regression) and the lower-
confidence PS-1 (pulsar subscription_name — backend already guards, just later)
are DEFERRED to R31 (after cap reset) along with 2 mongodb diff-regression
candidates surfaced by direct grep (`atlas_cluster_name`, `auth_source`).

## Ship set (3 units)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | MED | `backends/rabbitmq.py:377` | `virtual_host` validator uses `not virtual_host` (truthiness) → whitespace `" "` passes → opaque AMQP "unexpected frame" error at connect. |
| **B** | LOW | `settings/rabbitmq.py:471` + `backends/rabbitmq.py:372` | MIRRORED_QUEUES `not self.ha_mode` (truthiness) at BOTH the settings validator and the backend resolver — whitespace `ha_mode=" "` bypasses the fail-fast, emits a misleading "ha_mode required" later. Both siblings fixed for consistency (rule 7). |
| **C** | MED | `settings/redis.py:537` | SENTINEL `not self.sentinel_master_name` (truthiness) → whitespace `"  "` / `"\t\n"` bypasses the missing-field check → opaque sentinel discovery error. |

## Root cause (common)

All three are the R29-D pattern: a missing/required-field check written as
`not field` (truthiness) instead of strip-aware. `bool(" ") == True`, so a
whitespace value is treated as "set" and the fail-fast is bypassed; the
whitespace reaches the client lib and surfaces as an opaque connect-time error
(the meta-defect the R26–R29 settings-fail-fast theme closes).

## Fixes (minimal, TDD — mirror R29-D)

- **A:** `backends/rabbitmq.py:377` `not virtual_host` → `not virtual_host.strip()`.
- **B:** `settings/rabbitmq.py:471` `and not self.ha_mode` → `and not self.ha_mode.strip()`; `backends/rabbitmq.py:372` `and not ha_mode` → `and not ha_mode.strip()`.
- **C:** `settings/redis.py:537` `if not self.sentinel_master_name:` → strip-aware (`name = self.sentinel_master_name; if not name or not name.strip():`).

## DO-NOT-RE-FLAG additions after R30

- rabbitmq virtual_host + ha_mode (settings + backend) are strip-aware (R30-A/B).
- redis sentinel_master_name is strip-aware (R30-C).
- DEFERRED to R31 (cap-blocked, unverified): PS-1 pulsar subscription_name
  (LOW, backend already guards); mongodb atlas_cluster_name + auth_source
  (diff-regression grep candidates); utils-redaction-serialization dimension.

## Process note (429 cap)

The 5h usage cap (resets 07:50:18) blocked the R30 ultracode scan mid-run. This
round was completed via the established Claude-only fallback: extract scanner
candidates from the journal → inline-verify by the main loop (opus) → direct
TDD (no subagent fan-out). The subagent-driven scan + verify should resume at
R31 once the cap resets. This is the documented degraded mode (memory: 2026-07-13
direct-scan pivot, R25 reviewer-skipped-on-429).
