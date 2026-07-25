# Round 25 — SPEC: untouched-surfaces round (frontier NOT empty)

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_13296e2b-138` (6-dim find + adversarial verify;
> 15 agents, 0 errors, ~2.9M tokens, ~19 min). Base: `main` @ `93efa82`
> (post-R24).

## Headline

**The pivot to untouched surfaces paid off: 9 raw → 8 confirmed, 1 refuted.**
R24's "code surface clean" was an artifact of running EXHAUSTED dimensions
(input-validation caps, connect()-BaseException, clear_* swallow) — not a
hardened codebase. R25 rotated all 6 dimensions to **previously-unaudited
surfaces** (scheduler deep-audit, spider_mixin+serializers, security/deser,
utils+config, observability correctness, resource-bounds) and found 8 real
defects. **The frontier was not empty — it just needed new dimensions.** This
vindicates the "rotate to new attack surfaces" strategy over the R24 worry
that the cron cadence should drop.

## Scan result

**9 raw → 8 confirmed, 1 refuted.** Per-dimension: `scheduler-deep` EMPTY
(the scheduler's durability-bound push / dedup-marker / rollback are genuinely
correct), `spider-mixin-serializers` 2→1 confirmed/1 refuted, `security-deser`
1 confirmed, `utils-config` 1 confirmed, `observability` 3 confirmed, `resource-
bounds` 2 confirmed.

## Ship set (7 units)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | LOW | `queue/queue.py:774` | callback/errback resolution admits dunder methods — a crafted queue payload with `callback='__init__'` passes the `__self__ is spider` guard → Scrapy calls `spider.__init__(response)`, re-initializing the spider and corrupting state. Defense-in-depth (requires queue write access). Fix: reject `__`-prefixed (dunder) names before getattr. |
| **B** | LOW | `queue/queue.py:1198` | `_restore_snapshot` has unbounded `storage.retrieve()` → `bytes(state)` + `json.loads` — a corrupt/malicious multi-GB blob at the snapshot key OOM-kills worker startup. The lone unbounded storage-retrieve→deserialize surface (pop and pipeline-store paths are capped at `max_item_bytes`). Fix: `MAX_SNAPSHOT_BYTES` cap (16 MiB) mirroring the pop-path guard. |
| **C** | LOW | `storage/strategies/factory.py:75` | `create_storage_strategy` ad-hoc `int()` coercion silently truncates float thresholds (`50.9`→`50`, subverting the R21-D constructor guard) and leaks bare `TypeError`/`ValueError` instead of the codebase-standard `ConfigurationError`. Fix: `parse_int_setting(minimum=1)`. |
| **D** | MED | `queue/strategies/delay.py:333` | `queue/delay_depth` gauge emits only on push, NEVER on drain → pegs at peak and cannot fall. The runbook tells operators to alert on this gauge, but it can't clear. Fix: emit `on_delay_depth` at end of `_drain_ready` + on `clear()`. |
| **F** | LOW | `backends/connectors.py:752` | `backend/{connect,disconnect,retry}_count` emit only for the queue's ConnectionManager; dupefilter/pipeline managers stay `NullMonitor` in multi-backend deployments → silent undercount. Fix: call `connection_manager.set_monitor(self._monitor)` in dupefilter + pipeline `from_crawler`. |
| **G** | LOW | `settings/rocketmq.py:139` | `producer_group` Field is dead config — the apache 5.1.1 gRPC `Producer` is group-less (group is consumer-side only); 0 refs. R22-C defect class. Fix: remove the Field + test refs. |
| **H** | LOW | `settings/rocketmq.py:163` | `set_topic_prefix` + `storage_topic_prefix` Fields are vestigial dead config — RocketMQ rejects set/storage at config time (`RocketMQSetBackend`/`RocketMQStorageBackend` always raise). Fix: remove the Fields + test refs. |

## Deferred (1) — documented, NOT fixed this round

| Surface | Why deferred |
|---------|--------------|
| `queue/queue.py:540` (**R25-E**, MED) `queue/pop_rate_1m` freezes (doesn't fall to ~0) when pops stop, violating the documented "falling-edge to ~0 = stalled consumer" contract. | The correct fix is a **heartbeat daemon thread** that recomputes `len(_pop_timestamps)/window` every ~window/2 independent of pop events (mirroring `BatchedStorageStrategy`'s age-flusher). That introduces a **new concurrency surface** — a lock around `_pop_timestamps` + thread lifecycle + interaction with `close()` — that deserves a focused, carefully-tested round, not a rush inside an 8-unit round. Observability-only (no data loss); the scheduler's periodic poll (`scheduler.py:1910-1920`) partially mitigates the common backpressure-pause case; the genuine freeze (worker deadlock / engine pause) is the uncovered path. Document the limitation in the monitor contract this round; ship the heartbeat in a dedicated observability round. |

## Refuted (1) — intentional/documented

- `spider/spider_mixin.py:509` `get_queue()` ignores `SCRAPY_QUEUE_MAX_ITEM_BYTES`
  etc.: **INTENTIONAL** — the mixin is a convenience path; the scheduler
  `from_crawler` path is the documented source of truth for tuning; the runbook
  + docstrings say so explicitly. The default 1 MiB DoS cap IS applied via
  `DEFAULT_QUEUE_MAX_ITEM_BYTES`. Correctly killed.

## DO-NOT-RE-FLAG additions after R25

- callback/errback resolution rejects dunders (R25-A); snapshot restore is size-
  capped (R25-B); `create_storage_strategy` validates threshold via
  `parse_int_setting` (R25-C); `queue/delay_depth` emits on drain+clear (R25-D);
  multi-backend ConnectionManager monitor wiring (R25-F); rocketmq
  producer_group / set_topic_prefix / storage_topic_prefix removed (R25-G/H).

## Frontier observation

R25 disproves R24's "codebase fully hardened" reading: the frontier replenishes
when dimensions rotate to untouched surfaces. The cron cadence is still
productive. Productive next-round surfaces (still untouched): the heartbeat
fix for R25-E (deferred), a deeper `utils/` audit, the `monitor/` ABC +
`NullMonitor` contract, and the entry-point/3rd-party-backend registration path.
