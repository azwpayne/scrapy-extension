# Round 26 — SPEC: r25-regression + cross-cutting hardening

> Back-navigation: [../insight](./) ·Driven by durable cron `d1ad784b`.
> Scan: ultracode workflow `wf_262be653-4d7` (6-dim find + adversarial verify;
> 14 agents, 0 errors, ~2.6M tokens, ~37 min). Base: `main` @ `af80938` (post-R25).

## Headline

**8 raw → 7 confirmed, 1 refuted.** The **r25-diff-regression dimension caught 3
issues in my own R25 work** — including the headline MED: R25-B's 16 MiB snapshot
cap is too aggressive and silently drops *legitimate* large strategy snapshots
(persist is uncapped, restore is capped = an asymmetric data-loss trap). The
adversarial N-diff-regression dimension has now caught a self-shipped regression
in **6 consecutive rounds** (R17-B, R18-C, R19-B, R23-A, R25-A's test payload
shift, R26-A) — it is the single highest-ROI dimension in the scan and must
never be dropped.

Two dims EMPTY: `connectors-fence-deep` (the 2-layer generation fence is sound)
and `monitor-abc` (the Monitor ABC + NullMonitor no-op every hook correctly).

## Scan result

**8 raw → 7 confirmed, 1 refuted.** Per-dimension: `r25-regression` 4→3
confirmed/1 refuted, `connectors-fence-deep` EMPTY, `monitor-abc` EMPTY,
`request-ser-fields` 1, `settings-validators` 2, `lifecycle-ordering` 1.

## Ship set (7 units)

| ID | Sev | Surface | Defect (one line) |
|----|-----|---------|-------------------|
| **A** | MED | `queue/queue.py:1231` | **R25-B regression (self-caught):** the 16 MiB snapshot cap silently drops *legit* large snapshots on restart. At `queue_delay_max_held` default 100k × ~2.7 KB/entry, snapshots reach 70–270 MB; the cap triggers at ~6–12k items. `_persist_snapshot` writes the blob uncapped, then `_restore_snapshot` drops it — discoverable only on the NEXT restart via one WARNING line. |
| **G** | MED | `schedule/scheduler.py:1506` | `_close_locked` uses `except Exception` (not `BaseException`) for the 3 teardown guards and the `connection_manager.close()` is NOT in a `finally`. A BaseException (Ctrl+C/SystemExit) during `dupefilter.close()`/`queue.close()` escapes → CM close skipped → manager pinned (`_users` never decrements, socket/fd leak, registry pin to `MAX_MANAGERS=32`). R13/PR#54's close guard is incomplete on the BaseException axis. |
| **F** | MED | `settings/elasticsearch.py:179` | CLOUD mode accepts config with NO auth (no `api_key`, no `username`/`password`) → opaque `BackendConnectionError('health check returned false')` at connect (Elastic Cloud always 401s anonymous). The docstring half-considered basic_auth as an alternative but never required ≥1 auth method. |
| **B** | LOW | `tests/test_queue.py:1450` | R25-A's test-payload swap (`__class__`→`name`) shifted coverage from the `inspect.ismethod` guard arm to the `callable` arm — the ismethod arm is now untested, and the test name is misleading ("callable attribute" now tests a non-callable). |
| **C** | LOW | `tests/test_dupefilter.py:1273` | R25-F tests assert `set_monitor` was called but not *what* monitor — a refactor that passed `NullMonitor` by mistake would still pass. |
| **D** | LOW | `queue/queue.py:735` | `_validate_request_dict` misses `dumps_kwargs` (the only JsonRequest-specific attribute) — a crafted payload with non-dict `dumps_kwargs` passes validation and crashes `_request_from_dict` with an opaque `AttributeError` instead of the clean `TypeError` the validate layer exists to produce. |
| **E** | LOW | `settings/kafka.py:435` | CONFLUENT mode without `confluent_bootstrap_servers` silently falls back to the STANDALONE `localhost:9092` default → opaque SASL_SSL/PLAIN handshake error at connect (R9b closed the PLAINTEXT dimension but left the localhost-endpoint dimension open). |

## Refuted (1)

- `queue/queue.py:792` R25-A dunder reject "over-reaches" on `__mycallback__`:
  deliberate defense-in-depth (PEP 8 reserves `__*__` names for the system; the
  codebase's own R25-A test docstring scopes it to `__`-prefixed); the
  suggested fix is internally inconsistent (would still reject
  `__mycallback__`); no real-world incidence. Correctly killed.

## DO-NOT-RE-FLAG additions after R26

- snapshot cap raised to 128 MiB + persist-time warning + documented (R26-A);
  scheduler `_close_locked` is BaseException-safe with primary_error + CM-close-in-finally (R26-G); ES CLOUD requires ≥1 auth method (R26-F);
  `_validate_request_dict` covers `dumps_kwargs` (R26-D); kafka CONFLUENT
  rejects localhost-default-endpoint (R26-E); R25-A/F tests strengthened (R26-B/C).

## Frontier observation

R26 confirms R25's lesson: the frontier keeps replenishing when dimensions
rotate. R26's productive dims were `r25-regression` (self-bug catch), `settings-
validators` (cross-field gaps — a NEW dimension), and `lifecycle-ordering` (also
NEW). The `connectors-fence-deep` + `monitor-abc` dims returned EMPTY — those
subsystems are genuinely clean. Productive next-round candidates: a deeper
`_retry.py` audit, the entry-point/3rd-party-backend registration path, and the
R25-E deferred pop_rate heartbeat (still open).
