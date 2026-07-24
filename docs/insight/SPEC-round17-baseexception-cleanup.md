# SPEC — Round 17: BaseException cleanup-parity + abort null-ordering

> Back-nav: [Round 16 SPEC](SPEC-round16-post-r15-rescan.md) · [Iterative hardening](ITERATIVE-HARDENING-2026-07-21.md)
> Scan: ultracode `wf_de4e4b30-f02` (6-dim find + adversarial verify, 12 agents, ~1.9M tokens)
> Tree: `main 9a6a2b2` (base `45a3db6` = R16-A)

## Context & frontier state

Round 16 shipped R16-A (kafka + rocketmq `except BaseException` arm in `connect()`), closing
the R-mcc#60 / R-kacc#67 / R-pulsar#73 wedge cluster for the **BaseException** variant. The
Round-17 frontier scan re-ran 6 dimensions (incl. an R16-diff-regression dimension) on the
post-R16 tree.

**Result: 6 raw → 5 confirmed, 0 refuted, 1 thrash** (v1-docs CHANGELOG verify autocompact'd).
Diminishing-returns frontier as predicted — but every confirmed finding is real and closes the
**R16-A "connect() BaseException cleanup" cluster**.

## Problem statement

R16-A established the package standard: `connect()` must clean up partial candidates on
`Ctrl+C`/`SystemExit` (`BaseException`), not only on `Exception`. Two residual defects violate it:

### A — kafka `_abort_partial_connect` is close-then-null (R16-A's OWN regression) — MED
`KafkaBackend._abort_partial_connect` (kafka.py:401-408) nulls **after** `close()` under
`contextlib.suppress(Exception)` — which does NOT catch `BaseException`. R16-A's new
`except BaseException` arm routes INTO this helper while a `BaseException` is already in flight.
A **second** `Ctrl+C` during the blocking `KafkaProducer.close()` re-raises out of the `suppress`,
skips `self._producer = None` → `_producer` stays set → `is_connected()` lies True + producer
socket/thread leak. Diverges from mongodb `_discard_client` (null-first, mongodb.py:195-220) which
R16-A's own comment claims to mirror. The sibling R16-A fix in rocketmq (`_abort_partial_connect`,
rocketmq.py:253-268) is **correct** (null-first) — kafka is asymmetric within the same commit.

### B — rabbitmq `connect()` lacks the R16-A BaseException arm — MED
rabbitmq.py:491-534: the candidate-build try has only `except ConfigurationError` / `except Exception`
(498-508); the publish window (510-534, two lock `with` blocks) is unprotected; the file contains no
`except BaseException` anywhere (grep-confirmed). A `Ctrl+C` in the window between candidate return
(493-497) and publish (515) bypasses both except arms and never reaches the `if not published` close
(527) → the candidate pika `BlockingConnection` (background I/O/heartbeat thread + TCP FD) leaks
through graceful shutdown. **Resource leak, not wedge** — the candidate is never published to
`self._connection`/`self._channel` on the abort path, so `is_connected()` stays truthful.

### C — memcached `connect()` lacks the R16-A BaseException arm — LOW
memcached.py:143-153: `candidate.stats()` (146) is the first command to open the TCP socket; the
`except Exception` arm (147-153) closes the candidate, but a `Ctrl+C` during `stats()` bypasses it
(pymemcache opens the socket lazily) → candidate socket leaks. Bounded: candidate never published
(generation-fenced at 158-160), single FD, GC-reclaimable. Same R16-A parity gap as B (kafka /
rocketmq / dynamodb have the arm; memcached missed).

### D — R16-C contract test asserts on a mirror double — LOW (test-quality)
`tests/test_mock_connection_manager_contract.py` calls the `mock_connection_manager` fixture's
MagicMock-installed closure (conftest.py:38-55), not the real
`ConnectionManager._push_queue_with_durability` (connectors.py:1615-1673, translation at 1654-1659).
Bounded false-green: real production coverage lives in `test_connectors.py:1329-1428`. Failure is
future-tense (only bites if production translation changes AND the real-CM suite is deleted).

## Non-goals (DO-NOT-RE-FLAG)
- **pulsar / rabbitmq / memcached `connect()` `from None`** — DELIBERATE secret-redaction
  (`test_*_error_traceback_does_not_echo_driver_secrets` prove underlying broker errors carry driver
  secrets). R16 false-positive — do NOT change to `from e`.
- **bloom / cuckoo filters** — audited clean (never-FN contract holds; 10 scary findings refuted fire-11).
- **dynamodb `clear_storage` TOCTOU** — documented best-effort (R16-D comment corrected).
- **v1-docs CHANGELOG gap** — the thrashed verify; deferred to a doc-only follow-up (not this round).

## Units (4)
| ID | Sev | R16-reg | Surface | Fix |
|----|-----|---------|---------|-----|
| A | MED | ✅ | kafka.py:401-408 | null-first reorder (capture locals → null attrs → close locals under `try/except Exception`) |
| B | MED | — | rabbitmq.py:491-534 | `try/except BaseException` over build+publish; close candidate iff `not published` |
| C | LOW | — | memcached.py:137-153 | `except BaseException` arm: close candidate under `_swallow()`, re-raise |
| D | LOW | — | test_mock_connection_manager_contract.py | retitle as fixture-parity + add real-CM test exercising connectors.py:1654-1659 |

## Success criteria
- ruff clean; mypy --strict 0 issues / all files; pytest ≥ 3757 passed / 46 skipped; coverage ≥ 95%.
- Each unit ships as ONE atomic commit; TDD (RED before GREEN) for A/B/C/D.
- All work merged to `main`; only `main` remains (worktree branch deleted).
- Claude-only (no gemini / codex / GPT).
