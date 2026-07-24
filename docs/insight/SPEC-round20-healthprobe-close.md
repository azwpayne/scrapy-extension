# SPEC — Round 20: ES health-probe ApiError leak + pipeline close BaseException-swallow + on_pop_rate docstring

> Back-nav: [Round 19 SPEC](SPEC-round19-es-leak-hoist.md) · [Iterative hardening](ITERATIVE-HARDENING-2026-07-21.md)
> Scan: ultracode `wf_408fd496-5d5` (6-dim find+verify, exception-breadth audit centerpiece + r19-diff-regression, 9 agents, ~2.05M tokens)
> Tree: `main 598bbe9` (R19 head)

## Context & frontier state

Round 20 ran the **cross-backend exception-catch-breadth audit** (generalizing R19-A's ES `pop()`
finding to all 10 backends) plus the standing dimensions and an r19-diff-regression review.

**Result: 3 raw → 3 confirmed, 0 refuted.** The audit confirmed the **other 9 backends are clean**
for the catch-breadth pattern — only the ES **health-probe** remained (R19-A fixed `pop()` but
missed `is_connected()`/`ping()`). The r19-diff-regression dimension returned **empty** (R19
introduced no regressions). Deep diminishing returns — 3 LOW fixes.

## Problem statement

### A — ES `is_connected()`/`ping()` catches only TransportError — LOW (R19-A health-probe analog)
`is_connected()` (elasticsearch.py:181-187) wraps `self._client.ping()` in `except TransportError:
return False`. `TransportError` (transport-layer) and `ApiError` (HTTP-response hierarchy) are
**siblings**, not parent/child — so an `AuthenticationException`/`AuthorizationException`/
`UnsupportedProductError` raised by `ping()` escapes raw past the bool-return contract. This is the
health-probe analog of R19-A: R19-A fixed `pop()` (data hot-path) but missed the probe. Every sibling
ES hot-path catches `(ApiError, TransportError)`; every OTHER backend's `ping()` uses a broad catch
(redis `_REDIS_OPERATION_ERRORS`, mongodb `PyMongoError`, kafka `KafkaError`, others bare `Exception`).
ES is the sole narrow-catch outlier.

### B — Pipeline `_close_locked` swallows BaseException during `connection_manager.close()` — LOW
`_close_locked` (pipeline.py:411-432) wraps `storage_strategy.close()` in try/finally; the finally
wraps `connection_manager.close()` in `try/except BaseException: logger.exception(...)` with **no
`raise`**. When `storage_strategy.close()` succeeds and a Ctrl+C/SystemExit lands during the
(blocking) `connection_manager.close()` → backend `disconnect()`, the interrupt is swallowed — the
operator can't break out of a hung shutdown. Same defect class as the R-swallow cluster (PR #63 fixed
4 cleanup-CMs; missed this). Fix: mirror the dupefilter `primary_error` pattern (dupefilter.py:730-768)
— capture the strategy error, and when manager-close is the ONLY failure, re-raise it.

### C — `on_pop_rate` docstring prose claims tag "fixed at 1m" — LOW (docs)
`stats.py:216-218` states "The window tag is fixed at ``1m`` because … ``BackendQueue`` always passes
that window". This is false: the code emits a **dynamic** tag (`tag = '1m' if window_s ==
DEFAULT_POP_RATE_WINDOW_S else f'{window_s:g}s'`, stats.py:226), and `BackendQueue` forwards the
operator-configurable `SCRAPY_MONITOR_POP_RATE_WINDOW_S`. The docstring's own Args section two lines
below contradicts it. R19-C fixed the runbook + 4 sibling docstrings but skipped this prose
justification.

## Non-goals (DO-NOT-RE-FLAG — accumulated)
- bloom/cuckoo. · all connect() `from None` (secret-redaction). · pulsar `_RedactedStr`. · dynamodb
  `clear_storage` TOCTOU. · `_push_is_durable` pin. · connect()-BaseException cluster (9 — CLOSED).
  · ES `add()` :417 R-dupe-1 narrowing. · ES `pop()` (ApiError, TransportError) — R19-A fixed.
  · R17/R18/R19 arms. · **the 9 non-ES backends are CLEAN for the catch-breadth pattern** (audit
  confirmed) — do NOT re-audit unless a concrete new outlier appears.

## Units (3)
| ID | Sev | Surface | Fix |
|----|-----|---------|-----|
| A | LOW | elasticsearch.py:189 | `except TransportError` → `except (ApiError, TransportError): return False` |
| B | LOW | pipeline.py:411-432 | mirror dupefilter `primary_error` pattern — re-raise BaseException when no primary error |
| C | LOW | stats.py:216-218 | rewrite prose: dynamic window-tag, not "fixed at 1m" |

## Success criteria
- ruff clean; mypy --strict 0 issues; pytest ≥ 3767 passed / 46 skipped (unsandboxed); coverage ≥ 95%.
- Each unit: ONE atomic commit; TDD (RED before GREEN) for A + B; C is docs.
- All merged to `main`; only `main` remains. Claude-only.
