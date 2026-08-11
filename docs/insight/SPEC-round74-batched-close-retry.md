# SPEC — R74: BatchedStorageStrategy.close() abandons the requeued tail after a partial drain

> Back-navigation: [../insight/](.) · Round log in [MEMORY index](../../../../.claude/projects/-Users-payne-WorkSpace-project-individual-dev-web-crawler-scrapy-extension/memory/MEMORY.md).
> R83 fire (2026-08-11). No fresh scan — ships the R82-queued finding #2 (confirmed real, surgical, clean-file). R82's 4-dim scan produced it; this fire implements it. Queued siblings priority.py:213 and spider_mixin.py:182 remain for future fires.

## Context and audit evidence

`BatchedStorageStrategy.close()` (storage/strategies/batched.py:359-366) runs the final drain exactly once:

```python
        else:
            try:
                self._flush_serialized()
            except BaseException as error:
                if primary_error is None:
                    primary_error = error
            finally:
                self._flush_lock.release()
```

`_flush_serialized()` (batched.py:418-452) implements at-least-once under partial failure: it snapshots+clears `_buffer`, writes each item via `storage_backend.store(...)`, and on any `BaseException` (L428) calls `_requeue_tail(batch[i:])` to **re-enqueue this item + the remaining tail into `_buffer`** (L434), then **re-raises** (L452). `_requeue_tail` (494-513) prepends the tail to `_buffer` and adjusts `_in_flight_count`. The module docstring + the `_flush_serialized` comments (L429-433) explicitly promise "At-least-once: no silent loss."

**The gap:** `close()` captures that re-raised exception and re-raises it **without ever re-entering `_flush_serialized`** to retry the tail it just re-enqueued. After `close()` raises, `self._closed=True` (set at L295) and `self._stop` is set (L296) — no further flush runs, and the pipeline's `_close_locked` then closes the backend connection. So the requeued tail sits in a closed strategy's buffer and is never persisted. The re-enqueue was wasted work; at-least-once is broken at the final drain.

### Concrete failure scenario

`BatchedStorageStrategy(threshold=100)` (the default), `max_buffer_age_s=None`. Spider shuts down with 50 items buffered (threshold not yet reached). `close()` acquires `_flush_lock` and calls `_flush_serialized()`. `backend.store()` writes items 0..24, then raises a transient `Exception` on item 25 (a network blip — a store *exception*, distinct from crash-loss). `_flush_serialized` re-enqueues items 25..49 into `_buffer` (at-least-once) and re-raises. `close()` captures the blip as `primary_error`, releases the lock, re-raises. The pipeline closes the backend. Items 25..49 (25 items) are silently lost. The requeue fired but nothing retried it. With the default `threshold=100`, up to ~99 items can be buffered at close, so a single transient mid-drain blip can lose up to ~99 items — a one-write retry would persist them (the blip is transient).

The age-flush loop ALREADY retries on subsequent cycles (batched.py:571-604); `close()` is the only path that re-enqueues without retrying — an internal inconsistency the fix resolves.

## Goal

Honor the at-least-once contract at the final drain: when `close()`'s single `_flush_serialized()` fails with an ordinary store `Exception` while items remain re-enqueued, retry the drain once before propagating the error. Control signals (`KeyboardInterrupt`/`SystemExit`) remain honored (no retry, re-raised as today).

## Specification

In `close()`, replace the single `_flush_serialized()` call (L360-364) with an inner `try`/`except Exception`/retry-once, wrapped by the existing outer `except BaseException`:

```python
        else:
            try:
                try:
                    self._flush_serialized()
                except Exception as first_error:
                    if not self._buffer:
                        raise first_error
                    self._flush_serialized()  # retry once — tail re-enqueued by _flush_serialized
            except BaseException as error:
                if primary_error is None:
                    primary_error = error
            finally:
                self._flush_lock.release()
```

Semantics:
- **First drain succeeds** → done; no error. (Unchanged.)
- **First drain raises an ordinary store `Exception` with items re-enqueued** (`_buffer` non-empty) → retry once. If the retry succeeds, the transient first error is swallowed (items eventually persisted — at-least-once fulfilled). If the retry raises, its error propagates to the outer handler (no worse than today).
- **First drain raises with an empty `_buffer`** (nothing to retry — e.g. failure on the last item with no remaining tail) → `raise first_error` immediately; the outer handler records it. (No swallow of an error that retried nothing.)
- **First drain raises a control `BaseException`** (`KeyboardInterrupt`/`SystemExit`) → NOT caught by the inner `except Exception` → propagates to the outer `except BaseException` → `primary_error`, re-raised after the drain phase. No retry. (Byte-identical control-signal handling to today.)

`_flush_serialized` requires the caller to hold `_flush_lock` (its docstring); `close()` holds it throughout the `else` block and does not re-acquire it inside `_flush_serialized`, so the retry is deadlock-free.

## Plan and independently verifiable tasks

- **R74-1 (RED):** Add `test_close_retries_requeued_tail_after_transient_store_failure` to the batched-strategy test file: build a `BatchedStorageStrategy(threshold=100)` with a mock backend whose `store` raises on the 25th distinct call then succeeds; buffer 50 items; call `close()`; assert (a) all 50 items were eventually stored (`store.call_count == 50`) and (b) `close()` does NOT raise (transient blip recovered). Fails before fix (`store.call_count == 25`, `close()` raises, 25 items lost).
- **R74-2 (GREEN):** Apply the inner try/except-Exception/retry-once in `close()`.
- **R74-3 (no-regression):** `test_close_*` (healthy backend, wedged flusher, control-signal during drain) stay green. Add `test_close_does_not_retry_control_signal` asserting a `KeyboardInterrupt` from the first drain is NOT retried (`store.call_count` reflects one drain only) and is re-raised.
- **R74-4 (GATE):** `uv run ruff check .` + `uv run ruff format --check src tests conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension` all green (sandbox-off, default uv cache). Format-check IS enforced (R64).
- **R74-5 (ship):** atomic commit + `git push origin HEAD:main` (ff) + verify CI green.

## Acceptance criteria

- A transient mid-drain `Exception` no longer loses the requeued tail: `close()` retries once and persists all items (R74-1 passes).
- A persistent `Exception` (retry also fails) still propagates (R74-3 / R74-1 negative case).
- Control `BaseException` during the drain is NOT retried and IS re-raised (R74-3).
- All existing batched-strategy close/flush tests stay green; full suite = R82's 5109 + new tests.
- ruff check + ruff format --check + mypy --strict clean; CI on `main` green.

## Confidence / risk

- **Confidence:** high — source-verified requeue-then-raise in `_flush_serialized`; `close()` confirmed to re-raise without retry; age-flush loop already retries (so the pattern is established in-file); R82's opus verifier constructed the full scenario and could not refute.
- **Scope-risk:** narrow — one block in `close()`; no signature change; control-signal handling byte-identical.
- **Constraint:** touch only `batched.py` (clean — NOT in the dirty tree) + new tests.
- **Rejected:** retry in a bounded loop (N times) | a single retry matches the age-flush loop's one-cycle-at-a-time discipline and avoids unbounded close latency under a persistent failure; a persistent failure still propagates. Looping would also need a deadline, adding scope.
- **Rejected:** retry on `BaseException` (including control signals) | would delay `KeyboardInterrupt`/`SystemExit` and risk swallowing a control signal the operator sent; current code defers control signals only to complete the drain, never to retry past one — preserve that.
- **Directive:** the batched at-least-once contract requires every path that calls `_requeue_tail` to also provide a retry. The age-flush loop retries its cycles; `close()` must retry its single drain. If a future path re-enqueues without retrying, at-least-once is silently broken — add a retry or document why the loss is acceptable.
- **Not-tested:** real backend round-trip (mock seam); behavior when `_requeue_tail` itself raises under lock contention (invariant guard, out of scope).
