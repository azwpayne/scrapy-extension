# Round 42B — SPEC / PLAN / TASK: bounded batched-storage admission

**Base:** `main` after `3d8aa69`.

## Audit conclusion

`BatchedStorageStrategy` moves a full buffer into a flush snapshot before a
potentially blocking backend write.  Later `store()` calls therefore see an
empty buffer and can grow a second buffer without limit.  The `pending`
metric also omits the in-flight snapshot.  A normal `StorageError` is not an
adequate rejection signal because the pipeline's best-effort policy can treat
it as a successful item.

## Specification

1. Add `StorageBackpressureError(StorageError)`: it means the item was not
   admitted.  Its text is fixed and never contains a key or value.
2. `BatchedStorageStrategy.max_pending` is a strict non-boolean integer at
   least `threshold`; `None` yields `2 * threshold`.  It counts buffered plus
   in-flight items.
3. Snapshotting, release, tail requeue, and depth observation maintain the
   locked invariant `0 <= buffered + in_flight <= max_pending`.
4. `store()` checks capacity before appending and immediately raises
   `StorageBackpressureError(operation="store")` when full; it never waits for
   the flush lock merely to reject an item.
5. `pending` and monitor depth report all outstanding items.  Backend failure,
   monitor failure, and close/drain paths must release/requeue accounting
   without breaking FIFO behavior.
6. The pipeline re-raises `StorageBackpressureError` immediately.  Settings
   expose `SCRAPY_STORAGE_BUFFER_MAX_PENDING`; invalid values fail fast.
7. This is an item-count bound, not a byte-memory bound for direct callers;
   byte capacity is an explicit later concern.

## Plan

1. Add the exception and admission/accounting state.
2. Thread the setting through factory, pipeline, configuration validation, and
   operator documentation.
3. Add deterministic blocked-flush and recovery tests plus pipeline behavior.
4. Verify focused/full suites and create a standalone atomic commit.

## Task checklist

- [ ] Exception and bounded accounting implementation.
- [ ] Factory/settings/pipeline/docs integration.
- [ ] Concurrency, requeue, monitor, and fail-fast regressions.
- [ ] Full verification and atomic commit.
