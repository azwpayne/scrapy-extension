# R44 SPEC — Constructor-supplied dupefilter lifecycle

## Finding

`BackendScheduler.__init__(..., dupefilter=df)` stores `df` but hard-codes
`_owns_dupefilter = False` (`schedule/scheduler.py:610-613`). Both lifecycle
calls require that flag (`open`: 1256-1263; `close`: 1498-1509), so the
scheduler silently skips `df.open(spider)` and `df.close(reason)`.

`BackendDupeFilter.open(spider)` is load-bearing: it resolves `{spider}` in the
dedup key and applies clear-on-open. The constructor docstring calls
`dupefilter` an optional scheduler dependency but documents no caller-managed
lifecycle and exposes no `owns_dupefilter` switch. In contrast, borrowed
connection-manager ownership is explicit through `owns_connection_manager`.

## Trigger and harm

1. Construct `BackendScheduler(manager, dupefilter=df)`.
2. `df.key == "dedup:{spider}"`.
3. Call `scheduler.open(spider_a)`.
4. Current code skips `df.open(spider_a)` because `_owns_dupefilter` is false.
5. The literal key remains unresolved, so different spiders can share one
   backend dedup set; clear-on-open is also skipped.
6. `scheduler.close(reason)` skips `df.close(reason)`, leaving its lifecycle
   unreleased.

## Required behavior

A non-`None` dupefilter supplied to the constructor is scheduler-owned and must
receive exactly one `open(spider)` and one `close(reason)`. A `None` dupefilter
remains unmanaged. `from_crawler` behavior remains unchanged.

## Acceptance criteria

- Constructor with `dupefilter=df` sets `_owns_dupefilter` true.
- `scheduler.open(spider)` calls `df.open(spider)` exactly once.
- `scheduler.close(reason)` calls `df.close(reason)` exactly once.
- A real `BackendDupeFilter` resolves `dedup:{spider}` during scheduler open.
- Constructor with no dupefilter preserves current behavior.
- Existing from-crawler lifecycle, retry, and close tests remain green.

## Non-goals

No new ownership option, no refactor, and no changes to R33 retry semantics,
R34 reservation handling, or R35 scheduler BaseException teardown.
