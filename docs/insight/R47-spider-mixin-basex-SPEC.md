# R47 SPEC — Spider mixin BaseException-safe backend teardown

## Finding

`BackendSpiderMixin.close_backend()` detaches all references before closing
its scheduler, queue, dupefilter, and shared `ConnectionManager`, but catches
only `Exception` around each close. A `KeyboardInterrupt` or `SystemExit`
from any component aborts the loop and skips `manager.close()`, leaking the
mixin's manager acquire and its backend resources.

## Required behavior

After state detachment, close every component independently, retaining the
first `BaseException`; always attempt `manager.close()` exactly once; then
re-raise the first error. Ordinary `Exception` failures remain isolated and
logged, preserving the current signal-handler behavior.

## Acceptance criteria

- A component `KeyboardInterrupt` still reaches later component close hooks
  and `manager.close()` exactly once.
- A manager `SystemExit` is re-raised after all earlier close hooks.
- State remains detached and repeated close is idempotent.
- Existing ordinary-error isolation remains unchanged.
