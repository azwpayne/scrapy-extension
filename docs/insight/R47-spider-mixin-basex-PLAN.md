# R47 PLAN — Spider mixin BaseException-safe backend teardown

1. Add focused tests for component and manager process-control exceptions.
2. Replace the close loop with a first-error accumulator while retaining
   per-component cleanup and existing `Exception` logging.
3. Verify focused lifecycle tests, lint, strict typing, full pytest, and make
   one atomic conventional commit.
