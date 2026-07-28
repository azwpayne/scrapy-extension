# R64 TASK

- [x] Confirm a control exception from `store()` drops the unattempted snapshot tail.
- [x] Restore the tail before propagating any `BaseException`.
- [x] Add a KeyboardInterrupt retry-order regression.
- [x] Verify focused tests (53 passed), Ruff, strict mypy, and full suite before atomically committing R64.
