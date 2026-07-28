# R75 TASK

- [x] Confirm close can skip buffered data after interrupted join or threshold flush.
- [x] Drain through the close deadline while preserving the first control failure.
- [x] Add interrupted-join and slow-threshold-tail regressions.
- [x] Verify focused tests (55 passed), Ruff, strict mypy, and full suite before atomically committing R75.
