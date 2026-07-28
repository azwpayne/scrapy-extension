# R46 TASK — Local-delay timeout delivery

- [x] Audit both strategies and prove the one-drain timeout gap.
- [x] Add deterministic `pop` and `pop_with_ack` ready-during-timeout tests.
- [x] Bound backend waits by the next local release and retry after draining.
- [x] Run strategy-focused tests (111 passed), Ruff, strict mypy, and full
  pytest (3,889 passed, 43 skipped).
- [x] Update the ledger and create the R46 atomic conventional commit.
