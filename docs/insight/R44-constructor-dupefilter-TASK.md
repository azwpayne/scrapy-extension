# R44 TASK — Constructor-supplied dupefilter lifecycle

- [x] Confirm branch/worktree is isolated and clean.
- [x] Read constructor, open/close ownership gates, immediate callers, and
      existing ownership tests.
- [x] Write RED tests:
  - constructor-supplied dupefilter is opened with the spider;
  - it is closed with the reason;
  - real templated key resolves during scheduler open;
  - no-dupefilter constructor remains safe.
- [x] Run focused tests and confirm failure is specifically missing lifecycle calls.
- [x] Set `_owns_dupefilter = dupefilter is not None` in `__init__`.
- [x] Re-run focused tests (GREEN).
- [x] Run `uv run ruff check .`.
- [x] Run `uv run mypy --strict src/`.
- [x] Run `uv run pytest -q`.
- [x] Review the merged `main` diff for only requested files.
- [x] Update LEDGER with confirmed result and evidence.
- [ ] Create atomic conventional commit referencing R44-F1 and these docs.
- [ ] Push without force, create draft PR, and wait for real CI/review.
- [ ] Merge only if CI is green; delete only branches created by this round.
- [ ] Re-run all gates on synchronized main.
