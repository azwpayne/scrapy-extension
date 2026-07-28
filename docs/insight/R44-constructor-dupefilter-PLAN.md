# R44 PLAN — Constructor-supplied dupefilter lifecycle

## Approach

1. Add focused tests beside existing scheduler ownership tests.
2. Prove RED: constructor-supplied dupefilter receives neither `open` nor
   `close` on the current tree.
3. Minimal GREEN change in `BackendScheduler.__init__`:

   ```python
   self._owns_dupefilter = dupefilter is not None
   ```

4. Keep `from_crawler`'s explicit `_owns_dupefilter = True` assignment; it is
   redundant but documents the factory boundary and avoids adjacent cleanup.
5. Run focused tests, ruff, mypy strict, then the full pytest suite.

## Files

- `src/scrapy_extension/schedule/scheduler.py`: one-line ownership initialization.
- Existing scheduler test module: lifecycle regression tests.
- `docs/insight/R44-constructor-dupefilter-{SPEC,PLAN,TASK}.md`.
- `docs/insight/LEDGER.md`: mark F1 LANDED only after all gates pass.

## Risks and checks

- Existing tests that inject a dupefilter may begin receiving lifecycle calls.
  This is the intended contract; any test encoding borrowed ownership is a
  conflict that stops the change for design review rather than weakening tests.
- `open` failure cleanup must still close the owned dupefilter once.
- Repeated close must not double-release; existing lifecycle guards verify this.

## Verification

```bash
uv run pytest <focused scheduler tests> -v
uv run ruff check .
uv run mypy --strict src/
uv run pytest -q
```
