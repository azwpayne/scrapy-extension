# R138 PLAN

1. **RED F1** — add `_names`/work-stealing tests: colon-bearing worker id →
   hash physical name; collision pair resolves to two distinct names;
   colon-free id keeps `f"{q}:{w}"`; colon-bearing queue name + colon-free
   worker keeps the legacy name; priority bucket with a colon-bearing queue
   name keeps `f"{q}:p{level}"`.
   → Verify: new tests fail, existing legacy-name pins still pass.
2. **GREEN F1** — `physical_strategy_queue_name` gains the
   colon-in-discriminator fallback. → Verify: focused tests pass.
3. **RED F2** — memcached tests: disconnected + `allow_flush_all=True` →
   connection-classified error (no flush-all advisory); connected +
   `allow_flush_all=False` → unchanged `NotImplementedError` capability
   message. → Verify: RED on current tree.
4. **GREEN F2** — split the `snapshot is None` guard per the SPEC. → Verify:
   focused tests pass.
5. **RED F3** — doc-contract pin: `ZPOPMIN` present / `ZRANGEBYSCORE` absent in
   `RedisBackend.__doc__`. → Verify: RED.
6. **GREEN F3** — fix the one docstring line. → Verify: GREEN.
7. Full gate (ruff check, ruff format --check, `uv run --frozen pytest`,
   `mypy --strict src`) → Verify: all green.
8. Atomic commits: fix(queue) F1, fix(memcached) F2, docs(redis) F3,
   docs(insight) SPEC/PLAN/TASK + LEDGER rows → push `HEAD:main`.
