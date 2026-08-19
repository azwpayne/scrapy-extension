# R138 TASK checklist

- [ ] F1 RED tests (colon discriminator fallback + collision + compat pins)
- [ ] F1 GREEN (`_names.py`)
- [ ] F2 RED tests (memcached disconnected clear)
- [ ] F2 GREEN (`memcached.py`)
- [ ] F3 RED pin (redis docstring)
- [ ] F3 GREEN (`redis.py`)
- [ ] ruff check
- [ ] ruff format --check src tests conftest.py
- [ ] uv run --frozen pytest
- [ ] mypy --strict src
- [ ] atomic commits + push HEAD:main
- [ ] LEDGER rows (3 LANDED, 3 DIRTY-BLOCKED)
- [ ] memory round entry + MEMORY.md
