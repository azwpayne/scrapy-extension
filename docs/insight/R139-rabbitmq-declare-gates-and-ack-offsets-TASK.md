# R139 TASK checklist

- [ ] F1 RED/GREEN kafka legacy-ack explicit offsets + mixed-mode refusal
- [ ] F2+F3 RED/GREEN rabbitmq queue_len/clear_queue declare gates
- [ ] F4 RED/GREEN rabbitmq prefetch_size fail-fast + doc scope note
- [ ] F5 RED/GREEN rabbitmq pop-timeout docstring
- [ ] F6 RED/GREEN pulsar redelivery docstring/comments
- [ ] F7 RED/GREEN batched age-flusher deadline cadence
- [ ] ruff check
- [ ] ruff format --check src tests conftest.py
- [ ] uv run --frozen pytest
- [ ] mypy --strict src
- [ ] atomic commits + push HEAD:main
- [ ] LEDGER rows (6 LANDED + F6 deferred-part) + memory round entry + MEMORY.md
