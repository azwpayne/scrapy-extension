# SPEC-round60 — get_queue silently returns stale cached queue on rebind (non-consumer backends)

## Context and audit evidence

Found via the R68 deep-insight scan (ultracode, dim `spider-mixin`), confirmed
REAL by an opus adversarial verifier (high confidence), and **independently
re-verified by hand** against the current tree before implementation.

`BackendSpiderMixin.get_queue` (`src/scrapy_extension/spider/spider_mixin.py`)
caches a single `BackendQueue` on `self._queue` and constructs only when
`self._queue is None`:

```python
            name = queue_name or f"{self.name}:queue"      # line 558
            previous_claim = self._consumer_queue_name
            self._claim_consumer_queue(name)               # line 560 — no-op for non-consumer backends
            try:
                if self._queue is None:                    # line 562 — skips construction on a 2nd call
                    self._queue = BackendQueue(
                        connection_manager=manager,
                        queue_name=name,
                        ...
                    )
            except BaseException:
                self._consumer_queue_name = previous_claim
                raise
            return self._queue                             # line 575 — hands back the FIRST queue
```

**The bug (asymmetric contract enforcement):** `_claim_consumer_queue` (line 330)
raises `ConfigurationError` on a second *distinct* queue name **only for
consumer-scoped backends** (Kafka/RocketMQ, via `_CONSUMER_SCOPED_BACKENDS`). For
the non-consumer backends (Redis, MongoDB, ES, RabbitMQ, Pulsar, SQS) it is a
no-op — so a second `get_queue("other-name")` call hits the `if self._queue is
None` guard (False — already constructed), **skips construction, discards the
new name, and returns the stale first queue** (whose `queue_name` is still the
FIRST name). Data intended for `other-name` is silently routed to the first
queue.

`test_consumer_backend_rejects_second_logical_queue` (`tests/test_spider_mixin.py:237`)
proves the intended contract is **one queue per spider mixin instance** (raise on
a second distinct name) — but it only enforces it for consumer-scoped backends.
`test_caches_queue_instance` (line 1315) calls `get_queue()` twice with no args
(same default name), so it does not encode the different-name case. No test
asserts the silent-stale-return is intended for non-consumer backends.

**Severity: medium.** This is a silent data-misrouting correctness bug, not a
defensive-hardening gap. An operator who (mis)calls `get_queue` with two names on
a Redis/Mongo/ES/etc. spider gets the first queue for both, with no error.

## Goal

Make `get_queue` honor the one-queue-per-spider contract uniformly: a second call
with a *different* name raises `ConfigurationError` for **all** backends (not
just consumer-scoped ones), while same-name re-calls keep returning the cached
instance.

## Specification

Track the bound queue name in a dedicated attribute (not read back off the
possibly-mocked `self._queue` instance — the verifier confirmed reading
`self._queue.queue_name` breaks `test_concurrent_getter_constructs_component_once`,
which mocks `BackendQueue` so `queue_name` is a `MagicMock` ≠ any string).

1. In `BackendSpiderMixin.__init__` (`spider_mixin.py:103`), alongside
   `self._queue`, add:
   ```python
   self._queue_name: str | None = None
   ```
2. In `get_queue`, set `self._queue_name = name` when constructing, and add an
   `elif` branch that raises when the requested name differs:
   ```python
            try:
                if self._queue is None:
                    self._queue = BackendQueue(
                        connection_manager=manager,
                        queue_name=name,
                        spider=self,
                        queue_strategy=self._build_queue_strategy_from_settings(manager),
                    )
                    self._queue_name = name
                elif self._queue_name != name:
                    raise ConfigurationError(
                        f"{self.__class__.__name__} is already bound to queue "
                        f"{self._queue_name!r}; cannot rebind to {name!r}.",
                        setting_name="queue_name",
                        setting_value=name,
                    )
            except BaseException:
                self._consumer_queue_name = previous_claim
                raise
   ```

`ConfigurationError` is already imported (`spider_mixin.py:16`). Consumer-scoped
backends still raise first inside `_claim_consumer_queue` (line 560, before the
`try`), so their behavior and tests are untouched. Same-name re-calls hit
neither new branch (`_queue_name == name`). `test_concurrent_getter_constructs_component_once`
stays green because `self._queue_name` is a real string set alongside
construction, never read off the mocked `BackendQueue`.

## Plan and independently verifiable tasks

- **R60-1 — RED test.** Add `test_non_consumer_backend_rejects_rebind_to_different_queue_name`
  to `tests/test_spider_mixin.py` (next to `test_consumer_backend_rejects_second_logical_queue`),
  mirroring it for a Redis spider: `get_queue("first-queue")` then assert
  `pytest.raises(ConfigurationError, match="already bound to queue")` on
  `get_queue("second-queue")`, and assert a same-name re-call still returns the
  cached instance. → verify: FAILS on current code (`DID NOT RAISE` — the second
  call returns the stale first queue).
- **R60-2 — GREEN fix.** Add `self._queue_name` in `__init__`; set it on
  construction and add the `elif self._queue_name != name:` raise in `get_queue`.
  → verify: the R60-1 test PASSES; `test_caches_queue_instance` and
  `test_concurrent_getter_constructs_component_once` still pass.
- **R60-3 — no-regression.** Full spider_mixin test file green; `ruff check` +
  `ruff format --check` + `mypy --strict` green.

## Acceptance criteria

1. `get_queue("a")` then `get_queue("b")` on a Redis/Mongo/ES/RabbitMQ/Pulsar/SQS
   spider raises `ConfigurationError` (not silent stale return).
2. `get_queue("a")` then `get_queue("a")` still returns the cached instance
   (caching preserved).
3. Consumer-scoped backends (Kafka/RocketMQ) unchanged (still raise via
   `_claim_consumer_queue`).
4. `test_concurrent_getter_constructs_component_once` (which mocks `BackendQueue`)
   still passes.
5. Gate green: `uv run ruff check .` + `uv run ruff format --check src tests
   conftest.py` + `uv run pytest` + `uv run mypy --strict src/scrapy_extension`.
6. One atomic commit, ff-merged to `main`; CI green.
