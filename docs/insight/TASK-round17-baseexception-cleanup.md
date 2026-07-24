# TASK — Round 17: BaseException cleanup-parity + abort null-ordering

> Spec: [SPEC-round17-baseexception-cleanup.md](SPEC-round17-baseexception-cleanup.md)
> Plan:  [PLAN-round17-baseexception-cleanup.md](PLAN-round17-baseexception-cleanup.md)
> Constraints: one atomic commit per unit · all → main · Claude-only.

## Unit A — kafka `_abort_partial_connect` null-first (MED, R16-A regression)
**File:** `src/scrapy_extension/backends/kafka.py` (helper at 388-408); test `tests/test_kafka_connect_cleanup.py`.
**Reference (correct):** mongodb.py:193-220 `_discard_client`; rocketmq.py:253-268 `_abort_partial_connect`.
1. [RED] Add test: `mock_producer.close.side_effect = KeyboardInterrupt`; force the BaseException arm
   (or call `_abort_partial_connect()` directly after setting `_producer`/`_admin_client`); assert
   `backend._producer is None` and `backend._admin_client is None`. Add `SystemExit` variant. Run → RED.
2. [GREEN] Rewrite helper null-first:
   ```python
   producer = self._producer
   admin = self._admin_client
   self._producer = None
   self._admin_client = None
   for closer in (producer, admin):
     if closer is not None:
       try:
         closer.close()
       except Exception:
         logger.debug("Failed to close Kafka client during abort", exc_info=True)
   ```
3. Update the docstring so "mirrors mongodb" is accurate; note close-then-null under
   `suppress(Exception)` was the residual wedge (R17-A).
4. Run gate; commit `fix(kafka): null-first _abort_partial_connect so a second BaseException cannot re-wedge the producer (R16-A regression)`.

## Unit B — rabbitmq `connect()` BaseException abort arm (MED, resource leak)
**File:** `src/scrapy_extension/backends/rabbitmq.py:491-534`; test `tests/test_rabbitmq_connect_cleanup.py` (or sibling).
1. [RED] Test: candidate returned by `_connect_standalone` (mocked), then `KeyboardInterrupt` raised
   by `_publish_handles_locked`; assert `candidate.connection.close()` AND `candidate.channel.close()`
   called (no leak). Second test: BaseException AFTER `published=True` → candidate NOT closed (live conn).
   Run → RED.
2. [GREEN] Hoist `candidate: _RabbitMQCandidate | None = None` and `published = False` before the
   build try. Wrap build+publish (491-534) in `try: … except BaseException: if not published and
   candidate is not None: self._close_handles(candidate.channel, candidate.connection); raise`.
   Keep `ConfigurationError` re-raise and the `except Exception → BackendConnectionError` arms inside.
3. Run gate; commit `fix(rabbitmq): close candidate BlockingConnection on BaseException in connect() build→publish window`.

## Unit C — memcached `connect()` BaseException abort arm (LOW, FD leak)
**File:** `src/scrapy_extension/backends/memcached.py:137-153`; test `tests/test_memcached_connect_cleanup.py` (or sibling).
1. [RED] Test: `candidate.stats()` raises `KeyboardInterrupt`; assert `candidate.close()` called.
   Run → RED.
2. [GREEN] Add `except BaseException:` arm mirroring the existing `except Exception` (147-153):
   ```python
   except BaseException:
     if candidate is not None:
       with _swallow():
         candidate.close()
     raise
   ```
3. Run gate; commit `fix(memcached): close candidate socket on BaseException during connect() stats()`.

## Unit D — real-CM durability contract test + retitle (LOW, test-quality)
**File:** `tests/test_mock_connection_manager_contract.py`; ref `src/scrapy_extension/backends/connectors.py:1615-1673` (translation 1654-1659).
1. Retitle the existing test class/module docstring: "fixture-parity — pins the conftest closure;
   production translation asserted in test_connectors.py::TestOperationBoundQueueDurability."
2. Add `test_push_durability_translation_uses_real_connection_manager`: build a real
   `ConnectionManager(BackendType.REDIS)`, set `manager._backend` to a fake `QueueBackend` whose
   `_push_with_durability(..., require_durable=True)` raises `_DurablePushRequired`; call
   `manager._push_queue_with_durability(...)`; assert `QueueError` with `queue_name` + `operation="push"`.
3. Run gate; commit `test(connectors): assert _DurablePushRequired→QueueError translation on the real ConnectionManager`.

## Definition of done
- [ ] ruff clean · mypy --strict 0 issues · pytest ≥3757 passed · coverage ≥95%
- [ ] 4 atomic commits on `worktree-round17-baseexception`
- [ ] ff-merged to `main`, pushed, worktree branch deleted
- [ ] memory updated (R17 close-out note in `deep-insight-2026-07-23-ultracode.md`)
