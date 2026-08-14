# R132 PLAN — concurrent-disconnect handle races

> Spec: [R132-concurrent-disconnect-handle-races-SPEC.md](R132-concurrent-disconnect-handle-races-SPEC.md)
> Workflow: worktree `r92-scan` → RED → GREEN → gate → two atomic commits → push HEAD:main (ff) → ledger + memory.

## Phase 1 — TDD RED

### A. rocketmq subscription-generation race (`tests/test_rocketmq_resilience.py`)

Follow the file's existing `_connected_backend(mocker)` / TOCTOU conventions:

1. `test_subscribe_in_flight_reconnect_does_not_poison_subscribed_topics`:
   - `backend = _connected_backend(mocker)`; make the mock consumer's
     `subscribe` side effect call `backend.disconnect()` (mocks make this
     safe: `_shutdown_detached_clients` is best-effort) and return — this
     simulates a disconnect completing while the subscribe is in flight.
   - Call `backend._receive_message("q", 0.0)` (or `pop`) and assert:
     `pytest.raises(QueueError, match="reconnected")` AND
     `backend._subscribed_topics == set()` (the new generation's set is not
     poisoned).
   - Current code: no such error, set gets the topic → RED.
2. `test_subscribe_records_topic_when_generation_stable` (regression guard):
   - normal path: `pop`/`_receive_message` on a connected mock backend;
     assert the topic IS in `_subscribed_topics` afterwards and no error.

### B. mongodb handle capture (`tests/test_mongodb_backend.py`)

Deterministic window simulation via a class-level `PropertyMock` (a data
descriptor shadows the instance attribute, so every attribute read is
programmable):

1. `test_pop_collection_none_between_guard_and_use_is_typed_error`:
   - Build a connected mock backend as the file's existing tests do; keep the
     real stub collection object.
   - `patch.object(MongoDBBackend, "_queue_collection", new_callable=PropertyMock)`
     with `side_effect=[stub_collection, None]` (first read = the None guard
     passes on the stub; second read = the use site sees None, i.e. a
     concurrent `_discard_client` landed in the window).
   - Assert `pytest.raises((QueueError, BackendConnectionError))`.
   - Current code re-reads the attribute → raw `AttributeError` → RED.
   - Fixed code reads once into a local → uses the stub → no typed-error
     needed (the call succeeds); if the assertion then fails because no error
     is raised, weaken the assertion to "not AttributeError" — prefer:
     `with contextlib.suppress(...)`-free explicit form: assert the call does
     NOT raise AttributeError (accept success or typed error).
2. Sibling test for one storage op and one set op (e.g. `store`, `add`) with
   the same PropertyMock pattern on `_storage_collection` / `_set_collection`.
3. Guard-only test mirroring the rocketmq convention: set
   `backend._queue_collection = None` with `is_connected` patched True →
   clean `BackendConnectionError` (probably already covered; keep if not).

## Phase 2 — Implement (GREEN)

### A. `src/scrapy_extension/backends/rocketmq.py` — `_ensure_subscribed`

```python
        try:
            consumer.subscribe(topic_name)
        except Exception as e:
            raise QueueError(...) from e
        with self._connection_lock:
            if self._consumer is not consumer:
                reconnected = True
            else:
                reconnected = False
                self._subscribed_topics.add(topic_name)
        if reconnected:
            raise QueueError(
                f"RocketMQ reconnected while subscribing to queue {queue_name}; retry pop",
                queue_name=queue_name,
                operation="pop",
            )
```

- Identity check (`self._consumer is not consumer`) subsumes the generation
  number: every generation bump also replaces/nulls the consumer object
  (connect :362, abort :418, disconnect :437).
- The set write is now atomic with the identity check under the same lock
  `disconnect()` takes, so no clear/add interleaving survives.
- `subscribe()` (network I/O) stays outside the lock — disconnect is never
  blocked by an in-flight subscribe.

### B. `src/scrapy_extension/backends/mongodb.py` — local handle capture

At each of the 16 op-method sites, replace the guard-then-re-read pattern:

```python
        collection = self._queue_collection
        if collection is None:
            msg = "MongoDBBackend not connected: queue collection is None"
            raise BackendConnectionError(msg, backend_type="mongodb")
        ...
        collection.insert_one(doc)
```

Sites: push :1053, pop :1090, queue_len :1143, clear_queue :1172
(`_queue_collection`); add :1202, remove :1246, contains :1283, set_len :1320,
clear_set :1351 (`_set_collection`); store :1391, retrieve :1431, delete :1478,
exists :1514, ttl :1545, delete(expire variant) :1580, clear_storage :1618
(`_storage_collection`) — line numbers from HEAD `e606f1c`, re-locate by
grep before editing. Keep every existing message text byte-identical
(safe-list / contract stability). If a method uses the handle more than once,
the single captured local covers all uses.

## Phase 3 — Gate (in worktree, plain commands, pytest unsandboxed)

```bash
ruff check src tests conftest.py
ruff format --check src tests conftest.py
uv run --frozen pytest
mypy --strict src
```

## Phase 4 — Ship

1. Commit A: `fix(rocketmq): re-check consumer identity before recording a subscription`
   (code + resilience tests).
2. Commit B: `fix(mongodb): capture collection handles before the None guard`
   (code + backend tests).
3. `LEDGER.md`: add R132 rows (A: LANDED; B: LANDED).
4. Push `HEAD:main` (fast-forward on remote; local primary main is 32 behind
   with a dirty pyproject.toml — do NOT touch the primary tree).
5. Memory: round entry + MEMORY.md index update.
