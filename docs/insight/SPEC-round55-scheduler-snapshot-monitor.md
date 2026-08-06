# Round 55 — SPEC / PLAN / TASK: thread the resolved monitor into the snapshot ConnectionManager

## Context and audit evidence

`5c2f7c5` introduced a separate **snapshot `ConnectionManager`** (`BackendScheduler._snapshot_connection_manager`, acquired at scheduler.py:986/1185) that lets stateful queue-only strategies (delay / round_robin / time_wheel / ring_buffer) reuse the configured storage component for restart checkpoints when the queue backend is storage-incapable (Kafka/RabbitMQ/RocketMQ/SQS/Pulsar).

`BackendScheduler.open()` (scheduler.py:1472-1494) resolves the spider's monitor and threads it into **two** places:

```python
monitor = BackendScheduler._resolve_monitor_for_spider(spider, ...)
# R14-D follow-up: thread the resolved monitor into the ConnectionManager ...
self.connection_manager.set_monitor(monitor)        # line 1482 — QUEUE manager only
self._queue = BackendQueue(
    connection_manager=self.connection_manager,
    ...
    snapshot_connection_manager=self._snapshot_connection_manager,  # passed, but unmonitored
)
```

It **never** calls `self._snapshot_connection_manager.set_monitor(monitor)`. Repo-wide grep confirms `set_monitor` appears only at line 1482 (queue manager). The snapshot manager is a **distinct registry instance** (keyed redis vs kafka, connectors.py:1374/1532), so its `_monitor` stays the default `NullMonitor()` (connectors.py:1465) and its `on_connect`/`on_disconnect`/`on_retry` hooks (connectors.py:2037/2081/2088) all fire as no-ops — the **identical visibility gap R14-D closed for the queue manager**, reintroduced on the fresh snapshot acquire.

Operator impact: a Kafka-queue + Redis-storage delay-scheduler crawl where the Redis snapshot backend hits a network blip and reconnects → `backend/disconnect_count` + `backend/retry_count` bump for the **queue** backend (R14-D working) but stay at **zero** for the snapshot backend; the snapshot backend's degraded health is invisible in stats until the checkpoint persist fails.

## Goal

The snapshot `ConnectionManager` must receive the same resolved monitor as the queue manager, so its connect/disconnect/retry lifecycle hooks fire in production stats — closing the R14-D gap on the new snapshot path.

## Specification

- In `open()`, immediately after the queue-manager `set_monitor` (line 1482), thread the same `monitor` into `self._snapshot_connection_manager`, guarded by `if self._snapshot_connection_manager is not None:` (it is `None` when no stateful-snapshot pairing is configured).
- Match the R14-D pattern verbatim. `set_monitor` is idempotent (connectors.py:2379-2381), so the multi-spider overwrite semantics already accepted for the queue manager apply identically — no new concurrency concern.
- No `close()` change: `set_monitor` does not own the monitor (it is shared/external), and `close()` already releases the snapshot manager when owned (scheduler.py:1819-1828).
- Surgical: 3 lines + comment. Touch only `scheduler.py` (not in the user-dirty list).

## Plan and independently verifiable tasks

- [ ] **R55-1 — RED: snapshot-manager monitor test.** Add
      `test_scheduler_open_threads_monitor_into_snapshot_manager` to
      `tests/test_scheduler_snapshot_storage_pairing.py` mirroring the existing
      `_settings()` + `from_settings` + `open(_PairingSpider())` seam: assert
      `queue_manager.set_monitor.assert_called_once()` (R14-D path, passes) AND
      `snapshot_manager.set_monitor.assert_called_once()` (fails today).
- [ ] **R55-2 — GREEN: thread the monitor.** Add the guarded
      `self._snapshot_connection_manager.set_monitor(monitor)` after line 1482.
      Run the new test → PASS; confirm existing snapshot/scheduler tests still
      PASS (no behavior change when snapshot manager is None).
- [ ] **R55-3 — Verify.** `uv run ruff check .` then `uv run pytest` then
      `uv run mypy --strict src/scrapy_extension`. All green, no regressions.

## Acceptance criteria

1. After `open()`, `snapshot_manager.set_monitor` is called exactly once (with
   the same monitor passed to the queue manager) when a snapshot manager exists.
2. When no snapshot manager is configured (`_snapshot_connection_manager is
   None`), `open()` is unchanged — no AttributeError, no behavior change.
3. `queue_manager.set_monitor` (R14-D path) is unchanged.
4. `ruff check`, `pytest`, and `mypy --strict` are all clean; no other test
   regresses.
