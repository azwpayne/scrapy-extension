# Round 43B — SPEC / PLAN / TASK: non-hot proxy and durable-push privacy

**Base:** `main` after Round 42 verification.

## Audit conclusion

Round 42 protected breaker hot-path wrappers, but queue proxy forwarded methods
(`queue_len`, `clear_queue`, lifecycle/admin methods) still bind raw backend
methods.  A `BackendError` from them retains its message, queue metadata,
cause, and backend traceback.  Separately, `ConnectionManager`'s durable-push
translation keeps a logical queue/item/config graph in a public `QueueError`.

## Specification

1. A breaker proxy forwards administrative methods without affecting breaker
   state, but reconstructs escaped package `BackendError` subclasses through
   the same safe metadata contract as its hot path.  It preserves explicitly
   documented input/unsupported exceptions and all `BaseException` control
   flow.
2. The proxy must not change the established raw-unknown-exception contract:
   unknown plugin `RuntimeError` still propagates and remains breaker-visible.
3. Queue forwarded failures retain only a fixed operation enum and a static
   message; storage forwarded failures retain only fixed operation metadata.
4. `ConnectionManager._push_queue_with_durability()` rebuilds its public
   `QueueError` only after private manager/backend/item frames unwind.  The
   `require_durable` and `_DurablePushRequired` breaker-counting semantics are
   unchanged.
5. Tests inspect text, attributes, chains, formatted traceback, and package
   locals using endpoint/item/queue markers.

## Plan and task checklist

- [ ] Add a non-counting protected proxy wrapper for forwarded operations.
- [ ] Terminally reconstruct durable-push failures outside private frames.
- [ ] Add queue/storage/admin and durable-push marker regressions.
- [ ] Run focused/full CI checks and make one atomic commit.
