# SPEC-round80 — Reset RabbitMQ in-flight overflow warning flag on reconnect

**Round log:** R89 (SPEC counter round 80; offset R−9 from the round log).
**Source:** rabbitmq dimension of the R89 cap-scaled ultracode scan (opus find + adversarial verify), confirmed real + new + TDD-able + simple.
**Surface:** `backends/rabbitmq.py` — `_publish_handles_locked` / `_detach_handles_locked` (the reconnect path that installs/tears down an ack session). NOT in the dirty list → shippable.

## Context and audit evidence

`_track_in_flight` (`rabbitmq.py:1213-1245`) caps the diagnostic in-flight ack set at
`_MAX_IN_FLIGHT` and warns **once** on overflow via the one-shot guard
`_in_flight_overflow_warned` (init `False` at line 250, set `True` at line 1232, gated at
line 1231):

```python
if not self._in_flight_overflow_warned:
    self._in_flight_overflow_warned = True
    logger.warning("RabbitMQ in-flight ack set at cap (%d) — ...", _MAX_IN_FLIGHT)
```

The flag is initialized once in `__init__` and set `True` on the first overflow — but it is
**never reset**. The reconnect path clears every OTHER ack-session-scoped field:
`_publish_handles_locked` (lines 321-325) and `_detach_handles_locked` (lines 348-352) both
`.clear()` `_in_flight_tags`, `_pending_deliveries`, `_declared_queues`, and reset
`_last_delivery_tag` / `_last_delivery_queue` — but omit `_in_flight_overflow_warned`.

End-to-end mechanism (verified by reading the actual code):
1. A slow-ack / leak condition fills `_in_flight_tags` to the cap → the one-shot warning
   fires, `_in_flight_overflow_warned = True`.
2. A reconnect cycle runs (`connect()` → `_publish_handles_locked` at line 594;
   `disconnect()` → `_detach_handles_locked` at line 940), which clears `_in_flight_tags`
   (room to grow again) but leaves the flag latched `True`.
3. If the same chronic ack-leak recurs and refills the cap, `if not
   self._in_flight_overflow_warned` (line 1231) is `False` → **no warning is emitted**.
4. The operator gets the overflow signal exactly once per backend instance across the entire
   process lifetime, even across many reconnects — masking recurring leaks.

**Not documented intent.** The code's own docstring (lines 1216-1222) calls the set
"diagnostic" and the warning "warn-once on overflow"; but the connect-path handlers
deliberately clear every sibling reconnect-scoped field, and the flag being the lone
exception reads as oversight. This is **distinct from the SSL cleartext warning** (R2-B3),
which *correctly* persists per-instance: the SSL condition is a static config choice that
does not change across reconnects, so re-warning would flood. The overflow condition is a
transient runtime state tied to `_in_flight_tags`, which IS cleared on reconnect — so its
flag should reset alongside it.

**Severity: low** (pure diagnostic/observability — the broker still tracks delivery tags, so
ack correctness is unaffected per the code's own docstring). Shipped because the fix is
trivial, mirrors the sibling-field-clearing pattern, and closes a real observability gap.

## Goal

After a reconnect, a recurring in-flight-ack-set overflow must emit the warning again (one
shot per overflow episode, not once per process lifetime).

## Specification

Reset `_in_flight_overflow_warned = False` in both reconnect-path methods, next to the
existing `_in_flight_tags.clear()`:

- `_publish_handles_locked` (after line 324).
- `_detach_handles_locked` (after line 351).

No other field, signature, or method is touched. The SSL warning's per-instance debounce is
untouched (different flag, different semantics).

## Plan and independently verifiable tasks

- **R89-1 (RED):** add `test_in_flight_overflow_warning_flag_resets_on_detach` — construct
  `RabbitMQBackend(RabbitMQSettings())`, set `_in_flight_overflow_warned = True`, call
  `_detach_handles_locked()`, assert the flag is `False`. Run → fails (today it stays True).
- **R89-2 (GREEN):** add the reset to both reconnect methods. Run → passes.
- **R89-3 (HARDEN):** add `test_in_flight_overflow_warning_flag_resets_on_publish_handles` —
  same shape via `_publish_handles_locked(connection=…, channel=…, snapshot=None)` (the
  `connect()` path), proving both reset sites are covered.

## Acceptance criteria

- [ ] `pytest tests/test_rabbitmq_backend.py` green, including the 2 new tests.
- [ ] `uv run ruff check .` clean.
- [ ] `uv run ruff format --check src tests conftest.py` clean (CI enforces it).
- [ ] `uv run mypy --strict src/scrapy_extension` clean.
- [ ] `uv run pytest` full suite green (no regression).
- [ ] The SSL per-instance debounce test (`test_rabbitmq_backend_ssl_warning_debounces_across_reconnects`) still green.
- [ ] One atomic commit; ff-merge to `main`; CI green.
