# SPEC-round81 — Reset in-flight overflow warning flag on reconnect in Pulsar & SQS

**Round log:** R90 (SPEC counter round 81; offset R−9 from the round log).
**Source:** ndiff dimension of the R90 cap-scaled ultracode scan (opus find + adversarial verify). Both confirmed real + new + TDD-able + simple. The exact R89 rabbitmq sibling.
**Surface:** `backends/pulsar.py` + `backends/sqs.py` — the two remaining MQ backends that carry the `_in_flight_overflow_warned` flag. Both NOT in the dirty list → shippable. One logical change: complete the R89 (`d2269be`) pattern across every backend that has the flag.

## Context and audit evidence

R89 (`d2269be`) shipped the fix in `rabbitmq.py`: `_in_flight_overflow_warned` (the one-shot
overflow warning flag) must reset when the ack session is torn down / reinstalled, because the
overflow is a transient runtime condition tied to `_in_flight` (which IS cleared on reconnect) —
not a static config condition. The flag exists in exactly **3** backends (grep-confirmed):

| backend | init | gate | set-True | reset-on-reconnect? |
|---------|------|------|----------|---------------------|
| rabbitmq | :250 | :1233 | :1234 | ✅ R89 (:325, :353) |
| pulsar   | :338 | :872 | :873 | ❌ missing (:572, :612) |
| sqs      | :388 | :891 | :892 | ❌ missing (:559) |

kafka.py and rocketmq.py do NOT carry this flag (different ack-tracking), so pulsar + sqs are
the **complete** remaining set.

End-to-end mechanism (verified by reading the actual code):
1. A slow-ack / leak condition fills the diagnostic `_in_flight` set to `_MAX_IN_FLIGHT` → the
   one-shot warning fires, `_in_flight_overflow_warned = True`.
2. A reconnect/teardown clears `_in_flight` (room to refill) but, in pulsar/sqs, leaves the flag
   latched `True`:
   - `pulsar.py:571-572` (`_abort_failed_connect`, the connect-failure rollback) — `with self._in_flight_lock: self._in_flight.clear()`, no flag reset.
   - `pulsar.py:611-612` (`disconnect`) — same.
   - `sqs.py:555-559` (`disconnect`) — clears `_in_flight`/`_last_receipt`/`_last_receipt_epoch`/`_last_receipt_generation_key`, omits the flag.
3. If the chronic leak recurs and refills the cap, `if not self._in_flight_overflow_warned` is
   `False` → **no warning**. The operator gets the signal once per process lifetime, masking
   recurring leaks across reconnects.

Identical shape and semantics to R89/rabbitmq; the R89 SPEC explicitly reasoned the flag is
transient state tied to `_in_flight` and must reset when that set clears.

**Severity: low** (pure diagnostic/observability — the broker tracks delivery tags/receipt
handles, so ack correctness is unaffected per each backend's own docstring). Shipped because the
fix is trivial, mirrors a shipped precedent (R89), and closes the pattern everywhere it exists.

## Goal

After a reconnect/teardown in pulsar or sqs, a recurring in-flight-set overflow must emit the
warning again (one shot per overflow episode, not once per process lifetime) — matching rabbitmq.

## Specification

Add `self._in_flight_overflow_warned = False` inside the existing `with self._in_flight_lock:`
block, adjacent to `self._in_flight.clear()`, at:

- `pulsar.py:572` (`_abort_failed_connect` — connect-failure rollback).
- `pulsar.py:612` (`disconnect`).
- `sqs.py:559` (`disconnect`, after the `_last_receipt_generation_key = None` line).

No other field, signature, or method is touched. One commit, two files, three sites — completing
the R89 pattern.

## Plan and independently verifiable tasks

- **R90-1 (RED):** add `test_in_flight_overflow_warning_flag_resets_on_disconnect` to
  `tests/test_pulsar_backend.py` and `tests/test_sqs_backend.py` — construct backend, set
  `_in_flight_overflow_warned = True`, call `disconnect()`, assert `False`. Run → both fail.
- **R90-2 (GREEN):** add the reset at all 3 sites. Run → both pass.
- **R90-3 (HARDEN):** add `test_in_flight_overflow_warning_flag_resets_on_abort_failed_connect`
  to `tests/test_pulsar_backend.py` — wire `self._client`/`self._lifecycle_generation`, set the
  flag `True`, call `_abort_failed_connect(client, published_generation=gen)`, assert `False`
  (covers the pulsar connect-failure rollback site at :572, distinct from disconnect :612).

## Acceptance criteria

- [ ] `pytest tests/test_pulsar_backend.py tests/test_sqs_backend.py` green, incl. 3 new tests.
- [ ] `uv run ruff check .` clean.
- [ ] `uv run ruff format --check src tests conftest.py` clean (CI enforces it).
- [ ] `uv run mypy --strict src/scrapy_extension` clean.
- [ ] `uv run pytest` full suite green (no regression).
- [ ] The R89 rabbitmq reset tests still green.
- [ ] One atomic commit; ff-merge to `main`; CI green.
