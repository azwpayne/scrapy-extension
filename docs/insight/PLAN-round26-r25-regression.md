# Round 26 — PLAN: r25-regression + cross-cutting fixes

> Spec: [SPEC-round26-r25-regression.md](./SPEC-round26-r25-regression.md).
> TDD (RED → GREEN), each unit = one atomic conventional commit. Claude-Code-only.

## R26-A — `queue/queue.py` raise snapshot cap + persist warn (MED, headline)

Raise `_MAX_SNAPSHOT_BYTES` from 16 MiB → **128 MiB** (covers legit delay heaps
up to ~50k items at 2.7 KB; still bounded against the corrupt-blob OOM). Add a
**persist-time WARNING** in `_persist_snapshot`: if `len(state) > _MAX_SNAPSHOT_BYTES`,
log that the snapshot exceeds the restore cap and will be dropped on restart
(discoverable at close, not silent until the next restart). Keep persisting
(the operator may raise the cap or reduce `queue_delay_max_held` before
restart). Document the cap + the `queue_delay_max_held` relationship in
`docs/runbook.md` (Snapshot ownership section).

## R26-G — `schedule/scheduler.py` `_close_locked` BaseException-safe (MED)

Mirror `pipeline._close_locked` (R20-B, pipeline.py:416-457): widen the 3
teardown guards (`signal.disconnect`, `queue.close()`, `dupefilter.close()`)
from `except Exception` → `except BaseException` capturing the first into
`primary_error`; move `connection_manager.close()` into a `finally` (with its
own `except BaseException` feeding `primary_error` if still None); re-raise
`primary_error` at the end. This closes the R13/PR#54 leak: a Ctrl+C during
`dupefilter.close()` no longer skips the CM release.

## R26-F — `settings/elasticsearch.py` CLOUD requires auth (MED)

In `validate_mode_requirements`, after the existing `cloud_id` check, add: if
`mode == CLOUD` and `api_key is None` and not (`username` and `password`):
raise `ConfigurationError` naming the requirement (Elastic Cloud always 401s
anonymous → opaque health-check failure). Update the no-auth CLOUD test
(`test_elasticsearch_backend.py:90`) to pass `api_key=`.

## R26-D — `queue/queue.py` `_validate_request_dict` dumps_kwargs (LOW)

Add `require_type("dumps_kwargs", dict)` alongside the other `require_type`
calls (~line 735). Naturally gated on field presence (helper's `if field in
request_dict`), so non-JsonRequest payloads are unaffected; a crafted
JsonRequest payload with non-dict `dumps_kwargs` now raises a clean
`TypeError` naming the field.

## R26-E — `settings/kafka.py` CONFLUENT endpoint guard (LOW)

Narrow foot-gun fix (preserves the documented "reuse `bootstrap_servers`"
pattern): in `_validate_authentication`, raise `ConfigurationError` when
`mode == CONFLUENT` AND `confluent_bootstrap_servers` is unset AND
`bootstrap_servers` is still the STANDALONE `localhost:9092` default (clearly
wrong for Confluent Cloud). Update the no-endpoint CONFLUENT fixture
(`test_settings_validation.py:498-505`) to pass a real endpoint.

## R26-B — `tests/test_queue.py` ismethod-arm coverage (LOW)

Rename `test_pop_rejects_callable_attribute_that_is_not_bound_spider_method` →
`test_pop_rejects_non_callable_attribute` (it now tests the `callable` arm via
the `name` payload). Add `test_pop_rejects_callable_non_method_attribute`
exercising the `inspect.ismethod` arm (a classmethod or class-level function
attached to the spider — callable but not a bound instance method).

## R26-C — R25-F tests assert monitor type (LOW)

Strengthen `test_from_crawler_wires_monitor_into_connection_manager` in both
`test_dupefilter.py` and `test_pipeline.py`: add
`assert isinstance(mock_cm.set_monitor.call_args[0][0], ScrapyStatsMonitor)` so
a NullMonitor-by-mistake refactor is caught.

## Gate / Ship

ruff → mypy --strict → pytest (≥3796 / ≥95%). code-reviewer fan-out if the
rate limit has reset. ff-merge `worktree-round26` → `main` → push → delete
branch. Memory record.
