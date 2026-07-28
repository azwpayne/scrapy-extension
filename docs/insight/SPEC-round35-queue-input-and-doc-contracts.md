# Round 35 — SPEC / PLAN / TASK: queue input and runtime-contract alignment

**Base:** `main` @ `f8553f0`. **Scope:** queue input normalization and verified
operator/developer contract drift discovered by the round-35 parallel audit.

## Findings and specification

### R35-A — reject boolean queue routing values

`bool` is a subclass of `int` in Python. Consequently `delay=True` reached the
queue as a one-second delay, and `priority=True` selected the highest priority
bucket. Both violate the existing finite-number contracts and conflict with the
delay strategy's own boolean guard.

`BackendQueue` must reject boolean `request.meta["delay"]` and boolean public
`priority` values with `QueueError` before a strategy or backend is called. It
must preserve routing metadata on failure and settle an inherited replacement
ack token through its existing invalid-replacement path. `PriorityQueueStrategy`
must also reject booleans defensively for direct third-party callers.

### R35-B — synchronize observed runtime contracts

The breaker wraps traffic-bearing queue operations including `ack`/`nack`; only
administrative/lifecycle operations remain forwarded. The scheduler's strategy
warning applies only where `pop_with_ack` is not overridden, not to every
non-passthrough built-in. Their docstrings and the associated test narrative
must state that behavior.

Integration instructions must document the global
`SCRAPY_TEST_INTEGRATION=1` admission gate in addition to per-backend variables.
The developer guide must list all eight queue strategies.

## Plan and tasks

1. Add focused RED tests for boolean delay and priority at the public queue and
   direct priority-strategy seams.
2. Apply minimum ingress validation plus strategy defence-in-depth; prove GREEN
   with focused tests, ruff, strict mypy, and the non-integration suite.
3. Align breaker, manager, scheduler, test, contributor, and developer docs to
   observed behavior; validate formatting/lint and review the final diff.
4. Commit the independently deployable round as one atomic Git commit, then
   rescan changed boundaries before deciding whether another round is justified.

## Out of scope

No backend protocol, retry, persistence, or scheduling semantics change. The
package-artifact smoke-test expansion identified during audit is a separate CI
enhancement because its isolated environment needs runtime dependency design.
