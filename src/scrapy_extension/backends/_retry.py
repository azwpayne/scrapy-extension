"""Shared full-jitter backoff calculation (Risk 6 incremental).

The full-jitter exponential backoff calculation lives here so it is independently
unit-testable without spinning up a :class:`~scrapy_extension.backends.connectors.ConnectionManager`
+ real backend. ``ConnectionManager.connect`` uses its validated retry settings;
the DynamoDB clear path uses a separate fixed, bounded policy. Both call the
same mathematical helper without sharing retry counters, configuration, or
monitor events. Connection behavior matches the prior inline
``random.uniform(0, retry_delay * 2**attempt)`` for normal configs; the computed
delay is additionally capped at :data:`_MAX_BACKOFF_S` (R21-C) so a pathological
``retry_delay`` cannot overflow to ``inf``.

This is the first incremental extraction out of the (939-LOC) ``ConnectionManager``
god-class. The deep-insights report roadmap splits the rest into
``ConnectionManagerRegistry`` / ``ManagedConnection`` / ``RetryPolicy`` /
``DynamicBackendFactory`` / ``ConfigResolver`` / ``BreakerIntegration`` /
``MonitorWiring`` / ``CapabilityCatalog`` — each independently testable.
Lock-order invariant (``_registry_lock`` BEFORE instance ``_lock``) is documented
on ``ConnectionManager`` itself.
"""

from __future__ import annotations

__all__ = ["_MAX_BACKOFF_S", "compute_full_jitter_backoff"]

import random

# R21-C: upper bound on a single backoff sleep. Without it, a huge-but-finite
# base_delay (e.g. SCRAPY_RETRY_DELAY=1e303, which passes Field(ge=0) +
# _retry_policy's isfinite) multiplied by 2**attempt overflows IEEE-754 to inf,
# and random.uniform(0, inf) -> inf -> time.sleep(inf) raises OverflowError that
# aborts the retry loop with an opaque error. Mirrors throttle's ceiling
# discipline. Normal configs (default retry_delay=1.0) are unaffected at the
# low attempts real retries reach; the cap only binds pathological configs.
_MAX_BACKOFF_S: float = 3600.0


def compute_full_jitter_backoff(attempt: int, base_delay: float) -> float:
  """Full-jitter exponential backoff sleep duration (AWS Architecture Blog).

  ``delay = base_delay * 2**attempt``; full jitter returns ``uniform(0, delay)``.
  Full (not "equal"/"decorrelated") jitter prevents thundering herd when many
  workers retry simultaneously after a coordinated outage (e.g. Redis failover).
  The caller sleeps for the returned value.

  The computed ``delay`` is capped at :data:`_MAX_BACKOFF_S` (R21-C) so a huge
  finite ``base_delay`` cannot overflow to ``inf`` and trigger an
  ``OverflowError`` from ``time.sleep``. Normal configs are unaffected.

  Args:
      attempt: 0-based just-failed attempt index (0 = first failure → the first
          retry follows this sleep).
      base_delay: Caller-owned base delay in seconds. ConnectionManager passes
          its ``retry_delay`` setting; bounded backend policies may pass their
          own validated/fixed value.

  Returns:
      Seconds to sleep — ``uniform(0, min(base_delay * 2**attempt, _MAX_BACKOFF_S))``.
      Always non-negative and finite; 0 on the first attempt when ``base_delay`` is 0.
  """
  delay = min(base_delay * (2**attempt), _MAX_BACKOFF_S)
  # nosec B311: random.uniform is intentional full-jitter backoff, not a
  # cryptographic primitive. Switching to secrets would remove the bounded-
  # range API we rely on without improving security.
  return random.uniform(0, delay)  # nosec B311 - jitter, not cryptographic
