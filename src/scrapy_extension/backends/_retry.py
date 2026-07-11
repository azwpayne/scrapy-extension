"""Retry / backoff policy extracted from ConnectionManager (Risk 6 incremental).

The full-jitter exponential backoff calculation lives here so it is independently
unit-testable without spinning up a :class:`~scrapy_extension.backends.connectors.ConnectionManager`
+ real backend. ``ConnectionManager.connect`` calls :func:`compute_full_jitter_backoff`
— behavior is byte-identical to the prior inline ``random.uniform(0, retry_delay * 2**attempt)``.

This is the first incremental extraction out of the (939-LOC) ``ConnectionManager``
god-class. The deep-insights report roadmap splits the rest into
``ConnectionManagerRegistry`` / ``ManagedConnection`` / ``RetryPolicy`` /
``DynamicBackendFactory`` / ``ConfigResolver`` / ``BreakerIntegration`` /
``MonitorWiring`` / ``CapabilityCatalog`` — each independently testable.
Lock-order invariant (``_registry_lock`` BEFORE instance ``_lock``) is documented
on ``ConnectionManager`` itself.
"""

from __future__ import annotations

__all__ = ["compute_full_jitter_backoff"]

import random


def compute_full_jitter_backoff(attempt: int, base_delay: float) -> float:
  """Full-jitter exponential backoff sleep duration (AWS Architecture Blog).

  ``delay = base_delay * 2**attempt``; full jitter returns ``uniform(0, delay)``.
  Full (not "equal"/"decorrelated") jitter prevents thundering herd when many
  workers retry simultaneously after a coordinated outage (e.g. Redis failover).
  The caller sleeps for the returned value.

  Args:
      attempt: 0-based just-failed attempt index (0 = first failure → the first
          retry follows this sleep).
      base_delay: Base delay in seconds (the ``retry_delay`` setting).

  Returns:
      Seconds to sleep — ``uniform(0, base_delay * 2**attempt)``. Always
      non-negative; 0 on the first attempt when ``base_delay`` is 0.
  """
  delay = base_delay * (2**attempt)
  # nosec B311: random.uniform is intentional full-jitter backoff, not a
  # cryptographic primitive. Switching to secrets would remove the bounded-
  # range API we rely on without improving security.
  return random.uniform(0, delay)  # nosec B311 - jitter, not cryptographic
