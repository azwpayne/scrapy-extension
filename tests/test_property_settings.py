"""R14-G: Hypothesis property tests for the SV3 / U4 / U5 contracts.

These are NOT example-based unit tests; they fuzz the validators to prove
two invariants that hand-picked values can miss:

- **SV3 cross-field consistency** — every (``backpressure_pause_at``,
  ``backpressure_resume_at``) pair EITHER constructs successfully OR raises
  ``ConfigurationError`` whose ``setting_name`` is one of the two fields
  (stable, machine-readable). Never a bare ``ValidationError``, never a
  crash, never an unrelated ``setting_name``.

- **U4 sampling boundary** — ``queue_depth_sample_every`` is a pydantic
  ``Field(ge=1)``; values ``>= 1`` construct, values ``< 1`` reject. The
  rejection is a typed pydantic ``ValidationError`` whose error location
  is the field itself (stable), never an arbitrary exception.

- **U5 cap eviction (warn-only)** — ``DelayQueueStrategy`` NEVER refuses an
  item when the holding heap exceeds ``max_held``; it emits at most one
  WARNING per instance. Items are always accepted (a delayed item dropped
  would silently lose data — the load-bearing contract).

Hypothesis is configured for a bounded example budget so this stays a fast
gate (``max_examples=75`` per property) rather than a long fuzz run.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck, given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.queue.strategies.delay import DelayQueueStrategy

# ---------------------------------------------------------------------------
# SV3: backpressure pause/resume cross-field consistency.
# ---------------------------------------------------------------------------


_valid_pause_resume = st.tuples(
  st.one_of(st.none(), st.integers(min_value=-10, max_value=10_000)),
  st.one_of(st.none(), st.integers(min_value=-10, max_value=10_000)),
)


@hyp_settings(
  max_examples=75,
  deadline=None,
  suppress_health_check=[HealthCheck.too_slow],
)
@given(pair=_valid_pause_resume)
def test_sv3_backpressure_cross_field_always_typed(pair):
  """Any (pause_at, resume_at) pair → accept OR ConfigurationError w/ stable name.

  The validator must NEVER crash with a bare ``ValidationError`` or an
  unrelated ``setting_name`` — downstream log handlers index on
  ``setting_name`` to route alerts, so an unstable name would silently
  misfile backpressure misconfigurations.
  """
  from scrapy_extension.settings import Settings

  pause_at, resume_at = pair
  try:
    s = Settings(
      backpressure_pause_at=pause_at,
      backpressure_resume_at=resume_at,
    )
  except ConfigurationError as exc:
    # The setting_name MUST be one of the two validated fields — never
    # None, never a different field. This is the machine-readable contract
    # downstream handlers rely on.
    assert exc.setting_name in (
      "backpressure_pause_at",
      "backpressure_resume_at",
    ), (
      f"ConfigurationError.setting_name={exc.setting_name!r} is not one of "
      f"the backpressure fields — unstable name breaks alert routing. "
      f"pair=({pause_at!r}, {resume_at!r})"
    )
    return
  else:
    # Accepted: the cross-field invariant (resume <= pause) MUST hold.
    if pause_at is not None and resume_at is not None:
      assert resume_at <= pause_at, (
        f"validator accepted an inconsistent pair: resume_at={resume_at!r} "
        f"> pause_at={pause_at!r}"
      )
    # Non-negativity: both, if set, must be >= 0 (the validator's other arm).
    if pause_at is not None:
      assert pause_at >= 0
    if resume_at is not None:
      assert resume_at >= 0
    # The accepted values round-trip through the model.
    assert s.backpressure_pause_at == pause_at
    assert s.backpressure_resume_at == resume_at


# ---------------------------------------------------------------------------
# U4: queue_depth_sample_every sampling boundary.
# ---------------------------------------------------------------------------


@hyp_settings(
  max_examples=75,
  deadline=None,
  suppress_health_check=[HealthCheck.too_slow],
)
@given(value=st.integers(min_value=-100, max_value=10_000))
def test_u4_sample_every_boundary(value):
  """``queue_depth_sample_every`` is a Field(ge=1): <1 rejects, >=1 accepts.

  The rejection must be a pydantic ``ValidationError`` locating the field
  (never an arbitrary crash), and the accepted value must round-trip.
  """
  from pydantic import ValidationError

  from scrapy_extension.settings import Settings

  if value < 1:
    with pytest.raises(ValidationError) as exc_info:
      Settings(queue_depth_sample_every=value)
    # The error location must point at the offending field (stable contract
    # for surfacing the misconfiguration to operators).
    loc_strings = [".".join(str(p) for p in err["loc"]) for err in exc_info.value.errors()]
    assert any("queue_depth_sample_every" in loc for loc in loc_strings), (
      f"ValidationError did not locate queue_depth_sample_every: {loc_strings}"
    )
  else:
    s = Settings(queue_depth_sample_every=value)
    assert s.queue_depth_sample_every == value


# ---------------------------------------------------------------------------
# U5: DelayQueueStrategy cap is warn-only (items are NEVER refused).
# ---------------------------------------------------------------------------


def _make_delay_strategy(*, max_held: int) -> DelayQueueStrategy:
  """Build a DelayQueueStrategy with a mocked connection manager + clock."""
  cm = MagicMock()
  # Short fixed delay so every pushed item lands in the holding heap.
  return DelayQueueStrategy(
    connection_manager=cm,
    default_delay=100.0,
    max_held=max_held,
    clock=lambda: 0.0,
  )


@hyp_settings(
  max_examples=50,
  deadline=None,
  suppress_health_check=[HealthCheck.too_slow],
)
@given(
  max_held=st.integers(min_value=1, max_value=64),
  n_items=st.integers(min_value=0, max_value=128),
)
def test_u5_cap_never_refuses_items(max_held, n_items):
  """Pushing past ``max_held`` NEVER drops an item — the cap is warn-only.

  Every accepted item MUST land in the holding heap (len == n_items after
  the burst). A dropped delayed item would silently lose data, which is
  the load-bearing contract this property pins.
  """
  strategy = _make_delay_strategy(max_held=max_held)
  for i in range(n_items):
    strategy.push("q", f"item-{i}".encode(), delay=10.0)

  # The heap holds EVERY pushed item regardless of how far past max_held
  # the burst went — the soft cap only governs the warning, not admission.
  assert len(strategy._holding) == n_items, (
    f"cap refused items: pushed {n_items} but heap holds "
    f"{len(strategy._holding)} (max_held={max_held}) — a delayed item was "
    f"silently dropped"
  )


@hyp_settings(
  max_examples=25,
  deadline=None,
  suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(
  max_held=st.integers(min_value=1, max_value=16),
  n_over=st.integers(min_value=1, max_value=64),
)
def test_u5_cap_warning_fires_at_most_once(max_held, n_over, caplog):
  """The over-cap WARNING fires at most ONCE per process (module-level flag).

  Pushing ``max_held + n_over`` items crosses the cap; the warning must
  fire on the first crossing and NOT repeat for subsequent over-cap pushes
  (warn-once is the documented contract — per-push warnings would flood
  logs under a sustained burst). The flag is module-level so a multi-spider
  process logs the alert exactly once.
  """
  import logging

  # The warn-once flag is MODULE-LEVEL (intentional — one alert per process
  # even across many strategy instances). Reset before each example so the
  # property exercises the first-crossing path every time.
  import scrapy_extension.queue.strategies.delay as delay_mod

  delay_mod._over_cap_warned = False
  # caplog is function-scoped (not reset between hypothesis examples) — clear
  # it so each example only sees its OWN warnings.
  caplog.clear()

  strategy = _make_delay_strategy(max_held=max_held)
  with caplog.at_level(logging.WARNING, logger=delay_mod.logger.name):
    for i in range(max_held + n_over):
      strategy.push("q", f"item-{i}".encode(), delay=10.0)

  cap_warnings = [
    r for r in caplog.records
    if "max_held" in r.getMessage().lower() and r.levelno == logging.WARNING
  ]
  # Exactly one warning (warn-once) — never zero (cap WAS crossed), never >1.
  assert len(cap_warnings) == 1, (
    f"expected exactly one over-cap warning, got {len(cap_warnings)} "
    f"(max_held={max_held}, n_over={n_over})"
  )

  # The module-level flag is now sticky: a SECOND strategy instance pushing
  # over cap must NOT re-warn (one alert per process).
  assert delay_mod._over_cap_warned is True, (
    "warn-once flag was not set after the first crossing"
  )
  strategy2 = _make_delay_strategy(max_held=max_held)
  caplog.clear()
  with caplog.at_level(logging.WARNING, logger=delay_mod.logger.name):
    for i in range(max_held + n_over):
      strategy2.push("q", f"item-{i}".encode(), delay=10.0)
  second_cap_warnings = [
    r for r in caplog.records
    if "max_held" in r.getMessage().lower() and r.levelno == logging.WARNING
  ]
  assert not second_cap_warnings, (
    "second strategy instance re-fired the over-cap warning — warn-once "
    "flag must be module-level (one alert per process), not per-instance"
  )

  # Restore the flag so this property test doesn't pollute later suites.
  delay_mod._over_cap_warned = False
