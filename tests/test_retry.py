"""Tests for the extracted retry/backoff policy (Risk 6 incremental extraction).

The whole point of extracting :func:`compute_full_jitter_backoff` out of the
939-LOC ``ConnectionManager`` god-class is independent unit testability — these
bounds can now be pinned without spinning up a manager + real backend.
"""

from __future__ import annotations

from scrapy_extension.backends._retry import compute_full_jitter_backoff


class TestComputeFullJitterBackoff:
  """Full-jitter exponential backoff bounds (AWS Architecture Blog contract)."""

  def test_attempt_zero_upper_bound_is_base_delay(self) -> None:
    """attempt=0 → uniform(0, base_delay) → value within [0, base_delay]."""
    for _ in range(1000):
      v = compute_full_jitter_backoff(0, 1.0)
      assert 0.0 <= v <= 1.0

  def test_upper_bound_grows_exponentially_with_attempt(self) -> None:
    """attempt=N → upper bound = base_delay * 2**N (exponential backoff)."""
    for attempt, expected_cap in [(1, 2.0), (2, 4.0), (3, 8.0), (5, 32.0)]:
      for _ in range(500):
        v = compute_full_jitter_backoff(attempt, 1.0)
        assert 0.0 <= v <= expected_cap, (attempt, v, expected_cap)

  def test_base_delay_zero_always_zero(self) -> None:
    """base_delay=0 → delay=0 → uniform(0,0)=0 (no retry storm when disabled)."""
    for attempt in range(4):
      assert compute_full_jitter_backoff(attempt, 0.0) == 0.0

  def test_always_non_negative(self) -> None:
    """Full jitter never returns a negative sleep (would be a busy-spin bug)."""
    for attempt in range(6):
      for _ in range(500):
        assert compute_full_jitter_backoff(attempt, 2.5) >= 0.0

  def test_huge_base_delay_capped_not_overflow_to_inf(self) -> None:
    """R21-C: a huge finite base_delay * 2**attempt must not overflow to inf.

    Pre-fix, base_delay=1e303 * 2**18 overflowed IEEE-754 to inf, and
    random.uniform(0, inf) returned inf, so time.sleep(inf) raised OverflowError
    that aborted the retry loop with an opaque error. The computed delay is now
    capped (mirror throttle's ceiling discipline) so the sleep stays finite.
    """
    import math

    from scrapy_extension.backends._retry import _MAX_BACKOFF_S

    for _ in range(500):
      v = compute_full_jitter_backoff(20, 1e303)
      assert math.isfinite(v), v
      assert 0.0 <= v <= _MAX_BACKOFF_S, (v, _MAX_BACKOFF_S)

