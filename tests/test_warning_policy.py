"""Canaries for intentionally narrow pytest warning exemptions."""

import warnings

import pytest


def test_unrelated_user_warning_remains_an_error() -> None:
  """A dependency-specific exemption must not weaken the global policy."""
  with pytest.raises(UserWarning, match="warning policy canary"):
    warnings.warn("warning policy canary", UserWarning, stacklevel=1)
