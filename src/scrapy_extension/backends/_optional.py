"""Helpers for classifying optional-dependency import failures."""

from __future__ import annotations


def _is_missing_optional_dependency(exc: ImportError, dependency: str) -> bool:
  """Return whether ``exc`` says that ``dependency`` itself is missing."""
  if not isinstance(exc, ModuleNotFoundError):
    return False
  missing_name = exc.name
  if not missing_name:
    return False
  return missing_name == dependency or missing_name.startswith(f"{dependency}.")
