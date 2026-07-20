"""Strict coercion helpers for Scrapy component settings."""

from __future__ import annotations

import math
from typing import Any

from scrapy_extension.exceptions import ConfigurationError


def parse_int_setting(
  raw: object,
  setting_name: str,
  *,
  minimum: int | None = None,
  maximum: int | None = None,
) -> int:
  """Parse an integer without accepting bools or truncating floats."""
  if isinstance(raw, bool) or not isinstance(raw, (int, str)):
    raise ConfigurationError(
      f"{setting_name} must be an integer, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  try:
    value = int(raw)
  except (TypeError, ValueError, OverflowError) as exc:
    raise ConfigurationError(
      f"{setting_name} must be an integer, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    ) from exc
  if minimum is not None and value < minimum:
    raise ConfigurationError(
      f"{setting_name} must be >= {minimum}, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  if maximum is not None and value > maximum:
    raise ConfigurationError(
      f"{setting_name} must be <= {maximum}, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  return value


def parse_float_setting(
  raw: object,
  setting_name: str,
  *,
  minimum: float | None = None,
  maximum: float | None = None,
  minimum_exclusive: bool = False,
  maximum_exclusive: bool = False,
) -> float:
  """Parse a finite float and enforce optional inclusive/exclusive bounds."""
  if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
    raise ConfigurationError(
      f"{setting_name} must be a finite number, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  try:
    value = float(raw)
  except (TypeError, ValueError, OverflowError) as exc:
    raise ConfigurationError(
      f"{setting_name} must be a finite number, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    ) from exc
  if not math.isfinite(value):
    raise ConfigurationError(
      f"{setting_name} must be finite, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  if minimum is not None and (
    value <= minimum if minimum_exclusive else value < minimum
  ):
    operator = ">" if minimum_exclusive else ">="
    raise ConfigurationError(
      f"{setting_name} must be {operator} {minimum}, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  if maximum is not None and (
    value >= maximum if maximum_exclusive else value > maximum
  ):
    operator = "<" if maximum_exclusive else "<="
    raise ConfigurationError(
      f"{setting_name} must be {operator} {maximum}, got {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    )
  return value


def parse_bool_setting(raw: object, setting_name: str) -> bool:
  """Parse the boolean spellings supported by Scrapy without leaking errors."""
  if isinstance(raw, bool):
    return raw
  if isinstance(raw, int) and raw in (0, 1):
    return bool(raw)
  if isinstance(raw, str):
    normalized = raw.lower()
    if normalized in {"1", "true"}:
      return True
    if normalized in {"0", "false"}:
      return False
  raise ConfigurationError(
    f"{setting_name} must be one of 0/1 or true/false, got {raw!r}.",
    setting_name=setting_name,
    setting_value=raw,
  )


def get_bool_setting(
  settings: Any,
  setting_name: str,
  default: bool = False,
) -> bool:
  """Read a Scrapy boolean setting and translate its conversion errors."""
  raw = settings.get(setting_name, default)
  try:
    value = settings.getbool(setting_name, default)
  except (TypeError, ValueError, OverflowError) as exc:
    raise ConfigurationError(
      f"Invalid boolean value for {setting_name}: {raw!r}.",
      setting_name=setting_name,
      setting_value=raw,
    ) from exc
  # Some lightweight Settings test doubles leave getbool() as an unconfigured
  # mock. Fall back to the raw value in that case; real Scrapy always returns a
  # bool or raises during conversion.
  candidate = value if isinstance(value, (bool, int, str)) else raw
  return parse_bool_setting(candidate, setting_name)
