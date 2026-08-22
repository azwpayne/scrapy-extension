"""Strict coercion helpers for Scrapy component settings."""

from __future__ import annotations

import math
import re
from typing import Any

from scrapy_extension.exceptions import ConfigurationError

_SAFE_SETTING_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def _setting_label(setting_name: object) -> str:
    """Return a field label safe to include in a diagnostic.

    Setting names normally come from package constants, but these helpers are
    also part of the programmatic configuration surface.  Never let a caller
    turn a field label into a URI, header, or arbitrary diagnostic payload.
    """
    if type(setting_name) is str and _SAFE_SETTING_NAME.fullmatch(setting_name):
        return setting_name
    return "setting"


def parse_int_setting(
    raw: object,
    setting_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    """Parse an integer without accepting bools or truncating floats."""
    label = _setting_label(setting_name)
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        raise ConfigurationError(
            f"{label} must be an integer.",
            setting_name=setting_name,
            setting_value=raw,
        )
    try:
        value = int(raw)
    except (TypeError, ValueError, OverflowError):
        # Do not chain the conversion exception: it retains the raw input in
        # its message and traceback (notably for URI/credential-shaped text).
        raise ConfigurationError(
            f"{label} must be an integer.",
            setting_name=setting_name,
            setting_value=raw,
        ) from None
    if minimum is not None and value < minimum:
        raise ConfigurationError(
            f"{label} must be >= {minimum}.",
            setting_name=setting_name,
            setting_value=raw,
        )
    if maximum is not None and value > maximum:
        raise ConfigurationError(
            f"{label} must be <= {maximum}.",
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
    label = _setting_label(setting_name)
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise ConfigurationError(
            f"{label} must be a finite number.",
            setting_name=setting_name,
            setting_value=raw,
        )
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        raise ConfigurationError(
            f"{label} must be a finite number.",
            setting_name=setting_name,
            setting_value=raw,
        ) from None
    if not math.isfinite(value):
        raise ConfigurationError(
            f"{label} must be finite.",
            setting_name=setting_name,
            setting_value=raw,
        )
    if minimum is not None and (
        value <= minimum if minimum_exclusive else value < minimum
    ):
        operator = ">" if minimum_exclusive else ">="
        raise ConfigurationError(
            f"{label} must be {operator} {minimum}.",
            setting_name=setting_name,
            setting_value=raw,
        )
    if maximum is not None and (
        value >= maximum if maximum_exclusive else value > maximum
    ):
        operator = "<" if maximum_exclusive else "<="
        raise ConfigurationError(
            f"{label} must be {operator} {maximum}.",
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
        f"{_setting_label(setting_name)} must be one of 0/1 or true/false.",
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
    except (TypeError, ValueError, OverflowError):
        raise ConfigurationError(
            f"Invalid boolean value for {_setting_label(setting_name)}.",
            setting_name=setting_name,
            setting_value=raw,
        ) from None
    # Some lightweight Settings test doubles leave getbool() as an unconfigured
    # mock. Fall back to the raw value in that case; real Scrapy always returns a
    # bool or raises during conversion.
    candidate = value if isinstance(value, (bool, int, str)) else raw
    return parse_bool_setting(candidate, setting_name)
