"""Shared redaction helper for backend client-lib config dicts.

SEC-1 (round-6 security parity): the same ``_RedactedStr`` pattern first
introduced in the Kafka backend (to keep SASL passwords out of ``repr()``
dumps / repr-based captures of client config) is now applied uniformly to
every backend that hands a secret to a client-library config dict.

The wrapped value is a ``str`` subclass whose underlying value IS the real
secret, so client libraries (kafka-python, pika, pymongo, elasticsearch-py,
pulsar-client, boto3, redis-py) that consume it via ``str()`` semantics
keep working unchanged. Only ``repr()`` is masked — defense-in-depth against
accidental repr-based display/logging, NOT against ordinary string operations
or an adversary who can read process memory.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit

__all__ = ["_RedactedStr"]

_SENSITIVE_DIAGNOSTIC_FRAGMENTS = (
    "password",
    "secret",
    "token",
    "credential",
    "authorization",
    "api_key",
    "api-key",
    "apikey",
    "pass",
    "pwd",
    "private_key",
    "private-key",
    "receipt",
    "marker",
)
_URI_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_SENSITIVE_HEADER = re.compile(
    r"^[ \t]*(?:authorization|proxy-authorization|cookie|set-cookie|"
    r"x-api-key|x-auth-token)[ \t]*:",
    re.IGNORECASE,
)
_AUTH_SCHEME = re.compile(r"^(?:bearer|basic)\s+", re.IGNORECASE)
# ``urlsplit`` cannot recover userinfo without a ``scheme://`` prefix (the
# authority parses as a path and ``username``/``password`` stay ``None``), so
# any ``x:y@`` — or ``:y@``, since an empty username still carries a password —
# prefix before the first "/" is checked structurally.
_SCHEMELESS_USERINFO = re.compile(r"^[^/\s:@]*:[^/\s@]*@")


class _RedactedStr(str):
    """``str`` subclass that hides its value in ``repr()``.

    The str VALUE is the real secret so client libraries receive a usable
    string (``str(instance)`` returns the secret, indexing works, equality
    works). Only ``repr(instance)`` returns the mask, so ``repr(config_dict)``
    and repr-based local-variable dumps don't reveal the raw credential while
    the wrapper is retained.

    Note: this is defense-in-depth against accidental repr logging/capture,
    NOT against ordinary string formatting, serialization, or an adversary who
    can read process memory. The raw value is still reachable via
    ``str(instance)`` or by indexing.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<redacted>"


def _diagnostic_repr(value: Any) -> str:
    """Render caller-controlled transport values without credential-shaped text.

    URI-like values are transport material even when they do not currently carry
    userinfo.  Treating only ``user:password@host`` as sensitive would make a
    queue URL, Pulsar topic URI, or a future signed query parameter an accidental
    diagnostic disclosure.  Only exact strings are rendered: a ``str`` subclass
    can override methods such as ``lower`` or ``__repr__`` and is therefore not a
    safe diagnostic value.
    """
    if type(value) is not str:
        return "<redacted>"
    # The mask is an internal fixed point.  Keeping it unchanged makes this
    # helper safe to apply at more than one diagnostic boundary without
    # turning ``<redacted>`` into a quoted, progressively transformed value.
    if value == "<redacted>":
        return value
    # ``token=None`` is a package-owned, non-secret sentinel used by legacy
    # acknowledgement diagnostics.  It is safe only as this exact value; do
    # not generalize this into a caller-controlled "already safe" escape hatch.
    if value == "token=None":
        return repr(value)
    lowered = value.lower()
    if any(fragment in lowered for fragment in _SENSITIVE_DIAGNOSTIC_FRAGMENTS):
        return "<redacted>"
    if _SENSITIVE_HEADER.match(value) or _AUTH_SCHEME.match(value):
        return "<redacted>"
    if _URI_PREFIX.match(value):
        return "<redacted>"
    if _SCHEMELESS_USERINFO.match(value.split("/", 1)[0]):
        return "<redacted>"
    try:
        parsed = urlsplit(value)
        has_userinfo = parsed.username is not None or parsed.password is not None
    except ValueError:
        has_userinfo = True
    if has_userinfo:
        return "<redacted>"
    return repr(value)


def _redact(value: Any) -> Any:
    """Wrap ``value`` in ``_RedactedStr`` if it is a non-empty string.

    Idempotent: passing an already-redacted value returns it unchanged.
    Non-string / empty values pass through untouched so callers can use this
    unconditionally on the output of ``secret_value(...)`` without special-
    casing unset (``None``) or empty credentials.

    Args:
        value: The value to wrap (typically ``secret_value(self.config.password)``).

    Returns:
        A ``_RedactedStr`` wrapping ``value`` when ``value`` is a non-empty
        ``str``; otherwise ``value`` unchanged.
    """
    if type(value) is _RedactedStr:
        # Already redacted — return the SAME object (referential idempotency,
        # so _redact(_redact(x)) is _redact(x), matching the docstring claim).
        # A subclass is not trusted: it can override ``__repr__`` and defeat the
        # masking contract, so it is copied into the exact safe type below.
        return value
    if isinstance(value, str) and str.__len__(value) > 0:
        return _RedactedStr(value)
    return value
