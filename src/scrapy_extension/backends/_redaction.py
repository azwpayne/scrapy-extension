"""Shared redaction helper for backend client-lib config dicts.

SEC-1 (round-6 security parity): the same ``_RedactedStr`` pattern first
introduced in the Kafka backend (to keep SASL passwords out of ``repr()``
dumps / Sentry captures of client config) is now applied uniformly to every
backend that hands a secret to a client-library config dict.

The wrapped value is a ``str`` subclass whose underlying value IS the real
secret, so client libraries (kafka-python, pika, pymongo, elasticsearch-py,
pulsar-client, boto3, redis-py) that consume it via ``str()`` semantics
keep working unchanged. Only ``repr()`` is masked — defense-in-depth against
accidental logging, NOT against an adversary who can read process memory.
"""

from __future__ import annotations

from typing import Any

__all__ = ["_RedactedStr"]


class _RedactedStr(str):
  """``str`` subclass that hides its value in ``repr()``.

  The str VALUE is the real secret so client libraries receive a usable
  string (``str(instance)`` returns the secret, indexing works, equality
  works). Only ``repr(instance)`` returns the mask, so ``repr(config_dict)``
  and traceback dumps of locals don't reveal the raw credential.

  Note: this is defense-in-depth against accidental logging / Sentry
  capture, NOT against an adversary who can read process memory. The raw
  value is still reachable via ``str(instance)`` or by indexing.
  """

  __slots__ = ()

  def __repr__(self) -> str:
    return "<redacted>"


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
  if isinstance(value, _RedactedStr):
    # Already redacted — return the SAME object (referential idempotency,
    # so _redact(_redact(x)) is _redact(x), matching the docstring claim).
    return value
  if isinstance(value, str) and value:
    return _RedactedStr(value)
  return value
