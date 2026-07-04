"""Unit tests for the shared redaction helper (SEC-1).

``_redact`` / ``_RedactedStr`` are security-relevant: they keep SASL
passwords and other secrets out of ``repr()`` dumps of backend client
config dicts (defense-in-depth against accidental logging / Sentry
capture). Backends exercise the wrap-a-real-secret path indirectly, but
the idempotency, non-string/empty passthrough, repr-masking, and
str-semantics guarantees had no direct tests — these pin them so a
future change to this hot helper can't silently break the contract every
backend depends on.
"""

from __future__ import annotations

from scrapy_extension.backends._redaction import _redact, _RedactedStr


def test_redact_wraps_non_empty_string() -> None:
  """A non-empty string is wrapped in _RedactedStr carrying the same value."""
  wrapped = _redact("hunter2")
  assert isinstance(wrapped, _RedactedStr)
  assert wrapped == "hunter2"
  assert str(wrapped) == "hunter2"


def test_redact_is_idempotent_referential() -> None:
  """Redacting an already-redacted value returns the SAME object (not a fresh
  wrap) — pins the docstring's referential-idempotency claim so a future
  change can't silently double-wrap or break ``_redact(_redact(x)) is _redact(x)``."""
  once = _redact("secret")
  twice = _redact(once)
  assert twice is once


def test_redact_passes_through_non_string_values() -> None:
  """Non-string values (None, int, bytes) pass through untouched so callers
  can use ``_redact`` unconditionally on ``secret_value(...)`` output
  without special-casing unset (``None``) or non-string credentials."""
  assert _redact(None) is None
  assert _redact(123) == 123
  assert _redact(b"bytes") == b"bytes"


def test_redact_passes_through_empty_string() -> None:
  """An empty string passes through untouched (no empty _RedactedStr)."""
  result = _redact("")
  assert result == ""
  assert not isinstance(result, _RedactedStr)


def test_redacted_str_repr_is_masked() -> None:
  """``repr()`` of a redacted string is the mask, NOT the secret — the whole
  point: ``repr(config_dict)`` and traceback locals don't leak credentials."""
  assert repr(_redact("super-secret-token")) == "<redacted>"


def test_redacted_str_preserves_str_semantics() -> None:
  """The wrapped value behaves as the real string for client-lib consumption:
  indexing, length, and containment all work on the underlying secret. This
  is why kafka-python / pika / pymongo / elasticsearch-py / pulsar-client /
  boto3 / redis-py accept a ``_RedactedStr`` wherever a ``str`` is expected."""
  wrapped = _redact("abcdef")
  assert wrapped[0] == "a"
  assert len(wrapped) == 6
  assert "bcd" in wrapped
