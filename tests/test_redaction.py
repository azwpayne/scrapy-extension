"""Unit tests for the shared redaction helper (SEC-1).

``_redact`` / ``_RedactedStr`` are security-relevant: they keep SASL
passwords and other secrets out of ``repr()`` dumps of backend client
config dicts (defense-in-depth against accidental repr logging/capture).
Backends exercise the wrap-a-real-secret path indirectly, but
the idempotency, non-string/empty passthrough, repr-masking, and
str-semantics guarantees had no direct tests — these pin them so a
future change to this hot helper can't silently break the contract every
backend depends on.
"""

from __future__ import annotations

import json
from pathlib import Path

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
  point: repr-based config/local displays don't expose credentials."""
  wrapped = _redact("super-secret-token")
  assert repr(wrapped) == "<redacted>"
  assert "super-secret-token" not in repr({"token": wrapped})


def test_redacted_str_preserves_str_semantics() -> None:
  """The wrapped value behaves as the real string for client-lib consumption:
  indexing, length, and containment all work on the underlying secret. This
  is why kafka-python / pika / pymongo / elasticsearch-py / pulsar-client /
  boto3 / redis-py accept a ``_RedactedStr`` wherever a ``str`` is expected."""
  wrapped = _redact("abcdef")
  assert wrapped[0] == "a"
  assert len(wrapped) == 6
  assert "bcd" in wrapped


def test_redacted_str_ordinary_string_paths_expose_underlying_value() -> None:
  """SDK-compatible string/serialization paths deliberately keep the value."""
  wrapped = _redact("sdk-auth-secret")

  assert str(wrapped) == "sdk-auth-secret"
  assert f"{wrapped}" == "sdk-auth-secret"
  assert "%s" % wrapped == "sdk-auth-secret"  # noqa: UP031
  assert "{}".format(wrapped) == "sdk-auth-secret"  # noqa: UP032
  assert json.dumps({"secret": wrapped}) == '{"secret": "sdk-auth-secret"}'


def test_security_policy_matches_repr_only_redaction_boundary() -> None:
  """Keep the normative policy aligned with the executable string contract."""
  policy = (Path(__file__).resolve().parents[1] / "SECURITY.md").read_text(
    encoding="utf-8"
  )
  credential_section = policy.split("### Credential redaction", 1)[1].split(
    "\n### ", 1
  )[0]
  header_row = next(
    (
      line
      for line in credential_section.splitlines()
      if line.startswith("| Mechanism |")
    ),
    "",
  )
  header_cells = [
    cell.strip().lower() for cell in header_row.strip("|").split("|")
  ]
  contract_row = next(
    (
      line
      for line in credential_section.splitlines()
      if line.startswith("|") and "`_RedactedStr`" in line
    ),
    "",
  )
  cells = [cell.strip() for cell in contract_row.strip("|").split("|")]
  secret_str_row = next(
    (
      line
      for line in credential_section.splitlines()
      if line.startswith("|") and "Pydantic `SecretStr`" in line
    ),
    "",
  )
  secret_str_cells = [
    cell.strip() for cell in secret_str_row.strip("|").split("|")
  ]

  assert header_cells == [
    "mechanism",
    "masked display paths",
    "paths exposing the underlying secret",
  ]
  assert len(cells) == 3, "SECURITY.md must state both redaction boundaries"
  wrapper, masked_paths, exposed_paths = cells
  assert wrapper == "`_RedactedStr`"
  assert "`repr(value)`" in masked_paths
  for exposed_path in ("`str(value)`", "f-string", "`%s`", "JSON"):
    assert exposed_path in exposed_paths
  assert len(secret_str_cells) == 3
  assert secret_str_cells[0] == "Pydantic `SecretStr`"
  assert "`get_secret_value()`" in secret_str_cells[2]
  assert "JSON" in secret_str_cells[2]
  assert "repr/string form" not in credential_section
  assert "credential leakage in repr/str/logs" not in policy
