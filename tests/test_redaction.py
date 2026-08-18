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
import traceback

import pytest
from pydantic import SecretBytes, SecretStr

from scrapy_extension.backends._redaction import _redact, _RedactedStr
from scrapy_extension.backends.base import JSONSerializer


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


@pytest.mark.parametrize(
    ("wrapped", "marker"),
    [
        pytest.param(SecretStr("secret-string-marker"), "secret-string-marker"),
        pytest.param(SecretBytes(b"secret-bytes-marker"), "secret-bytes-marker"),
    ],
)
def test_json_serializer_rejects_secret_wrappers_without_leaking_exception_graph(
    wrapped: SecretStr | SecretBytes, marker: str
) -> None:
    """Secret wrappers fail closed and remain masked throughout the failure."""
    with pytest.raises(TypeError) as exc_info:
        JSONSerializer().serialize({"credential": wrapped})

    error = exc_info.value
    surfaces = [
        str(error),
        repr(error),
        repr(error.args),
        repr(error.__dict__),
        "".join(traceback.format_exception(error)),
    ]
    current: BaseException | None = error
    while current is not None:
        surfaces.extend((repr(current.args), repr(current.__dict__)))
        trace = current.__traceback__
        while trace is not None:
            if "/src/scrapy_extension/" in trace.tb_frame.f_code.co_filename:
                surfaces.append(repr(trace.tb_frame.f_locals))
            trace = trace.tb_next
        current = current.__cause__ or current.__context__

    assert type(wrapped).__name__ in str(error)
    assert all(marker not in surface for surface in surfaces)
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    "wrapped",
    [
        pytest.param(SecretStr("explicit-string"), id="SecretStr"),
        pytest.param(SecretBytes(b"explicit-bytes"), id="SecretBytes"),
    ],
)
def test_json_serializer_persists_only_caller_explicit_secret_unwrap(
    wrapped: SecretStr | SecretBytes,
) -> None:
    """An explicit unwrap produces ordinary caller-owned serializable data."""
    unwrapped = wrapped.get_secret_value()
    serializer = JSONSerializer()

    encoded = serializer.serialize({"credential": unwrapped})

    assert type(unwrapped) in {str, bytes}
    assert serializer.deserialize(encoded) == {"credential": unwrapped}
