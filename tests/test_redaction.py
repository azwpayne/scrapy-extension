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

import gc
import json
import traceback
from types import CodeType, FrameType, FunctionType, ModuleType

import pytest
from pydantic import SecretBytes, SecretStr

from scrapy_extension.backends._redaction import _redact, _RedactedStr
from scrapy_extension.backends.base import JSONSerializer


def _exception_library_graph(error: BaseException) -> list[object]:
    """Collect exception fields and recursively reachable library-frame locals."""
    roots: list[object] = [error]
    trace = error.__traceback__
    while trace is not None:
        if "/src/scrapy_extension/" in trace.tb_frame.f_code.co_filename:
            roots.append(dict(trace.tb_frame.f_locals))
        trace = trace.tb_next

    reachable: list[object] = []
    pending: list[object] = roots
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        value_id = id(value)
        if value_id in seen:
            continue
        seen.add(value_id)
        reachable.append(value)

        if isinstance(value, BaseException):
            pending.extend(
                candidate
                for candidate in (
                    value.args,
                    value.__dict__,
                    value.__cause__,
                    value.__context__,
                )
                if candidate is not None
            )
            continue
        if isinstance(
            value,
            (
                str,
                bytes,
                bytearray,
                CodeType,
                FrameType,
                FunctionType,
                ModuleType,
                type,
            ),
        ):
            continue
        pending.extend(gc.get_referents(value))
    return reachable


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
    """The terminal public failure retains no sensitive library-owned objects."""
    serializer = JSONSerializer()
    raw_secret = wrapped.get_secret_value()
    payload = {"request": [{"nested": ("safe", [wrapped])}]}

    with pytest.raises(TypeError) as exc_info:
        serializer.serialize(payload)

    error = exc_info.value
    surfaces = [
        str(error),
        repr(error),
        repr(error.args),
        repr(error.__dict__),
        "".join(traceback.format_exception(error)),
    ]
    reachable = _exception_library_graph(error)
    reachable_ids = {id(value) for value in reachable}
    reachable_secrets = [
        value.get_secret_value()
        for value in reachable
        if type(value).__name__ in {"SecretStr", "SecretBytes"}
    ]

    assert type(wrapped).__name__ in str(error)
    assert all(marker not in surface for surface in surfaces)
    assert reachable_secrets == []
    assert id(serializer) not in reachable_ids
    assert id(payload) not in reachable_ids
    assert id(wrapped) not in reachable_ids
    assert id(raw_secret) not in reachable_ids
    assert [value for value in reachable if isinstance(value, BaseException)] == [error]
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("wrapped", "marker"),
    [
        pytest.param(SecretStr("key-string-marker"), "key-string-marker"),
        pytest.param(SecretBytes(b"key-bytes-marker"), "key-bytes-marker"),
    ],
)
@pytest.mark.parametrize("nested", [False, True], ids=["direct-key", "nested-key"])
def test_json_serializer_rejects_secret_wrapper_keys_without_reachable_secrets(
    wrapped: SecretStr | SecretBytes, marker: str, *, nested: bool
) -> None:
    """Secret-bearing mapping keys cross the same clean terminal boundary."""
    serializer = JSONSerializer()
    raw_secret = wrapped.get_secret_value()
    secret_mapping = {wrapped: "safe-value"}
    payload: object = {"request": [secret_mapping]} if nested else secret_mapping

    with pytest.raises(TypeError) as exc_info:
        serializer.serialize(payload)

    error = exc_info.value
    surfaces = [
        str(error),
        repr(error),
        repr(error.args),
        repr(error.__dict__),
        "".join(traceback.format_exception(error)),
    ]
    reachable = _exception_library_graph(error)
    reachable_ids = {id(value) for value in reachable}
    reachable_secrets = [
        value.get_secret_value()
        for value in reachable
        if type(value).__name__ in {"SecretStr", "SecretBytes"}
    ]

    assert type(wrapped).__name__ in str(error)
    assert all(marker not in surface for surface in surfaces)
    assert reachable_secrets == []
    assert id(serializer) not in reachable_ids
    assert id(payload) not in reachable_ids
    assert id(secret_mapping) not in reachable_ids
    assert id(wrapped) not in reachable_ids
    assert id(raw_secret) not in reachable_ids
    assert [value for value in reachable if isinstance(value, BaseException)] == [error]
    assert error.__cause__ is None
    assert error.__context__ is None


def test_json_serializer_preserves_explicitly_unwrapped_secret_key_semantics() -> None:
    """Only an unwrapped string becomes a valid JSON key; bytes stay invalid."""
    serializer = JSONSerializer()
    string_key = SecretStr("explicit-key").get_secret_value()
    bytes_key = SecretBytes(b"explicit-key").get_secret_value()

    encoded = serializer.serialize({string_key: "value"})

    assert serializer.deserialize(encoded) == {string_key: "value"}
    with pytest.raises(
        TypeError, match=r"^JSON object keys must be strings, got bytes$"
    ):
        serializer.serialize({bytes_key: "value"})


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
