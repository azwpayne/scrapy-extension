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
import logging
import traceback
from types import CodeType, FrameType, FunctionType, ModuleType

import pytest
from pydantic import SecretBytes, SecretStr

from scrapy_extension.backends import _redaction as redaction_module
from scrapy_extension.backends._redaction import _diagnostic_repr, _redact, _RedactedStr
from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import (
    BackendConnectionError,
    ConfigurationError,
    QueueError,
    SerializationError,
)
from scrapy_extension.exceptions.base import (
    _looks_sensitive_text,
    _redact_message,
    _redact_setting_value,
    _secret_text,
)


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


def test_redact_does_not_trust_redacted_string_subclasses() -> None:
    class _HostileRedacted(_RedactedStr):
        def __repr__(self) -> str:
            return "hostile-secret"

    wrapped = _redact(_HostileRedacted("secret"))
    assert type(wrapped) is _RedactedStr
    assert repr(wrapped) == "<redacted>"


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


@pytest.mark.parametrize(
    "header",
    [
        " Authorization: Bearer credential",
        "authorization : Bearer credential",
        "\tCOOKIE\t: session=credential",
        " Set-Cookie : session=credential",
        " X-Api-Key : credential",
        " x-auth-token\t: credential",
    ],
)
def test_diagnostic_repr_redacts_sensitive_headers_with_spacing_and_case(
    header: str,
) -> None:
    assert redaction_module._SENSITIVE_HEADER.match(header) is not None
    assert _diagnostic_repr(header) == "<redacted>"


def test_diagnostic_repr_handles_untrusted_and_plain_values() -> None:
    """Diagnostic rendering fails closed for objects and parses safe strings."""
    assert _diagnostic_repr(object()) == "<redacted>"
    assert _diagnostic_repr("<redacted>") == "<redacted>"
    assert _diagnostic_repr("plain diagnostic") == repr("plain diagnostic")
    assert _diagnostic_repr("//[") == "<redacted>"


def test_diagnostic_repr_has_a_fixed_point_for_its_mask_and_sentinel() -> None:
    assert _diagnostic_repr(_diagnostic_repr("https://sqs.example/queue")) == (
        "<redacted>"
    )
    assert _diagnostic_repr("Bearer\topaque-value") == "<redacted>"
    assert _diagnostic_repr("Basic opaque-value") == "<redacted>"
    assert _diagnostic_repr("token=None") == repr("token=None")


def test_sensitive_text_recognizes_static_transport_forms() -> None:
    assert not _looks_sensitive_text(object())
    assert _looks_sensitive_text("Authorization: Bearer value")
    assert _looks_sensitive_text("Basic value")
    assert _looks_sensitive_text("https://broker.example/queue")
    assert _looks_sensitive_text("http://user:password@example.test")
    assert _looks_sensitive_text("//[")


@pytest.mark.parametrize(
    "value",
    [
        "user:hunter2@queue.internal:5672",
        "user:@queue.internal:5672",
        "consumer:s3cret@kafka.internal:9092/topic",
        pytest.param(":hunter2@queue.internal:5672", id="empty-username"),
    ],
)
def test_schemeless_userinfo_is_transport_credential_material(value: str) -> None:
    """``urlsplit`` cannot recover userinfo without a ``scheme://`` prefix, so
    any ``x:y@`` prefix before the first "/" must be treated as transport
    credential material by both redaction helpers (R141-F4)."""
    assert _looks_sensitive_text(value)
    assert _diagnostic_repr(value) == "<redacted>"


def test_schemeless_authority_without_userinfo_stays_diagnosable() -> None:
    """Plain ``host:port`` authority text (no ``@``) is not credential-shaped."""
    assert not _looks_sensitive_text("queue.internal:5672")
    assert _diagnostic_repr("queue.internal:5672") == repr("queue.internal:5672")


def test_message_redaction_masks_schemeless_userinfo_in_message_text() -> None:
    """A schemeless ``user:pw@host`` URI interpolated into message text — without
    also being supplied as a setting value — must still be masked (adversarial
    V3-2): the message-level ``scheme://`` fallback cannot see single-colon
    userinfo, and an empty username (``:pw@host``) is equally credential-shaped
    (adversarial V3-1)."""
    redacted = _redact_message(
        "Failed to connect to user:hunter2@queue.internal:5672 upstream."
    )
    assert "hunter2" not in redacted
    assert "user:" not in redacted
    assert "queue.internal:5672" not in redacted

    empty_username = _redact_message(
        "Failed to connect to :hunter2@queue.internal:5672."
    )
    assert "hunter2" not in empty_username
    assert ":hunter2@" not in empty_username


def test_message_redaction_preserves_colon_text_without_userinfo() -> None:
    """Ratios/times carry colons but no userinfo ``@``: they must stay intact."""
    message = "Retry after 30s; ratio 3:4, time 12:30, depth 4:2."
    assert _redact_message(message) == message


def test_message_redaction_masks_bare_auth_scheme_credentials() -> None:
    """X5-2: a bare ``basic``/``Bearer`` credential token carries its secret
    with no ``scheme://``, userinfo, or header-line wrapper, so the message
    fallback needs its own scheme+token form (mirrors ``_AUTH_SCHEME``)."""
    assert _redact_message("basic dXNlcjpwYXNz") == "***REDACTED***"
    assert _redact_message("Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig") == (
        "***REDACTED***"
    )
    assert _redact_message("BASIC b3BhcXVl") == "***REDACTED***"


def test_message_redaction_masks_auth_scheme_credentials_on_every_line() -> None:
    """The form is line-anchored like the header patterns: every line of a
    multi-line message is checked and only the scheme+token span is masked."""
    redacted = _redact_message(
        "handshake rejected\nbasic dXNlcjpwYXNz by peer\nBASIC b3BhcXVl"
    )
    assert redacted == "handshake rejected\n***REDACTED*** by peer\n***REDACTED***"


def test_message_redaction_keeps_ordinary_basic_and_bearer_prose() -> None:
    """误伤 guard (X5-2): only a line-initial ``scheme + credential-shaped
    token`` is masked. Mid-sentence mentions, a tokenless trailing scheme
    word, all-caps abbreviation tokens, and the settings safe-listed static
    message that starts with ``basic authentication`` must survive verbatim."""
    sentence = "The tutorial covers basic stuff before advanced topics."
    assert _redact_message(sentence) == sentence
    news = "The bearer of bad news arrived at noon."
    assert _redact_message(news) == news
    assert _redact_message("Basic HTTP authentication is not supported.") == (
        "Basic HTTP authentication is not supported."
    )
    assert _redact_message("The remaining settings are basic") == (
        "The remaining settings are basic"
    )
    safe_listed = (
        "basic authentication requires both username and password; set 'password'."
    )
    assert _redact_message(safe_listed) == safe_listed


def test_raw_space_passwords_are_a_documented_rfc3986_boundary() -> None:
    """X1-1 decision (fix round 3): RFC 3986 §3.2.1 forbids raw whitespace in
    userinfo (it must be percent-encoded), so ``user:hu nter2@host`` is an
    RFC-invalid credential form. The heuristic layers deliberately decline
    to widen the password class to internal whitespace — that would mask
    ordinary ``name: value @mention`` prose (see the email control). This
    test pins the documented contract boundary so widening it stays a
    conscious decision rather than a silent regression."""
    schemeless = "connect user:hu nter2@host failed"
    assert _redact_message(schemeless) == schemeless
    assert not _looks_sensitive_text("user:hu nter2@host")

    # The scheme:// variant truncates at the raw space: the pre-space
    # userinfo is masked and the non-credential tail survives (pinned).
    truncated = _redact_message("connect redis://user:hu nter2@host failed")
    assert truncated == "connect ***REDACTED*** nter2@host failed"

    # Controls: the RFC-valid form stays fully masked, and the
    # whitespace-permissive alternative would have eaten this ordinary text.
    assert _redact_message("connect redis://user:hunter2@host failed") == (
        "connect ***REDACTED*** failed"
    )
    email = "mail admin@example.com about the 3:4 retry ratio"
    assert _redact_message(email) == email


def test_static_token_none_message_survives_repeated_redaction() -> None:
    message = "Legacy ack(token=None) refused; ack each delivery token instead."
    error = QueueError(message)
    assert str(error) == message
    assert _redact_message(message) == message
    assert _redact_message(_redact_message(message)) == message


def test_message_redaction_masks_every_header_and_uri_parameter() -> None:
    message = (
        "Authorization: Bearer header-secret\n"
        "Cookie: session=cookie-secret\n"
        "https://broker.example/path;signature=opaque-signature"
    )
    redacted = _redact_message(message)
    assert "header-secret" not in redacted
    assert "cookie-secret" not in redacted
    assert "opaque-signature" not in redacted
    assert "Authorization:" not in redacted
    assert "Cookie:" not in redacted


def test_configuration_redaction_forces_opaque_value_from_untrusted_label() -> None:
    class _HostileLabel(str):
        def __new__(cls, value: str) -> _HostileLabel:
            return str.__new__(cls, value)

        def lower(self) -> str:
            raise AssertionError("hostile label must not be inspected")

    marker = "opaque-42x"
    error = ConfigurationError(
        f"invalid value {marker}",
        setting_name=_HostileLabel("plugin-label"),
        setting_value=marker,
    )
    assert marker not in str(error)
    assert marker not in repr(error.__dict__)


def test_configuration_redaction_drops_bytes_mapping_keys() -> None:
    marker = b"opaque-key-42x"
    error = ConfigurationError("invalid settings", setting_value={marker: "value"})
    assert marker.decode() not in repr(error.setting_value)
    assert error.setting_value == {"***REDACTED***": "value"}


def test_exception_metadata_subclasses_cannot_reintroduce_opaque_values() -> None:
    class _HostileLabel(str):
        def __new__(cls, value: str) -> _HostileLabel:
            return str.__new__(cls, value)

        def lower(self) -> str:
            raise AssertionError("metadata label must not be inspected")

    marker = "opaque-metadata-42x"
    backend_error = BackendConnectionError(
        f"connection failed: {marker}", backend_type=_HostileLabel(marker)
    )
    serialization_error = SerializationError(
        f"serialization failed: {marker}", serializer=_HostileLabel(marker)
    )
    assert marker not in str(backend_error)
    assert marker not in str(serialization_error)
    assert marker not in repr(backend_error.__dict__)
    assert marker not in repr(serialization_error.__dict__)


class _HostileDiagnosticValue:
    def __repr__(self) -> str:
        raise AssertionError("repr must not run")

    def __iter__(self):
        raise AssertionError("iteration must not run")


class _HostileDiagnosticDict(dict[str, object]):
    def __repr__(self) -> str:
        raise AssertionError("repr must not run")

    def items(self):
        raise AssertionError("iteration must not run")


def test_configuration_redaction_handles_aliases_cycles_and_hostile_values() -> None:
    from scrapy_extension.exceptions import ConfigurationError

    marker = "opaque-alias-secret"
    hostile = _HostileDiagnosticValue()
    hostile_mapping = _HostileDiagnosticDict(passphrase=marker)
    value: dict[str, object] = {
        "pass": marker,
        "pwd": marker,
        "private_key": marker,
        "api-key": marker,
        "nested": {"safe": hostile, "hostile_mapping": hostile_mapping},
    }
    value["cycle"] = value

    error = ConfigurationError("invalid settings", setting_value=value)

    assert error.setting_value == {
        "pass": "***REDACTED***",
        "pwd": "***REDACTED***",
        "private_key": "***REDACTED***",
        "***REDACTED***": "***REDACTED***",
        "nested": {
            "safe": "***REDACTED***",
            "hostile_mapping": "***REDACTED***",
        },
        "cycle": "***REDACTED***",
    }
    assert marker not in repr(error)


def test_configuration_redaction_copies_all_recursive_container_kinds() -> None:
    """Lists, tuples, sets, frozensets, cycles, and bytes are graph-safe."""
    marker = "recursive-container-secret"
    cycle: list[object] = []
    cycle.append(cycle)
    value = {
        "list": [marker, b"opaque-bytes"],
        "tuple": (marker, 7),
        "set": {marker, "safe"},
        "frozen": frozenset({marker, "safe"}),
        "cycle": cycle,
        "binary": marker.encode(),
        "invalid_binary": b"\\xff\\xfe",
    }

    error = ConfigurationError("invalid settings", setting_value=value)

    assert error.setting_value["list"] == ["***REDACTED***", b"opaque-bytes"]
    assert error.setting_value["tuple"] == ("***REDACTED***", 7)
    assert error.setting_value["set"] == {"***REDACTED***", "safe"}
    assert error.setting_value["frozen"] == frozenset({"***REDACTED***", "safe"})
    assert error.setting_value["cycle"] == ["***REDACTED***"]
    assert error.setting_value["binary"] == "***REDACTED***"
    assert error.setting_value["invalid_binary"] == b"\\xff\\xfe"
    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert marker not in str(error)


def test_configuration_redaction_handles_secret_wrappers_and_opaque_protocols() -> None:
    """Secret wrappers and hostile protocol implementations never escape."""
    marker = "secret-wrapper-private-marker"

    class SecretStrWithoutGetter:
        pass

    class SecretBytesWithFailingGetter:
        def get_secret_value(self) -> bytes:
            raise RuntimeError(marker)

    # Type-name detection is intentionally dependency-free; these exact names
    # exercise its fail-closed getter branches without retaining the marker.
    SecretStrWithoutGetter.__name__ = "SecretStr"
    SecretBytesWithFailingGetter.__name__ = "SecretBytes"
    assert _secret_text(SecretStrWithoutGetter()) is None
    assert _secret_text(SecretBytesWithFailingGetter()) is None
    for wrapped in (SecretStr(marker), SecretBytes(marker.encode())):
        error = ConfigurationError(
            "invalid settings", setting_name="safe", setting_value=wrapped
        )
        assert error.setting_value == "***REDACTED***"
        assert marker not in repr(error.args)
        assert marker not in repr(error.__dict__)

    class HostileString(str):
        def __new__(cls, value: str) -> HostileString:
            return str.__new__(cls, value)

        def lower(self) -> str:
            raise AssertionError("hostile string must not be inspected")

        def __repr__(self) -> str:
            return marker

    class HostileBytes(bytes):
        def decode(self, encoding: str = "utf-8") -> str:
            del encoding
            raise AssertionError("hostile bytes must not be decoded dynamically")

        def __repr__(self) -> str:
            return marker

    class HostileMapping(dict[str, object]):
        def items(self):
            raise AssertionError("hostile mapping must not be iterated")

        def __repr__(self) -> str:
            return marker

    class HostileSet(set[str]):
        def __iter__(self):
            raise AssertionError("hostile set must not be iterated")

        def __repr__(self) -> str:
            return marker

    value = {
        "string": HostileString(marker),
        "bytes": HostileBytes(marker.encode()),
        "mapping": HostileMapping(secret=marker),
        "set": HostileSet({marker}),
    }
    error = ConfigurationError(f"invalid settings: {marker}", setting_value=value)

    assert error.setting_value == {
        "string": "***REDACTED***",
        "bytes": "***REDACTED***",
        "mapping": "***REDACTED***",
        "set": "***REDACTED***",
    }
    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert marker not in repr(error)
    assert marker not in str(error)


def test_configuration_redaction_scrubs_opaque_values_from_messages() -> None:
    marker = "opaque-configuration-value-42x"
    nested: object = [({"ordinary": marker},)]

    error = ConfigurationError(
        f"invalid configuration value: {marker}",
        setting_name="ordinary",
        setting_value=nested,
    )

    assert marker not in str(error)
    assert marker not in repr(error.args)


@pytest.mark.parametrize(
    "empty_secret",
    ["", SecretStr(""), b""],
)
def test_configuration_error_empty_secret_does_not_interleave_mask(
    empty_secret: object,
) -> None:
    """An empty secret is an empty ``str.replace`` needle: replacing with it
    would splice the mask between every character of the static message
    (R141-F13). The message must survive verbatim."""
    error = ConfigurationError(
        "Invalid password value", setting_name="password", setting_value=empty_secret
    )

    assert str(error) == "Invalid password value"
    assert error.setting_value == "***REDACTED***"


@pytest.mark.parametrize(
    "whitespace_secret",
    [SecretStr(" "), " ", "\t"],
    ids=["secretstr-space", "str-space", "str-tab"],
)
def test_configuration_error_whitespace_secret_does_not_interleave_mask(
    whitespace_secret: object,
) -> None:
    """A whitespace-only secret is a truthy whole-message ``str.replace``
    needle: every space in the static message would be swapped for the mask
    (adversarial V3-3). Whitespace carries no secret material, so the message
    must survive verbatim while the attribute stays redacted."""
    error = ConfigurationError(
        "Invalid password value",
        setting_name="password",
        setting_value=whitespace_secret,
    )

    assert str(error) == "Invalid password value"
    assert error.setting_value == "***REDACTED***"


def test_configuration_error_nonempty_secret_still_masks_message() -> None:
    """Control for the whitespace guard: real secret text stays masked."""
    error = ConfigurationError(
        "Invalid hunter2 value",
        setting_name="password",
        setting_value=SecretStr("hunter2"),
    )

    assert "hunter2" not in str(error)
    assert "***REDACTED***" in str(error)
    assert error.setting_value == "***REDACTED***"


def test_configuration_redaction_fails_closed_for_invalid_utf8() -> None:
    marker = b"opaque-binary-marker"
    error = ConfigurationError(
        "invalid binary setting",
        setting_name="ordinary",
        setting_value=marker + b"\\xff",
    )

    assert error.setting_value == "***REDACTED***"
    assert marker.decode() not in repr(error.__dict__)


def test_configuration_redaction_fails_closed_for_baseexception_secret_getter() -> None:
    marker = "hostile-secret-getter-marker"

    class _Signal(BaseException):
        pass

    class _HostileSecret:
        def get_secret_value(self) -> str:
            raise _Signal(marker)

    _HostileSecret.__name__ = "SecretStr"
    error = ConfigurationError(
        f"invalid setting: {marker}",
        setting_name="ordinary",
        setting_value=_HostileSecret(),
    )

    assert str(error) == "Invalid configuration."
    assert marker not in repr(error.__dict__)


def test_configuration_redaction_handles_multiline_headers() -> None:
    marker = "multiline-header-private-marker"
    error = ConfigurationError(
        "request failed\nAuthorization:\n  Bearer " + marker,
    )

    assert marker not in str(error)
    assert "Authorization" not in str(error)


def test_configuration_redaction_bounds_recursive_message_collection() -> None:
    marker = "bounded-secret-marker"
    nested: object = marker
    for _ in range(18):
        nested = [nested]

    # The copied attribute is still bounded and therefore safe even when the
    # diagnostic-message collector intentionally stops descending at depth 16.
    error = ConfigurationError("invalid settings", setting_value=nested)
    assert marker not in repr(error.__dict__)
    assert marker not in repr(error.setting_value)
    assert _redact_setting_value("value", "safe", _depth=17) == "***REDACTED***"


def test_configuration_redaction_logs_only_sanitized_surfaces(caplog) -> None:
    marker = "logging-secret-marker"
    error = ConfigurationError(
        "invalid settings",
        setting_name="password",
        setting_value=marker,
    )

    with caplog.at_level(logging.ERROR, logger="scrapy_extension.security-test"):
        try:
            raise error
        except ConfigurationError:
            logging.getLogger("scrapy_extension.security-test").exception(
                "configuration validation failed"
            )

    assert marker not in repr(error.args)
    assert marker not in repr(error.__dict__)
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in "".join(traceback.format_exception(error))
    assert marker not in caplog.text


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
