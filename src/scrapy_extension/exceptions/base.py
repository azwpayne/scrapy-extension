"""Custom exceptions for scrapy-extension.

This module defines the exception hierarchy used throughout the package.
"""

from __future__ import annotations

import re
from typing import cast
from urllib.parse import urlsplit

_REDACTED = "***REDACTED***"
_SAFE_SETTING_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_URI_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_SENSITIVE_HEADER = re.compile(
    r"^[ \t]*(?:authorization|proxy-authorization|cookie|set-cookie|"
    r"x-api-key|x-auth-token)[ \t]*:",
    re.IGNORECASE | re.MULTILINE,
)
_SENSITIVE_HEADER_LINE = re.compile(
    r"^[ \t]*(?:authorization|proxy-authorization|cookie|set-cookie|"
    r"x-api-key|x-auth-token)[ \t]*:[ \t]*[^\r\n]*"
    r"(?:\r?\n[ \t]+[^\r\n]*)*",
    re.IGNORECASE | re.MULTILINE,
)
_AUTH_SCHEME = re.compile(r"^(?:bearer|basic)\s+", re.IGNORECASE)
# Message-level sibling of ``_AUTH_SCHEME`` (X5-2): a bare credential token
# (``basic dXNlcjpwYXNz``, ``Bearer eyJhbGc...``) interpolates into exception
# text with no ``scheme://``, userinfo, or header-line wrapper, so none of the
# structural fallbacks below can see it.  The form mirrors ``_AUTH_SCHEME``'s
# scheme set, case-insensitive matching, and start anchor (``re.MULTILINE``
# extends that anchor to every line of a multi-line message, like the header
# patterns).  The token must additionally be credential-shaped — mixed-case
# or containing a digit/symbol — so ordinary prose keeps a line-initial
# ``basic authentication ...`` static message (settings safe-list) and
# ``Basic HTTP``-style all-caps abbreviations intact.
_AUTH_SCHEME_CREDENTIAL = re.compile(
    r"^(?i:basic|bearer)[ \t]+(?=\S*(?:[^A-Za-z\s]|[A-Z]\S*[a-z]|[a-z]\S*[A-Z]))\S+",
    re.MULTILINE,
)
# ``urlsplit`` cannot recover userinfo without a ``scheme://`` prefix (the
# authority parses as a path and ``username``/``password`` stay ``None``), so
# any ``x:y@`` — or ``:y@``, since an empty username still carries a password —
# prefix before the first "/" is checked structurally.
_SCHEMELESS_USERINFO = re.compile(r"^[^/\s:@]*:[^/\s@]*@")
_SENSITIVE_NAME_FRAGMENTS = (
    "password",
    "secret",
    "api_key",
    "apikey",
    "token",
    "credential",
    "authorization",
    "pass",
    "pwd",
    "private_key",
    "privatekey",
    "api-key",
    "header",
    "cookie",
    "access_key",
    "secret_key",
    "marker",
    "uri",
    "url",
)


def _contains_sensitive_fragment(value: str) -> bool:
    """Match secret aliases across the common underscore/hyphen spellings."""
    normalized = value.lower().replace("-", "_")
    return any(
        fragment.replace("-", "_") in normalized
        for fragment in _SENSITIVE_NAME_FRAGMENTS
    )


def _is_sensitive_name(name: object) -> bool:
    """Heuristic: does this setting name suggest the value is secret?"""
    # Do not call methods on a caller-defined ``str`` subclass while deciding
    # whether a field is sensitive.  Exact built-in strings are the only safe
    # diagnostic labels.
    if type(name) is not str:
        return False
    return _contains_sensitive_fragment(name)


def _is_secret_value(value: object) -> bool:
    """Detect SecretStr / SecretBytes from pydantic without importing pydantic."""
    return type(value).__name__ in {"SecretStr", "SecretBytes"}


def _secret_text(value: object) -> str | bytes | None:
    """Read a pydantic secret only when its wrapper type is exact."""
    if not _is_secret_value(value):
        return None
    getter = getattr(value, "get_secret_value", None)
    if not callable(getter):
        return None
    try:
        raw = getter()
    except BaseException:
        # Secret wrappers are caller-controlled protocol objects. If their
        # accessor is hostile (including a custom BaseException), fail closed
        # and let the caller retain only the wrapper's redacted placeholder.
        return None
    return raw if type(raw) in {str, bytes} else None


def _secret_as_text(value: str | bytes) -> str:
    """Convert an exact secret leaf to text without invoking representation hooks."""
    if type(value) is bytes:
        return value.decode("utf-8", "replace")
    return cast(str, value)


def _looks_sensitive_text(value: object) -> bool:
    """Recognize transport/credential text without invoking custom repr code."""
    if type(value) is not str:
        return False
    if _contains_sensitive_fragment(value):
        return True
    if _SENSITIVE_HEADER.match(value) or _AUTH_SCHEME.match(value):
        return True
    if _URI_PREFIX.match(value):
        return True
    if _SCHEMELESS_USERINFO.match(value.split("/", 1)[0]):
        return True
    try:
        parsed = urlsplit(value)
        return parsed.username is not None or parsed.password is not None
    except ValueError:
        return True


def _redact_message(
    message: object,
    setting_value: object = None,
    *,
    force_setting_value: bool = False,
) -> str:
    """Remove transport tokens and supplied values from exception text.

    Caller-controlled values are not safe merely because their field name looks
    ordinary. At a terminal configuration boundary, every exact string/UTF-8
    byte leaf is collected so an opaque value interpolated into ``message`` cannot
    survive. Cycles are ignored, while depth/protocol failures fail closed with a static message;
    continuing with a partially inspected graph would make the redaction
    contract depend on the hostile object's traversal behaviour.
    """
    if type(message) is not str:
        return "Invalid configuration."
    safe_message = message
    sensitive_values: set[str] = set()
    inspection_failed = False

    def collect(
        value: object,
        name: object,
        seen: set[int],
        depth: int,
        force: bool,
    ) -> None:
        nonlocal inspection_failed
        del name
        # ``force`` requests value replacement for a boundary that has already
        # classified its input as untrusted. Other exception classes retain
        # useful static messages while heuristic transport/credential leaves are
        # still redacted.
        if depth > 16:
            inspection_failed = True
            return
        if id(value) in seen:
            return
        if value is None or type(value) in {bool, int, float}:
            return
        if isinstance(value, str) and type(value) is not str:
            try:
                text = str.__str__(value)
            except BaseException:
                inspection_failed = True
                return
            if text:
                sensitive_values.add(text)
            return
        if isinstance(value, bytes) and type(value) is not bytes:
            try:
                text = bytes.decode(value, "utf-8")
            except BaseException:
                inspection_failed = True
                return
            if text:
                sensitive_values.add(text)
            return
        if type(value) is str:
            if force or _looks_sensitive_text(value):
                sensitive_values.add(value)
            return
        if type(value) is bytes:
            try:
                text = value.decode("utf-8")
            except UnicodeDecodeError:
                # An undecodable value cannot be compared against the message;
                # retaining the original would expose opaque binary material.
                inspection_failed = True
                return
            if force or _looks_sensitive_text(text):
                sensitive_values.add(text)
            return
        secret = _secret_text(value)
        if secret is not None:
            sensitive_values.add(_secret_as_text(secret))
            return
        if isinstance(value, dict):
            mapping = cast(dict[object, object], value)
            seen.add(id(value))
            try:
                # Base descriptors bypass hostile subclass protocol overrides.
                entries = tuple(
                    mapping.items()
                    if type(value) is dict
                    else dict.items(cast(dict[object, object], value))
                )
            except BaseException:
                inspection_failed = True
                return
            for key, item in entries:
                # Ordinary ASCII field labels are metadata, not secret leaves;
                # collecting a one-character label would corrupt unrelated text
                # (for example, ``bad`` would become ``b***d``). Non-field keys
                # remain untrusted and are inspected as values.
                if not (
                    type(key) is str and _SAFE_SETTING_NAME.fullmatch(key) is not None
                ):
                    collect(key, None, seen, depth + 1, force)
                collect(item, key, seen, depth + 1, force)
            return
        if isinstance(value, list):
            seen.add(id(value))
            try:
                list_entries: list[object] = list(
                    cast(list[object], value)
                    if type(value) is list
                    else list.__iter__(cast(list[object], value))
                )
            except BaseException:
                inspection_failed = True
                return
            for item in list_entries:
                collect(item, None, seen, depth + 1, force)
            return
        if isinstance(value, tuple):
            seen.add(id(value))
            try:
                tuple_entries: tuple[object, ...] = tuple(
                    cast(tuple[object, ...], value)
                    if type(value) is tuple
                    else tuple.__iter__(cast(tuple[object, ...], value))
                )
            except BaseException:
                inspection_failed = True
                return
            for item in tuple_entries:
                collect(item, None, seen, depth + 1, force)
            return
        if isinstance(value, set):
            seen.add(id(value))
            try:
                set_entries: set[object] = set(
                    cast(set[object], value)
                    if type(value) is set
                    else set.__iter__(cast(set[object], value))
                )
            except BaseException:
                inspection_failed = True
                return
            for item in set_entries:
                collect(item, None, seen, depth + 1, force)
            return
        if isinstance(value, frozenset):
            seen.add(id(value))
            try:
                frozenset_entries: frozenset[object] = frozenset(
                    cast(frozenset[object], value)
                    if type(value) is frozenset
                    else frozenset.__iter__(cast(frozenset[object], value))
                )
            except BaseException:
                inspection_failed = True
                return
            for item in frozenset_entries:
                collect(item, None, seen, depth + 1, force)
            return
        # Arbitrary plugin objects are not introspected.  Their representation
        # may carry a value we cannot safely discover, so fail closed below.
        inspection_failed = True

    collect(setting_value, None, set(), 0, force_setting_value)
    if inspection_failed:
        # A partially traversed graph is not a safe basis for preserving a
        # caller-supplied diagnostic.  The copied public setting value is also
        # bounded/redacted by ``_redact_setting_value``.
        return "Invalid configuration."
    for value in sorted(sensitive_values, key=len, reverse=True):
        if not value or value.isspace():
            # An empty needle would interleave the mask between every character
            # of the message, and a whitespace-only needle (for example a
            # single space secret) would swap every space for the mask.
            # Whitespace carries no secret material, so skip it instead.
            continue
        if len(value) == 1 and (value.isalnum() or value == "_"):
            # A one-character queue/key name must not rewrite ordinary words
            # that merely contain that character (for example ``"q"`` in
            # ``"queue"``).  Keep standalone occurrences redacted while
            # preserving the static diagnostic text around them.
            safe_message = re.sub(
                rf"(?<![A-Za-z0-9_]){re.escape(value)}(?![A-Za-z0-9_])",
                _REDACTED,
                safe_message,
            )
        else:
            safe_message = safe_message.replace(value, _REDACTED)

    # A caller can interpolate a URI/header into a message without also passing
    # it as ``setting_value``. Replace the complete transport token, while
    # leaving safe scheme-only documentation such as ``mongodb://`` intact.
    #
    # X1-1 boundary decision (fix round 3): an RFC 3986-invalid password that
    # contains a raw space (``redis://user:hu nter2@host`` — userinfo must
    # percent-encode spaces, RFC 3986 §3.2.1) truncates this replacement at
    # the space and the ``nter2@host`` tail survives. Raw-whitespace
    # passwords are outside the heuristic coverage by design: allowing
    # internal whitespace in the password class would mask ordinary
    # ``name: value @mention`` prose. This is a documented contract boundary,
    # not an oversight.
    safe_message = re.sub(
        r"(?i)\b[a-z][a-z0-9+.-]*://[^\s'\"),]+",
        _REDACTED,
        safe_message,
    )
    # ``urlsplit`` cannot recover userinfo without a ``scheme://`` prefix, so a
    # schemeless ``user:pw@host`` (or empty-username ``:pw@host``) that exists
    # only in the message text needs its own fallback.  The lookbehind keeps
    # the pattern off colon-bearing ratio/time text that lacks userinfo.
    #
    # X1-1 boundary decision: the raw-space password form
    # ``user:hu nter2@host`` deliberately misses here too (the password class
    # excludes whitespace); see the RFC 3986 contract note above.
    safe_message = re.sub(
        r"(?<![\w/@.])[^/\s:@]*:[^/\s@]*@[^\s'\"),]*",
        _REDACTED,
        safe_message,
    )
    # Bare ``Authorization``-scheme credentials (X5-2) carry no structural
    # wrapper at all; the line-anchored scheme+token form is masked on its own.
    safe_message = _AUTH_SCHEME_CREDENTIAL.sub(_REDACTED, safe_message)
    safe_message = _SENSITIVE_HEADER_LINE.sub("<redacted-header>:", safe_message)
    safe_message = re.sub(
        r"(?i)(?:password|pass|pwd|private[_-]?key|secret|token|credential|"
        r"authorization|api[_-]?key)"
        r"\s*[=:]\s*(?!none\b)[^\s,;]+",
        lambda match: (
            match.group(0).split("=", 1)[0].split(":", 1)[0] + "=" + _REDACTED
        ),
        safe_message,
    )
    if _SENSITIVE_HEADER.search(safe_message):
        safe_message = _SENSITIVE_HEADER.sub("<redacted-header>:", safe_message)
    return safe_message


def _safe_diagnostic_label(value: object) -> str | None:
    """Retain ordinary metadata labels without invoking custom string hooks."""
    if type(value) is str and not _looks_sensitive_text(value):
        return value
    return None


def _safe_setting_name(name: object) -> str | None:
    """Keep field labels while rejecting URI/header-shaped labels and probes."""
    if type(name) is not str or not _SAFE_SETTING_NAME.fullmatch(name):
        return None
    # Test and plugin marker values are commonly passed as labels by mistake.
    # They are not package field names and retaining them defeats traceback-free
    # configuration diagnostics.
    if "marker" in name.lower():
        return None
    return name


def _safe_mapping_key(key: object) -> object:
    """Retain only ordinary configuration labels as nested mapping keys."""
    if type(key) is str:
        return key if _SAFE_SETTING_NAME.fullmatch(key) else _REDACTED
    if type(key) in {bool, int, float, type(None)}:
        return key
    return _REDACTED


def _redact_setting_value(
    value: object,
    setting_name: object,
    *,
    _seen: set[int] | None = None,
    _depth: int = 0,
) -> object:
    """Copy nested configuration containers without secret or transport leaves.

    This is deliberately a bounded, cycle-aware copier.  Retaining the original
    mapping/list or an arbitrary plugin object on an exception would let
    ``repr(exc)`` or later logging recover credentials through a nested reference.
    """
    if _depth > 16:
        return _REDACTED
    # A hostile key subclass can override ``lower`` or other protocol methods.
    # Treat values under such keys as sensitive instead of attempting to inspect
    # the key or retaining an opaque value beneath a redacted key.
    if setting_name is not None and type(setting_name) not in {
        str,
        bool,
        int,
        float,
        bytes,
        type(None),
    }:
        return _REDACTED
    if value is None:
        # Preserve the historical contract for omitted secret fields. URI/URL
        # fields can legitimately be omitted and therefore retain ``None``.
        lowered_name = setting_name.lower() if type(setting_name) is str else ""
        if _is_sensitive_name(setting_name) and not any(
            fragment in lowered_name for fragment in ("uri", "url")
        ):
            return _REDACTED
        return None
    if _is_sensitive_name(setting_name) or _is_secret_value(value):
        return _REDACTED
    if type(value) is str:
        return _REDACTED if _looks_sensitive_text(value) else value
    if type(value) is bytes:
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            # Invalid UTF-8 cannot be classified as a safe diagnostic value.
            # Do not retain opaque binary material on the exception object.
            return _REDACTED
        return _REDACTED if decoded and _looks_sensitive_text(decoded) else value

    if _seen is None:
        _seen = set()
    value_id = id(value)
    if value_id in _seen:
        return _REDACTED
    if type(value) is dict:
        mapping = cast(dict[object, object], value)
        _seen.add(value_id)
        redacted: dict[object, object] = {}
        try:
            entries = tuple(mapping.items())
        except Exception:
            return _REDACTED
        for key, item in entries:
            safe_key = _safe_mapping_key(key)
            if _is_sensitive_name(key):
                redacted[safe_key] = _REDACTED
            else:
                redacted[safe_key] = _redact_setting_value(
                    item, key, _seen=_seen, _depth=_depth + 1
                )
        return redacted
    if type(value) is list:
        _seen.add(value_id)
        return [
            _redact_setting_value(item, setting_name, _seen=_seen, _depth=_depth + 1)
            for item in cast(list[object], value)
        ]
    if type(value) is tuple:
        _seen.add(value_id)
        return tuple(
            _redact_setting_value(item, setting_name, _seen=_seen, _depth=_depth + 1)
            for item in cast(tuple[object, ...], value)
        )
    if type(value) is set:
        _seen.add(value_id)
        return {
            _redact_setting_value(item, setting_name, _seen=_seen, _depth=_depth + 1)
            for item in cast(set[object], value)
        }
    if type(value) is frozenset:
        _seen.add(value_id)
        return frozenset(
            _redact_setting_value(item, setting_name, _seen=_seen, _depth=_depth + 1)
            for item in cast(frozenset[object], value)
        )
    # Primitive non-secret values remain useful in configuration diagnostics.
    if type(value) in {bool, int, float}:
        return value
    # Do not retain arbitrary plugin objects: their repr/attributes may contain
    # credentials and they are not needed after validation has failed.
    return _REDACTED


class BackendError(Exception):
    """Base exception for all backend-related errors.

    All custom exceptions in this package inherit from BackendError,
    allowing users to catch all backend-related errors with a single
    except clause.
    """


class BackendOperationTimeout(BackendError):
    """A bounded reactor-facing backend operation exceeded its wait budget.

    The worker performing a synchronous third-party SDK call may still be
    unwinding after this error is delivered to the reactor. Callers must keep
    operation ordering/ownership fences in place until that worker completes.
    """

    def __init__(self, operation: str, timeout: float) -> None:
        safe_operation = (
            operation
            if type(operation) is str and not _looks_sensitive_text(operation)
            else "backend-operation"
        )
        super().__init__(f"Backend operation timed out: {safe_operation}.")
        self.operation = safe_operation
        self.timeout = timeout


class BackendConnectionError(BackendError):
    """Exception raised for connection-related errors.

    This includes failures to establish initial connections, lost
    connections during operation, and authentication failures.

    Attributes:
        backend_type: The type of backend that failed to connect.
        message: Explanation of the error.
    """

    def __init__(self, message: str, backend_type: str | None = None) -> None:
        safe_message = _redact_message(
            message,
            backend_type,
            force_setting_value=backend_type is not None
            and type(backend_type) is not str,
        )
        safe_backend_type = (
            backend_type
            if type(backend_type) is str and not _looks_sensitive_text(backend_type)
            else (None if backend_type is None else "backend")
        )
        super().__init__(safe_message)
        self.backend_type = safe_backend_type
        self.message = safe_message


class QueueError(BackendError):
    """Exception raised for queue operation errors.

    This includes failures to push/pop items, queue full conditions,
    and serialization errors for queue items.

    Attributes:
        queue_name: The name of the queue where the error occurred.
        operation: The operation being performed (push, pop, etc.).
    """

    def __init__(
        self,
        message: str,
        queue_name: str | None = None,
        operation: str | None = None,
    ) -> None:
        safe_queue_name = (
            queue_name
            if type(queue_name) is str and not _looks_sensitive_text(queue_name)
            else (None if queue_name is None else _REDACTED)
        )
        safe_message = _redact_message(
            message,
            queue_name,
            force_setting_value=queue_name is not None and type(queue_name) is not str,
        )
        super().__init__(safe_message)
        self.queue_name = safe_queue_name
        self.operation = _safe_diagnostic_label(operation)


class QueueOutcomeIndeterminateError(QueueError):
    """A queue mutation may have committed before its response was lost."""


class SetOutcomeIndeterminateError(BackendConnectionError):
    """A set mutation may have committed before its response was lost."""


class StorageError(BackendError):
    """Exception raised for storage operation errors.

    This covers failures in StorageBackend operations (``store`` / ``retrieve``
    / ``delete`` / ``exists`` / ``ttl`` / ``list_storage_keys`` /
    ``clear_storage``). Raising
    ``StorageError`` instead of returning a silent sentinel (``None`` / ``False``)
    prevents the item pipeline from treating a failed write as a success.

    ``except BackendError`` catches every storage-path failure uniformly
    across memcached / dynamodb / mongodb (mirrors the queue-op ``QueueError``
    contract).

    Attributes:
        operation: The storage operation that failed (``store``, ``retrieve``,
            ``delete``, ``exists``, ``ttl``, ``list_storage_keys``,
            ``clear_storage``).
        key: The storage key the operation was performed on, or ``None`` for
            keyless operations (``clear_storage``).
    """

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        key: str | None = None,
    ) -> None:
        safe_key = (
            key
            if type(key) is str and not _looks_sensitive_text(key)
            else (None if key is None else _REDACTED)
        )
        safe_message = _redact_message(
            message,
            key,
            force_setting_value=key is not None and type(key) is not str,
        )
        super().__init__(safe_message)
        self.operation = _safe_diagnostic_label(operation)
        self.key = safe_key


class StorageOutcomeIndeterminateError(StorageError):
    """A storage mutation may have committed before its response was lost."""


class StorageBackpressureError(StorageError):
    """Item admission was rejected because the batched buffer is full.

    Unlike a backend :class:`StorageError`, this means the item was never
    accepted by the in-process batching strategy.  The message is intentionally
    fixed: rejected keys and values must not become part of an operational
    exception surface.
    """

    def __init__(self, *, operation: str | None = "store") -> None:
        super().__init__(
            "Batched storage is at capacity.",
            operation=operation,
            key=None,
        )


class SerializationError(BackendError):
    """Exception raised for serialization/deserialization errors.

    This includes JSON encoding/decoding errors and other data
    transformation failures.

    Attributes:
        data: The data that failed to serialize/deserialize.
        serializer: The serializer that failed.
    """

    def __init__(
        self,
        message: str,
        data: object = None,
        serializer: str | None = None,
    ) -> None:
        safe_data = _redact_setting_value(data, "data")
        safe_message = _redact_message(
            message,
            data,
            force_setting_value=(
                isinstance(data, (str, bytes)) and type(data) not in {str, bytes}
            ),
        )
        safe_message = _redact_message(
            safe_message,
            serializer,
            force_setting_value=serializer is not None and type(serializer) is not str,
        )
        super().__init__(safe_message)
        self.data = safe_data
        self.serializer = _safe_diagnostic_label(serializer)


class ConfigurationError(BackendError):
    """Exception raised for configuration errors.

    This includes invalid settings, missing required parameters,
    and validation failures.

    The ``setting_value`` attribute is automatically redacted when either:
    - The value is a pydantic ``SecretStr`` / ``SecretBytes`` (detected by type
      name, no pydantic import required)
    - The ``setting_name`` contains a sensitive fragment (``password``,
      ``secret``, ``api_key``, ``apikey``, ``api-key``, ``token``, ``credential``,
      ``pass``, ``pwd``, ``private_key``)

    Redaction prevents accidental secret leaks via ``repr(exc)`` or
    debug-logging the exception. The raw value is never retained on the
    exception object once redacted.

    Attributes:
        setting_name: The name of the setting that caused the error.
        setting_value: The invalid value (or ``***REDACTED***`` if sensitive).
    """

    setting_name: str | None
    setting_value: object

    def __init__(
        self,
        message: str,
        setting_name: str | None = None,
        setting_value: object = None,
    ) -> None:
        safe_name = _safe_setting_name(setting_name)
        secret = _secret_text(setting_value)
        if secret is None and _is_sensitive_name(setting_name):
            if type(setting_value) is str or type(setting_value) is bytes:
                secret = setting_value
        safe_message = _redact_message(
            message,
            setting_value,
            # Configuration diagnostics are a terminal boundary: an opaque
            # value may be a credential even when its label is innocuous.
            force_setting_value=setting_value is not None
            or (
                setting_name is not None
                and (safe_name is None or _is_sensitive_name(setting_name))
            ),
        )
        if secret is not None:
            # An empty needle would interleave the mask between every character
            # of the message; a whitespace-only needle would swap every space
            # for the mask. Neither carries secret material, so skip both.
            secret_text = _secret_as_text(secret)
            if secret_text and not secret_text.isspace():
                safe_message = safe_message.replace(secret_text, _REDACTED)
        super().__init__(safe_message)
        self.setting_name = safe_name
        if setting_name is not None and safe_name is None:
            self.setting_value = _REDACTED
        else:
            self.setting_value = _redact_setting_value(setting_value, safe_name)
