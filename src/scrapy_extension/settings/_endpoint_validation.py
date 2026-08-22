"""Strict, no-I/O validation for backend endpoint authorities."""

from __future__ import annotations

import ipaddress
import unicodedata


def has_invalid_percent_escape(value: object) -> bool:
    """Return whether a text value contains a malformed percent escape."""
    if type(value) is not str:
        return True
    return any(
        character == "%"
        and (
            index + 2 >= len(value)
            or value[index + 1] not in "0123456789abcdefABCDEF"
            or value[index + 2] not in "0123456789abcdefABCDEF"
        )
        for index, character in enumerate(value)
    )


def _has_forbidden_endpoint_text(value: str) -> bool:
    """Return whether an endpoint contains Unicode or control/space text."""
    return not value.isascii() or any(
        character.isspace() or unicodedata.category(character).startswith("C")
        for character in value
    )


def _is_legacy_numeric_ipv4(value: str) -> bool:
    """Reject numeric IPv4 lookalikes which socket APIs reinterpret as literals."""
    labels = value.split(".")
    if not labels:
        return False
    return all(
        label.isdigit()
        or (
            label.lower().startswith("0x")
            and len(label) > 2
            and all(character in "0123456789abcdef" for character in label[2:].lower())
        )
        for label in labels
    )


def parse_endpoint_host(value: object, *, allow_brackets: bool = True) -> str | None:
    """Parse one bare DNS/IP host without performing DNS or network I/O.

    The return value is canonical for IP literals and preserves the spelling of
    valid DNS names.  ``None`` means the value is not a supported host authority.
    """
    if type(value) is not str or not value or _has_forbidden_endpoint_text(value):
        return None

    host = value
    if host.startswith("[") or host.endswith("]"):
        if not allow_brackets or not host.startswith("[") or not host.endswith("]"):
            return None
        host = host[1:-1]
        if not host or "%" in host:
            return None
        try:
            address = ipaddress.IPv6Address(host)
        except ValueError:
            return None
        return str(address)

    if any(character in host for character in "[]/@?#\\%"):
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass

    hostname = host[:-1] if host.endswith(".") else host
    if not hostname or len(hostname) > 253 or _is_legacy_numeric_ipv4(hostname):
        return None
    labels = hostname.split(".")
    if any(
        not label
        or len(label) > 63
        or not label[0].isascii()
        or not label[-1].isascii()
        or not (label[0].isalnum() and label[-1].isalnum())
        or any(
            not (character.isascii() and (character.isalnum() or character == "-"))
            for character in label
        )
        for label in labels
    ):
        return None
    return host


def parse_endpoint_port(value: object) -> int | None:
    """Parse an ASCII TCP port in the inclusive 1..65535 range.

    Avoid converting an unbounded digit string directly with ``int``.  Apart
    from being unnecessary for a five-digit port, Python deliberately limits
    very large integer conversions and would otherwise leak ``ValueError`` out
    of this no-I/O validator.
    """
    if (
        type(value) is not str
        or not value
        or not value.isascii()
        or not value.isdecimal()
    ):
        return None
    significant = value.lstrip("0")
    if not significant or len(significant) > 5:
        return None
    port = int(significant)
    return port if 1 <= port <= 65535 else None


def parse_host_port_authority(
    value: object,
    *,
    default_port: int | None = None,
    require_port: bool = False,
) -> tuple[str, int | None] | None:
    """Parse a strict host with an optional port.

    Bare IPv6 is accepted only as a portless host.  A port-bearing IPv6
    authority must use ``[IPv6]:port`` so a colon cannot be reinterpreted.
    """
    if type(value) is not str or not value or _has_forbidden_endpoint_text(value):
        return None
    if default_port is not None and (
        type(default_port) is not int or not 1 <= default_port <= 65535
    ):
        return None

    text = value
    host: str
    port: int | None = None
    if text.startswith("["):
        closing = text.find("]")
        if closing <= 1:
            return None
        if text.find("[", 1) != -1 or text.find("]", closing + 1) != -1:
            return None
        host = parse_endpoint_host(text[: closing + 1]) or ""
        remainder = text[closing + 1 :]
        if remainder:
            if not remainder.startswith(":"):
                return None
            port = parse_endpoint_port(remainder[1:])
            if port is None:
                return None
    elif text.count(":") == 1:
        host_text, port_text = text.split(":", 1)
        host = parse_endpoint_host(host_text, allow_brackets=False) or ""
        port = parse_endpoint_port(port_text)
        if not host or port is None:
            return None
    else:
        host = parse_endpoint_host(text, allow_brackets=False) or ""
        if not host:
            return None

    if not host:
        return None
    if port is None:
        if require_port:
            return None
        port = default_port
    return host, port
