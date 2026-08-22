"""No-network broker endpoint grammars shared by Kafka and RocketMQ.

The clients accept endpoint strings directly, so accepting a URL, a malformed
port, or a hostname with non-ASCII/control characters postpones a configuration
mistake until a driver attempts I/O.  Keep the grammar here deliberately small
and return static ``ConfigurationError`` messages: callers must never receive
the rejected endpoint back through diagnostics.
"""

from __future__ import annotations

import ipaddress
from typing import NoReturn

from scrapy_extension.exceptions.base import ConfigurationError
from scrapy_extension.settings._endpoint_validation import parse_endpoint_port

KAFKA_BROKER_ENDPOINTS_ERROR = (
    "Kafka broker endpoints must be a comma-separated list of valid host[:port] values."
)
ROCKETMQ_NAMESRV_ENDPOINTS_ERROR = (
    "RocketMQ namesrv_address must be one DNS endpoint or a "
    "semicolon-separated IPv4 endpoint list."
)


def _raise_invalid_kafka_endpoints(setting_name: str) -> NoReturn:
    raise ConfigurationError(
        KAFKA_BROKER_ENDPOINTS_ERROR,
        setting_name=setting_name,
    )


def _raise_invalid_rocketmq_endpoints() -> NoReturn:
    raise ConfigurationError(
        ROCKETMQ_NAMESRV_ENDPOINTS_ERROR,
        setting_name="namesrv_address",
    )


def _contains_forbidden_characters(value: object) -> bool:
    """Reject controls and non-ASCII before a parser can reinterpret them."""
    if type(value) is not str:
        return True
    return not value.isascii() or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    )


def _parse_port(value: str) -> str | None:
    """Return a canonical valid ASCII TCP port, or ``None`` if invalid."""
    port = parse_endpoint_port(value)
    return str(port) if port is not None else None


def _parse_ipv4(value: str) -> str | None:
    """Return canonical IPv4, rejecting malformed numeric lookalikes."""
    try:
        address = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError:
        return None
    return str(address)


def _is_pseudo_ipv4(value: str) -> bool:
    """Recognize non-canonical IPv4 forms before DNS validation.

    A string such as ``999.1.1.1``, ``127.1``, or ``0x7f.0.0.1`` must not become
    a DNS hostname merely because every label is syntactically valid DNS.  Some
    socket implementations reinterpret those forms as IPv4 literals.  Valid
    canonical IPv4 is handled first.
    """
    labels = value.split(".")
    return bool(labels) and all(_is_numeric_ipv4_component(label) for label in labels)


def _is_numeric_ipv4_component(value: str) -> bool:
    """Return whether one label participates in a legacy numeric IPv4 spelling."""
    if value.isdigit():
        return True
    lowered = value.lower()
    return (
        lowered.startswith("0x")
        and len(lowered) > 2
        and all(character in "0123456789abcdef" for character in lowered[2:])
    )


def _parse_dns(value: str) -> str | None:
    """Return an ASCII DNS hostname, or ``None`` when its labels are invalid."""
    if not value or len(value) > 253 or _is_pseudo_ipv4(value):
        return None
    labels = value.split(".")
    if any(not label or len(label) > 63 for label in labels):
        return None
    for label in labels:
        if not label[0].isalnum() or not label[-1].isalnum():
            return None
        if any(not (character.isalnum() or character == "-") for character in label):
            return None
    return value


def _parse_host(value: str) -> tuple[str, str] | None:
    """Classify one unbracketed host as IPv4 or DNS.

    The return value is ``(kind, canonical_host)``.  IPv6 is deliberately not
    accepted here because it needs its own bracket-aware grammar.
    """
    ipv4 = _parse_ipv4(value)
    if ipv4 is not None:
        return "ipv4", ipv4
    dns = _parse_dns(value)
    if dns is not None:
        return "dns", dns
    return None


def _parse_kafka_endpoint(value: str) -> str | None:
    """Normalize one Kafka endpoint without performing DNS or network I/O.

    A valid raw IPv6 literal is unambiguously treated as *portless* and gains
    brackets.  For example, ``::1`` becomes ``[::1]``.  Some valid IPv6 strings
    can also visually resemble an IPv6-plus-port spelling; no parser can infer
    that intent without changing valid raw IPv6 semantics, so explicit ports are
    reliably expressed with ``[ipv6]:port``.
    """
    if _contains_forbidden_characters(value):
        return None
    endpoint = value.strip(" ")
    if (
        not endpoint
        or "%" in endpoint
        or any(character.isspace() for character in endpoint)
    ):
        return None

    if endpoint.startswith("["):
        closing = endpoint.find("]")
        if closing <= 1:
            return None
        literal = endpoint[1:closing]
        remainder = endpoint[closing + 1 :]
        if not remainder:
            port = None
        elif remainder.startswith(":"):
            port = _parse_port(remainder[1:])
            if port is None:
                return None
        else:
            return None
        try:
            address = ipaddress.IPv6Address(literal)
        except ipaddress.AddressValueError:
            return None
        normalized = f"[{address.compressed}]"
        return normalized if port is None else f"{normalized}:{port}"

    colon_count = endpoint.count(":")
    if colon_count == 0:
        parsed_host = _parse_host(endpoint)
        return parsed_host[1] if parsed_host is not None else None
    if colon_count == 1:
        host, port_text = endpoint.split(":", 1)
        parsed_host = _parse_host(host)
        port = _parse_port(port_text)
        if parsed_host is None or port is None:
            return None
        return f"{parsed_host[1]}:{port}"

    try:
        address = ipaddress.IPv6Address(endpoint)
    except ipaddress.AddressValueError:
        return None
    return f"[{address.compressed}]"


def normalize_kafka_broker_endpoints(value: object, setting_name: str) -> str:
    """Validate and canonicalize one comma-separated Kafka endpoint string."""
    if type(value) is not str:
        _raise_invalid_kafka_endpoints(setting_name)
    if _contains_forbidden_characters(value):
        _raise_invalid_kafka_endpoints(setting_name)
    normalized: list[str] = []
    for member in value.split(","):
        endpoint = _parse_kafka_endpoint(member)
        if endpoint is None:
            _raise_invalid_kafka_endpoints(setting_name)
        normalized.append(endpoint)
    if not normalized:
        _raise_invalid_kafka_endpoints(setting_name)
    return ",".join(normalized)


def _parse_rocketmq_endpoint(value: str) -> tuple[str, str] | None:
    """Parse a strict explicit-port RocketMQ endpoint as ``(kind, endpoint)``."""
    if _contains_forbidden_characters(value):
        return None
    endpoint = value.strip(" ")
    if not endpoint or any(character.isspace() for character in endpoint):
        return None
    if endpoint.count(":") != 1:
        return None
    host, port_text = endpoint.split(":", 1)
    parsed_host = _parse_host(host)
    port = _parse_port(port_text)
    if parsed_host is None or port is None:
        return None
    kind, normalized_host = parsed_host
    return kind, f"{normalized_host}:{port}"


def normalize_rocketmq_namesrv_endpoints(value: object) -> str:
    """Validate the RocketMQ 5.1.1 proxy endpoint contract.

    The locked SDK accepts only one DNS address but can use a semicolon-separated
    list of IPv4 addresses.  Rejecting all other forms before client import
    prevents its ambiguous address resolver from reaching a network call.
    """
    if type(value) is not str or _contains_forbidden_characters(value):
        _raise_invalid_rocketmq_endpoints()
    parsed: list[tuple[str, str]] = []
    for member in value.split(";"):
        endpoint = _parse_rocketmq_endpoint(member)
        if endpoint is None:
            _raise_invalid_rocketmq_endpoints()
        parsed.append(endpoint)
    if not parsed:
        _raise_invalid_rocketmq_endpoints()
    kinds = {kind for kind, _ in parsed}
    if kinds == {"dns"} and len(parsed) == 1:
        return parsed[0][1]
    if kinds == {"ipv4"}:
        return ";".join(endpoint for _, endpoint in parsed)
    _raise_invalid_rocketmq_endpoints()
