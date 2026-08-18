"""Small shared primitives for transport security policy enforcement."""

from __future__ import annotations

from ipaddress import ip_address

from scrapy_extension.exceptions.base import ConfigurationError


def is_loopback_host(host: object) -> bool:
    """Return whether a literal host is restricted to this machine.

    This deliberately performs no DNS lookup. ``*.localhost`` remains
    untrusted because its resolution policy is externally controlled; only the
    exact hostname (with at most one trailing dot) and strict literal loopback
    addresses receive the local-development exception.
    """
    if not isinstance(host, str):
        return False

    candidate = host.lower()
    if candidate in {"localhost", "localhost."}:
        return True
    if "%" in candidate:
        # ``ip_address`` accepts IPv6 scope IDs on supported Python versions,
        # but a scoped address is not the host-only literal this policy expects.
        return False

    bracketed = candidate.startswith("[") or candidate.endswith("]")
    if bracketed:
        if not (candidate.startswith("[") and candidate.endswith("]")):
            return False
        candidate = candidate[1:-1]

    try:
        address = ip_address(candidate)
    except ValueError:
        return False
    if bracketed and address.version != 6:
        return False
    if address.version == 6 and address.ipv4_mapped is not None:
        return False
    return address.is_loopback and not address.is_unspecified


def normalize_allow_remote_plaintext(value: object) -> bool:
    """Parse canonical environment booleans without accepting truthy lookalikes."""
    if type(value) is bool:
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise ConfigurationError(
        "allow_remote_plaintext must be a boolean.",
        setting_name="allow_remote_plaintext",
    )


def validate_allow_remote_plaintext(value: object) -> bool:
    """Require an exact boolean even when mutable settings bypass Pydantic."""
    if type(value) is not bool:
        raise ConfigurationError(
            "allow_remote_plaintext must be a boolean.",
            setting_name="allow_remote_plaintext",
        )
    return value


def require_remote_plaintext_opt_in(
    backend_name: str, allow_remote_plaintext: object
) -> None:
    """Reject remote anonymous plaintext unless its risk is explicitly accepted."""
    if validate_allow_remote_plaintext(allow_remote_plaintext) is not True:
        raise ConfigurationError(
            (
                f"Remote unauthenticated plaintext {backend_name} connections "
                "require allow_remote_plaintext=True. Enable TLS or use this "
                "override only for a trusted private network."
            ),
            setting_name="allow_remote_plaintext",
        )
