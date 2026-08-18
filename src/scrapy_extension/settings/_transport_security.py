"""Small shared primitives for transport security policy enforcement."""

from __future__ import annotations

from ipaddress import ip_address

from scrapy_extension.exceptions.base import ConfigurationError


def is_loopback_host(host: object) -> bool:
    """Return whether a literal host is restricted to this machine.

    This deliberately performs no DNS lookup. ``*.localhost`` remains
    untrusted because its resolution policy is externally controlled; only the
    exact hostname and literal loopback addresses receive the local-development
    exception.
    """
    if not isinstance(host, str):
        return False
    normalized = host.lower().rstrip(".")
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


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
