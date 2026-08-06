"""Small shared primitives for transition-period transport security warnings."""

from __future__ import annotations

import warnings
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


def validate_allow_remote_plaintext(value: object) -> bool:
    """Require a boolean opt-in even when mutable settings bypass Pydantic."""
    if type(value) is not bool:
        raise ConfigurationError(
            "allow_remote_plaintext must be a boolean.",
            setting_name="allow_remote_plaintext",
        )
    return value


def warn_remote_unauthenticated_plaintext(
    backend_name: str, allow_remote_plaintext: object
) -> bool:
    """Warn about a remote plaintext transport until the opt-in is explicit.

    The message intentionally includes no endpoint or credential values. The
    warning source remains inside this module so Python never renders a caller
    source line that may contain inline secrets.
    """
    allowed = validate_allow_remote_plaintext(allow_remote_plaintext)
    if not allowed:
        warnings.warn(
            (
                f"Remote unauthenticated plaintext {backend_name} connections are "
                "deprecated and will be rejected in a future release. Enable TLS "
                "or set allow_remote_plaintext=True only for a trusted private "
                "network."
            ),
            FutureWarning,
            stacklevel=1,
        )
    return allowed
