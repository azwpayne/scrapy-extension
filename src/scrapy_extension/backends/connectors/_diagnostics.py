"""Retry-backoff and best-effort diagnostic-logging helpers."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import ParamSpec, TypeVar

_P = ParamSpec("_P")

_T = TypeVar("_T")


def _wait_for_retry_backoff(
    retirement_event: threading.Event,
    delay: float,
) -> bool:
    """Wait for retry delay, returning early when the manager is retired."""
    return retirement_event.wait(delay)


def _log_diagnostic(
    log_call: Callable[..., object],
    message: str,
    *args: object,
    **kwargs: object,
) -> None:
    """Emit best-effort diagnostics without changing lifecycle control flow.

    Backend operations and monitor callbacks retain their existing exception
    semantics.  Only a logging handler is untrusted here: an application may
    install a handler that raises a control-flow ``BaseException``, but that
    diagnostic must not interrupt an already-selected recovery or teardown path.
    """
    try:
        log_call(message, *args, **kwargs)
    except BaseException:
        pass
