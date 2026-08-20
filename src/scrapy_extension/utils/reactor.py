"""Bounded adapters for synchronous backend SDK calls.

The bundled backend ABCs are intentionally synchronous.  Scrapy lifecycle,
item-pipeline, and signal hooks can return Deferreds, so those calls are moved
off the reactor thread here.  The worker operation is never force-killed on a
wait timeout: the ordered Deferred remains authoritative until it settles,
while the caller-facing Deferred fails fast with a typed timeout.

Scrapy's ``Scheduler.enqueue_request``, ``next_request`` and
``has_pending_requests`` methods cannot return Deferreds.  They do not use this
module; their synchronous latency contract is documented and their manager
retry sleeps are bounded separately.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from twisted.internet.defer import Deferred
from twisted.internet.threads import deferToThread

from scrapy_extension.exceptions import BackendOperationTimeout

_T = TypeVar("_T")

DEFAULT_REACTOR_IO_TIMEOUT_S = 5.0
MAX_REACTOR_IO_TIMEOUT_S = 60.0


def _reactor() -> Any:
    """Resolve Twisted's installed reactor lazily.

    Importing ``twisted.internet.reactor`` at package import time would install
    SelectReactor before Scrapy can honor ``TWISTED_REACTOR``.
    """
    from twisted.internet import reactor

    return reactor


def reactor_is_running() -> bool:
    """Return whether a Twisted reactor is currently dispatching callbacks."""
    return bool(getattr(_reactor(), "running", False))


def defer_to_thread_ordered(
    function: Callable[..., _T],
    *args: Any,
    timeout: float,
    operation: str,
    **kwargs: Any,
) -> tuple[Deferred[_T], Deferred[_T]]:
    """Run one synchronous call in the thread pool and expose a bounded waiter.

    Returns ``(operation, bounded)``.  ``operation`` settles only when the SDK
    call really finishes and is suitable for an ordering chain.  ``bounded``
    mirrors its result unless the configured wait budget expires first.  A timed
    out SDK call is not cancelled because Python cannot safely stop a running
    thread; retaining the operation Deferred is what prevents later writes or
    lifecycle release from overtaking it.
    """
    operation_deferred: Deferred[_T] = deferToThread(function, *args, **kwargs)
    bounded: Deferred[_T] = Deferred()
    timed_out = False
    settled = False

    def finish_success(value: _T) -> _T:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            bounded.callback(value)
        return value

    def finish_failure(failure: Any) -> Any:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            bounded.errback(failure)
        # Keep the operation's original failure available to an ordering chain.
        return failure

    operation_deferred.addCallbacks(finish_success, finish_failure)

    def expire() -> None:
        nonlocal timed_out, settled
        if settled or bounded.called:
            return
        timed_out = True
        bounded.errback(BackendOperationTimeout(operation, timeout))

    call_later: Any = _reactor().callLater
    timeout_call: Any = call_later(timeout, expire)

    def cancel_timeout(result: Any) -> Any:
        if timeout_call.active():
            timeout_call.cancel()
        return result

    bounded.addBoth(cancel_timeout)
    return operation_deferred, bounded


def bounded_deferred(
    source: Deferred[_T],
    *,
    timeout: float,
    operation: str,
) -> Deferred[_T]:
    """Apply a caller-visible timeout without cancelling ``source``."""
    bounded: Deferred[_T] = Deferred()
    timed_out = False
    settled = False

    def success(value: _T) -> _T:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            bounded.callback(value)
        return value

    def failure(failure_value: Any) -> Any:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            bounded.errback(failure_value)
        return failure_value

    source.addCallbacks(success, failure)

    def expire() -> None:
        nonlocal timed_out, settled
        if settled or bounded.called:
            return
        timed_out = True
        bounded.errback(BackendOperationTimeout(operation, timeout))

    call_later: Any = _reactor().callLater
    timeout_call: Any = call_later(timeout, expire)

    def cancel_timeout(result: Any) -> Any:
        if timeout_call.active():
            timeout_call.cancel()
        return result

    bounded.addBoth(cancel_timeout)
    return bounded


def defer_to_thread_bounded(
    function: Callable[..., _T],
    *args: Any,
    timeout: float,
    operation: str,
    **kwargs: Any,
) -> Deferred[_T]:
    """Run a synchronous call off-reactor and return its bounded Deferred."""
    operation_deferred, bounded = defer_to_thread_ordered(
        function,
        *args,
        timeout=timeout,
        operation=operation,
        **kwargs,
    )
    # ``bounded`` is the public result, but its authoritative worker can outlive
    # a timeout.  Observe that dropped operation independently; this must not be
    # attached to the public view or an in-time worker failure would be masked.
    operation_deferred.addErrback(lambda _failure: None)
    return bounded


__all__ = [
    "DEFAULT_REACTOR_IO_TIMEOUT_S",
    "MAX_REACTOR_IO_TIMEOUT_S",
    "defer_to_thread_bounded",
    "defer_to_thread_ordered",
    "reactor_is_running",
]
