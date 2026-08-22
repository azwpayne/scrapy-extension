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


def _safe_errback(deferred: Deferred[Any], error: Any) -> None:
    """Settle an internal public view without letting user callbacks escape."""
    if deferred.called:
        return
    try:
        deferred.errback(error)
    except BaseException:
        # A caller callback is not lifecycle authority. It may reject the
        # notification (or a test double may deliberately raise); the Deferred
        # is nevertheless called and the accepted worker remains independent.
        pass


def _safe_callback(deferred: Deferred[Any], value: Any) -> None:
    """Notify a public view while isolating callback failures from its worker."""
    if deferred.called:
        return
    try:
        deferred.callback(value)
    except BaseException:
        pass


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
    # Allocate both views before submitting work.  The thread adapter and the
    # reactor timer are provider boundaries and may reject submission
    # synchronously (for example while the reactor is shutting down).  A failed
    # submission must still leave callers with settled Deferreds rather than an
    # opening/closing attempt that owns resources forever.
    operation_deferred: Deferred[_T] = Deferred()
    bounded: Deferred[_T] = Deferred()
    timed_out = False
    settled = False

    def finish_success(value: _T) -> _T:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            _safe_callback(bounded, value)
        return value

    def finish_failure(failure: Any) -> Any:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            _safe_errback(bounded, failure)
        # Keep the operation's original failure available to an ordering chain.
        return failure

    try:
        submitted = deferToThread(function, *args, **kwargs)
    except BaseException as exc:
        operation_deferred._reactor_submission_failure = "thread"  # type: ignore[attr-defined]
        _safe_errback(operation_deferred, exc)
        # Use the exception independently for the public branch. Deferred
        # callbacks are allowed to mutate/consume a Failure and the two
        # ownership views must remain independently settled.
        _safe_errback(bounded, exc)
        return operation_deferred, bounded

    # The real worker Deferred is authoritative.  It is deliberately not
    # replaced with a wrapper: a running Python thread cannot be cancelled, so
    # ownership must remain tied to the worker's actual completion.
    operation_deferred = submitted
    try:
        operation_deferred.addCallbacks(finish_success, finish_failure)
    except BaseException as exc:
        # The worker was accepted even though this adapter could not attach its
        # mirror callbacks. Fail the caller-facing view now, but return the
        # worker Deferred unchanged so lifecycle owners can still fence its real
        # completion and never release its manager early.
        operation_deferred._reactor_callback_failure = True  # type: ignore[attr-defined]
        timed_out = True
        _safe_errback(bounded, exc)
        return operation_deferred, bounded

    timeout_call: Any | None = None

    def expire() -> None:
        nonlocal timed_out, settled
        if settled or bounded.called:
            return
        timed_out = True
        _safe_errback(bounded, BackendOperationTimeout(operation, timeout))

    try:
        call_later: Any = _reactor().callLater
        timeout_call = call_later(timeout, expire)
    except BaseException as exc:
        operation_deferred._reactor_submission_failure = "timer"  # type: ignore[attr-defined]
        # The worker was accepted, so do not pretend it stopped: its Deferred
        # remains authoritative and all lifecycle owners continue to wait for it.
        # Only the caller-visible adapter setup failed synchronously.
        timed_out = True
        _safe_errback(bounded, exc)
        return operation_deferred, bounded

    def cancel_timeout(result: Any) -> Any:
        if timeout_call is not None:
            try:
                if timeout_call.active():
                    timeout_call.cancel()
            except BaseException:
                # Timer cleanup is advisory after the lifecycle result settled.
                pass
        return result

    try:
        bounded.addBoth(cancel_timeout)
    except BaseException as exc:
        # Callback installation is another provider boundary. The timer and
        # worker remain authoritative; settle the public view instead of
        # returning a Deferred that can wait forever.
        timed_out = True
        _safe_errback(bounded, exc)
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
            _safe_callback(bounded, value)
        return value

    def failure(failure_value: Any) -> Any:
        nonlocal settled
        settled = True
        if not timed_out and not bounded.called:
            _safe_errback(bounded, failure_value)
        return failure_value

    try:
        source.addCallbacks(success, failure)
    except BaseException as exc:
        # ``source`` is owned by the caller and may already be executing. A
        # callback-attachment failure must not cancel or replace it, but the
        # bounded public view must still settle deterministically.
        timed_out = True
        _safe_errback(bounded, exc)
        return bounded

    timeout_call: Any | None = None

    def expire() -> None:
        nonlocal timed_out, settled
        if settled or bounded.called:
            return
        timed_out = True
        _safe_errback(bounded, BackendOperationTimeout(operation, timeout))

    try:
        call_later: Any = _reactor().callLater
        timeout_call = call_later(timeout, expire)
    except BaseException as exc:
        # ``source`` remains owned by its caller.  The bounded view reports the
        # adapter submission failure immediately without cancelling a source that
        # may already be executing.
        timed_out = True
        _safe_errback(bounded, exc)
        return bounded

    def cancel_timeout(result: Any) -> Any:
        if timeout_call is not None:
            try:
                if timeout_call.active():
                    timeout_call.cancel()
            except BaseException:
                pass
        return result

    try:
        bounded.addBoth(cancel_timeout)
    except BaseException as exc:
        timed_out = True
        _safe_errback(bounded, exc)
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
    try:
        operation_deferred.addErrback(lambda _failure: None)
    except BaseException:
        # The public view is already independent of this observer.  A broken
        # observer must not turn a settled caller result into a synchronous
        # lifecycle failure or cancel the accepted worker.
        pass
    return bounded


__all__ = [
    "DEFAULT_REACTOR_IO_TIMEOUT_S",
    "MAX_REACTOR_IO_TIMEOUT_S",
    "defer_to_thread_bounded",
    "defer_to_thread_ordered",
    "reactor_is_running",
]
