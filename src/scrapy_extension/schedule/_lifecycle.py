"""Scheduler lifecycle helpers: deferred lifecycle results, attempt tokens,
and signal receiver leases.

Extracted from ``scheduler.py`` (pure move). This module must not log:
caplog/logger tests pin the ``scrapy_extension.schedule.scheduler``
logger name; if logging is ever needed here, reuse that historical name."""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from twisted.internet.defer import Deferred
from twisted.python.failure import Failure as TwistedFailure

if TYPE_CHECKING:
    from twisted.internet.defer import Deferred


@dataclass(frozen=True)
class _DeferredLifecycleResult:
    """Authoritative lifecycle completion plus its caller-facing timeout view."""

    operation: Deferred[Any]
    bounded: Deferred[Any]


_LifecycleContinuation = _DeferredLifecycleResult | Deferred[Any] | None


def _lifecycle_operation(result: _LifecycleContinuation) -> Deferred[Any] | None:
    """Return the authoritative Deferred hidden by one lifecycle result."""
    if isinstance(result, _DeferredLifecycleResult):
        if result.bounded is not result.operation:
            # The outer lifecycle owns the public timeout view. Consume the
            # nested view we flatten so a late nested failure cannot be reported
            # as an unhandled Deferred after the outer view has settled.
            result.bounded.addErrback(lambda _failure: None)
        return result.operation
    if isinstance(result, Deferred):
        return result
    return None


def _chain_lifecycle_result(
    source: Deferred[Any],
    continuation: Callable[[Any], _LifecycleContinuation],
    *,
    preserve_failure: Any | Callable[[], Any | None] | None = None,
) -> Deferred[Any]:
    """Flatten one lifecycle continuation without leaking its result wrapper.

    Twisted chains a Deferred returned from a callback, but it treats the
    package's ``(_operation, _bounded)`` pair as an ordinary value. Returning
    that value from a close callback therefore publishes completion too early.
    This helper also creates a separate destination Deferred: a close
    continuation may be attached to its own opening Deferred without forming a
    self-chain when that opening Deferred is also the lifecycle source.
    """
    chained: Deferred[Any] = Deferred()

    def settle_success(value: Any) -> Any:
        chained.callback(value)
        return value

    def settle_failure(failure: Any) -> None:
        chained.errback(failure)
        return None

    def flatten(value: Any) -> None:
        source_failure = value if isinstance(value, TwistedFailure) else None
        failure = preserve_failure() if callable(preserve_failure) else preserve_failure
        if failure is None:
            failure = source_failure
        try:
            result = continuation(value)
        except BaseException as exc:
            settle_failure(
                failure if failure is not None else TwistedFailure(exc)  # type: ignore[no-untyped-call]
            )
            return None
        operation = _lifecycle_operation(result)
        if operation is None:
            if failure is not None:
                settle_failure(failure)
            else:
                settle_success(result)
            return None

        def operation_success(result_value: Any) -> Any:
            if isinstance(result_value, TwistedFailure):
                settle_failure(failure if failure is not None else result_value)
            elif failure is not None:
                settle_failure(failure)
            else:
                settle_success(result_value)
            return result_value

        def operation_failure(operation_failure_value: Any) -> None:
            settle_failure(failure if failure is not None else operation_failure_value)
            # The destination observed the operation's failure; consume this
            # branch so a public timeout cannot leave a second unhandled Failure.
            return None

        operation.addCallbacks(operation_success, operation_failure)
        return None

    # Close teardown must also run when an opening stage fails (including a
    # cancellation requested by close); the continuation decides which failure,
    # if any, remains authoritative. Returning None consumes the source branch;
    # ``chained`` retains the public result independently.
    source.addBoth(flatten)
    return chained


class _SchedulerAttemptToken:
    """Frame-scoped lifecycle ownership reclaimable after its call unwinds."""

    __slots__ = ("pending", "thread_id")

    def __init__(self) -> None:
        self.thread_id = threading.get_ident()
        # A Deferred lifecycle callback outlives the call frame. Keep the close
        # reservation authoritative until that callback settles it; otherwise a
        # concurrent close could reclaim the same resources while the hook is
        # still running.
        self.pending = False

    @property
    def active(self) -> bool:
        """Whether the owning close attempt still holds this token."""
        if self.pending:
            return True
        try:
            frame = sys._current_frames().get(self.thread_id)  # noqa: SLF001
        except Exception:  # noqa: BLE001 - stale ownership must be reclaimable
            return False
        while frame is not None:
            try:
                if frame.f_locals.get("owner_token") is self:
                    return True
            except Exception:  # noqa: BLE001 - fail toward bounded reclamation
                return False
            frame = frame.f_back
        return False


class _SignalReceiver:
    """Invocation-unique, weak-referenceable signal receiver proxy."""

    __slots__ = ("__weakref__", "handler")

    def __init__(self, handler: Callable[..., Any]) -> None:
        self.handler = handler


class _ResponseSignalReceiver(_SignalReceiver):
    def __call__(self, response: Any, request: Any, spider: Any) -> Any:
        return self.handler(response, request, spider)


class _SpiderErrorSignalReceiver(_SignalReceiver):
    def __call__(self, failure: Any, response: Any, spider: Any) -> Any:
        return self.handler(failure, response, spider)


@dataclass(frozen=True, slots=True)
class _SignalLease:
    """One exact Scrapy/PyDispatcher registration owned by the scheduler."""

    manager: Any
    signal: Any
    receiver: _SignalReceiver
