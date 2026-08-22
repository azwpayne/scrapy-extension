"""Generation-scoped admission and draining for opaque backend clients.

A connection generation is an ownership boundary, not just an integer.  The
record captured while admission is locked is retained by every admitted
operation until its SDK call has returned.  Retirement first stops admission,
then waits for those exact records to drain; only the detached record may be
closed afterwards.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Condition, get_ident
from typing import Generic, TypeVar

_T = TypeVar("_T")


class GenerationUnavailable(RuntimeError):
    """The current generation cannot admit a new operation."""

    def __init__(self, operation: str, queue_name: str | None = None) -> None:
        self.operation = operation
        self.queue_name = queue_name
        super().__init__(operation)


@dataclass(slots=True, eq=False)
class GenerationRecord(Generic[_T]):
    """One atomically published generation and its immutable operation value."""

    generation: int
    key: object
    value: _T
    accepting: bool = True
    active_leases: int = 0
    active_threads: dict[int, int] | None = None
    deferred_finalizers: list[Callable[[], None]] = field(default_factory=list)
    finalizer_registered: bool = False
    finalization_errors: list[BaseException] = field(default_factory=list)


class GenerationLeaseGate(Generic[_T]):
    """Publish, lease, retire, and drain one backend-client generation.

    The gate deliberately knows nothing about a driver or an error policy.  It
    never closes ``value`` and never times out an authoritative lease.  A caller
    may stop waiting for its own view, but the lease remains active until the
    operation's ``finally`` block releases it.
    """

    def __init__(self) -> None:
        self.condition: Condition = Condition()
        self._next_generation = 0
        self._current: GenerationRecord[_T] | None = None

    @property
    def current(self) -> GenerationRecord[_T] | None:
        """Return the current record for diagnostics (not admission)."""
        with self.condition:
            return self._current

    def publish(self, value: _T) -> GenerationRecord[_T]:
        """Atomically publish a fresh accepting generation."""
        with self.condition:
            if self._current is not None and self._current.accepting:
                return self._current
            self._next_generation += 1
            record = GenerationRecord(
                generation=self._next_generation,
                key=object(),
                value=value,
            )
            self._current = record
            self.condition.notify_all()
            return record

    def retire(self) -> GenerationRecord[_T] | None:
        """Stop admission and detach the current record at one linearization point."""
        with self.condition:
            record = self._current
            if record is not None:
                record.accepting = False
                self._current = None
                self.condition.notify_all()
            return record

    def _run_finalizer(
        self, record: GenerationRecord[_T], finalizer: Callable[[], None]
    ) -> None:
        """Run deferred retirement cleanup without masking the SDK operation."""
        try:
            finalizer()
        except BaseException as error:
            with self.condition:
                record.finalization_errors.append(error)

    @contextmanager
    def lease(
        self,
        operation: str,
        *,
        queue_name: str | None = None,
    ) -> Iterator[GenerationRecord[_T]]:
        """Admit one operation and hold its generation until it completes."""
        with self.condition:
            record = self._current
            if record is None or not record.accepting:
                raise GenerationUnavailable(operation, queue_name)
            record.active_leases += 1
            if record.active_threads is None:
                record.active_threads = {}
            thread_id = get_ident()
            record.active_threads[thread_id] = (
                record.active_threads.get(thread_id, 0) + 1
            )
        try:
            yield record
        finally:
            with self.condition:
                # A lease owns exactly one increment.  Defensive underflow
                # protection keeps a faulty caller from waking drain early.
                if record.active_leases > 0:
                    record.active_leases -= 1
                thread_id = get_ident()
                if record.active_threads is not None:
                    owned = record.active_threads.get(thread_id, 0)
                    if owned <= 1:
                        record.active_threads.pop(thread_id, None)
                    else:
                        record.active_threads[thread_id] = owned - 1
                finalizers: list[Callable[[], None]] = []
                if record.active_leases == 0:
                    finalizers = record.deferred_finalizers
                    record.deferred_finalizers = []
                    self.condition.notify_all()
            # A reentrant disconnect cannot wait for this lease. Its finalizer is
            # therefore run only after the SDK call has returned and this lease has
            # released, never from inside the SDK call itself.
            for finalizer in finalizers:
                self._run_finalizer(record, finalizer)

    def drain(
        self,
        record: GenerationRecord[_T] | None,
        finalizer: Callable[[], None] | None = None,
    ) -> BaseException | None:
        """Wait for one retired record, preserving a control exception.

        ``KeyboardInterrupt``/``SystemExit`` do not release an operation lease.
        They are remembered, the authoritative drain still completes, and the
        caller can re-raise the exact signal after detached handles are closed.
        """
        if record is None:
            if finalizer is not None:
                finalizer()
            return None
        control_error: BaseException | None = None
        run_now = False
        with self.condition:
            # A driver callback can synchronously call disconnect() from the same
            # thread that owns the SDK lease. Waiting for that lease would deadlock,
            # but closing here would invalidate the handle mid-callback. Register
            # cleanup on the record instead; the last lease release runs it after
            # the outer SDK operation has returned. Other owners remain authoritative
            # and are always drained before the handle is closed.
            owned_by_caller = (
                0
                if record.active_threads is None
                else record.active_threads.get(get_ident(), 0)
            )
            while record.active_leases > owned_by_caller:
                try:
                    self.condition.wait()
                except BaseException as error:
                    if control_error is None:
                        control_error = error
            if finalizer is not None and not record.finalizer_registered:
                # A retired generation has one ownership finalizer.  Repeated
                # teardown callers may present the same record, but must never
                # close its opaque handles twice.
                record.finalizer_registered = True
                if record.active_leases == 0:
                    run_now = True
                else:
                    record.deferred_finalizers.append(finalizer)
        if run_now and finalizer is not None:
            # Capture cleanup failures so a synchronous disconnect can preserve its
            # established cleanup contract, while a reentrant disconnect never
            # injects a late close failure into the SDK callback.
            self._run_finalizer(record, finalizer)
        if control_error is not None:
            # A signal raised by Condition.wait otherwise retains this frame in its
            # traceback, including ``record`` and the retired SDK handle graph. The
            # caller still receives the exact same control object, but its private
            # wait/lease state is no longer reachable through the propagated signal.
            control_error.__traceback__ = None
            control_error.__cause__ = None
            control_error.__context__ = None
            control_error.__suppress_context__ = True
        return control_error


__all__ = [
    "GenerationLeaseGate",
    "GenerationRecord",
    "GenerationUnavailable",
]
