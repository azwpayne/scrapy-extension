"""Scrapy dupefilter lifecycle/declaration compatibility helpers.

Extracted from ``scheduler.py`` (pure move). This module must not log:
caplog/logger tests pin the ``scrapy_extension.schedule.scheduler``
logger name; if logging is ever needed here, reuse that historical name."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from inspect import getattr_static, signature
from typing import TYPE_CHECKING, Any

from scrapy.http import Request

if TYPE_CHECKING:
    from scrapy import Spider


_MISSING_STATIC_ATTRIBUTE = object()


def _call_dupefilter_open(dupefilter: object, spider: Spider) -> Any:
    """Call a dupefilter's Scrapy-2.17 lifecycle hook compatibly.

    Scrapy's stable ``BaseDupeFilter`` contract is ``open()``; the scheduler's
    own ``open(spider)`` argument must not be forwarded to generic filters such
    as ``RFPDupeFilter``.  ``BackendDupeFilter`` predates that contract in this
    package because it uses the spider to resolve ``{spider}`` keys, so retain
    its package-specific call.  Required-argument and variadic third-party test
    doubles remain supported for source compatibility.
    """
    from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter

    open_hook = getattr(dupefilter, "open")
    if isinstance(dupefilter, BackendDupeFilter):
        return open_hook(spider)
    try:
        parameters = tuple(signature(open_hook).parameters.values())
    except (TypeError, ValueError):
        # Some proxy objects (including older Scrapy extensions) do not expose
        # a signature. Their historical spider-aware form is the safest fallback.
        return open_hook(spider)
    if any(
        parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
        for parameter in parameters
    ) or any(
        parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        and parameter.default is parameter.empty
        for parameter in parameters
    ):
        return open_hook(spider)
    return open_hook()


def _static_declaration_rank(component: object, name: str) -> int | None:
    """Return how close a class-declared capability is in the concrete MRO.

    Per-instance attributes (including autospec-created ``MagicMock`` methods)
    and dynamic ``__getattr__`` values are not stable protocol declarations.
    """
    if (
        getattr_static(component, name, _MISSING_STATIC_ATTRIBUTE)
        is _MISSING_STATIC_ATTRIBUTE
    ):
        return None
    for index, cls in enumerate(type(component).__mro__):
        if name in vars(cls):
            return index
    return None


def _atomic_dupefilter_methods(
    dupefilter: object,
) -> (
    tuple[
        Callable[[Request, object], Any],
        Callable[[object], None],
        Callable[[object], None] | None,
        Callable[[object], None],
        Callable[[object], None],
    ]
    | None
):
    """Resolve an explicitly compatible transactional dupefilter extension."""
    try:
        instance_attributes = object.__getattribute__(dupefilter, "__dict__")
    except (AttributeError, TypeError):
        instance_attributes = {}
    protocol_names = (
        "request_seen_with_reservation",
        "commit_reservation",
        "rollback_reservation",
        "rollback_reservation_intent",
    )
    if isinstance(instance_attributes, Mapping) and any(
        name in instance_attributes for name in protocol_names
    ):
        # A coherent extension is a class-level protocol. Per-instance shadows
        # (including autospec mocks) can expose an arbitrary partial combination.
        return None
    if (
        isinstance(instance_attributes, Mapping)
        and "request_seen" in instance_attributes
    ):
        # Scrapy's stable hook is intentionally monkeypatchable per instance. An
        # inherited extension must not bypass that closer policy override.
        return None
    atomic_rank = _static_declaration_rank(
        dupefilter,
        "request_seen_with_reservation",
    )
    commit_rank = _static_declaration_rank(dupefilter, "commit_reservation")
    rollback_rank = _static_declaration_rank(dupefilter, "rollback_reservation")
    intent_rank = _static_declaration_rank(
        dupefilter,
        "rollback_reservation_intent",
    )
    if (
        atomic_rank is None
        or commit_rank is None
        or rollback_rank is None
        or intent_rank is None
    ):
        return None

    # Existing BackendDupeFilter subclasses may override only Scrapy's stable
    # request_seen() hook. An inherited newer extension must not bypass that
    # custom policy unless the subclass also declares the atomic method at least
    # as close in the MRO.
    standard_rank = _static_declaration_rank(dupefilter, "request_seen")
    if standard_rank is not None and standard_rank < atomic_rank:
        return None
    canonical_rank = _static_declaration_rank(
        dupefilter,
        "_atomic_protocol_request_seen",
    )
    if canonical_rank is not None and canonical_rank == standard_rank:
        canonical_standard = getattr_static(
            dupefilter,
            "_atomic_protocol_request_seen",
        )
        current_standard = getattr_static(dupefilter, "request_seen")
        if current_standard is not canonical_standard:
            return None

    atomic = getattr(dupefilter, "request_seen_with_reservation")
    commit = getattr(dupefilter, "commit_reservation")
    volatile_commit: Callable[[object], None] | None = None
    if _static_declaration_rank(dupefilter, "commit_volatile_reservation") is not None:
        candidate = getattr(dupefilter, "commit_volatile_reservation")
        if callable(candidate):
            volatile_commit = candidate
    rollback = getattr(dupefilter, "rollback_reservation")
    rollback_intent = getattr(dupefilter, "rollback_reservation_intent")
    if (
        not callable(atomic)
        or not callable(commit)
        or not callable(rollback)
        or not callable(rollback_intent)
    ):
        return None
    return atomic, commit, volatile_commit, rollback, rollback_intent
