"""Duplicate filter component for scrapy-extension.

This module provides a Scrapy dupefilter component using backend set interfaces.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from threading import Lock, RLock
from typing import TYPE_CHECKING, Protocol

from scrapy_extension.backends.base import _validate_key_name
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import DEFAULT_MEMORY_MAXSIZE
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.exceptions.base import BackendConnectionError, ConfigurationError
from scrapy_extension.monitor import NullMonitor, ScrapyStatsMonitor
from scrapy_extension.monitor.base import Monitor
from scrapy_extension.utils._config import (
  get_bool_setting,
  parse_float_setting,
  parse_int_setting,
)
from scrapy_extension.utils.request import request_fingerprint

if TYPE_CHECKING:
  from scrapy import Spider
  from scrapy.crawler import Crawler
  from scrapy.http import Request
  from scrapy.settings import Settings

  from scrapy_extension.backends.connectors import ConnectionManager

  class _Fingerprinter(Protocol):
    """Duck type for Scrapy's request fingerprinter.

    Mirrors ``scrapy.http.request.RequestFingerprinter`` (and any custom
    ``REQUEST_FINGERPRINTER_CLASS``) — the ``fingerprint(request) -> bytes``
    contract. Used so ``BackendDupeFilter`` can honor a configured custom
    fingerprinter instead of always defaulting to the module function.
    """

    def fingerprint(self, request: Request) -> bytes: ...

logger = logging.getLogger(__name__)

# Module-level warn-once flag for the cuckoo-filter-full degradation (Theme C,
# R7-A). Mirrors the factory.py:31 ``_warned`` pattern so a long-running crawl
# doesn't have its log spammed by per-request filter-full signals: the first
# time the cuckoo filter exhausts ``_MAX_KICKS`` we warn once per process,
# bump ``dupefilter/filter_full`` on every occurrence, and treat the overflow
# item as NOT-seen (allow enqueue). Tests reset this for isolation.
_filter_full_warned: bool = False

# Module-level warn-once flag for the transient-backend-error degradation (Risk 4).
# Mirrors ``_filter_full_warned``: a long-running crawl shouldn't have its log
# spammed by per-request transient-outage signals. The first time the SetBackend
# raises BackendConnectionError we warn once per process, bump ``errors/dedup``
# on every occurrence via the monitor, and treat the item as NOT-seen (allow
# enqueue — a duplicate fetch during a transient outage is strictly better than
# a crashed crawl). Tests reset this for isolation.
_backend_error_warned: bool = False

# Non-removable filters (notably Bloom) cannot compensate a successful add
# after the scheduler's later queue push fails. Keep a bounded, one-shot retry
# allowance per fingerprint instead. 1,024 limits failure-path memory while
# covering a useful transient queue-outage window; overflow evicts FIFO.
_DEFAULT_RETRY_ALLOWANCE_LIMIT = 1_024


class BackendDupeFilter:
  """Scrapy duplicate filter using a pluggable membership-filter strategy.

  Delegates duplicate detection to a
  :class:`~scrapy_extension.dupefilter.filters.base.MembershipFilter`. The
  default strategy is ``SetMembershipFilter`` (exact, cross-worker,
  byte-identical to the previous hardcoded ``SetBackend`` behavior); other
  strategies (memory, bloom, cuckoo) are selected via ``SCRAPY_DEDUP_STRATEGY``
  (wired in ``from_settings``).

  Attributes:
      connection_manager: The connection manager for backend access.
      key: The key for the fingerprints set / filter scope.
      debug: Whether to log filtered requests.
  """

  def __init__(
    self,
    connection_manager: ConnectionManager,
    key: str = "dupefilter",
    *,
    debug: bool = False,
    fingerprinter: _Fingerprinter | None = None,
    membership_filter: MembershipFilter | None = None,
    monitor: Monitor | None = None,
    clear_on_open: bool = False,
    owns_connection_manager: bool = True,
  ) -> None:
    """Initialize the dupefilter.

    Args:
        connection_manager: Connection manager for backend access.
        key: Key for the fingerprints set / filter scope. May contain the
            literal placeholder ``"{spider}"``; when present it is
            substituted with ``spider.name`` at :meth:`open` time so each
            spider gets its own dedup scope (C8 fix).
        debug: Whether to log filtered requests.
        fingerprinter: Optional Scrapy request fingerprinter. When provided
            (normally threaded from ``crawler.request_fingerprinter`` via
            ``from_crawler``), fingerprints respect a configured
            ``REQUEST_FINGERPRINTER_CLASS``. When ``None``, falls back to
            ``scrapy.utils.request.fingerprint`` — which is byte-identical to
            the default fingerprinter, so omitting this is fully backward-
            compatible (verified R45).
        membership_filter: Optional membership-filter strategy. When ``None``
            (default), a ``SetMembershipFilter`` is built from the connection
            manager and key — preserving the pre-strategy behavior exactly.
            Pass a custom filter (memory, bloom, cuckoo, ...) to override.
        monitor: Optional observability monitor. When ``None`` (default),
            :class:`~scrapy_extension.monitor.NullMonitor` (no-op). Wired to a
            :class:`~scrapy_extension.monitor.ScrapyStatsMonitor` in
            :meth:`from_crawler` when ``crawler.stats`` is available, so dedup
            hit/miss stats are default-on. Emitted hooks are additive.
        clear_on_open: When True, :meth:`open` clears any prior fingerprints
            before the run begins (C5 fix). Default False → zero compat break
            (re-running a spider sees the prior run's fingerprints, as before).
        owns_connection_manager: Whether :meth:`close` releases the supplied
            manager. Defaults to True for factory-created standalone
            dupefilters; composite owners can pass False and release their
            single shared acquire after all borrowed components are closed.
    """
    self.connection_manager = connection_manager
    self.key = key
    self.debug = debug
    self.clear_on_open = clear_on_open
    self._fingerprinter = fingerprinter
    self._monitor: Monitor = monitor if monitor is not None else NullMonitor()
    # Use ``is None`` (not ``or``): a MembershipFilter defines __len__, so an
    # empty filter (len == 0) would be falsy and ``or`` would wrongly discard
    # it. Only fall through when no filter was supplied at all.
    self._filter: MembershipFilter = (
      membership_filter
      if membership_filter is not None
      else SetMembershipFilter(connection_manager, key)
    )
    self._retry_allowances: OrderedDict[bytes, None] = OrderedDict()
    self._retry_allowance_lock = Lock()
    self._retry_allowance_limit = _DEFAULT_RETRY_ALLOWANCE_LIMIT
    self._retry_allowance_overflow_warned = False
    self._manager_released = False
    self._owns_connection_manager = owns_connection_manager
    self._lifecycle_lock = RLock()
    self._opened = False
    self._opened_spider: Spider | None = None
    self._closed = False
    # R14-D: thread the monitor into MemoryMembershipFilter so its LRU
    # eviction can emit ``on_filter_saturation`` (was log-warning only).
    # ``set_monitor`` exists only on the memory filter; guard via hasattr so
    # set/bloom/cuckoo filters are unaffected. The dupefilter owns the
    # monitor, so the filter emits through the same channel as cuckoo
    # saturation (which the dupefilter emits directly via getattr).
    self._set_filter_monitor()

  def _set_filter_monitor(self) -> None:
    """Thread the currently resolved monitor into filters that support it."""
    set_monitor = getattr(self._filter, "set_monitor", None)
    if callable(set_monitor):
      set_monitor(self._monitor)

  @classmethod
  def from_settings(cls, settings: Settings) -> BackendDupeFilter:
    """Create dupefilter from Scrapy settings.

    Backend selection: ``SCRAPY_SET_BACKEND_TYPE`` /
    ``SCRAPY_SET_BACKEND_SETTINGS`` override the global
    ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so the dedup set
    can bind to a different backend than the queue or storage pipeline
    (multi-backend coexistence). Unset → falls back to the global keys.

    Args:
        settings: Scrapy settings object.

    Returns:
        A new BackendDupeFilter instance.
    """
    from scrapy_extension.backends.connectors import (
      ConnectionManager,
      resolve_backend_config,
    )
    from scrapy_extension.dupefilter.filters.factory import (
      DedupeStrategy,
      build_membership_filter,
    )

    raw_strategy = settings.get(
      "SCRAPY_DEDUP_STRATEGY", DedupeStrategy.SET.value
    )
    try:
      strategy = DedupeStrategy(raw_strategy)
    except ValueError as e:
      valid = ", ".join(repr(m.value) for m in DedupeStrategy)
      raise ConfigurationError(
        f"Invalid SCRAPY_DEDUP_STRATEGY {raw_strategy!r}. Valid: {valid}.",
        setting_name="SCRAPY_DEDUP_STRATEGY",
        setting_value=str(raw_strategy),
      ) from e
    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_SET_BACKEND_TYPE",
      settings_key="SCRAPY_SET_BACKEND_SETTINGS",
      required_capabilities={"set"} if strategy is DedupeStrategy.SET else set(),
      component_name="set",
    )
    manager = ConnectionManager.get_manager(
      backend_type=backend_type,
      settings=backend_settings,
    )
    try:
      key = settings.get("SCRAPY_DUPEFILTER_KEY", "dupefilter")
      # getpriority() distinguishes an absent setting from an explicitly stored
      # None; Settings.get(name, default) intentionally treats both alike.
      memory_maxsize = (
        settings.get("SCRAPY_DEDUP_MEMORY_MAXSIZE")
        if settings.getpriority("SCRAPY_DEDUP_MEMORY_MAXSIZE") is not None
        else DEFAULT_MEMORY_MAXSIZE
      )
      if memory_maxsize is not None:
        memory_maxsize = parse_int_setting(
          memory_maxsize,
          "SCRAPY_DEDUP_MEMORY_MAXSIZE",
          minimum=1,
        )
      bloom_capacity = parse_int_setting(
        settings.get("SCRAPY_DEDUP_BLOOM_CAPACITY", 1_000_000),
        "SCRAPY_DEDUP_BLOOM_CAPACITY",
        minimum=1,
      )
      bloom_error_rate = parse_float_setting(
        settings.get("SCRAPY_DEDUP_BLOOM_ERROR_RATE", 0.001),
        "SCRAPY_DEDUP_BLOOM_ERROR_RATE",
        minimum=0.0,
        maximum=1.0,
        minimum_exclusive=True,
        maximum_exclusive=True,
      )
      cuckoo_capacity = parse_int_setting(
        settings.get("SCRAPY_DEDUP_CUCKOO_CAPACITY", 1_000_000),
        "SCRAPY_DEDUP_CUCKOO_CAPACITY",
        minimum=1,
      )
      cuckoo_error_rate = parse_float_setting(
        settings.get("SCRAPY_DEDUP_CUCKOO_ERROR_RATE", 0.001),
        "SCRAPY_DEDUP_CUCKOO_ERROR_RATE",
        minimum=0.0,
        maximum=1.0,
        minimum_exclusive=True,
        maximum_exclusive=True,
      )
      strict = get_bool_setting(
        settings,
        "SCRAPY_DEDUP_STRICT",
      )
      debug = get_bool_setting(
        settings,
        "DUPEFILTER_DEBUG",
      )
      clear_on_open = get_bool_setting(
        settings,
        "SCRAPY_DUPEFILTER_CLEAR_ON_OPEN",
      )
      if not isinstance(key, str):
        raise ConfigurationError(
          f"SCRAPY_DUPEFILTER_KEY must be a string, got {key!r}.",
          setting_name="SCRAPY_DUPEFILTER_KEY",
          setting_value=key,
        )
      try:
        _validate_key_name(
          key.replace("{spider}", "spider"),
          "SCRAPY_DUPEFILTER_KEY",
        )
      except ValueError as exc:
        raise ConfigurationError(
          str(exc),
          setting_name="SCRAPY_DUPEFILTER_KEY",
          setting_value=key,
        ) from exc
      try:
        membership_filter = build_membership_filter(
          strategy,
          manager,
          key=key,
          memory_maxsize=memory_maxsize,
          bloom_capacity=bloom_capacity,
          bloom_error_rate=bloom_error_rate,
          cuckoo_capacity=cuckoo_capacity,
          cuckoo_error_rate=cuckoo_error_rate,
          strict=strict,
        )
      except ConfigurationError:
        raise
      except (TypeError, ValueError, OverflowError) as exc:
        constructor_setting = {
          DedupeStrategy.MEMORY: "SCRAPY_DEDUP_MEMORY_MAXSIZE",
          DedupeStrategy.BLOOM: "SCRAPY_DEDUP_BLOOM_CAPACITY",
          DedupeStrategy.CUCKOO: "SCRAPY_DEDUP_CUCKOO_CAPACITY",
        }.get(strategy, "SCRAPY_DEDUP_STRATEGY")
        raise ConfigurationError(
          f"Invalid {constructor_setting}: {exc}",
          setting_name=constructor_setting,
          setting_value=settings.get(constructor_setting),
        ) from exc
      return cls(
        connection_manager=manager,
        key=key,
        debug=debug,
        membership_filter=membership_filter,
        clear_on_open=clear_on_open,
      )
    except BaseException:
      try:
        manager.close()
      except BaseException:
        logger.exception(
          "Failed to release ConnectionManager after dupefilter factory failure"
        )
      raise

  @classmethod
  def from_crawler(cls, crawler: Crawler) -> BackendDupeFilter:
    """Create dupefilter from crawler.

    Threads ``crawler.request_fingerprinter`` so the dupefilter honors a
    configured ``REQUEST_FINGERPRINTER_CLASS`` (otherwise fingerprints are
    byte-identical to the default — see ``__init__``).

    Args:
        crawler: The Scrapy crawler instance.

    Returns:
        A new BackendDupeFilter instance.
    """
    dupefilter = cls.from_settings(crawler.settings)
    try:
      dupefilter._fingerprinter = getattr(crawler, "request_fingerprinter", None)
      # Default-on observability: wire a ScrapyStatsMonitor when crawler.stats is
      # available so dedup hit/miss counts show up on the Scrapy stats dump
      # without an explicit ``monitor=`` kwarg. Additive — existing stats untouched.
      stats = getattr(crawler, "stats", None)
      if stats is not None:
        dupefilter._monitor = ScrapyStatsMonitor(stats)
        dupefilter._set_filter_monitor()
      return dupefilter
    except BaseException:
      try:
        dupefilter.close("crawler-factory-failed")
      except BaseException:
        logger.exception(
          "Failed to release ConnectionManager after dupefilter crawler factory failure"
        )
      raise

  def open(self, spider: Spider | None = None) -> None:
    """Open the dupefilter and its membership filter.

    Called by Scrapy's engine with no args (stock scheduler calls
    ``self.df.open()``); accepts an optional ``spider`` for explicit invocation
    (e.g. from a custom scheduler's ``open(spider)``). When a spider is
    provided, two additive behaviors activate (both default-off / no-op when
    no spider is passed or the relevant setting is unset):

    1. ``{spider}`` key templating (C8): if :attr:`key` contains the literal
       placeholder ``"{spider}"``, it is substituted with ``spider.name`` and
       the resolved key is propagated to the underlying membership filter
       (for backend-backed filters like :class:`SetMembershipFilter`) so each
       spider gets its own dedup scope. Keys without the placeholder are
       passed through unchanged.
    2. ``SCRAPY_DUPEFILTER_CLEAR_ON_OPEN`` (C5): when the dupefilter was
       constructed with ``clear_on_open=True``, any prior fingerprints are
       cleared before the run begins — so a re-run / resume crawl is not
       silently blocked by stale state.

    Args:
        spider: The spider opening the crawl (optional).
    """
    with self._lifecycle_lock:
      if self._closed:
        raise RuntimeError("dupefilter is closed")
      if self._opened:
        if spider is self._opened_spider:
          return
        raise RuntimeError("dupefilter is already open for a different spider")
      try:
        if spider is not None:
          _validate_key_name(spider.name, field_name="spider.name")
          self._resolve_spider_key(spider)
        self._clear_retry_allowances()
        self._filter.open()
        if self.clear_on_open:
          self.clear()
      except BaseException:
        try:
          self._close_locked()
        except BaseException:
          logger.exception("Failed to clean up dupefilter after open failure")
        raise
      self._opened = True
      self._opened_spider = spider

  def _resolve_spider_key(self, spider: Spider) -> None:
    """Substitute ``{spider}`` in :attr:`key` with ``spider.name``, propagating
    to the underlying membership filter.

    No-op when the key does not contain the placeholder. Only backend-backed
    filters (those exposing a mutable ``key`` attribute, e.g.
    :class:`SetMembershipFilter`) receive the propagated update; in-process
    filters (memory/bloom/cuckoo) ignore the key entirely, so the placeholder
    has no effect there — consistent with their per-process scope.
    """
    if "{spider}" not in self.key:
      return
    templated = self.key
    resolved = templated.replace("{spider}", spider.name)
    self.key = resolved
    # Propagate to backend-backed filters that expose a writable ``key``. The
    # filter was built from the same templated key, so only rewrite when its
    # key still equals the templated form — a caller who passed a custom
    # filter with a different key is not silently overwritten.
    if getattr(self._filter, "key", None) == templated:
      self._filter.key = resolved  # type: ignore[attr-defined]

  def close(self, reason: str) -> None:
    """Close the dupefilter and its membership filter.

    Args:
        reason: The reason for closing.
    """
    del reason
    with self._lifecycle_lock:
      self._close_locked()

  def _close_locked(self) -> None:
    """Release one dupefilter lifecycle while ``_lifecycle_lock`` is held."""
    if self._closed:
      return
    self._closed = True
    self._opened = False
    self._opened_spider = None
    self._clear_retry_allowances()
    filter_failed = False
    try:
      self._filter.close()
    except BaseException:
      filter_failed = True
      raise
    finally:
      if self._owns_connection_manager and not self._manager_released:
        self._manager_released = True
        try:
          self.connection_manager.close()
        except BaseException:
          if filter_failed:
            logger.exception(
              "ConnectionManager close failed while propagating filter close error"
            )
          else:
            raise

  def clear(self) -> None:
    """Clear all tracked fingerprints via the membership filter.

    Used by :meth:`open` when ``clear_on_open=True`` (C5 fix). Delegates to
    the underlying strategy's :meth:`MembershipFilter.clear`, so every
    concrete filter (set/memory/bloom/cuckoo) supports it.
    """
    with self._lifecycle_lock:
      if self._closed:
        raise RuntimeError("dupefilter is closed")
      self._clear_retry_allowances()
      self._filter.clear()

  def log(self, request: Request, spider: Spider) -> None:
    """Log a filtered request.

    Args:
        request: The filtered request.
        spider: The spider instance.
    """
    if self.debug:
      logger.debug(
        "Filtered duplicate request: %s",
        request.url,
        extra={"spider": spider},
      )

  def request_seen(self, request: Request) -> bool:
    """Check whether a request was seen while the filter is active."""
    with self._lifecycle_lock:
      if self._closed:
        raise RuntimeError("dupefilter is closed")
      return self._request_seen_unlocked(request)

  def _request_seen_unlocked(self, request: Request) -> bool:
    """Check if a request has been seen before.

    Args:
        request: The request to check.

    Returns:
        True if the request is a duplicate, False otherwise.
    """
    fingerprint = self.request_fingerprint(request)
    encoded_fingerprint = fingerprint.encode()

    # Non-removable filters retain their original bit/fingerprint after a
    # failed queue push. ``forget`` grants exactly one retry miss; deletion
    # under the shared lock is the linearization point, so concurrent callers
    # cannot consume the same allowance twice. The underlying retained marker
    # makes every other caller a duplicate before and after that one retry.
    if self._consume_retry_allowance(encoded_fingerprint):
      self._monitor.on_dedup_miss(fingerprint)
      return False

    try:
      added = self._filter.add(encoded_fingerprint)
    except NotImplementedError as exc:
      raise RuntimeError(
        "Configured backend does not support set/duplicate filtering; "
        "use a backend with SetBackend or disable BackendDupeFilter."
      ) from exc
    except FilterFull:
      # Membership-filter-full graceful degradation (Theme C, R7-A).
      #
      # ``CuckooMembershipFilter.add`` raises ``FilterFull`` once it exhausts
      # ``_MAX_KICKS`` (filter past capacity) — a correct low-level signal.
      # For a crawler, a dead spider is worse than a duplicate fetch: Scrapy
      # and the downstream pipeline handle occasional duplicates, but a crashed
      # long-running crawl loses all in-flight progress. So at this layer we
      # degrade gracefully: warn once per process (module-level
      # ``_filter_full_warned``, mirrors factory.py:31 ``_warned``), emit
      # ``monitor.on_filter_full()`` so a wired stats collector increments
      # ``dupefilter/filter_full`` via the monitor contract (no private-attr
      # reach), and treat the overflow item as NOT-seen (allow enqueue). Dedup
      # stays effective within capacity; overflow items may re-fetch — strictly
      # better than crashing. This arm is intentionally separate from the
      # ``NotImplementedError`` arm above (different meaning: unsupported vs.
      # full). ``FilterFull`` is caught by TYPE (not by string-matching the
      # message), so the cuckoo layer is free to reword its message without
      # silently disabling this guard.
      self._handle_filter_full(fingerprint)
      return False
    except BackendConnectionError as exc:
      # Transient-backend-error graceful degradation (Risk 4).
      #
      # A transient Redis/MongoDB/ES outage during dedup raises
      # BackendConnectionError from the SetBackend. Left uncaught it propagates
      # to the Scrapy engine and crashes the crawl — contradicting the
      # codebase's documented "a dead spider is worse than a duplicate fetch"
      # philosophy (applied for FilterFull above and in the BLE001-guarded
      # monitor hooks). Mirror the FilterFull arm: warn once per process, emit
      # ``monitor.on_error("dedup", exc)`` so a wired collector increments
      # ``errors/dedup``, and degrade to not-seen (allow the request through).
      # The tradeoff is possible duplicate fetches during the outage window —
      # strictly better than crawl death. Distinct from the NotImplementedError
      # arm (unsupported backend, still raises RuntimeError) and the FilterFull
      # arm (filter at capacity).
      self._handle_backend_error(fingerprint, exc)
      return False

    # add() returns True when the item was newly added; a duplicate maps to False.
    seen = not added
    if seen:
      self._monitor.on_dedup_hit(fingerprint)
    else:
      self._monitor.on_dedup_miss(fingerprint)
    # U2 operability: if the filter exposes saturation (cuckoo + bloom as of
    # R14-D), emit the leading fill-ratio signal after each add. Cheap (one
    # property read + one monitor hook), and lets operators see the filter
    # APPROACHING full (e.g. >0.9) before the FilterFull overflow path fires.
    # Set/memory filters don't expose ``saturation`` here — memory instead
    # emits ``on_filter_saturation`` directly at LRU-eviction time (R14-D),
    # threaded via its own monitor ref. Filters with no ``saturation``
    # property stay silent on this path; their gauge stays at ``None``
    # (untouched), not misleadingly at 0.0.
    sat = getattr(self._filter, "saturation", None)
    if sat is not None:
      cap = getattr(self._filter, "capacity", None)
      self._monitor.on_filter_saturation(len(self._filter), cap)
    return seen

  def forget(self, request: Request) -> None:
    """Compensate a new fingerprint whose subsequent queue push failed.

    Filters with atomic deletion remove the reservation immediately. Filters
    such as Bloom that raise ``NotImplementedError`` retain their marker and
    receive one bounded retry allowance instead. The next matching
    :meth:`request_seen` atomically consumes that allowance and returns a miss;
    a successful queue push consumes no further state, while another push
    failure calls ``forget`` again and re-arms one allowance.

    Allowances are unique per fingerprint and capped at 1,024 entries. At the
    cap, the oldest allowance is evicted. Insertion and consumption share one
    lock, giving concurrent callers a single linearization order and ensuring
    no allowance can admit two queue pushes.

    Args:
        request: The request whose newly-added fingerprint must be compensated.
    """
    with self._lifecycle_lock:
      if self._closed:
        raise RuntimeError("dupefilter is closed")
      fingerprint = self.request_fingerprint(request).encode()
      try:
        self._filter.remove(fingerprint)
      except NotImplementedError:
        self._grant_retry_allowance(fingerprint)

  def _grant_retry_allowance(self, fingerprint: bytes) -> None:
    """Insert or refresh one bounded retry allowance for ``fingerprint``."""
    warn_overflow = False
    with self._retry_allowance_lock:
      if fingerprint in self._retry_allowances:
        self._retry_allowances.move_to_end(fingerprint)
        return
      if len(self._retry_allowances) >= self._retry_allowance_limit:
        self._retry_allowances.popitem(last=False)
        if not self._retry_allowance_overflow_warned:
          self._retry_allowance_overflow_warned = True
          warn_overflow = True
      self._retry_allowances[fingerprint] = None
    if warn_overflow:
      logger.warning(
        "Dedup retry allowances reached the %d-entry bound; evicting the "
        "oldest failed-push allowance",
        self._retry_allowance_limit,
      )

  def _consume_retry_allowance(self, fingerprint: bytes) -> bool:
    """Atomically consume at most one allowance for ``fingerprint``."""
    with self._retry_allowance_lock:
      if fingerprint not in self._retry_allowances:
        return False
      del self._retry_allowances[fingerprint]
      return True

  def _clear_retry_allowances(self) -> None:
    """Discard transient failed-push allowances at lifecycle boundaries."""
    with self._retry_allowance_lock:
      self._retry_allowances.clear()

  def _handle_filter_full(self, fingerprint: str) -> None:
    """Degrade gracefully when the membership filter reports it is full.

    Warn once per process (module-level ``_filter_full_warned``), emit
    ``monitor.on_filter_full()`` so a wired stats collector increments
    ``dupefilter/filter_full``, and emit a dedup-miss hook so observability
    stays consistent with the not-seen outcome the caller returns.

    Args:
        fingerprint: The request fingerprint that triggered the overflow.
    """
    global _filter_full_warned
    if not _filter_full_warned:
      _filter_full_warned = True
      logger.warning(
        "Dedup membership filter is full (filter_full); degrading — overflow "
        "requests will be treated as not-seen and may re-fetch. Increase the "
        "filter capacity or switch to an exact dedup strategy "
        "(SCRAPY_DEDUP_STRATEGY=set). This filter_full warning fires once per "
        "process; subsequent overflows are counted via the "
        "dupefilter/filter_full stat only."
      )
    # Count the degradation via the monitor contract — ScrapyStatsMonitor
    # increments ``dupefilter/filter_full``; NullMonitor is a no-op. Replaces
    # an earlier reach into ``self._monitor._stats`` (private attribute).
    self._monitor.on_filter_full()
    # Keep the monitor's dedup-miss accounting consistent with the not-seen
    # outcome the caller returns for the overflow item.
    self._monitor.on_dedup_miss(fingerprint)

  def _handle_backend_error(self, fingerprint: str, exc: BaseException) -> None:
    """Degrade gracefully when the membership-filter backend is transiently down.

    Risk 4: a transient :class:`BackendConnectionError` from the SetBackend
    (Redis/MongoDB/ES outage during dedup) must not crash the crawl. Mirror
    :meth:`_handle_filter_full`: warn once per process (module-level
    ``_backend_error_warned``), emit ``monitor.on_error("dedup", exc)`` so a
    wired stats collector increments ``errors/dedup``, and emit a dedup-miss
    hook so observability stays consistent with the not-seen outcome the
    caller returns. The tradeoff is possible duplicate fetches until the
    backend recovers — strictly better than crawl death.

    Args:
        fingerprint: The request fingerprint being checked when the error fired.
        exc: The transient ``BackendConnectionError`` from the SetBackend.
    """
    global _backend_error_warned
    if not _backend_error_warned:
      _backend_error_warned = True
      logger.warning(
        "Dedup backend transiently unavailable (%s); degrading — requests "
        "will be treated as not-seen and may re-fetch until the backend "
        "recovers. This warning fires once per process; subsequent "
        "transient backend errors are counted via the errors/dedup stat only.",
        type(exc).__name__,
      )
    self._monitor.on_error("dedup", exc)
    self._monitor.on_dedup_miss(fingerprint)

  def request_fingerprint(self, request: Request) -> str:
    """Generate a fingerprint for a request.

    Uses the configured Scrapy fingerprinter (``crawler.request_fingerprinter``)
    when one was provided via ``from_crawler``; otherwise falls back to
    ``scrapy.utils.request.fingerprint``. The two are byte-identical for the
    default fingerprinter (verified R45), so this only diverges when the
    operator has set a custom ``REQUEST_FINGERPRINTER_CLASS`` — exactly the
    case that should diverge.

    Args:
        request: The request to fingerprint.

    Returns:
        A unique fingerprint string (hex).
    """
    if self._fingerprinter is not None:
      return self._fingerprinter.fingerprint(request).hex()
    return request_fingerprint(request)
