"""Integration tests for the dedup strategy factory + from_settings wiring (subsystem ①)."""

from __future__ import annotations

import logging

import pytest
from scrapy.http import Request

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters import factory as factory_module
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.factory import (
  DedupeStrategy,
  build_membership_filter,
)
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter


def _make_settings(mocker, overrides=None):
  """Build a mock Scrapy settings object + patched ConnectionManager."""
  from scrapy_extension.backends.connectors import ConnectionManager

  mock_manager = mocker.Mock()
  mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)
  base = {"SCRAPY_BACKEND_TYPE": "redis", "SCRAPY_DUPEFILTER_KEY": "dupefilter"}
  if overrides:
    base.update(overrides)
  mock_settings = mocker.Mock()
  mock_settings.get.side_effect = lambda k, default=None: base.get(k, default)
  mock_settings.getbool.side_effect = lambda k, default=False: base.get(k, default)
  mock_settings.getdict.return_value = {}
  return mock_settings, mock_manager


class TestBuildMembershipFilter:
  """The factory maps each strategy to the right concrete filter."""

  def test_set_strategy(self, mock_connection_manager) -> None:
    flt = build_membership_filter(
      DedupeStrategy.SET, mock_connection_manager, key="k"
    )
    assert isinstance(flt, SetMembershipFilter)
    assert flt.key == "k"

  def test_memory_strategy(self, mock_connection_manager) -> None:
    flt = build_membership_filter(
      DedupeStrategy.MEMORY, mock_connection_manager, memory_maxsize=10
    )
    assert isinstance(flt, MemoryMembershipFilter)

  def test_bloom_strategy(self, mock_connection_manager) -> None:
    flt = build_membership_filter(
      DedupeStrategy.BLOOM,
      mock_connection_manager,
      bloom_capacity=100,
      bloom_error_rate=0.01,
    )
    assert isinstance(flt, BloomMembershipFilter)

  def test_cuckoo_strategy(self, mock_connection_manager) -> None:
    flt = build_membership_filter(
      DedupeStrategy.CUCKOO,
      mock_connection_manager,
      cuckoo_capacity=100,
      cuckoo_error_rate=0.01,
    )
    assert isinstance(flt, CuckooMembershipFilter)

  def test_every_strategy_returns_membership_filter(
    self, mock_connection_manager
  ) -> None:
    for strat in DedupeStrategy:
      assert isinstance(
        build_membership_filter(strat, mock_connection_manager), MembershipFilter
      )

  def test_invalid_strategy_string_raises(self) -> None:
    with pytest.raises(ValueError, match="not a valid DedupeStrategy"):
      DedupeStrategy("bogus")


class TestFromSettingsStrategyWiring:
  """from_settings selects the strategy from SCRAPY_DEDUP_STRATEGY."""

  def test_default_is_set(self, mocker) -> None:
    settings, _ = _make_settings(mocker)
    df = BackendDupeFilter.from_settings(settings)
    assert isinstance(df._filter, SetMembershipFilter)

  def test_memory_strategy(self, mocker) -> None:
    settings, _ = _make_settings(mocker, {"SCRAPY_DEDUP_STRATEGY": "memory"})
    df = BackendDupeFilter.from_settings(settings)
    assert isinstance(df._filter, MemoryMembershipFilter)

  def test_bloom_strategy(self, mocker) -> None:
    settings, _ = _make_settings(
      mocker,
      {
        "SCRAPY_DEDUP_STRATEGY": "bloom",
        "SCRAPY_DEDUP_BLOOM_CAPACITY": 100,
        "SCRAPY_DEDUP_BLOOM_ERROR_RATE": 0.01,
      },
    )
    df = BackendDupeFilter.from_settings(settings)
    assert isinstance(df._filter, BloomMembershipFilter)

  def test_cuckoo_strategy(self, mocker) -> None:
    settings, _ = _make_settings(
      mocker,
      {
        "SCRAPY_DEDUP_STRATEGY": "cuckoo",
        "SCRAPY_DEDUP_CUCKOO_CAPACITY": 100,
        "SCRAPY_DEDUP_CUCKOO_ERROR_RATE": 0.01,
      },
    )
    df = BackendDupeFilter.from_settings(settings)
    assert isinstance(df._filter, CuckooMembershipFilter)

  def test_invalid_strategy_raises(self, mocker) -> None:
    settings, _ = _make_settings(mocker, {"SCRAPY_DEDUP_STRATEGY": "bogus"})
    with pytest.raises(ValueError, match="not a valid DedupeStrategy"):
      BackendDupeFilter.from_settings(settings)

  def test_preserves_key_and_debug(self, mocker) -> None:
    settings, _ = _make_settings(
      mocker,
      {"SCRAPY_DUPEFILTER_KEY": "my:filter", "DUPEFILTER_DEBUG": True},
    )
    df = BackendDupeFilter.from_settings(settings)
    assert df.key == "my:filter"
    assert df.debug is True


class TestDupeFilterWithProbabilisticStrategy:
  """End-to-end: a bloom-strategy dupefilter dedups real Scrapy requests."""

  def test_new_then_duplicate_bloom(self, mock_connection_manager) -> None:
    flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager, membership_filter=flt
    )
    req = Request(url="https://example.com/page")
    assert df.request_seen(req) is False  # new
    assert df.request_seen(req) is True  # duplicate (no false negatives)

  def test_distinct_requests_not_dupes_bloom(
    self, mock_connection_manager
  ) -> None:
    flt = BloomMembershipFilter(capacity=100, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager, membership_filter=flt
    )
    assert df.request_seen(Request(url="https://example.com/a")) is False
    assert df.request_seen(Request(url="https://example.com/b")) is False

  def test_new_then_duplicate_cuckoo(self, mock_connection_manager) -> None:
    flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
    df = BackendDupeFilter(
      connection_manager=mock_connection_manager, membership_filter=flt
    )
    req = Request(url="https://example.com/page")
    assert df.request_seen(req) is False
    assert df.request_seen(req) is True


class TestPerProcessStrategyWarning:
  """D3 (Theme C): factory warns once when a per-process dedup strategy is selected.

  Bloom / cuckoo / memory filters are in-process — cross-worker duplicates pass
  silently. Operators assuming distributed dedup need a loud signal at selection
  time, not just in class docstrings. The warning is emitted exactly once per
  process per strategy (idempotent via a module-level ``_warned`` set).
  """

  @pytest.fixture(autouse=True)
  def _reset_factory_warning_cache(self):
    """Reset the module-level warning cache so each test re-triggers the check.

    ``_warned`` is process-global; without reset the second test in this
    class would observe zero warnings purely because the first already fired
    one — masking regressions. This is test isolation, not gaming: the
    production behavior we assert (warn-once per process) is verified by
    ``test_warning_emitted_only_once_per_process``.
    """
    factory_module._warned.clear()
    yield
    factory_module._warned.clear()

  def test_bloom_strategy_emits_exactly_one_warning(
    self, mock_connection_manager, caplog
  ) -> None:
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=factory_module.__name__):
      build_membership_filter(
        DedupeStrategy.BLOOM,
        mock_connection_manager,
        bloom_capacity=100,
        bloom_error_rate=0.01,
      )
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING and r.name == factory_module.__name__
    ]
    assert len(warnings) == 1, f"expected exactly one warning, got {len(warnings)}"
    msg = warnings[0].getMessage()
    # Must state the per-process scope and point at the distributed alternative.
    assert "per-process" in msg.lower() or "in-process" in msg.lower()
    assert "set" in msg.lower()

  def test_cuckoo_strategy_emits_warning(self, mock_connection_manager, caplog) -> None:
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=factory_module.__name__):
      build_membership_filter(
        DedupeStrategy.CUCKOO,
        mock_connection_manager,
        cuckoo_capacity=100,
        cuckoo_error_rate=0.01,
      )
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING and r.name == factory_module.__name__
    ]
    assert len(warnings) == 1

  def test_memory_strategy_emits_warning(self, mock_connection_manager, caplog) -> None:
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=factory_module.__name__):
      build_membership_filter(
        DedupeStrategy.MEMORY, mock_connection_manager, memory_maxsize=10
      )
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING and r.name == factory_module.__name__
    ]
    assert len(warnings) == 1

  def test_set_strategy_emits_no_warning(self, mock_connection_manager, caplog) -> None:
    """The default set strategy is cross-worker (backend-backed) → no warning."""
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=factory_module.__name__):
      build_membership_filter(DedupeStrategy.SET, mock_connection_manager, key="k")
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING and r.name == factory_module.__name__
    ]
    assert len(warnings) == 0

  def test_warning_emitted_only_once_per_process(
    self, mock_connection_manager, caplog
  ) -> None:
    """Building a per-process strategy twice in the same process warns once.

    Guards against log spam when many dupefilters are constructed (e.g. one
    per spider in a multi-spider process).
    """
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=factory_module.__name__):
      build_membership_filter(
        DedupeStrategy.BLOOM,
        mock_connection_manager,
        bloom_capacity=100,
        bloom_error_rate=0.01,
      )
      build_membership_filter(
        DedupeStrategy.BLOOM,
        mock_connection_manager,
        bloom_capacity=200,
        bloom_error_rate=0.02,
      )
    warnings = [
      r
      for r in caplog.records
      if r.levelno == logging.WARNING and r.name == factory_module.__name__
    ]
    assert len(warnings) == 1
