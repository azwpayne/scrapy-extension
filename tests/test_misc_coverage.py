"""Misc coverage gaps: monitor module, cuckoo alt-index remove, throttle len."""

from __future__ import annotations

from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.queue.strategies.throttle import ThrottleQueueStrategy


def test_monitor_module_importable() -> None:
  """Cover the empty monitor namespace module (TYPE_CHECKING stub)."""
  import scrapy_extension.monitor

  assert scrapy_extension.monitor is not None


def test_cuckoo_remove_via_alt_index() -> None:
  """Exercise the alternate-bucket (i2) branch of CuckooMembershipFilter.remove.

  A fingerprint normally lives in i1; after a kick it can land in i2. We place
  one in i2 directly to hit the i2 branch deterministically.
  """
  flt = CuckooMembershipFilter(capacity=100, error_rate=0.01)
  item = b"alt-index-item"
  fp, i1 = flt._fingerprint(item)
  i2 = flt._alt_index(i1, fp)
  flt._buckets[i2].append(fp)
  flt._count += 1
  assert item in flt
  assert flt.remove(item) is True  # i2 branch
  assert item not in flt


def test_throttle_queue_len(mock_connection_manager) -> None:
  """Cover ThrottleQueueStrategy.queue_len delegation."""
  strat = ThrottleQueueStrategy(mock_connection_manager)
  mock_connection_manager.get_queue_backend().queue_len.return_value = 5
  assert strat.queue_len("q") == 5
