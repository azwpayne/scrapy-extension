"""Unit E (folded): scheduler envelope + queue_key templating.

Round-2 hardening (C6 HIGH + C8 HIGH):

- **C6 / E1**: ``enqueue_request`` ran ``dupefilter.request_seen`` OUTSIDE
  its try/except — partial connectivity (queue up, dedup backend down)
  crashed the spider with an unhandled ``QueueError``. The fix moves the
  ``request_seen`` call INSIDE the try-block so a dedup-backend outage
  degrades to default-enqueue (no URL lost) + a ``scheduler/dupefilter_error``
  stat increment.

- **C8 / E2**: ``queue_key`` had no ``{spider}`` templating → two spiders
  on one Redis shared one queue (silent cross-spider request leakage).
  The fix substitutes ``spider.name`` into ``queue_key`` at ``open()`` when
  the template token is present. Default key unchanged.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from scrapy import Request, Spider

from scrapy_extension.exceptions import BackendError, QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler


def _stats_counter() -> tuple[dict[str, int], Any]:
  """Minimal stats-collector-like object so we can assert inc_value counts.

  Returns ``(counts_dict, stats_instance)`` — assert via the dict.
  """
  counts: dict[str, int] = {}

  class _Stats:
    def inc_value(self, key: str, count: int = 1, **_: Any) -> None:
      counts[key] = counts.get(key, 0) + count

    def get_value(self, key: str, default: int = 0) -> int:
      return counts.get(key, default)

  return counts, _Stats()


class _FakeSpider(Spider):
  name = "foo"

  def __init__(self) -> None:
    # Bypass Scrapy's Spider.__init__ (which needs a crawler context for
    # type-checking only). We set just what the scheduler reads.
    self.crawler = None  # type: ignore[assignment]


class TestEnqueueEnvelopeDedupeFailure:
  """E1: dupefilter.request_seen raising does NOT crash enqueue_request."""

  def test_request_seen_queue_error_does_not_raise_and_increments_stat(self) -> None:
    """C6: a QueueError from request_seen -> default-enqueue + stat, no raise."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.side_effect = QueueError("dedup backend down")

    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      dupefilter=dupefilter,
    )
    # Wire a real BackendQueue mock so push() succeeds.
    queue = MagicMock(name="BackendQueue")
    queue.push.return_value = None
    scheduler._queue = queue
    scheduler._spider = _FakeSpider()  # type: ignore[assignment]

    request = Request("https://example.com/test")

    # MUST NOT raise — degraded mode (default-enqueue, no URL lost).
    result = scheduler.enqueue_request(request)

    assert result is True  # enqueued, not dropped
    # push WAS called (degrade to enqueue, don't lose the URL).
    queue.push.assert_called_once()
    # Stat incremented so the outage is observable.
    assert counts.get("scheduler/dupefilter_error") == 1

  def test_request_seen_backend_error_also_handled(self) -> None:
    """E1: any BackendError (parent of QueueError) is also caught."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.side_effect = BackendError("dedup outage")

    scheduler = BackendScheduler(
      connection_manager=manager,
      stats=stats,
      dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.return_value = None
    scheduler._queue = queue

    request = Request("https://example.com/test")

    result = scheduler.enqueue_request(request)  # must not raise
    assert result is True
    assert counts.get("scheduler/dupefilter_error") == 1


class TestQueueKeySpiderTemplating:
  """E2: {spider} in queue_key is substituted with spider.name at open()."""

  def test_queue_key_with_spider_template_substituted_at_open(self) -> None:
    """C8: queue_key='q:{spider}' + spider.name='foo' -> resolved 'q:foo'."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    scheduler = BackendScheduler(
      connection_manager=manager,
      queue_key="q:{spider}",
    )
    spider = _FakeSpider()  # type: ignore[assignment]

    scheduler.open(spider)

    # The resolved queue_name on the BackendQueue carries spider.name.
    assert isinstance(scheduler._queue, BackendQueue)
    assert scheduler._queue.queue_name == "q:foo"

  def test_default_queue_key_unchanged(self) -> None:
    """Default 'scheduler:queue' (no template token) -> unchanged at open()."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    scheduler = BackendScheduler(connection_manager=manager)
    spider = _FakeSpider()  # type: ignore[assignment]

    scheduler.open(spider)

    assert isinstance(scheduler._queue, BackendQueue)
    assert scheduler._queue.queue_name == "scheduler:queue"

  def test_queue_key_attribute_unchanged_when_no_template(self) -> None:
    """Without {spider}, the public queue_key attr stays the literal string."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    scheduler = BackendScheduler(
      connection_manager=manager,
      queue_key="myqueue",
    )
    spider = _FakeSpider()  # type: ignore[assignment]

    scheduler.open(spider)
    # No template token -> public attr is the literal key.
    assert scheduler.queue_key == "myqueue"

  def test_queue_key_attribute_substituted_when_template(self) -> None:
    """With {spider}, the public queue_key attr reflects the substituted value."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    scheduler = BackendScheduler(
      connection_manager=manager,
      queue_key="q:{spider}",
    )
    spider = _FakeSpider()  # type: ignore[assignment]

    scheduler.open(spider)
    # Public attr reflects substituted key (post-open).
    assert scheduler.queue_key == "q:foo"


# Keep an explicit Any alias so type-checkers don't gripe about the mock spider.
_FakeSpiderType: Any = _FakeSpider
