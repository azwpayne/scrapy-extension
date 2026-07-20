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

import pytest
from scrapy import Request, Spider

from scrapy_extension.exceptions import BackendError, QueueError, SerializationError
from scrapy_extension.queue.queue import BACKEND_ACK_TOKEN_META_KEY, BackendQueue
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

  def test_default_queue_key_is_backend_neutral(self) -> None:
    """The default key must also be valid as an MQ topic/queue name."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    scheduler = BackendScheduler(connection_manager=manager)
    spider = _FakeSpider()  # type: ignore[assignment]

    scheduler.open(spider)

    assert isinstance(scheduler._queue, BackendQueue)
    assert scheduler._queue.queue_name == "scheduler-queue"

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

  def test_template_scheduler_rejects_reopen_after_close(self) -> None:
    """Template resolution cannot revive a scheduler with a retired manager."""
    manager = MagicMock(name="ConnectionManager")
    manager.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    scheduler = BackendScheduler(
      connection_manager=manager,
      queue_key="q:{spider}",
    )
    first_spider = MagicMock(name="FirstSpider")
    first_spider.name = "spider_one"
    first_spider.crawler = None
    second_spider = MagicMock(name="SecondSpider")
    second_spider.name = "spider_two"
    second_spider.crawler = None

    scheduler.open(first_spider)
    assert scheduler._queue is not None
    assert scheduler._queue.queue_name == "q:spider_one"
    scheduler.close("first-finished")

    with pytest.raises(RuntimeError, match="closed"):
      scheduler.open(second_spider)

    assert scheduler._queue is None
    assert scheduler.queue_key == "q:spider_one"


# Keep an explicit Any alias so type-checkers don't gripe about the mock spider.
_FakeSpiderType: Any = _FakeSpider


class TestEnqueueBranchClosure:
  """G1-G7: close the uncovered enqueue_request resilience branches.

  Characterization tests — every branch is correct on static read; these pin
  the behavior so a future refactor can't silently drop a degrade-path.
  See docs/superpowers/specs/2026-07-02-scheduler-branch-closure-design.md.
  """

  def test_G1_dedup_hit_with_no_spider_skips_log_returns_false(self) -> None:
    """G1: request_seen=True + _spider=None → skip dupefilter.log, return False.

    Covers the ``_spider is None`` arm of the dedup-hit block (616->618) — the
    existing envelope tests always set ``_spider``, so the skip-log branch
    was unreached.
    """
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.return_value = True
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue  # _spider intentionally left None (never opened)

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False
    queue.push.assert_not_called()  # dedup-hit short-circuits before push
    dupefilter.log.assert_not_called()  # _spider None → log skipped
    assert not counts, f"dedup-hit should bump no stats, got {counts}"

  def test_duplicate_replacement_acks_its_original_delivery(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.return_value = True
    scheduler = BackendScheduler(
      connection_manager=manager, dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    request = Request(
      "https://example.com/already-seen",
      meta={BACKEND_ACK_TOKEN_META_KEY: "old-token"},
    )

    assert scheduler.enqueue_request(request) is False

    queue.ack.assert_called_once_with(token="old-token")
    queue.push.assert_not_called()
    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta

  def test_final_download_failure_nacks_before_calling_original_errback(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(connection_manager=manager)
    queue = MagicMock(name="BackendQueue")
    original_errback = MagicMock(
      name="original_errback", side_effect=RuntimeError("errback failed")
    )
    queued_request = Request(
      "https://example.com/download-failure",
      errback=original_errback,
      meta={BACKEND_ACK_TOKEN_META_KEY: "delivery-token"},
    )
    queue.pop.return_value = queued_request
    scheduler._queue = queue

    request = scheduler.next_request()
    assert request is queued_request
    assert request.errback is not original_errback
    failure = MagicMock(name="Failure")
    failure.request = request

    assert request.errback is not None
    with pytest.raises(RuntimeError, match="errback failed"):
      request.errback(failure)

    queue.nack.assert_called_once_with(token="delivery-token")
    original_errback.assert_called_once_with(failure)
    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta

  def test_handled_download_failure_acks_after_original_errback(self) -> None:
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(connection_manager=manager)
    queue = MagicMock(name="BackendQueue")
    original_errback = MagicMock(name="original_errback", return_value=None)
    queued_request = Request(
      "https://example.com/download-failure",
      errback=original_errback,
      meta={BACKEND_ACK_TOKEN_META_KEY: "delivery-token"},
    )
    queue.pop.return_value = queued_request
    scheduler._queue = queue
    request = scheduler.next_request()
    assert request is not None and request.errback is not None
    failure = MagicMock(name="Failure")
    failure.request = request

    assert request.errback(failure) is None

    queue.ack.assert_called_once_with(token="delivery-token")
    queue.nack.assert_not_called()
    original_errback.assert_called_once_with(failure)
    assert BACKEND_ACK_TOKEN_META_KEY not in request.meta

  def test_deferred_errback_finalizes_after_resolution(self) -> None:
    from twisted.internet.defer import Deferred

    scheduler = BackendScheduler(connection_manager=MagicMock())
    queue = MagicMock(name="BackendQueue")
    deferred: Deferred[None] = Deferred()
    queued_request = Request(
      "https://example.com/download-failure",
      errback=MagicMock(return_value=deferred),
      meta={BACKEND_ACK_TOKEN_META_KEY: "delivery-token"},
    )
    queue.pop.return_value = queued_request
    scheduler._queue = queue
    request = scheduler.next_request()
    assert request is not None and request.errback is not None
    failure = MagicMock(request=request)

    assert request.errback(failure) is deferred
    queue.ack.assert_not_called()
    queue.nack.assert_not_called()
    deferred.callback(None)

    queue.ack.assert_called_once_with(token="delivery-token")
    queue.nack.assert_not_called()

  def test_failed_deferred_errback_nacks(self) -> None:
    from twisted.internet.defer import Deferred

    scheduler = BackendScheduler(connection_manager=MagicMock())
    queue = MagicMock(name="BackendQueue")
    deferred: Deferred[None] = Deferred()
    queued_request = Request(
      "https://example.com/download-failure",
      errback=MagicMock(return_value=deferred),
      meta={BACKEND_ACK_TOKEN_META_KEY: "delivery-token"},
    )
    queue.pop.return_value = queued_request
    scheduler._queue = queue
    request = scheduler.next_request()
    assert request is not None and request.errback is not None
    failure = MagicMock(request=request)
    result = request.errback(failure)
    result.addErrback(lambda _failure: None)

    deferred.errback(RuntimeError("async errback failed"))

    queue.nack.assert_called_once_with(token="delivery-token")
    queue.ack.assert_not_called()

  def test_async_errback_acks_after_awaited_success(self) -> None:
    scheduler = BackendScheduler(connection_manager=MagicMock())
    queue = MagicMock(name="BackendQueue")

    async def handled(_failure):
      return None

    queued_request = Request(
      "https://example.com/download-failure",
      errback=handled,
      meta={BACKEND_ACK_TOKEN_META_KEY: "delivery-token"},
    )
    queue.pop.return_value = queued_request
    scheduler._queue = queue
    request = scheduler.next_request()
    assert request is not None and request.errback is not None
    failure = MagicMock(request=request)

    awaitable = request.errback(failure)
    with pytest.raises(StopIteration):
      awaitable.send(None)

    queue.ack.assert_called_once_with(token="delivery-token")
    queue.nack.assert_not_called()

  def test_G2_dont_filter_skips_dedup_and_pushes(self) -> None:
    """G2: request.dont_filter=True → dedup NOT consulted, push proceeds."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(name="DupeFilter")
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue

    result = scheduler.enqueue_request(
      Request("https://example.com/a", dont_filter=True)
    )

    assert result is True
    dupefilter.request_seen.assert_not_called()  # dont_filter short-circuit
    queue.push.assert_called_once()
    assert counts.get("scheduler/enqueued") == 1

  def test_G3_no_dupefilter_pushes_straight(self) -> None:
    """G3: dupefilter=None → dedup skipped, push proceeds, returns True."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=None,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is True
    queue.push.assert_called_once()
    assert counts.get("scheduler/enqueued") == 1

  def test_G4_serialization_error_returns_false_and_bumps_stat(self) -> None:
    """G4: push raises SerializationError → except arm + stat, return False."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=None,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = SerializationError("too big")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False
    assert counts.get("scheduler/serialization_errors") == 1

  def test_G5_dedup_outage_without_stats_still_enqueues(self) -> None:
    """G5: request_seen→QueueError + stats=None → fallback push succeeds, True.

    Covers the stats-None sub-branch inside the dedup-outage arm — the
    ``if self.stats:`` guard at 632 must skip the stat bump without crashing.
    """
    manager = MagicMock(name="ConnectionManager")
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.side_effect = QueueError("dedup down")
    scheduler = BackendScheduler(
      connection_manager=manager, stats=None, dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is True
    queue.push.assert_called_once()  # fallback push fired (degrade-to-enqueue)

  def test_G6_dedup_outage_then_fallback_push_fails_returns_false(self) -> None:
    """G6: request_seen→QueueError AND fallback push→QueueError → return False.

    Covers the inner ``except (QueueError, SerializationError, BackendError)``
    at 638-640 — the rare double-failure where BOTH the dedup check and the
    fallback enqueue raise.
    """
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    dupefilter = MagicMock(name="DupeFilter")
    dupefilter.request_seen.side_effect = QueueError("dedup down")
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=dupefilter,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = QueueError("queue also down")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False
    assert queue.push.call_count == 1  # only the fallback push (initial never reached)
    # Outage stat WAS bumped (entered dedup-outage arm) before the fallback failed.
    assert counts.get("scheduler/dupefilter_error") == 1

  def test_G7_push_phase_failure_bumps_queue_error(self) -> None:
    """G7: dupefilter=None (phase=push), push→QueueError → queue_error stat.

    Covers the ``phase == "push"`` arm (643-646) — distinct from the
    dedup-outage arm because the failure is on the actual enqueue push.
    """
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=None,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = QueueError("push failed")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False
    assert counts.get("scheduler/queue_error") == 1

  def test_G4b_serialization_error_without_stats(self) -> None:
    """G4b: SerializationError + stats=None → return False, no crash.

    Covers the stats-None sub-branch of the SerializationError arm (625->627)
    — mirrors G5's stats-None characterization for the dedup-outage arm.
    """
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(
      connection_manager=manager, stats=None, dupefilter=None,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = SerializationError("too big")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False

  def test_G7b_push_phase_failure_without_stats(self) -> None:
    """G7b: push-phase failure + stats=None → return False, no crash.

    Covers the stats-None sub-branch of the push-phase arm (644->646) —
    mirrors G4b/G5 for the third except-arm.
    """
    manager = MagicMock(name="ConnectionManager")
    scheduler = BackendScheduler(
      connection_manager=manager, stats=None, dupefilter=None,
    )
    queue = MagicMock(name="BackendQueue")
    queue.push.side_effect = QueueError("push failed")
    scheduler._queue = queue

    result = scheduler.enqueue_request(Request("https://example.com/a"))

    assert result is False


class TestAckNackFailureObservability:
  """R-ack-obs (ultracode workflow v2): the deferred-ack signal handlers
  ``_on_response_received`` / ``_on_spider_error`` swallow ack/nack
  ``QueueError`` with ``logger.exception`` only and bump NO stat counter.
  At-least-once is PRESERVED (the message stays unacked → visibility-timeout
  redelivery on MQ backends), so the swallow is correct — but operators
  cannot detect ack/nack failure storms via Scrapy stats. Mirror the
  ``scheduler/dupefilter_error`` / ``scheduler/queue_error`` pattern: bump
  ``scheduler/ack_error`` / ``scheduler/nack_error`` so the storm is visible.
  """

  def test_ack_failure_increments_stat(self) -> None:
    """A QueueError from BackendQueue.ack → logger.exception + scheduler/ack_error."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=MagicMock(),
    )
    queue = MagicMock(name="BackendQueue")
    queue.ack.side_effect = QueueError("broker down")
    scheduler._queue = queue

    request = Request("https://example.com/x", meta={"_backend_ack_token": "tok"})
    scheduler._on_response_received(
      response=MagicMock(), request=request, spider=_FakeSpider(),
    )  # MUST NOT raise (at-least-once preserved via redelivery)

    queue.ack.assert_called_once_with(token="tok")
    assert counts.get("scheduler/ack_error") == 1

  def test_nack_failure_increments_stat(self) -> None:
    """A QueueError from BackendQueue.nack → logger.exception + scheduler/nack_error."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=MagicMock(),
    )
    queue = MagicMock(name="BackendQueue")
    queue.nack.side_effect = QueueError("broker down")
    scheduler._queue = queue

    failed_request = Request("https://example.com/x", meta={"_backend_ack_token": "tok"})
    response = MagicMock()
    response.request = failed_request
    scheduler._on_spider_error(
      failure=MagicMock(), response=response, spider=_FakeSpider(),
    )  # MUST NOT raise

    queue.nack.assert_called_once_with(token="tok")
    assert counts.get("scheduler/nack_error") == 1

  @pytest.mark.parametrize("operation", ["ack", "nack"])
  def test_backend_error_from_terminal_operation_is_observed(
    self, operation: str
  ) -> None:
    """Connection-level backend failures follow the same observable path."""
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=MagicMock(),
      stats=stats,
    )
    queue = MagicMock(name="BackendQueue")
    getattr(queue, operation).side_effect = BackendError("connection retired")
    scheduler._queue = queue
    request = Request(
      "https://example.com/x",
      meta={BACKEND_ACK_TOKEN_META_KEY: "tok"},
    )

    getattr(scheduler, f"_{operation}_request_token")(
      request,
      log_message=f"{operation} failed",
    )

    getattr(queue, operation).assert_called_once_with(token="tok")
    assert counts.get(f"scheduler/{operation}_error") == 1
    assert request.meta[BACKEND_ACK_TOKEN_META_KEY] == "tok"

  def test_ack_success_does_not_increment_error_stat(self) -> None:
    """Guard: a successful ack bumps no error counter (no false-positive signal)."""
    manager = MagicMock(name="ConnectionManager")
    counts, stats = _stats_counter()
    scheduler = BackendScheduler(
      connection_manager=manager, stats=stats, dupefilter=MagicMock(),
    )
    queue = MagicMock(name="BackendQueue")  # ack succeeds (no side_effect)
    scheduler._queue = queue

    request = Request("https://example.com/x", meta={"_backend_ack_token": "tok"})
    scheduler._on_response_received(response=MagicMock(), request=request, spider=_FakeSpider())

    queue.ack.assert_called_once_with(token="tok")
    assert counts.get("scheduler/ack_error") is None

  def test_successful_ack_consumes_token_before_later_spider_error(self) -> None:
    """Scrapy emits response_received before spider_error; terminate only once."""
    scheduler = BackendScheduler(
      connection_manager=MagicMock(),
      stats=MagicMock(),
      dupefilter=MagicMock(),
    )
    queue = MagicMock(name="BackendQueue")
    scheduler._queue = queue
    request = Request(
      "https://example.com/x",
      meta={"_backend_ack_token": "tok"},
    )
    response = MagicMock()
    response.request = request

    scheduler._on_response_received(
      response=response,
      request=request,
      spider=_FakeSpider(),
    )
    scheduler._on_spider_error(
      failure=MagicMock(),
      response=response,
      spider=_FakeSpider(),
    )

    queue.ack.assert_called_once_with(token="tok")
    queue.nack.assert_not_called()
    assert "_backend_ack_token" not in request.meta
