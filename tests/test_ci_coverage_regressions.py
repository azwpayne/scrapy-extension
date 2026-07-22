"""Focused regressions for defensive paths enforced by the CI coverage floor."""

from __future__ import annotations

import sys
from datetime import date, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import PurePosixPath
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID
from weakref import ref

import pytest
from pydantic import SecretStr, ValidationError
from scrapy import Request
from scrapy.settings import Settings
from twisted.python.failure import Failure as TwistedFailure

from scrapy_extension.backends import connectors as connectors_module
from scrapy_extension.dupefilter import dupefilter as dupefilter_module
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters import factory as dedupe_factory_module
from scrapy_extension.dupefilter.filters.base import MembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.exceptions import (
  ConfigurationError,
  QueueError,
  SerializationError,
)
from scrapy_extension.monitor.base import NullMonitor
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.strategies import factory as queue_factory_module
from scrapy_extension.queue.strategies.base import (
  QueueStrategy,
  _PreparedQueuePush,
  normalize_queue_timeout,
)
from scrapy_extension.schedule import scheduler as scheduler_module
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings.rabbitmq import (
  RabbitMQSettings,
  _secret_text,
  normalize_rabbitmq_host,
  parse_rabbitmq_node,
  validate_rabbitmq_connection,
)


class _RegistryEnum(str, Enum):
  VALUE = "value"


class _ControlSignal(BaseException):
  """Process-control sentinel used to exercise preservation paths."""


class _Stats:
  def __init__(self) -> None:
    self.counts: dict[str, int] = {}

  def inc_value(self, key: str, count: int = 1, **_: Any) -> None:
    self.counts[key] = self.counts.get(key, 0) + count


def _scheduler(*, stats: Any = None, dupefilter: Any = None) -> BackendScheduler:
  return BackendScheduler(
    connection_manager=MagicMock(name="ConnectionManager"),
    stats=stats,
    dupefilter=dupefilter,
  )


def _queue(
  *,
  strategy: Any = None,
  spider: Any = None,
) -> BackendQueue:
  manager = MagicMock(name="ConnectionManager")
  selected_strategy = (
    strategy if strategy is not None else MagicMock(spec=QueueStrategy)
  )
  return BackendQueue(
    connection_manager=manager,
    queue_name="coverage-queue",
    spider=spider,
    queue_strategy=selected_strategy,
    monitor=NullMonitor(),
    depth_sample_every=1,
  )


def _run_immediate(awaitable: Any) -> Any:
  """Drive an awaitable that deliberately performs no reactor or socket I/O."""
  while True:
    try:
      awaitable.send(None)
    except StopIteration as completed:
      return completed.value


@pytest.mark.parametrize(
  "value",
  [
    _RegistryEnum.VALUE,
    None,
    1.25,
    b"bytes",
    bytearray(b"bytes"),
    memoryview(b"bytes"),
    date(2026, 7, 22),
    time(1, 2, 3),
    timedelta(days=1, seconds=2, microseconds=3),
    Decimal("1.25"),
    UUID("12345678-1234-5678-1234-567812345678"),
    PurePosixPath("/tmp/example"),
    range(1, 8, 2),
    1 + 2j,
    BackendScheduler,
    sys,
    {"b", "a"},
    lambda: None,
  ],
)
def test_registry_normalization_supports_every_stable_value_kind(value: Any) -> None:
  normalized = connectors_module._normalize_registry_value(value, set())

  assert isinstance(normalized, list)
  assert normalized


def test_registry_normalization_handles_cycles_instance_state_and_slots() -> None:
  cyclic: list[Any] = []
  cyclic.append(cyclic)
  assert "cycle" in repr(
    connectors_module._normalize_registry_value(cyclic, set())
  )

  class WithDict:
    def __init__(self) -> None:
      self.value = "state"

  assert connectors_module._normalize_registry_value(WithDict(), set())[0] == "object"

  class WithPrivateSlot:
    __slots__ = "__value"

    def __init__(self) -> None:
      self.__value = "state"

  assert (
    connectors_module._normalize_registry_value(WithPrivateSlot(), set())[0]
    == "object"
  )

  class WithSparseSlots:
    __slots__ = ("__dict__", "__weakref__", "missing", "value")

    def __init__(self) -> None:
      self.value = "state"

  assert (
    connectors_module._normalize_registry_value(WithSparseSlots(), set())[0]
    == "object"
  )


def test_connection_setting_adapter_ignores_non_string_flat_keys() -> None:
  adapted = connectors_module._adapt_backend_settings(
    {1: "ignored", "SCRAPY_REDIS_HOST": "localhost"},
    "redis",
    {},
  )

  assert adapted["host"] == "localhost"
  assert 1 not in adapted


def test_connection_setting_merge_separates_colliding_backend_retry_delay() -> None:
  merged = connectors_module._merge_connection_manager_settings(
    {},
    {},
    {"retry_delay": 9},
    frozenset({"retry_delay"}),
  )

  assert merged["retry_delay"] == 9
  internal_key = connectors_module._CONNECTION_MANAGER_INTERNAL_KEYS["retry_delay"]
  assert merged[internal_key] == 1.0


def test_rabbitmq_host_and_node_validation_covers_boundary_syntax() -> None:
  assert _secret_text(SecretStr("secret")) == "secret"
  assert normalize_rabbitmq_host(" [::1] ") == "::1"

  invalid_hosts: list[Any] = [None, "", "bad/host"]
  for host in invalid_hosts:
    with pytest.raises(ConfigurationError):
      normalize_rabbitmq_host(host)

  invalid_nodes: list[Any] = [
    None,
    "",
    "[::1",
    "[::1]suffix",
    "[::1]:wrong",
    "host:",
    "host:wrong",
    "not:valid:ipv6",
    "host:0",
  ]
  for node in invalid_nodes:
    with pytest.raises(ConfigurationError):
      parse_rabbitmq_node(node, 5672)

  assert parse_rabbitmq_node("host", 5672) == ("host", 5672)
  assert parse_rabbitmq_node("127.0.0.1", 5672) == ("127.0.0.1", 5672)


def test_rabbitmq_connection_validation_rejects_untyped_transport_values() -> None:
  base: dict[str, Any] = {
    "host": "localhost",
    "port": 5672,
    "cluster_nodes": (),
    "username": "crawler",
    "password": "secret",
    "ssl_enabled": False,
    "ssl_cafile": None,
    "ssl_certfile": None,
    "ssl_keyfile": None,
    "ssl_verify_mode": "CERT_REQUIRED",
  }
  for updates in (
    {"port": True},
    {"ssl_enabled": "false"},
    {"ssl_cafile": " "},
  ):
    with pytest.raises(ConfigurationError):
      validate_rabbitmq_connection(**(base | updates))


def test_rabbitmq_settings_before_validator_accepts_only_mapping_input() -> None:
  with pytest.raises(ValidationError):
    RabbitMQSettings.model_validate("not-a-mapping")


def test_scheduler_protocol_discovery_rejects_dynamic_or_noncallable_hooks() -> None:
  class DynamicOnly:
    pass

  dynamic = DynamicOnly()
  dynamic.protocol = object()
  assert scheduler_module._static_declaration_rank(dynamic, "protocol") is None

  class SlotOnly:
    __slots__ = ()

  assert scheduler_module._atomic_dupefilter_methods(SlotOnly()) is None

  class InvalidAtomic:
    request_seen_with_reservation = None
    commit_volatile_reservation = object()

    def commit_reservation(self, reservation: object) -> None:
      del reservation

    def rollback_reservation(self, reservation: object) -> None:
      del reservation

    def rollback_reservation_intent(self, owner: object) -> None:
      del owner

  assert scheduler_module._atomic_dupefilter_methods(InvalidAtomic()) is None

  class ValidAtomic(InvalidAtomic):
    def request_seen_with_reservation(
      self,
      request: Request,
      owner: object,
    ) -> object:
      del request, owner
      return object()

  methods = scheduler_module._atomic_dupefilter_methods(ValidAtomic())
  assert methods is not None
  assert methods[2] is None


def test_deferred_ack_group_terminal_paths_are_idempotent() -> None:
  scheduler = _scheduler()
  group = scheduler_module._DeferredReplacementAckGroup(scheduler, "source")
  child = group.new_child()
  assert child is not None

  child.nack()
  assert group._terminal is True
  assert group.new_child() is None
  group.seal()
  group.accept(999)
  group.abort()

  queue = MagicMock(name="Queue")
  scheduler._queue = queue
  accepted = scheduler_module._DeferredReplacementAckGroup(scheduler, "source-2")
  accepted_child = accepted.new_child()
  assert accepted_child is not None
  accepted.accept(999)
  accepted.seal()
  accepted_child.ack()
  accepted_child.ack()
  queue.ack.assert_called_once_with(token="source-2")


def test_scheduler_explicit_ack_and_nack_fail_closed_without_queue() -> None:
  scheduler = _scheduler()
  request = Request("https://example.com/no-queue", meta={"_backend_ack_token": "t"})

  assert scheduler._ack_token("t", log_message="ack") is False
  assert scheduler._nack_token("t", log_message="nack") is False
  scheduler._ack_request_token(Request("https://example.com/no-token"), log_message="ack")
  scheduler._nack_request_token(Request("https://example.com/no-token"), log_message="nack")
  scheduler._ack_request_token(object(), log_message="ack")  # type: ignore[arg-type]
  scheduler._nack_request_token(object(), log_message="nack")  # type: ignore[arg-type]
  assert request.meta["_backend_ack_token"] == "t"


def test_download_failure_wrapper_handles_scalar_replacement_and_conflict() -> None:
  stats = _Stats()
  scheduler = _scheduler(stats=stats)
  queue = MagicMock(name="Queue")
  scheduler._queue = queue
  wrapper = scheduler_module._BackendDownloadFailureErrback(scheduler, None)

  failed = Request(
    "https://example.com/failed",
    meta={"_backend_ack_token": "failed-token"},
  )
  failure = SimpleNamespace(request=failed)
  assert wrapper(failure) is failure
  queue.nack.assert_called_once_with(token="failed-token")

  handled = Request(
    "https://example.com/handled",
    meta={"_backend_ack_token": "handled-token"},
  )
  assert wrapper._finish_success(handled, "handled") == "handled"
  queue.ack.assert_called_with(token="handled-token")
  assert wrapper._finish_success(None, 1) == 1

  twisted_request = Request(
    "https://example.com/twisted",
    meta={"_backend_ack_token": "twisted-token"},
  )
  twisted = TwistedFailure(RuntimeError("failed"))
  assert wrapper._finish_success(twisted_request, twisted) is twisted
  queue.nack.assert_called_with(token="twisted-token")

  no_token = Request("https://example.com/no-token")
  replacement = Request("https://example.com/replacement")
  assert wrapper._transfer_request(no_token, replacement) is replacement

  conflict_source = Request(
    "https://example.com/conflict-source",
    meta={"_backend_ack_token": "source-token"},
  )
  conflict = Request(
    "https://example.com/conflict",
    meta={"_backend_ack_token": "other-token"},
  )
  assert wrapper._transfer_request(conflict_source, conflict) is conflict
  assert conflict.meta["_backend_ack_token"] == "other-token"
  assert stats.counts["scheduler/ack_transfer_conflict"] == 1
  queue.nack.assert_called_with(token="source-token")


def test_download_failure_wrapper_settles_sync_and_async_streams() -> None:
  scheduler = _scheduler()
  queue = MagicMock(name="Queue")
  scheduler._queue = queue
  wrapper = scheduler_module._BackendDownloadFailureErrback(scheduler, None)

  sync_request = Request(
    "https://example.com/sync",
    meta={"_backend_ack_token": "sync-token"},
  )
  assert list(wrapper._finish_success(sync_request, [1, "two"])) == [1, "two"]
  queue.ack.assert_called_with(token="sync-token")

  def broken_without_replacement() -> Any:
    yield 1
    raise RuntimeError("stream failed")

  failed_request = Request(
    "https://example.com/sync-fail",
    meta={"_backend_ack_token": "sync-fail-token"},
  )
  with pytest.raises(RuntimeError, match="stream failed"):
    list(wrapper._finish_success(failed_request, broken_without_replacement()))
  queue.nack.assert_called_with(token="sync-fail-token")

  def broken_after_replacement() -> Any:
    yield Request("https://example.com/child")
    raise RuntimeError("child stream failed")

  source = Request(
    "https://example.com/source",
    meta={"_backend_ack_token": "source-token"},
  )
  with pytest.raises(RuntimeError, match="child stream failed"):
    list(wrapper._finish_success(source, broken_after_replacement()))
  queue.nack.assert_called_with(token="source-token")

  async def run_async_paths() -> None:
    async_request = Request(
      "https://example.com/async",
      meta={"_backend_ack_token": "async-token"},
    )

    async def values() -> Any:
      yield 1

    assert [value async for value in wrapper._finish_success(async_request, values())] == [1]

    async def broken() -> Any:
      yield Request("https://example.com/async-child")
      raise RuntimeError("async stream failed")

    async_source = Request(
      "https://example.com/async-source",
      meta={"_backend_ack_token": "async-source-token"},
    )
    with pytest.raises(RuntimeError, match="async stream failed"):
      _ = [
        value
        async for value in wrapper._finish_success(async_source, broken())
      ]

    async def failed_awaitable() -> Any:
      raise RuntimeError("awaitable failed")

    await_request = Request(
      "https://example.com/await",
      meta={"_backend_ack_token": "await-token"},
    )
    with pytest.raises(RuntimeError, match="awaitable failed"):
      await wrapper._finish_awaitable(await_request, failed_awaitable())

  _run_immediate(run_async_paths())
  queue.ack.assert_called_with(token="async-token")
  queue.nack.assert_any_call(token="async-source-token")
  queue.nack.assert_any_call(token="await-token")


def test_scheduler_restores_and_does_not_double_wrap_errbacks() -> None:
  scheduler = _scheduler()
  original = MagicMock(name="original_errback")
  request = Request(
    "https://example.com/wrapped",
    errback=original,
    meta={"_backend_ack_token": "token"},
  )

  scheduler._wrap_download_failure(request)
  wrapped = request.errback
  assert isinstance(wrapped, scheduler_module._BackendDownloadFailureErrback)
  scheduler._wrap_download_failure(request)
  assert request.errback is wrapped
  scheduler._restore_original_errback(request)
  assert request.errback is original

  no_token = Request("https://example.com/plain", errback=original)
  scheduler._wrap_download_failure(no_token)
  assert no_token.errback is original


def test_scheduler_atomic_cleanup_preserves_primary_signals(monkeypatch) -> None:
  stats = _Stats()
  scheduler = _scheduler(stats=stats)

  def ordinary_failure(_: object) -> None:
    raise RuntimeError("rollback failed")

  scheduler._rollback_atomic_reservation(
    object(),
    ordinary_failure,
    preserve_primary=False,
  )
  scheduler._rollback_atomic_reservation(
    object(),
    ordinary_failure,
    preserve_primary=True,
  )

  signal = _ControlSignal()

  def process_control(_: object) -> None:
    raise signal

  with pytest.raises(_ControlSignal) as raised:
    scheduler._rollback_atomic_reservation(
      object(),
      process_control,
      preserve_primary=False,
    )
  assert raised.value is signal
  scheduler._rollback_atomic_reservation(
    object(),
    process_control,
    preserve_primary=True,
  )

  monkeypatch.setattr(
    scheduler_module.logger,
    "exception",
    MagicMock(side_effect=_ControlSignal()),
  )
  scheduler._rollback_atomic_reservation(
    object(),
    ordinary_failure,
    preserve_primary=True,
  )
  scheduler._commit_atomic_reservation(object(), ordinary_failure)
  assert stats.counts["scheduler/dupefilter_rollback_error"] >= 3
  assert stats.counts["scheduler/dupefilter_commit_error"] == 1


def test_scheduler_legacy_dupefilter_cleanup_covers_all_failure_classes() -> None:
  stats = _Stats()
  request = Request("https://example.com/rollback")

  unsupported = MagicMock(spec=["request_seen", "log"])
  scheduler = _scheduler(stats=stats, dupefilter=unsupported)
  scheduler._rollback_dupefilter_reservation(request)

  ordinary = MagicMock(name="ordinary_dupefilter")
  ordinary.forget.side_effect = RuntimeError("forget failed")
  scheduler.dupefilter = ordinary
  scheduler._rollback_dupefilter_reservation(request)
  scheduler._rollback_dupefilter_reservation(request, preserve_primary=True)

  signal = _ControlSignal()
  process_control = MagicMock(name="process_control_dupefilter")
  process_control.forget.side_effect = signal
  scheduler.dupefilter = process_control
  with pytest.raises(_ControlSignal) as raised:
    scheduler._rollback_dupefilter_reservation(request)
  assert raised.value is signal
  scheduler._rollback_dupefilter_reservation(request, preserve_primary=True)

  assert stats.counts["scheduler/dupefilter_rollback_error"] >= 4


def test_scheduler_close_isolates_owned_dupefilter_failure() -> None:
  manager = MagicMock(name="ConnectionManager")
  dupefilter = MagicMock(name="DupeFilter")
  dupefilter.close.side_effect = RuntimeError("close failed")
  scheduler = BackendScheduler(manager, dupefilter=dupefilter)
  scheduler._owns_dupefilter = True
  scheduler._queue = MagicMock(name="Queue")

  scheduler.close("done")

  dupefilter.close.assert_called_once_with("done")
  manager.close.assert_called_once_with()


def _new_dupefilter(
  membership_filter: MembershipFilter | None = None,
) -> BackendDupeFilter:
  return BackendDupeFilter(
    connection_manager=MagicMock(name="ConnectionManager"),
    membership_filter=(
      membership_filter
      if membership_filter is not None
      else MemoryMembershipFilter(maxsize=None)
    ),
  )


def test_dupefilter_receipts_reject_invalid_and_stale_values() -> None:
  dupefilter = _new_dupefilter()
  for operation in (
    dupefilter.commit_reservation,
    dupefilter.commit_volatile_reservation,
    dupefilter.rollback_reservation,
  ):
    with pytest.raises(TypeError):
      operation(object())

  request = Request("https://example.com/stale")
  decision = dupefilter.request_seen_with_reservation(request)
  assert decision.reservation is not None
  dupefilter.rollback_reservation(decision.reservation)
  dupefilter.rollback_reservation(decision.reservation)
  dupefilter.rollback_reservation_intent(object())

  stale_commit = dupefilter.request_seen_with_reservation(request.replace())
  assert stale_commit.reservation is not None
  dupefilter._reservation_epoch += 1
  dupefilter.commit_reservation(stale_commit.reservation)

  stale_volatile = dupefilter.request_seen_with_reservation(
    Request("https://example.com/stale-volatile")
  )
  assert stale_volatile.reservation is not None
  dupefilter._reservation_epoch += 1
  dupefilter.commit_volatile_reservation(stale_volatile.reservation)


def test_dupefilter_volatile_marker_refresh_and_bounded_eviction(caplog) -> None:
  dupefilter = _new_dupefilter()
  dupefilter._volatile_fingerprint_limit = 1

  def reservation_for(url: str, fingerprint: bytes) -> Any:
    owner = object()
    request = Request(url)
    reservation = dupefilter_module._DedupReservation(
      fingerprint,
      dupefilter._reservation_epoch,
      owner,
      request,
      fingerprint.decode(),
    )
    dupefilter._active_reservations[id(reservation)] = reservation
    dupefilter._reservations_by_owner[id(owner)] = reservation
    return reservation

  first = reservation_for("https://example.com/first", b"first")
  dupefilter.commit_volatile_reservation(first)
  refreshed = reservation_for("https://example.com/first-again", b"first")
  dupefilter.commit_volatile_reservation(refreshed)
  second = reservation_for("https://example.com/second", b"second")
  dupefilter.commit_volatile_reservation(second)

  assert list(dupefilter._volatile_fingerprints) == [b"second"]
  assert "Volatile queue dedup shadow reached" in caplog.text


def test_dupefilter_scheduler_probe_translates_unsupported_set_backend() -> None:
  membership = MagicMock(spec=MembershipFilter)
  membership.__contains__.side_effect = NotImplementedError
  membership.saturation = None
  dupefilter = _new_dupefilter(membership)

  with pytest.raises(RuntimeError, match="does not support set"):
    dupefilter.request_seen_with_reservation(
      Request("https://example.com/unsupported")
    )


def test_dupefilter_retry_allowance_refreshes_and_evicts_oldest(caplog) -> None:
  dupefilter = _new_dupefilter()
  dupefilter._retry_allowance_limit = 1

  dupefilter._grant_retry_allowance(b"first")
  dupefilter._grant_retry_allowance(b"first")
  dupefilter._grant_retry_allowance(b"second")

  assert not dupefilter._consume_retry_allowance(b"first")
  assert dupefilter._consume_retry_allowance(b"second")
  assert "Dedup retry allowances reached" in caplog.text


def test_dupefilter_monitor_fence_and_diagnostics_fail_open(monkeypatch) -> None:
  token = dupefilter_module._MonitorFenceToken(123, "token")

  class BadFrame:
    f_back = None

    @property
    def f_locals(self) -> Any:
      raise RuntimeError("frame unavailable")

  monkeypatch.setattr(
    dupefilter_module.sys,
    "_current_frames",
    lambda: {123: BadFrame()},
  )
  assert token.active is False

  dupefilter = _new_dupefilter()
  monkeypatch.setattr(
    dupefilter_module.logger,
    "warning",
    MagicMock(side_effect=RuntimeError("logger unavailable")),
  )
  dupefilter._warn_monitor_overflow()


def test_dupefilter_interrupted_decision_compensation_is_best_effort(monkeypatch) -> None:
  dupefilter = _new_dupefilter()
  owner = object()
  request = Request("https://example.com/compensate")
  reservation = dupefilter_module._DedupReservation(
    b"fingerprint",
    dupefilter._reservation_epoch,
    owner,
    request,
    "fingerprint",
  )
  dupefilter._active_reservations[id(reservation)] = reservation
  dupefilter._reservations_by_owner[id(owner)] = reservation
  dupefilter._compensate_interrupted_decision(reservation, owner)
  assert not dupefilter._active_reservations

  owner_only = object()
  owner_reservation = dupefilter_module._DedupReservation(
    b"owner",
    dupefilter._reservation_epoch,
    owner_only,
    request,
    "owner",
  )
  dupefilter._active_reservations[id(owner_reservation)] = owner_reservation
  dupefilter._reservations_by_owner[id(owner_only)] = owner_reservation
  dupefilter._compensate_interrupted_decision(None, owner_only)
  assert not dupefilter._active_reservations

  dupefilter._active_reservations = MagicMock()
  dupefilter._active_reservations.get.side_effect = _ControlSignal()
  monkeypatch.setattr(
    dupefilter_module.logger,
    "debug",
    MagicMock(side_effect=_ControlSignal()),
  )
  dupefilter._compensate_interrupted_decision(reservation, owner)


def test_dupefilter_monitor_fallback_cleanup_preserves_process_signal() -> None:
  request = Request("https://example.com/monitor-fallback")
  active_tokens: set[Any] = set()

  class FailCleanupGet(dict[int, Any]):
    calls = 0

    def get(self, key: int, default: Any = None) -> Any:
      self.calls += 1
      if self.calls == 2:
        raise RuntimeError("cleanup lookup failed")
      return super().get(key, default)

  active_requests = FailCleanupGet(
    {id(request): (ref(request), active_tokens)}
  )
  dupefilter = _new_dupefilter()
  dupefilter._active_monitor_requests = active_requests
  dupefilter._monitor = MagicMock(name="Monitor")
  signal = _ControlSignal()
  dupefilter._monitor.on_dedup_hit.side_effect = signal

  with pytest.raises(_ControlSignal) as raised:
    dupefilter._emit_monitor(("on_dedup_hit", ("fingerprint",), request))

  assert raised.value is signal
  assert id(request) not in active_requests


def test_dupefilter_close_preserves_filter_error_when_diagnostics_fail(
  monkeypatch,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  membership = MagicMock(spec=MembershipFilter)
  membership.saturation = None
  dupefilter = BackendDupeFilter(
    connection_manager=manager,
    membership_filter=membership,
  )
  dupefilter._owns_connection_manager = True
  primary = _ControlSignal()

  def release_then_fail() -> None:
    dupefilter._filter_released = True
    raise primary

  membership.close.side_effect = release_then_fail
  manager.close.side_effect = RuntimeError("manager close failed")
  monkeypatch.setattr(
    dupefilter_module.logger,
    "error",
    MagicMock(side_effect=_ControlSignal()),
  )

  with pytest.raises(_ControlSignal) as raised:
    dupefilter.close("coverage")

  assert raised.value is primary
  manager.close.assert_called_once_with()


def test_dupefilter_factory_rejects_non_string_key_after_manager_acquisition(
  monkeypatch,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  monkeypatch.setattr(
    connectors_module.ConnectionManager,
    "get_manager",
    MagicMock(return_value=manager),
  )

  with pytest.raises(ConfigurationError, match="must be a string"):
    BackendDupeFilter.from_settings(
      Settings(
        {
          "SCRAPY_DEDUP_STRATEGY": "memory",
          "SCRAPY_DUPEFILTER_KEY": 123,
        }
      )
    )

  manager.close.assert_called_once_with()


@pytest.mark.parametrize(
  ("values", "spider_name", "manager_created"),
  [
    ({"SCRAPY_QUEUE_KEY": 123}, None, False),
    ({"SCRAPY_QUEUE_KEY": "queue"}, "bad/spider", False),
    (
      {
        "SCRAPY_QUEUE_STRATEGY": "work_stealing",
        "SCRAPY_QUEUE_PEER_IDS": 123,
      },
      None,
      True,
    ),
  ],
)
def test_scheduler_factory_rejects_invalid_key_and_worker_inputs(
  monkeypatch,
  values: dict[str, Any],
  spider_name: str | None,
  manager_created: bool,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  monkeypatch.setattr(
    connectors_module.ConnectionManager,
    "get_manager",
    MagicMock(return_value=manager),
  )

  with pytest.raises(ConfigurationError):
    BackendScheduler.from_settings(Settings(values), spider_name=spider_name)

  if manager_created:
    manager.close.assert_called_once_with()
  else:
    manager.close.assert_not_called()


def test_scheduler_factory_wraps_strategy_constructor_and_cleanup_failures(
  monkeypatch,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  manager.close.side_effect = _ControlSignal()
  monkeypatch.setattr(
    connectors_module.ConnectionManager,
    "get_manager",
    MagicMock(return_value=manager),
  )
  monkeypatch.setattr(
    queue_factory_module,
    "build_queue_strategy",
    MagicMock(side_effect=ValueError("constructor failed")),
  )
  monkeypatch.setattr(
    scheduler_module.logger,
    "exception",
    MagicMock(side_effect=_ControlSignal()),
  )

  with pytest.raises(ConfigurationError, match="Invalid SCRAPY_QUEUE_STRATEGY"):
    BackendScheduler.from_settings(Settings())


def test_scheduler_crawler_factory_preserves_primary_when_close_fails(
  monkeypatch,
) -> None:
  scheduler = _scheduler()
  scheduler.close = MagicMock(side_effect=_ControlSignal())  # type: ignore[method-assign]
  monkeypatch.setattr(
    BackendScheduler,
    "from_settings",
    MagicMock(return_value=scheduler),
  )
  monkeypatch.setattr(
    scheduler_module,
    "load_object",
    MagicMock(side_effect=RuntimeError("load failed")),
  )
  monkeypatch.setattr(
    scheduler_module.logger,
    "exception",
    MagicMock(side_effect=_ControlSignal()),
  )
  crawler = SimpleNamespace(
    settings=Settings({"DUPEFILTER_CLASS": "invalid.Class"}),
    stats=MagicMock(),
  )

  with pytest.raises(RuntimeError, match="load failed"):
    BackendScheduler.from_crawler(crawler)


def test_dupefilter_factory_wraps_constructor_and_cleanup_failures(
  monkeypatch,
) -> None:
  manager = MagicMock(name="ConnectionManager")
  manager.close.side_effect = _ControlSignal()
  monkeypatch.setattr(
    connectors_module.ConnectionManager,
    "get_manager",
    MagicMock(return_value=manager),
  )
  monkeypatch.setattr(
    dedupe_factory_module,
    "build_membership_filter",
    MagicMock(side_effect=ValueError("constructor failed")),
  )
  monkeypatch.setattr(
    dupefilter_module.logger,
    "exception",
    MagicMock(side_effect=_ControlSignal()),
  )

  with pytest.raises(ConfigurationError, match="Invalid SCRAPY_DEDUP_MEMORY_MAXSIZE"):
    BackendDupeFilter.from_settings(
      Settings({"SCRAPY_DEDUP_STRATEGY": "memory"})
    )


def test_dupefilter_crawler_factory_preserves_primary_when_close_fails(
  monkeypatch,
) -> None:
  dupefilter = MagicMock(name="DupeFilter")
  dupefilter.close.side_effect = _ControlSignal()
  monkeypatch.setattr(
    BackendDupeFilter,
    "from_settings",
    MagicMock(return_value=dupefilter),
  )
  monkeypatch.setattr(
    dupefilter_module.logger,
    "exception",
    MagicMock(side_effect=_ControlSignal()),
  )

  class BadCrawler:
    settings = Settings()
    stats = None

    @property
    def request_fingerprinter(self) -> Any:
      raise RuntimeError("fingerprinter failed")

  with pytest.raises(RuntimeError, match="fingerprinter failed"):
    BackendDupeFilter.from_crawler(BadCrawler())


def test_queue_rejects_unstringifiable_source_before_serialization() -> None:
  class BadSource:
    def __str__(self) -> str:
      raise RuntimeError("cannot stringify")

  queue = _queue()
  request = Request(
    "https://example.com/source",
    meta={"source": BadSource()},
  )

  with pytest.raises(QueueError, match="Invalid queue source"):
    queue._push(request, 0)


def test_queue_rejects_unknown_durability_receipt_for_replacement() -> None:
  strategy = MagicMock(spec=QueueStrategy)
  strategy._prepare_push.return_value = _PreparedQueuePush(
    backend_route=True,
    _commit=lambda _item, _require_durable: False,
  )
  queue = _queue(strategy=strategy)
  request = Request(
    "https://example.com/replacement",
    meta={"_backend_ack_token": "source-token"},
  )

  with pytest.raises(QueueError, match="no valid worker-crash durability receipt"):
    queue._push(request, 0)


def test_queue_empty_payload_preserves_ack_failure_when_nack_also_fails() -> None:
  strategy = MagicMock(spec=QueueStrategy)
  strategy.pop_with_ack.return_value = (None, "token")
  strategy.queue_len.return_value = 0
  queue = _queue(strategy=strategy)
  queue._ack = MagicMock(side_effect=QueueError("ack failed"))  # type: ignore[method-assign]
  queue._nack = MagicMock(side_effect=QueueError("nack failed"))  # type: ignore[method-assign]

  with pytest.raises(QueueError, match="ack failed"):
    queue._pop(0)

  queue._nack.assert_called_once_with(token="token")


def test_queue_pop_isolates_monitor_failure_from_deserialization_error() -> None:
  strategy = MagicMock(spec=QueueStrategy)
  strategy.pop_with_ack.return_value = (b"not-a-request", None)
  strategy.queue_len.return_value = 0
  queue = _queue(strategy=strategy)
  serializer = MagicMock(name="Serializer")
  serializer.deserialize.return_value = []
  queue.__dict__["_serializer"] = serializer
  queue._monitor = MagicMock(name="Monitor")
  queue._monitor.on_error.side_effect = RuntimeError("monitor failed")

  with pytest.raises(SerializationError, match="must be a JSON object"):
    queue._pop(0)


def test_queue_preserves_malformed_payload_error_when_ack_fails() -> None:
  strategy = MagicMock(spec=QueueStrategy)
  strategy.pop_with_ack.return_value = (b"not-a-request", "token")
  strategy.queue_len.return_value = 0
  queue = _queue(strategy=strategy)
  serializer = MagicMock(name="Serializer")
  serializer.deserialize.return_value = []
  queue.__dict__["_serializer"] = serializer
  queue._ack = MagicMock(side_effect=QueueError("ack failed"))  # type: ignore[method-assign]

  with pytest.raises(SerializationError, match="must be a JSON object"):
    queue._pop(0)

  queue._ack.assert_called_once_with(token="token")


@pytest.mark.parametrize(
  "payload",
  [
    {"flags": ["ok", 1]},
    {"headers": {1: "value"}},
    {"headers": {"name": [object()]}},
    {"priority": True},
  ],
)
def test_queue_wire_validator_rejects_nested_type_drift(
  payload: dict[str, Any],
) -> None:
  with pytest.raises(TypeError):
    BackendQueue._validate_request_dict(payload)


def test_queue_wire_validator_accepts_scalar_and_list_header_values() -> None:
  payload: dict[str, Any] = {
    "headers": {
      "text": "value",
      "bytes": b"value",
      "list": ["text", b"bytes"],
    },
    "priority": 0.0,
  }

  BackendQueue._validate_request_dict(payload)

  assert payload["priority"] == 0


def test_queue_body_codec_and_callback_validation_fail_closed() -> None:
  with pytest.raises(SerializationError, match="Unsupported queued request body codec"):
    BackendQueue._decode_body({"_scrapy_extension_body_codec": "future"})

  queue = _queue(spider=SimpleNamespace())
  with pytest.raises(ValueError, match="not found"):
    queue._request_from_dict({"callback": "missing"})


def test_queue_invalid_replacement_and_stats_failures_do_not_mask_primary() -> None:
  stats = MagicMock(name="Stats")
  stats.inc_value.side_effect = RuntimeError("stats failed")
  spider = SimpleNamespace(crawler=SimpleNamespace(stats=stats))
  queue = _queue(spider=spider)
  queue._ack = MagicMock(side_effect=QueueError("ack failed"))  # type: ignore[method-assign]
  request = Request(
    "https://example.com/invalid",
    meta={"_backend_ack_token": "token"},
  )

  queue._terminate_invalid_replacement(request, "token")
  assert request.meta["_backend_ack_token"] == "token"
  queue._inc_stat("coverage/stat")


def test_queue_close_waits_for_an_in_progress_close() -> None:
  queue = _queue()
  queue._accepting_operations = False
  queue._close_complete = False
  operation_gate = MagicMock(name="OperationGate")
  operation_gate.wait.side_effect = lambda: setattr(
    queue,
    "_close_complete",
    True,
  )
  queue._operation_gate = operation_gate

  queue.close()

  operation_gate.wait.assert_called_once_with()


def test_queue_strategy_defensive_defaults_and_local_durability_gate() -> None:
  class OverflowFloat(float):
    def __float__(self) -> float:
      raise OverflowError("cannot normalize")

  with pytest.raises(ValueError, match="finite non-negative"):
    normalize_queue_timeout(OverflowFloat(1.0))

  strategy = MagicMock(spec=QueueStrategy)
  assert (
    QueueStrategy.is_push_durable(strategy, delay=0.0, source="coverage")
    is False
  )

  published: list[bytes] = []
  prepared = _PreparedQueuePush.local(
    queue_name="coverage-queue",
    strategy_name="LocalCoverageStrategy",
    publish=published.append,
  )
  with pytest.raises(QueueError, match="not worker-crash durable"):
    prepared.commit(b"blocked", require_durable=True)
  assert published == []
  assert prepared.commit(b"accepted") is False
  assert published == [b"accepted"]


def test_module_type_normalization_does_not_depend_on_module_state() -> None:
  module = ModuleType("coverage_fixture")
  assert connectors_module._normalize_registry_value(module, set()) == [
    "module",
    "coverage_fixture",
  ]
