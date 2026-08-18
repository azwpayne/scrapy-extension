"""Snapshot storage pairing for stateful queue strategies.

The scheduler already supports independent queue and storage backend settings.
These tests pin the narrow bridge that lets stateful *queue-only* strategies
reuse the configured storage component for their restart checkpoint without
changing normal queue traffic or the legacy all-in-one backend behavior.
"""

from __future__ import annotations

from unittest.mock import call

import pytest
from scrapy.http import Request
from scrapy.settings import Settings

from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.schedule.scheduler import BackendScheduler


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "SCRAPY_QUEUE_BACKEND_TYPE": "kafka",
        "SCRAPY_STORAGE_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_STRATEGY": "delay",
    }
    values.update(overrides)
    return Settings(values)


@pytest.mark.parametrize(
    "strategy",
    ["delay", "round_robin", "time_wheel", "ring_buffer"],
)
def test_queue_only_stateful_strategy_acquires_storage_snapshot_manager(
    mocker,
    strategy: str,
) -> None:
    """Each snapshot-capable Kafka strategy checkpoints through Redis."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    get_manager = mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        side_effect=[queue_manager, snapshot_manager],
    )

    scheduler = BackendScheduler.from_settings(
        _settings(SCRAPY_QUEUE_STRATEGY=strategy)
    )

    assert get_manager.call_args_list == [
        call(
            backend_type="kafka",
            settings={"__connection_manager_queue_scope": "scheduler-queue"},
        ),
        call(backend_type="redis", settings={}),
    ]
    assert scheduler._snapshot_connection_manager is snapshot_manager
    assert scheduler._owns_snapshot_connection_manager is True
    scheduler.close("test-finished")
    snapshot_manager.close.assert_called_once_with()
    queue_manager.close.assert_called_once_with()


def test_passthrough_queue_only_strategy_does_not_acquire_snapshot_manager(
    mocker,
) -> None:
    """A stateless Kafka queue keeps its historical single manager acquire."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    get_manager = mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        return_value=queue_manager,
    )

    scheduler = BackendScheduler.from_settings(
        _settings(SCRAPY_QUEUE_STRATEGY="passthrough")
    )

    get_manager.assert_called_once_with(
        backend_type="kafka",
        settings={"__connection_manager_queue_scope": "scheduler-queue"},
    )
    assert scheduler._snapshot_connection_manager is None
    scheduler.close("test-finished")
    queue_manager.close.assert_called_once_with()


def test_storage_capable_queue_keeps_its_own_snapshot_manager(mocker) -> None:
    """A full Redis queue ignores a separate storage component for checkpoints."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    get_manager = mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        return_value=queue_manager,
    )

    scheduler = BackendScheduler.from_settings(
        _settings(
            SCRAPY_QUEUE_BACKEND_TYPE="redis",
            SCRAPY_STORAGE_BACKEND_TYPE="mongodb",
        )
    )

    get_manager.assert_called_once_with(backend_type="redis", settings={})
    assert scheduler._snapshot_connection_manager is None
    scheduler.close("test-finished")
    queue_manager.close.assert_called_once_with()


def test_legacy_queue_only_global_backend_keeps_best_effort_snapshot_skip(
    mocker,
) -> None:
    """No separate storage type preserves the old queue-only startup behavior."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    get_manager = mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        return_value=queue_manager,
    )

    scheduler = BackendScheduler.from_settings(
        Settings(
            {
                "SCRAPY_BACKEND_TYPE": "kafka",
                "SCRAPY_QUEUE_STRATEGY": "delay",
            }
        )
    )

    get_manager.assert_called_once_with(
        backend_type="kafka",
        settings={"__connection_manager_queue_scope": "scheduler-queue"},
    )
    assert scheduler._snapshot_connection_manager is None
    scheduler.close("test-finished")


def test_explicit_storage_backend_without_storage_capability_stays_fail_fast(
    mocker,
) -> None:
    """An explicit invalid storage override is never silently downgraded."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    get_manager = mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        return_value=queue_manager,
    )

    with pytest.raises(ConfigurationError, match="does not support the storage"):
        BackendScheduler.from_settings(_settings(SCRAPY_STORAGE_BACKEND_TYPE="kafka"))

    get_manager.assert_called_once_with(
        backend_type="kafka",
        settings={"__connection_manager_queue_scope": "scheduler-queue"},
    )
    queue_manager.close.assert_called_once_with()


def test_second_manager_acquire_failure_releases_queue_manager(mocker) -> None:
    """A storage acquire failure cannot leak the already-acquired queue manager."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    acquire_error = RuntimeError("snapshot acquire failed")
    mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        side_effect=[queue_manager, acquire_error],
    )

    with pytest.raises(RuntimeError, match="snapshot acquire failed"):
        BackendScheduler.from_settings(_settings())

    queue_manager.close.assert_called_once_with()


def test_factory_failure_after_second_acquire_releases_both_managers_in_reverse_order(
    mocker,
) -> None:
    """A failed scheduler construction releases both successful acquires."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    order: list[str] = []
    queue_manager.close.side_effect = lambda: order.append("queue")
    snapshot_manager.close.side_effect = lambda: order.append("snapshot")
    mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        side_effect=[queue_manager, snapshot_manager],
    )
    constructor_error = RuntimeError("scheduler construction failed")
    mocker.patch.object(BackendScheduler, "__init__", side_effect=constructor_error)

    with pytest.raises(RuntimeError, match="scheduler construction failed"):
        BackendScheduler.from_settings(_settings())

    assert order == ["snapshot", "queue"]


def test_scheduler_closes_queue_then_owned_snapshot_manager_then_queue_manager(
    mocker,
) -> None:
    """The final snapshot must finish before either manager release."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    queue = mocker.MagicMock(name="queue")
    order: list[str] = []
    queue.close.side_effect = lambda: order.append("queue-close")
    snapshot_manager.close.side_effect = lambda: order.append("snapshot-release")
    queue_manager.close.side_effect = lambda: order.append("queue-release")
    scheduler = BackendScheduler(
        connection_manager=queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    scheduler.close("test-finished")
    scheduler.close("duplicate-close")

    assert order == ["queue-close", "snapshot-release", "queue-release"]
    snapshot_manager.close.assert_called_once_with()
    queue_manager.close.assert_called_once_with()


def test_untyped_queue_close_interruption_still_releases_owned_managers(mocker) -> None:
    """A plain mock interruption is not classified as a checkpoint failure."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    queue = mocker.MagicMock(name="queue")
    queue.close.side_effect = KeyboardInterrupt("queue close interrupted")
    scheduler = BackendScheduler(
        connection_manager=queue_manager,
        snapshot_connection_manager=snapshot_manager,
        owns_snapshot_connection_manager=True,
    )
    scheduler._queue = queue

    with pytest.raises(KeyboardInterrupt, match="queue close interrupted"):
        scheduler.close("test-finished")

    snapshot_manager.close.assert_called_once_with()
    queue_manager.close.assert_called_once_with()


def test_direct_snapshot_manager_remains_caller_owned(mocker) -> None:
    """Direct scheduler injection does not transfer the external acquire."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    scheduler = BackendScheduler(
        connection_manager=queue_manager,
        snapshot_connection_manager=snapshot_manager,
    )

    scheduler.close("test-finished")

    queue_manager.close.assert_called_once_with()
    snapshot_manager.close.assert_not_called()


class _PairingSpider:
    name = "snapshot-pairing"
    crawler = None


def test_scheduler_persists_kafka_delay_snapshot_through_configured_storage(
    mocker,
) -> None:
    """The configured external manager is used end-to-end for a delayed request."""
    queue_manager = mocker.MagicMock(name="queue-manager")
    queue_manager.get_storage_backend.side_effect = AssertionError(
        "Kafka queue manager must not serve snapshot storage"
    )
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    storage = mocker.MagicMock(name="storage")
    snapshot_manager.get_storage_backend.return_value = storage
    order: list[str] = []
    storage.store.side_effect = lambda _key, _state: order.append("snapshot-store")
    snapshot_manager.close.side_effect = lambda: order.append("snapshot-release")
    queue_manager.close.side_effect = lambda: order.append("queue-release")
    mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        side_effect=[queue_manager, snapshot_manager],
    )
    scheduler = BackendScheduler.from_settings(_settings())

    scheduler.open(_PairingSpider())
    assert scheduler.enqueue_request(
        Request("https://example.com", meta={"delay": 60.0})
    )
    scheduler.close("test-finished")

    queue_manager.get_storage_backend.assert_not_called()
    storage.retrieve.assert_called_once()
    assert storage.store.call_count == 2  # immutable chunk, then authoritative manifest
    assert order == [
        "snapshot-store",
        "snapshot-store",
        "snapshot-release",
        "queue-release",
    ]


def test_scheduler_open_threads_monitor_into_snapshot_manager(mocker) -> None:
    """R55: open() must thread the resolved monitor into the snapshot
    ConnectionManager too, not just the queue manager -- otherwise the snapshot
    backend's connect/disconnect/retry hooks fire against the default
    NullMonitor and its lifecycle is invisible in stats (the identical gap R14-D
    closed for the queue manager, reintroduced in 5c2f7c5's snapshot acquire).
    """
    queue_manager = mocker.MagicMock(name="queue-manager")
    snapshot_manager = mocker.MagicMock(name="snapshot-manager")
    mocker.patch(
        "scrapy_extension.schedule.scheduler.ConnectionManager.get_manager",
        side_effect=[queue_manager, snapshot_manager],
    )
    scheduler = BackendScheduler.from_settings(_settings())

    scheduler.open(_PairingSpider())

    # R14-D path (queue manager) -- already wired.
    queue_manager.set_monitor.assert_called_once()
    # R55: the snapshot manager must receive the same resolved monitor.
    snapshot_manager.set_monitor.assert_called_once()
    scheduler.close("test-finished")
