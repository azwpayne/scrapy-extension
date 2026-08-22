"""R141-F1: snapshot restore fences persistence on storage resolution failure.

Startup used to resolve snapshot storage non-strictly, so a transient
``get_storage_backend`` failure was indistinguishable from a storage-incapable
backend: ``_restore_snapshot`` returned without the persistence fence. When
storage recovered by close time, ``_persist_snapshot`` then committed the empty
clean-start state over the authoritative manifest and retired a legacy
checkpoint it had never read — every held item of a stateful strategy was
silently lost. These tests pin the corrected contract:

- A resolution failure at startup fences persistence (isomorphic to the
  current-manifest read-failure fence): the manifest and any legacy checkpoint
  stay intact until ``reset_snapshot`` authorizes a replacement.
- A successful restore keeps ordinary close semantics.
- ``NotImplementedError`` (storage-incapable) stays a harmless no-op and never
  fences; a later close may persist normally.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from scrapy_extension.exceptions import QueueError
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.queue.snapshot import SnapshotRepository

_SNAPSHOT_KEY = "queue:snapshot:v3:0::1:q"
_LEGACY_SNAPSHOT_KEY = "queue:snapshot:q"


def _stateful_storage(initial: dict[str, bytes]):
    """Storage mock whose retrieve/store/delete calls mutate shared state."""
    state = dict(initial)
    storage = MagicMock(name="StatefulStorageBackend")
    storage.retrieve.side_effect = lambda key: state.get(key)
    storage.store.side_effect = lambda key, value: state.__setitem__(key, value)
    storage.delete.side_effect = lambda key: state.pop(key, None)
    return storage, state


def _wired_cm(storage: MagicMock) -> MagicMock:
    cm = MagicMock(name="ConnectionManager")
    cm.get_storage_backend.return_value = storage
    cm.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    return cm


def _flaky_resolver_cm(storage: MagicMock, first_error: BaseException) -> MagicMock:
    """Manager whose first ``get_storage_backend`` call fails, then recovers."""
    calls = {"count": 0}

    def resolve() -> Any:
        calls["count"] += 1
        if calls["count"] == 1:
            raise first_error
        return storage

    cm = MagicMock(name="FlakyConnectionManager")
    cm.get_storage_backend.side_effect = resolve
    cm.get_queue_backend.return_value = MagicMock(name="QueueBackend")
    return cm


def _commit_authoritative_checkpoint(storage: MagicMock, state: dict[str, bytes]):
    """Publish a v7 manifest checkpoint and return its committed bytes."""
    SnapshotRepository(storage).commit(_SNAPSHOT_KEY, b"authoritative")
    return state[_SNAPSHOT_KEY]


def test_startup_resolution_failure_fences_close_until_explicit_reset() -> None:
    """A transient startup resolution failure must not license an empty close."""
    storage, state = _stateful_storage({})
    authoritative_manifest = _commit_authoritative_checkpoint(storage, state)
    state[_LEGACY_SNAPSHOT_KEY] = b"legacy checkpoint"
    strategy = MagicMock(name="Strategy")
    strategy.snapshot.return_value = b"replacement"
    cm = _flaky_resolver_cm(storage, RuntimeError("transient resolution failure"))

    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert queue._snapshot_persistence_fenced is True
    strategy.restore.assert_not_called()

    # The fence must survive the storage recovery: an ordinary close cannot
    # overwrite the never-read authoritative manifest or retire the legacy key.
    with pytest.raises(QueueError, match="fenced"):
        queue.close()

    assert state[_SNAPSHOT_KEY] == authoritative_manifest
    assert state[_LEGACY_SNAPSHOT_KEY] == b"legacy checkpoint"
    strategy.snapshot.assert_not_called()
    strategy.close.assert_not_called()

    # Explicit operator recovery may publish a replacement; only then may the
    # never-read legacy checkpoint be retired.
    queue.reset_snapshot()
    queue.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state == b"replacement"
    assert _LEGACY_SNAPSHOT_KEY not in state
    strategy.close.assert_called_once_with()


def test_successful_restore_keeps_ordinary_close_semantics() -> None:
    """Control: a healthy startup restore neither fences nor blocks close."""
    storage, state = _stateful_storage({})
    _commit_authoritative_checkpoint(storage, state)
    strategy = MagicMock(name="Strategy")
    strategy.snapshot.return_value = b"replacement"
    cm = _wired_cm(storage)

    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert queue._snapshot_persistence_fenced is False
    strategy.restore.assert_called_once_with(b"authoritative")

    queue.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state == b"replacement"
    strategy.close.assert_called_once_with()


def test_storage_incapable_startup_and_close_is_a_harmless_noop() -> None:
    """A storage-incapable backend never fences: startup no-op, clean close."""
    strategy = MagicMock(name="Strategy")
    strategy.snapshot.return_value = None
    cm = MagicMock(name="ConnectionManager")
    cm.get_storage_backend.side_effect = NotImplementedError("queue-only backend")
    cm.get_queue_backend.return_value = MagicMock(name="QueueBackend")

    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert queue._snapshot_persistence_fenced is False
    strategy.restore.assert_not_called()

    queue.close()

    strategy.close.assert_called_once_with()
    assert queue._close_complete is True


def test_incapable_startup_does_not_fence_a_later_persisting_close() -> None:
    """``NotImplementedError`` means storage-incapable, not unreachable state.

    Unlike a resolution failure, an incapable startup claim carries no
    authoritative checkpoint to protect, so a close that can resolve storage
    persists normally without ``reset_snapshot``.
    """
    storage, state = _stateful_storage({})
    _commit_authoritative_checkpoint(storage, state)
    strategy = MagicMock(name="Strategy")
    strategy.snapshot.return_value = b"replacement"
    cm = _flaky_resolver_cm(storage, NotImplementedError("queue-only backend"))

    queue = BackendQueue(
        connection_manager=cm,
        queue_name="q",
        queue_strategy=strategy,
        monitor=MagicMock(),
    )

    assert queue._snapshot_persistence_fenced is False
    strategy.restore.assert_not_called()

    queue.close()

    assert SnapshotRepository(storage).read(_SNAPSHOT_KEY).state == b"replacement"
    strategy.close.assert_called_once_with()
