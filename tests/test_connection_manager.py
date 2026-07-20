"""Tests for connection manager."""

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError


def test_connection_manager_get_manager_singleton():
  """Test that get_manager returns singleton for same params."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS)
  assert manager1 is manager2


def test_connection_manager_close_evicts_from_registry():
  """R1-P1-8: close() must remove the manager from the class-level registry.

  Without eviction, get_manager returns the closed instance on the next call
  — masking state across reconnect cycles and across tests.
  """
  manager = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "close-test-host"}
  )
  assert manager.settings == {"host": "close-test-host"}

  manager.close()

  # Registry no longer contains the key; a new get_manager creates a fresh instance.
  manager_after = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "close-test-host"}
  )
  assert manager_after is not manager


def test_close_bare_instance_does_not_evict_registered_peer():
  """A bare ``ConnectionManager(...)`` (not inserted via get_manager) sharing a
  registry key with a registered peer must NOT evict that peer on close().

  ``close()`` evicts by registry key (``cls._managers.pop(key, None)``). Without
  an identity check, a bare instance constructed in tests — whose key collides
  with a registered manager — silently evicts the peer on close(): the peer
  disappears from the registry while still held by its caller, so the next
  ``get_manager(same key)`` creates a second live manager (split-brain /
  connection leak). The fix guards the pop with ``is self``.
  """
  registered = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "bare-peer-host"}
  )
  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "bare-peer-host"}
  )
  assert ConnectionManager._managers.get(key) is registered

  # Bare instance — NOT inserted via get_manager — sharing the same key.
  bare = ConnectionManager(BackendType.REDIS, {"host": "bare-peer-host"})
  assert bare._users == 0
  bare.close()  # must not raise, must not evict the registered peer

  # The registered peer is STILL in the registry (bare close did not evict it).
  assert ConnectionManager._managers.get(key) is registered, (
    "bare ConnectionManager.close() evicted a registered peer sharing the key"
  )

  # Cleanup.
  registered.close()


def test_connection_manager_clear_registry():
  """R1-P1-8: clear_registry() wipes all managers — for test isolation."""
  ConnectionManager.get_manager(BackendType.REDIS, {"host": "h1"})
  ConnectionManager.get_manager(BackendType.REDIS, {"host": "h2"})
  assert len(ConnectionManager._managers) >= 2

  ConnectionManager.clear_registry()

  assert ConnectionManager._managers == {}


def test_connection_manager_different_params():
  """Test that different params return different managers."""
  manager1 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "localhost"})
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, {"host": "other"})
  assert manager1 is not manager2


def test_connection_manager_create_mongodb_backend(mocker):
  """Test ConnectionManager creates MongoDB backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.mongodb.MongoDBBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.MONGODB)
  backend = manager._create_backend()

  mock_backend.assert_called_once()
  assert backend == mock_instance


def test_connection_manager_create_kafka_backend(mocker):
  """Test ConnectionManager creates Kafka backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.kafka.KafkaBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.KAFKA)
  backend = manager._create_backend()

  mock_backend.assert_called_once()


def test_connection_manager_create_rabbitmq_backend(mocker):
  """Test ConnectionManager creates RabbitMQ backend."""
  mock_backend = mocker.patch("scrapy_extension.backends.rabbitmq.RabbitMQBackend")
  mock_instance = mocker.MagicMock()
  mock_backend.return_value = mock_instance

  manager = ConnectionManager(BackendType.RABBITMQ)
  backend = manager._create_backend()

  mock_backend.assert_called_once()


def test_connection_manager_get_manager_same_settings_order():
  """Same settings with different key order should resolve to same manager."""
  settings_a = {"a": 1, "b": 2}
  settings_b = {"b": 2, "a": 1}

  manager1 = ConnectionManager.get_manager(BackendType.REDIS, settings_a)
  manager2 = ConnectionManager.get_manager(BackendType.REDIS, settings_b)

  assert manager1 is manager2


def test_connection_manager_get_set_backend_not_supported(mocker):
  """get_set_backend should raise NotImplementedError for unsupported backend."""
  manager = ConnectionManager(BackendType.KAFKA)
  # We need to set _backend to something that is not a SetBackend but is a Backend subclass
  mock_backend = mocker.MagicMock()
  mock_backend.is_connected.return_value = True
  manager._backend = mock_backend

  with pytest.raises(NotImplementedError):
    manager.get_set_backend()


def test_attempt_connection_calls_disconnect_on_failure(mocker):
  """R25-A1: failed connect() must release backend resources (pools, sockets).

  Without this guard, each retry leaks one Redis/MongoDB connection pool.
  RedisBackend.connect() allocates the client (and its pool) at line 150,
  then pings at line 151. A ping failure leaves ``self._client`` holding
  an orphaned pool. On retry, ConnectionManager creates a NEW backend
  with a NEW pool; the old one is garbage-collected without ``close()``,
  leaking the pool until the GC finalizer runs (which redis-py doesn't
  guarantee promptly).
  """
  manager = ConnectionManager(BackendType.REDIS)

  mock_backend = mocker.MagicMock()
  mock_backend.connect.side_effect = ConnectionError("ping failed")
  mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

  with pytest.raises(ConnectionError):
    manager._attempt_connection()

  mock_backend.connect.assert_called_once()
  mock_backend.disconnect.assert_called_once()


def test_attempt_connection_disconnect_failure_is_swallowed(mocker):
  """R25-A1: cleanup failures during connect-failure path must not mask the original error.

  If backend.disconnect() itself raises (e.g., broken pipe on attempted
  close), we should still propagate the original connect error, not the
  cleanup error. The operator needs to know the connect failed, not that
  cleanup also failed.
  """
  manager = ConnectionManager(BackendType.REDIS)

  mock_backend = mocker.MagicMock()
  mock_backend.connect.side_effect = ConnectionError("original connect failure")
  mock_backend.disconnect.side_effect = RuntimeError("cleanup also failed")
  mocker.patch.object(manager, "_create_backend", return_value=mock_backend)

  with pytest.raises(ConnectionError, match="original connect failure"):
    manager._attempt_connection()


def test_close_swallows_backend_disconnect_error_and_still_evicts(mocker):
  """R44-A1: close() must not propagate a backend-specific disconnect error.

  R25-A1 hardened the connect-path's disconnect cleanup with
  ``contextlib.suppress(Exception)`` because disconnecting a possibly-broken
  backend can raise anything (OSError from the socket layer, a
  backend-specific error the backend's own disconnect didn't swallow).
  ``close()`` faced the identical scenario but caught only
  ``(RuntimeError, ValueError, AttributeError)``. An ``OSError`` (or any
  backend exception outside that tuple) propagated out of close(), skipped
  the registry-eviction code that runs after the try/finally, and broke the
  caller's close chain (scheduler.close, _on_spider_closed). Now catches
  ``Exception`` so close() always completes cleanup — matching R25-A1.
  """
  # Register via get_manager so the eviction branch is exercisable. Unique
  # host isolates this test's registry key from other tests.
  manager = ConnectionManager.get_manager(
    BackendType.REDIS, {"host": "r44-close-error-test"}
  )

  mock_backend = mocker.MagicMock()
  # OSError is NOT a subclass of (RuntimeError, ValueError, AttributeError),
  # so the old narrow tuple would let it propagate out of close().
  mock_backend.disconnect.side_effect = OSError("broken pipe during close")
  manager._backend = mock_backend

  # Must not raise.
  manager.close()

  # Cleanup completed despite the disconnect error.
  assert manager._backend is None
  # Registry evicted even though disconnect raised (the code path after the
  # try/finally — the part the old bug skipped).
  key = ConnectionManager._registry_key(
    BackendType.REDIS, {"host": "r44-close-error-test"}
  )
  assert key not in ConnectionManager._managers


def test_connect_retry_sleep_outside_backend_lock(mocker):
  """A2: ``time.sleep`` during retry backoff must NOT be called while
  ``_lock`` is held.

  The old ``backend`` property held ``self._lock`` across the entire
  ``connect()`` call, and ``connect()`` calls ``time.sleep`` between retry
  attempts. That blocks every peer thread sharing the manager — even ones
  that would have found ``_backend`` already populated. The fix separates
  the fast connected-check (lock-free read) from the slow connect path so
  the lock is never held across ``time.sleep``.

  This test is a behavioral guard: if the implementation regresses to
  holding the lock across the retry sleep, the mock ``time.sleep`` will be
  observed to run while a separate ``_lock.acquire`` is pending in another
  thread, surfacing the contention. The simpler structural assertion here:
  ``connect()`` performs its retries WITHOUT owning ``_lock``.
  """

  manager = ConnectionManager(
    BackendType.REDIS, {"retry_attempts": 3, "retry_delay": 0.01}
  )

  # Force _create_backend to keep failing so all retry attempts fire and
  # each calls time.sleep.
  mocker.patch.object(
    ConnectionManager,
    "_create_backend",
    side_effect=ConnectionError("transient"),
  )
  mock_sleep = mocker.patch("scrapy_extension.backends.connectors.time.sleep")

  # Track whether _lock is held at the moment time.sleep is invoked.
  lock_held_during_sleep: list[bool] = []

  def sleep_observer(_delay):
    # Try to acquire the lock non-blockingly. If it's held by the connect
    # path (the bug), this returns False.
    acquired = manager._lock.acquire(blocking=False)
    lock_held_during_sleep.append(not acquired)
    if acquired:
      manager._lock.release()

  mock_sleep.side_effect = sleep_observer

  # Call connect() directly (not via the backend property) — the fix is
  # about connect() not holding the lock across sleep. connect() itself
  # must be lock-free on the retry path.
  with pytest.raises(Exception, match="Failed to connect"):  # noqa: B017 - testing retry exhaustion
    manager.connect()

  assert mock_sleep.call_count == 3  # 1 initial attempt + 3 retries
  # The load-bearing assertion: _lock must NOT be held during any sleep.
  assert lock_held_during_sleep, "time.sleep was never observed"
  assert not any(lock_held_during_sleep), (
    "_lock was held across time.sleep during retry backoff — this blocks "
    "peer threads sharing the manager. connect() must run its retry loop "
    "without holding _lock."
  )


@pytest.mark.parametrize(
  ("settings", "setting_name"),
  [
    ({"retry_attempts": -1}, "retry_attempts"),
    ({"retry_attempts": 21}, "retry_attempts"),
    ({"retry_attempts": True}, "retry_attempts"),
    ({"retry_attempts": "many"}, "retry_attempts"),
    ({"retry_delay": -0.1}, "retry_delay"),
    ({"retry_delay": float("inf")}, "retry_delay"),
    ({"retry_delay": True}, "retry_delay"),
    ({"manager_retry_attempts": -1}, "retry_attempts"),
    ({"manager_retry_delay": float("nan")}, "retry_delay"),
  ],
)
def test_connect_rejects_invalid_retry_policy_before_backend_creation(
  mocker, settings, setting_name
):
  manager = ConnectionManager(BackendType.REDIS, settings)
  create_backend = mocker.patch.object(manager, "_create_backend")

  with pytest.raises(ConfigurationError) as exc_info:
    manager.connect()

  assert exc_info.value.setting_name == setting_name
  create_backend.assert_not_called()


def test_connect_normalizes_string_retry_policy(mocker):
  manager = ConnectionManager(
    BackendType.REDIS,
    {"retry_attempts": "0", "retry_delay": "0.25"},
  )
  backend = mocker.MagicMock()
  create_backend = mocker.patch.object(
    manager,
    "_create_backend",
    return_value=backend,
  )

  manager.connect()

  create_backend.assert_called_once_with()
  assert manager._backend is backend


def test_connect_does_not_retry_configuration_errors(mocker):
  """Static configuration cannot recover through network retry backoff."""
  manager = ConnectionManager(
    BackendType.REDIS,
    {"retry_attempts": 3, "retry_delay": 0.25},
  )
  attempt = mocker.patch.object(
    manager,
    "_attempt_connection",
    side_effect=ConfigurationError(
      "invalid backend setting",
      setting_name="host",
    ),
  )
  sleep = mocker.patch("scrapy_extension.backends.connectors.time.sleep")

  with pytest.raises(ConfigurationError, match="invalid backend setting"):
    manager.connect()

  attempt.assert_called_once_with()
  sleep.assert_not_called()


def test_backend_property_concurrent_first_connect_single_connect(mocker):
  """A2 + thread-safety: when N threads hit the ``backend`` property
  concurrently on a fresh manager, exactly ONE ``connect()`` runs and all
  threads see the same connected backend. The fast lock-free read must not
  let two threads both enter the slow path.

  This pins both the double-checked-locking invariant AND that the lock is
  released between the connect path's retry sleeps (so peers aren't
  serialized on the backoff).
  """
  import threading

  manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 1})
  mock_backend = mocker.MagicMock()
  mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)
  mocker.patch("scrapy_extension.backends.connectors.time.sleep")

  n = 15
  barrier = threading.Barrier(n)
  results: list[object] = []
  errors: list[BaseException] = []

  def worker():
    try:
      barrier.wait()
      results.append(manager.backend)
    except BaseException as e:  # noqa: BLE001
      errors.append(e)

  threads = [threading.Thread(target=worker) for _ in range(n)]
  for t in threads:
    t.start()
  for t in threads:
    t.join()

  assert errors == []
  assert len(results) == n
  # Every thread observed the same connected backend.
  assert all(r is mock_backend for r in results)
  # Exactly one _create_backend call (single connect).
  assert ConnectionManager._create_backend.call_count == 1


def test_direct_concurrent_connect_calls_create_one_backend(mocker):
  """Public ``connect()`` calls share one connection attempt.

  ``BackendSpiderMixin`` invokes ``ConnectionManager.connect()`` directly from
  the ``spider_opened`` signal, so the single-connect guarantee cannot live
  only in the lazy ``backend`` property. Block the first caller inside the
  backend factory: a racing direct caller must not enter the factory and build
  a second connection that would overwrite (and leak) the first one.
  """
  import threading

  manager = ConnectionManager(BackendType.REDIS, {"retry_attempts": 0})
  first_factory_entered = threading.Event()
  release_first_factory = threading.Event()
  second_factory_entered = threading.Event()
  factory_lock = threading.Lock()
  factory_calls = 0
  backends = [mocker.MagicMock(name="backend-one"), mocker.MagicMock(name="backend-two")]

  def create_backend():
    nonlocal factory_calls
    with factory_lock:
      factory_calls += 1
      call_number = factory_calls
    if call_number == 1:
      first_factory_entered.set()
      assert release_first_factory.wait(timeout=2.0)
    else:
      second_factory_entered.set()
    return backends[call_number - 1]

  mocker.patch.object(manager, "_create_backend", side_effect=create_backend)
  errors: list[BaseException] = []

  def connect() -> None:
    try:
      manager.connect()
    except BaseException as exc:  # noqa: BLE001 - surface thread failures
      errors.append(exc)

  first = threading.Thread(target=connect, daemon=True)
  second = threading.Thread(target=connect, daemon=True)
  first.start()
  assert first_factory_entered.wait(timeout=2.0)
  second.start()
  try:
    assert not second_factory_entered.wait(timeout=0.2)
  finally:
    release_first_factory.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

  assert not first.is_alive()
  assert not second.is_alive()
  assert errors == []
  assert factory_calls == 1
  assert manager._backend is backends[0]


# ===========================================================================
# R14-E — Lifecycle bounds (long-run leak prevention)
# ===========================================================================


def test_managers_registry_capped_under_settings_churn(mocker):
  """R14-E HIGH: settings churn must not grow ``_managers`` unbounded.

  A crawler with rotating per-spider credentials / unique ``group_id``
  produces a fresh registry entry per distinct settings dict. Without a
  cap, prior entries (each holding a live ``Backend`` + open sockets)
  linger forever. The registry is now an LRU ``OrderedDict`` capped at
  ``MAX_MANAGERS``; on overflow the oldest *genuinely-orphaned* entry
  (``_users <= 0``) is evicted and its backend disconnected.
  """
  ConnectionManager.clear_registry()
  cap = ConnectionManager.MAX_MANAGERS
  # Patch _create_backend so we don't touch the network; track disconnect
  # calls so we can assert the victim was torn down.
  mock_backends: list = []
  disconnected: list = []

  class _FakeBackend:
    def __init__(self) -> None:
      mock_backends.append(self)

    def connect(self) -> None:
      pass

    def disconnect(self) -> None:
      disconnected.append(self)

    def is_connected(self) -> bool:
      return True

  mocker.patch.object(
    ConnectionManager, "_create_backend", side_effect=lambda: _FakeBackend()
  )

  n = 64  # double the cap
  try:
    for i in range(n):
      # Each iteration: acquire a manager with distinct settings, force
      # its backend to materialize (so disconnect has something to tear
      # down), then release. The entry becomes orphaned (``_users <= 0``)
      # and is eligible for LRU eviction on the NEXT insert.
      mgr = ConnectionManager.get_manager(
        BackendType.REDIS, {"host": f"churn-{i}"}
      )
      _ = mgr.backend  # materialize the backend
      mgr.close()
    # Registry must be at-or-under the cap after the churn.
    assert len(ConnectionManager._managers) <= cap, (
      f"registry grew to {len(ConnectionManager._managers)} > cap {cap} "
      "under settings churn — LRU eviction did not fire"
    )
    # At least one victim's backend was disconnected (many more, in fact).
    assert len(disconnected) > 0, "no orphaned manager was disconnected"
  finally:
    ConnectionManager.clear_registry()
    mocker.stopall()


def test_managers_registry_does_not_evict_actively_held_manager(mocker):
  """R14-E CRITICAL: an actively-held manager (``_users > 0``) is never evicted.

  Force-eviction would corrupt the holder's connection. When the cap is
  reached with ALL entries live, we stop evicting and warn-once instead.
  """
  ConnectionManager.clear_registry()
  cap = ConnectionManager.MAX_MANAGERS
  mocker.patch.object(
    ConnectionManager,
    "_create_backend",
    return_value=mocker.MagicMock(),
  )
  held_managers: list = []
  try:
    # Acquire ``cap`` distinct managers WITHOUT closing them — all live.
    for i in range(cap):
      held_managers.append(
        ConnectionManager.get_manager(BackendType.REDIS, {"host": f"live-{i}"})
      )
    assert len(ConnectionManager._managers) == cap
    # All are actively held.
    assert all(m._users > 0 for m in ConnectionManager._managers.values())

    # Acquire one MORE — would normally trigger eviction, but every entry
    # is live, so the new one is added without evicting any holder.
    extra = ConnectionManager.get_manager(
      BackendType.REDIS, {"host": "extra-live"}
    )
    held_managers.append(extra)
    # Registry is now over cap (cap + 1) — but NO held manager was evicted.
    assert len(ConnectionManager._managers) == cap + 1
    # Every originally-held manager is still in the registry.
    for m in held_managers[:-1]:
      assert m._users > 0
      assert m in ConnectionManager._managers.values()
  finally:
    # Release every holder so teardown is clean.
    for m in held_managers:
      m.close()
    ConnectionManager.clear_registry()
    mocker.stopall()


def test_close_resets_circuit_breaker(mocker):
  """R14-E MED: ``close()`` resets the breaker so a reconnect doesn't inherit stale OPEN state.

  Without reset, an orphan-evicted or torn-down manager whose breaker had
  tripped OPEN would leave the breaker stuck OPEN — and since the breaker
  is per-manager, a fresh manager created from the same settings inherits
  nothing (good), but a manager that reconnects after teardown (kept alive
  by an external ref) would stay OPEN forever.
  """
  ConnectionManager.clear_registry()
  mock_backend = mocker.MagicMock()
  mocker.patch.object(ConnectionManager, "_create_backend", return_value=mock_backend)

  mgr = ConnectionManager.get_manager(BackendType.REDIS, {"host": "breaker-test"})
  _ = mgr.backend  # materialize
  # Manually construct + trip the breaker to simulate a failure run.
  from scrapy_extension.backends.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
  )

  breaker = CircuitBreaker(name="test", failure_threshold=1)
  # Trip it: one failure crosses the threshold.
  try:
    breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
  except RuntimeError:
    pass
  assert breaker.state is BreakerState.OPEN
  mgr._breaker = breaker
  mgr._breaker_configured = True

  mgr.close()

  assert breaker.state is BreakerState.CLOSED, (
    "close() did not reset the circuit breaker — a reconnecting manager "
    "would inherit a stale OPEN state"
  )
  ConnectionManager.clear_registry()
  mocker.stopall()



# ---------------------------------------------------------------------------
# R14-G: A2 single-connect-owner error-signal threading test.
#
# ``_get_backend`` splits fast/slow paths: the first thread to enter the slow
# path takes ownership of connecting; peers wait on ``_connected_event``
# (released by the owner in a ``finally``). The load-bearing invariant: if the
# owner's ``connect()`` raises, ALL peer waiters must (a) receive the same
# exception and (b) NOT hang — ``_connected_event.set()`` must run in the
# ``finally`` block so peers wake up.
# ---------------------------------------------------------------------------


def test_owner_connect_failure_signals_all_peer_waiters(mocker):
  """Owner's ``connect()`` raises → every peer waiter re-raises + event is set.

  Constructs a manager whose backend ``connect()`` always raises, then calls
  ``get_queue_backend()`` from multiple threads simultaneously. Without the
  A2 finally-set, peers would block on ``_connected_event.wait()`` forever
  (the test would hang). With it, every peer re-raises the owner's exception.
  """
  import threading

  # Unique settings so this manager is isolated in the class-level registry.
  settings = {"retry_attempts": 1, "retry_delay": 0, "host": "owner-fail-test"}
  manager = ConnectionManager.get_manager(BackendType.REDIS, settings)
  ConnectionManager.clear_registry()  # evict the just-created empty shell

  # Re-create so we control it directly, then patch its backend factory.
  manager = ConnectionManager.get_manager(BackendType.REDIS, settings)

  connect_error = BackendConnectionError(
    "simulated owner connect failure", backend_type="redis"
  )

  class _FailingBackend:
    def __init__(self, *_args, **_kwargs) -> None:
      self.backend_type = "redis"

    def connect(self) -> None:
      raise connect_error

    def disconnect(self) -> None:
      pass

    def is_connected(self) -> bool:
      return False

    def ping(self) -> bool:
      return False

  mocker.patch.object(manager, "_create_backend", return_value=_FailingBackend())

  results: dict[str, object] = {}
  start_gate = threading.Event()
  errors: list[BaseException] = []
  errors_lock = threading.Lock()

  def _worker(worker_id: str) -> None:
    # Wait for the green light so all workers race into _get_backend together.
    start_gate.wait(timeout=5.0)
    try:
      manager.get_queue_backend()
      results[worker_id] = "no-error"
    except BaseException as exc:  # noqa: BLE001 — capture every peer's outcome
      with errors_lock:
        errors.append(exc)
      results[worker_id] = type(exc).__name__

  threads = [threading.Thread(target=_worker, args=(f"w{i}",)) for i in range(4)]
  for t in threads:
    t.start()
  # Release all workers at once to maximize owner/peer contention.
  start_gate.set()
  for t in threads:
    t.join(timeout=10.0)

  # No thread may still be alive (would mean _connected_event was never set).
  assert not any(t.is_alive() for t in threads), (
    f"a peer waiter hung — _connected_event.set() did not fire in the owner "
    f"finally block. results={results}"
  )

  # Every worker must have received a BackendConnectionError (re-raised by
  # the owner path) — never a silent success and never a different exception
  # type. ``connect()``'s retry loop wraps the raw ``connect_error`` as
  # "Failed to connect after N attempts: ...", so we assert TYPE equality +
  # chain identity (the original ``connect_error`` is preserved in ``__cause__``
  # or the wrapped message) rather than instance identity.
  assert len(errors) == 4, (
    f"expected 4 errors (one per worker), got {len(errors)}; results={results}"
  )
  for exc in errors:
    assert isinstance(exc, BackendConnectionError), (
      f"peer received a non-BackendConnectionError: got {exc!r}"
    )
    # The owner's raw connect_error must be visible somewhere in the chain
    # (it is the __cause__ of the wrapped retry-loop exception, or the
    # exception message itself).
    chain_text = ""
    cur: BaseException | None = exc
    while cur is not None:
      chain_text += f"{cur}\n"
      cur = cur.__cause__
    assert "simulated owner connect failure" in chain_text, (
      f"owner's original error not preserved in peer's exception chain: "
      f"{chain_text}"
    )

  # The event must be set (the finally ran) — otherwise a later waiter would
  # hang on the next ``get_queue_backend()`` call.
  assert manager._connected_event.is_set(), (
    "_connected_event not set after owner failure — peers on the next call "
    "would hang (permanent stall after one connect failure)"
  )

  manager.close()
  ConnectionManager.clear_registry()
  mocker.stopall()


def test_last_close_during_connect_cannot_publish_orphan_backend(mocker):
  """A manager evicted while connect is slow must never resurrect afterwards."""
  import threading

  manager = ConnectionManager.get_manager(
    BackendType.REDIS,
    {"host": "close-during-connect", "retry_attempts": 0},
  )
  connect_entered = threading.Event()
  release_connect = threading.Event()
  backend = mocker.MagicMock(name="slow-backend")

  def slow_connect() -> None:
    connect_entered.set()
    assert release_connect.wait(timeout=2.0)

  backend.connect.side_effect = slow_connect
  mocker.patch.object(manager, "_create_backend", return_value=backend)
  outcomes: list[BaseException | object] = []

  def materialize() -> None:
    try:
      outcomes.append(manager.backend)
    except BaseException as exc:  # noqa: BLE001 - capture thread outcome
      outcomes.append(exc)

  thread = threading.Thread(target=materialize, daemon=True)
  thread.start()
  assert connect_entered.wait(timeout=2.0)

  manager.close()
  release_connect.set()
  thread.join(timeout=2.0)

  assert not thread.is_alive()
  assert len(outcomes) == 1
  assert isinstance(outcomes[0], BackendConnectionError)
  assert manager._backend is None
  backend.disconnect.assert_called_once_with()
  key = ConnectionManager._registry_key(manager.backend_type, manager.settings)
  assert key not in ConnectionManager._managers


def test_backend_property_rejects_released_manager(mocker):
  """A warmed manager is terminal after its final holder releases it."""
  manager = ConnectionManager.get_manager(
    BackendType.REDIS,
    {"host": "access-after-close", "retry_attempts": 0},
  )
  backend = mocker.MagicMock(name="connected-backend")
  create_backend = mocker.patch.object(
    manager,
    "_create_backend",
    return_value=backend,
  )

  assert manager.backend is backend
  manager.close()

  with pytest.raises(BackendConnectionError, match="released"):
    _ = manager.backend

  backend.disconnect.assert_called_once_with()
  create_backend.assert_called_once_with()
