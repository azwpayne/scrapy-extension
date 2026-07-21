"""Tests for BackendSpiderMixin."""

import os
import threading
from unittest.mock import Mock

import pytest
from scrapy import Spider, signals

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.exceptions import ConfigurationError
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

# Redis password fixture - use env var to avoid S105 warnings
REDIS_PASSWORD = os.environ.get("TEST_REDIS_PASSWORD", "test_password_placeholder")


class _ObservedLock:
  """RLock wrapper that exposes when a second lifecycle operation arrives."""

  def __init__(self) -> None:
    self._lock = threading.RLock()
    self._counter_lock = threading.Lock()
    self._attempts = 0
    self.second_attempted = threading.Event()

  def __enter__(self):
    with self._counter_lock:
      self._attempts += 1
      if self._attempts == 2:
        self.second_attempted.set()
    self._lock.acquire()
    return self

  def __exit__(self, *_exc_info):
    self._lock.release()


class TestBackendSpiderMixinInit:
  """Test BackendSpiderMixin.__init__."""

  def test_init_sets_connection_manager_to_none(self):
    """Test that __init__ initializes _connection_manager to None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    assert spider._connection_manager is None

  def test_init_sets_queue_to_none(self):
    """Test that __init__ initializes _queue to None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    assert spider._queue is None

  def test_init_sets_dupefilter_to_none(self):
    """Test that __init__ initializes _dupefilter to None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    assert spider._dupefilter is None

  def test_init_sets_scheduler_to_none(self):
    """Test that __init__ initializes _scheduler to None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    assert spider._scheduler is None

  def test_init_does_not_raise(self):
    """Test that __init__ does not raise when called properly."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    # Should not raise
    spider = TestSpider()
    assert spider.name == "test_spider"


class TestBackendSpiderMixinFromCrawler:
  def test_configured_spider_sets_up_after_crawler_attachment(self, mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=manager,
    )
    crawler = mocker.MagicMock()

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider.from_crawler(crawler)

    assert spider.crawler is crawler
    assert spider._connection_manager is manager
    acquire.assert_called_once_with(backend_type=BackendType.REDIS, settings={})
    crawler.signals.connect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    crawler.signals.connect.assert_any_call(
      spider._on_spider_closed, signals.spider_closed
    )

  def test_early_setup_is_finalized_without_second_acquire(self, mocker):
    manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=manager,
    )
    crawler = mocker.MagicMock()

    class EarlySetupSpider(BackendSpiderMixin, Spider):
      name = "early_setup_spider"
      backend_type = BackendType.REDIS

      def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setup_backend()

    spider = EarlySetupSpider.from_crawler(crawler)

    assert spider._connection_manager is manager
    acquire.assert_called_once_with(backend_type=BackendType.REDIS, settings={})
    crawler.signals.connect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    crawler.signals.connect.assert_any_call(
      spider._on_spider_closed, signals.spider_closed
    )

  def test_unconfigured_spider_does_not_implicitly_acquire(self, mocker):
    acquire = mocker.patch.object(ConnectionManager, "get_manager")
    crawler = mocker.MagicMock()

    class TestSpider(BackendSpiderMixin, Spider):
      name = "plain_spider"

    spider = TestSpider.from_crawler(crawler)

    assert spider._connection_manager is None
    acquire.assert_not_called()


class TestSetupBackend:
  """Test setup_backend method."""

  def test_setup_backend_success(self, mocker):
    """Test successful setup_backend call.

    2026-07-10 (§C): setup_backend now acquires via the singleton accessor
    ``ConnectionManager.get_manager`` (not the constructor), so patch that.
    """
    mock_manager = mocker.MagicMock(spec=ConnectionManager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    result = spider.setup_backend()

    assert result is mock_manager
    assert spider._connection_manager is mock_manager

  def test_setup_backend_uses_singleton_get_manager(self, mocker):
    """2026-07-10 (DEEP-INSIGHT-2026-07-10 §C): setup_backend must acquire via
    ``ConnectionManager.get_manager`` (the refcounted singleton registry), NOT
    construct ``ConnectionManager(...)`` directly. Direct construction
    bypasses the registry, defeating refcounting + LRU eviction and leaving
    the spider outside the co-located-sharing model.

    RED pre-fix: setup_backend calls the constructor directly, so the patched
    ``get_manager`` is never invoked → ``call_count == 0`` and the returned
    manager is a real ConnectionManager (not the mock) → both asserts fail.
    """
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    get_manager_spy = mocker.patch.object(
      ConnectionManager, "get_manager", return_value=mock_manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    result = spider.setup_backend()

    assert result is mock_manager
    assert get_manager_spy.call_count == 1

  def test_consumer_backend_scope_is_unique_per_spider_instance(self) -> None:
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.KAFKA

    first = TestSpider()
    second = TestSpider()
    first_manager = first.setup_backend()
    second_manager = second.setup_backend()
    try:
      assert first_manager is not second_manager
    finally:
      first.close_backend()
      second.close_backend()

  def test_consumer_backend_rejects_second_logical_queue(self, mocker) -> None:
    manager = mocker.MagicMock(spec=ConnectionManager)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.KAFKA

    spider = TestSpider()
    spider.setup_backend()
    first = spider.get_queue("first-queue")

    with pytest.raises(ConfigurationError, match="one logical consumer queue"):
      spider.get_queue("second-queue")

    assert spider.get_queue("first-queue") is first

  def test_consumer_backend_queue_and_scheduler_must_share_name(self, mocker) -> None:
    manager = mocker.MagicMock(spec=ConnectionManager)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ

    spider = TestSpider()
    spider.setup_backend()
    spider.get_queue("custom-queue")

    with pytest.raises(ConfigurationError, match="one logical consumer queue"):
      spider.get_scheduler()

  def test_setup_backend_is_idempotent(self, mocker):
    """Repeated setup must not leak a manager acquire."""
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    get_manager = mocker.patch.object(
      ConnectionManager, "get_manager", return_value=mock_manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    first = spider.setup_backend()
    second = spider.setup_backend()

    assert first is mock_manager
    assert second is mock_manager
    get_manager.assert_called_once()

  def test_setup_backend_rolls_back_acquire_when_signal_wiring_fails(self, mocker):
    """A half-wired spider must not retain a manager or stale signal handler."""
    manager = mocker.MagicMock(spec=ConnectionManager)
    get_manager = mocker.patch.object(
      ConnectionManager, "get_manager", return_value=manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    signal_manager = mocker.MagicMock()
    signal_manager.connect.side_effect = [None, RuntimeError("closed signal bus")]
    spider.crawler = mocker.MagicMock(signals=signal_manager)

    with pytest.raises(RuntimeError, match="closed signal bus"):
      spider.setup_backend()

    get_manager.assert_called_once_with(backend_type=BackendType.REDIS, settings={})
    manager.close.assert_called_once_with()
    signal_manager.disconnect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    assert spider._connection_manager is None
    assert spider._signals_connected is False

  def test_setup_backend_first_signal_failure_has_nothing_to_disconnect(self, mocker):
    """Rollback touches only handlers whose connect call completed."""
    manager = mocker.MagicMock(spec=ConnectionManager)
    mocker.patch.object(ConnectionManager, "get_manager", return_value=manager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    signal_manager = mocker.MagicMock()
    signal_manager.connect.side_effect = RuntimeError("signal bus unavailable")
    spider.crawler = mocker.MagicMock(signals=signal_manager)

    with pytest.raises(RuntimeError, match="signal bus unavailable"):
      spider.setup_backend()

    signal_manager.disconnect.assert_not_called()
    manager.close.assert_called_once_with()
    assert spider._connection_manager is None
    assert spider._connected_signals is None
    assert spider._signals_connected is False

  def test_concurrent_setup_acquires_manager_once(self, mocker):
    """Concurrent setup calls pair with exactly one registry acquire."""
    import threading

    manager = mocker.MagicMock(spec=ConnectionManager)
    factory_entered = threading.Event()
    release_factory = threading.Event()

    def get_manager(**_kwargs):
      factory_entered.set()
      assert release_factory.wait(timeout=2.0)
      return manager

    acquire = mocker.patch.object(
      ConnectionManager, "get_manager", side_effect=get_manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    results: list[ConnectionManager] = []
    errors: list[BaseException] = []

    def setup() -> None:
      try:
        results.append(spider.setup_backend())
      except BaseException as exc:  # noqa: BLE001 - surface worker failure
        errors.append(exc)

    first = threading.Thread(target=setup, daemon=True)
    second = threading.Thread(target=setup, daemon=True)
    first.start()
    assert factory_entered.wait(timeout=2.0)
    second.start()
    release_factory.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert errors == []
    assert results == [manager, manager]
    acquire.assert_called_once_with(backend_type=BackendType.REDIS, settings={})

  def test_setup_backend_connects_signals_after_crawler_is_attached(self, mocker):
    """A later setup wires signals without acquiring the manager again."""
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    get_manager = mocker.patch.object(
      ConnectionManager, "get_manager", return_value=mock_manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    first = spider.setup_backend()

    mock_crawler = mocker.MagicMock()
    spider.crawler = mock_crawler
    second = spider.setup_backend()
    third = spider.setup_backend()

    assert first is mock_manager
    assert second is mock_manager
    assert third is mock_manager
    get_manager.assert_called_once()
    assert mock_crawler.signals.connect.call_count == 2
    mock_crawler.signals.connect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    mock_crawler.signals.connect.assert_any_call(
      spider._on_spider_closed, signals.spider_closed
    )

  def test_setup_backend_moves_signals_to_replacement_crawler(self, mocker):
    """Changing crawler without closing first must detach the old dispatcher."""
    manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
      ConnectionManager, "get_manager", return_value=manager
    )

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    first_signals = mocker.MagicMock()
    second_signals = mocker.MagicMock()
    spider.crawler = mocker.MagicMock(signals=first_signals)
    spider.setup_backend()

    spider.crawler = mocker.MagicMock(signals=second_signals)
    assert spider.setup_backend() is manager

    acquire.assert_called_once_with(backend_type=BackendType.REDIS, settings={})
    assert first_signals.disconnect.call_count == 2
    assert second_signals.connect.call_count == 2
    assert spider._connected_signals is second_signals

  def test_setup_backend_shares_singleton_across_spiders(self):
    """2026-07-11 (§C intent, no mocks): two spiders with identical backend
    config must acquire the SAME ConnectionManager via the singleton registry.
    This is the actual purpose of routing ``setup_backend`` through
    ``get_manager`` — co-located sharing + refcounting + LRU. The call-site
    test above only proves the accessor NAME is used; this one proves the
    sharing semantics end-to-end against the real registry.
    """
    from scrapy_extension.backends.connectors import ConnectionManager

    class SharedSpiderA(BackendSpiderMixin, Spider):
      name = "shared_singleton_a"
      backend_type = BackendType.REDIS
      redis_db = 97  # distinctive settings → distinctive registry key

    class SharedSpiderB(BackendSpiderMixin, Spider):
      name = "shared_singleton_b"
      backend_type = BackendType.REDIS
      redis_db = 97  # identical → same registry key

    spider1 = SharedSpiderA()
    spider2 = SharedSpiderB()
    try:
      cm1 = spider1.setup_backend()
      cm2 = spider2.setup_backend()
      # Singleton: same backend_type:settings_hash → same instance.
      assert cm1 is cm2
      # Two acquires → refcount at least 2 (robust to any pre-existing entry).
      assert cm1._users >= 2
      assert isinstance(cm1, ConnectionManager)
    finally:
      # Release both so the registry entry evicts (no cross-test pollution).
      spider1.close_backend()
      spider2.close_backend()

  def test_setup_backend_without_backend_type_raises(self):
    """Test that setup_backend raises RuntimeError when backend_type is None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="backend_type must be set"):
      spider.setup_backend()

  def test_setup_backend_raises_with_custom_error_message(self):
    """Test that the error message includes the spider class name."""

    class MyCustomSpider(BackendSpiderMixin, Spider):
      name = "my_custom_spider"

    spider = MyCustomSpider()

    with pytest.raises(RuntimeError, match="MyCustomSpider.backend_type must be set"):
      spider.setup_backend()

  def test_setup_backend_builds_settings_from_redis_shortcuts(self):
    """Test that setup_backend builds settings from Redis shortcut attributes."""
    # We verify settings by checking what gets passed to ConnectionManager

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      redis_host = "localhost"
      redis_port = 6379
      redis_db = 1
      redis_password = REDIS_PASSWORD

    spider = TestSpider()
    # Mock the settings building process directly
    result = spider._build_backend_settings()
    assert result["host"] == "localhost"
    assert result["port"] == 6379
    assert result["db"] == 1
    assert result["password"] == REDIS_PASSWORD

  def test_setup_backend_merges_explicit_backend_settings(self):
    """Test that explicit backend_settings are merged with shortcuts."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      backend_settings = {"custom_key": "custom_value", "host": "override_host"}
      redis_host = "shortcut_host"
      redis_port = 6379

    spider = TestSpider()
    result = spider._build_backend_settings()
    # Explicit settings should be overridden by shortcuts
    assert result["host"] == "shortcut_host"
    assert result["port"] == 6379
    assert result["custom_key"] == "custom_value"

  def test_setup_backend_calls_connect_signals(self):
    """Test that setup_backend connects Scrapy signals."""
    # We can't easily test the actual signal connection without a real crawler,
    # but we verify _build_backend_settings works (called by setup_backend)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      redis_host = "localhost"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["host"] == "localhost"


class TestBuildRedisSettings:
  """Test _build_redis_settings method."""

  def test_returns_empty_dict_when_no_shortcuts(self):
    """Test that _build_redis_settings returns {} when no shortcuts are set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result == {}

  def test_includes_host_when_set(self):
    """Test that host is included when set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      redis_host = "localhost"

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result["host"] == "localhost"

  def test_includes_port_when_set(self):
    """Test that port is included when set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      redis_port = 6379

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result["port"] == 6379

  def test_includes_db_when_set(self):
    """Test that db is included when set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      redis_db = 2

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result["db"] == 2

  def test_includes_password_when_set(self):
    """Test that password is included when set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      redis_password = REDIS_PASSWORD

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result["password"] == REDIS_PASSWORD

  def test_includes_all_shortcuts_together(self):
    """Test that all Redis shortcuts are included when all are set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      redis_host = "localhost"
      redis_port = 6379
      redis_db = 0
      redis_password = REDIS_PASSWORD

    spider = TestSpider()
    result = spider._build_redis_settings()
    assert result == {
      "host": "localhost",
      "port": 6379,
      "db": 0,
      "password": REDIS_PASSWORD,
    }


class TestBuildBackendSettings:
  """Test _build_backend_settings method."""

  def test_returns_empty_dict_when_no_settings(self):
    """Test that _build_backend_settings returns {} when no settings are configured."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result == {}

  def test_redis_type_uses_redis_shortcuts(self):
    """Test that Redis backend type uses Redis shortcut settings."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      redis_host = "redis.example.com"
      redis_port = 6380

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["host"] == "redis.example.com"
    assert result["port"] == 6380

  def test_mongodb_type_uses_mongodb_shortcuts(self):
    """Test that MongoDB backend type uses MongoDB shortcut settings."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.MONGODB
      mongodb_uri = "mongodb://localhost:27017"
      mongodb_db = "scrapy"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["uri"] == "mongodb://localhost:27017"
    assert result["database"] == "scrapy"

  def test_mongodb_uri_optional(self):
    """Test that MongoDB uri is optional."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.MONGODB
      mongodb_db = "scrapy"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert "uri" not in result
    assert result["database"] == "scrapy"

  def test_mongodb_db_optional(self):
    """Test that MongoDB database is optional."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.MONGODB
      mongodb_uri = "mongodb://localhost:27017"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["uri"] == "mongodb://localhost:27017"
    assert "database" not in result

  def test_kafka_type_uses_kafka_shortcuts(self):
    """Test that Kafka backend type uses Kafka shortcut settings."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.KAFKA
      kafka_bootstrap_servers = "kafka1:9092,kafka2:9092"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["bootstrap_servers"] == "kafka1:9092,kafka2:9092"

  def test_kafka_bootstrap_servers_optional(self):
    """Test that Kafka bootstrap_servers is optional."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.KAFKA

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert "bootstrap_servers" not in result

  def test_rabbitmq_type_uses_rabbitmq_shortcuts(self):
    """Test that RabbitMQ backend type uses RabbitMQ shortcut settings."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.RABBITMQ
      rabbitmq_url = "amqp://guest:guest@localhost:5672/"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["url"] == "amqp://guest:guest@localhost:5672/"

  def test_rabbitmq_url_optional(self):
    """Test that RabbitMQ url is optional."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.RABBITMQ

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert "url" not in result

  def test_rabbitmq_does_not_fall_through_to_elasticsearch(self):
    """R43: rabbitmq branch must not fall through to elasticsearch.

    Previously the branch was ``elif backend_value == "rabbitmq" and
    self.rabbitmq_url is not None:`` — the only branch that combined the
    backend guard with a field check. With ``backend_type=RABBITMQ`` and
    ``rabbitmq_url`` unset, the elif was False and control fell into the
    elasticsearch branch, merging ES shortcut attrs into a rabbitmq
    backend. Now branches on backend_type alone, like the other 5 backends.
    """

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.RABBITMQ
      # Cross-contamination: ES attrs set on a rabbitmq spider
      elasticsearch_hosts = ["http://es:9200"]
      elasticsearch_cloud_id = "dep:dXMtY2VudHJhbA=="
      elasticsearch_api_key = "encoded-key"

    spider = TestSpider()
    result = spider._build_backend_settings()
    # RabbitMQ selected -> no ES keys leaked in, no url either
    assert result == {}

  def test_explicit_backend_settings_merged_first(self):
    """Test that explicit backend_settings are merged first."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      backend_settings = {"foo": "bar", "host": "explicit_host"}

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["foo"] == "bar"
    assert result["host"] == "explicit_host"

  def test_shortcuts_override_explicit_settings(self):
    """Test that shortcut attributes override explicit backend_settings."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      backend_settings = {"host": "explicit_host", "port": 9999}
      redis_host = "shortcut_host"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["host"] == "shortcut_host"
    assert result["port"] == 9999

  def test_elasticsearch_shortcuts(self):
    """R24-A1: ElasticSearch backend type now has shortcut attributes.

  Previously the mixin defined shortcuts only for Redis/MongoDB/Kafka/
  RabbitMQ — ES users had to use backend_settings explicitly. R24-A1
  added elasticsearch_hosts / elasticsearch_cloud_id / elasticsearch_api_key
  for symmetry.
  """

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ELASTICSEARCH
      elasticsearch_hosts = ["http://es:9200"]
      elasticsearch_cloud_id = "my-deployment:dXMtY2VudHJhbA=="
      elasticsearch_api_key = "encoded-key"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["hosts"] == ["http://es:9200"]
    assert result["cloud_id"] == "my-deployment:dXMtY2VudHJhbA=="
    assert result["api_key"] == "encoded-key"

  def test_elasticsearch_explicit_settings_still_work(self):
    """Explicit backend_settings remain a valid path for ES configuration."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ELASTICSEARCH
      backend_settings = {"hosts": ["http://localhost:9200"]}

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["hosts"] == ["http://localhost:9200"]

  def test_rocketmq_shortcuts(self):
    """R24-A1: RocketMQ backend now has shortcut attributes.

  Mirrors the existing Redis/MongoDB/Kafka/RabbitMQ shortcut pattern.
  """

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ
      rocketmq_namesrv_address = "rmq:9876"
      rocketmq_access_key = "AK"
      rocketmq_secret_key = "SK"
      rocketmq_tls_enabled = True

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["namesrv_address"] == "rmq:9876"
    assert result["access_key"] == "AK"
    assert result["secret_key"] == "SK"
    assert result["tls_enabled"] is True

  def test_rocketmq_tls_false_is_not_dropped(self):
    """An explicit false value remains distinct from an unset shortcut."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ
      rocketmq_tls_enabled = False

    assert TestSpider()._build_backend_settings() == {"tls_enabled": False}

  def test_rocketmq_namesrv_address_only(self):
    """namesrv set, access/secret unset → only namesrv shortcut present.

    Covers the False branches of the access_key/secret_key ``is not None``
    guards (the all-set case is exercised by ``test_rocketmq_shortcuts``).
    """

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ
      rocketmq_namesrv_address = "rmq:9876"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result == {"namesrv_address": "rmq:9876"}

  def test_rocketmq_access_key_without_secret(self):
    """access_key set, secret unset → secret_key guard takes the False branch."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ
      rocketmq_namesrv_address = "rmq:9876"
      rocketmq_access_key = "AK"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result == {"namesrv_address": "rmq:9876", "access_key": "AK"}

  def test_rocketmq_all_attrs_unset_yields_empty(self):
    """No rocketmq shortcut attrs set → empty dict (all three guards False)."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.ROCKETMQ

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result == {}

  def test_dispatch_accepts_backend_type_as_plain_string(self):
    """``backend_type`` may be a registry-key string (round-5 R5-1:
    resolve_backend_config returns strings), not just a BackendType enum.
    The dispatch must resolve "redis" the same as BackendType.REDIS.
    """

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      # plain string, not the enum — runtime dispatch accepts both (the
      # class attr is typed BackendType | None, but _build_backend_settings
      # deliberately handles plain strings per round-5 R5-1).
      backend_type = "redis"  # type: ignore[assignment]
      redis_host = "redis.example.com"

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["host"] == "redis.example.com"

  def test_dispatch_unknown_backend_contributes_no_shortcuts(self):
    """A backend with no shortcut-builder entry (e.g. Pulsar/SQS/Memcached/
    DynamoDB) contributes nothing — explicit backend_settings still flow
    through. Covers the ``builder_name is None`` branch."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.PULSAR
      backend_settings = {"service_url": "pulsar://localhost:6650"}

    spider = TestSpider()
    result = spider._build_backend_settings()
    # Explicit settings survive; no shortcut builder fires for Pulsar.
    assert result == {"service_url": "pulsar://localhost:6650"}

  def test_dispatch_none_backend_type_yields_only_explicit_settings(self):
    """``_build_backend_settings`` with ``backend_type=None`` returns only the
    explicit ``backend_settings`` (no shortcut builder fires). Unlike
    ``setup_backend`` (which raises on None), the builder is safe to call
    directly with a missing backend_type."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = None
      backend_settings = {"foo": "bar"}

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result == {"foo": "bar"}


class TestConnectSignals:
  """Test _connect_signals method."""

  def test_connects_signals_when_crawler_exists(self, mocker):
    """Test that signals are connected when crawler is available."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    mock_signals = mocker.MagicMock()
    mock_crawler = mocker.MagicMock()
    mock_crawler.signals = mock_signals
    spider.crawler = mock_crawler

    spider._connect_signals()

    mock_signals.connect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    mock_signals.connect.assert_any_call(
      spider._on_spider_closed, signals.spider_closed
    )

  def test_does_not_connect_signals_when_no_crawler(self):
    """Test that no error is raised when crawler is not set."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    object.__setattr__(spider, "crawler", None)

    # Should not raise
    spider._connect_signals()

  def test_does_not_connect_signals_when_crawler_is_false(self):
    """Test that no error is raised when crawler is False."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    object.__setattr__(spider, "crawler", False)

    # Should not raise
    spider._connect_signals()


class TestOnSpiderOpened:
  """Test _on_spider_opened method."""

  def test_calls_connect_on_connection_manager(self, mocker):
    """Test that _on_spider_opened calls connect on the manager."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    mock_manager = mocker.MagicMock()
    spider._connection_manager = mock_manager

    spider._on_spider_opened(spider)

    mock_manager.connect.assert_called_once()

  def test_does_nothing_when_connection_manager_is_none(self):
    """Test that _on_spider_opened does nothing when manager is None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = None

    # Should not raise
    spider._on_spider_opened(spider)

  def test_does_nothing_when_spider_is_not_self(self, mocker):
    """Test that _on_spider_opened ignores other spider instances."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock()
    spider._connection_manager = mock_manager

    other_spider = TestSpider()
    spider._on_spider_opened(other_spider)

    mock_manager.connect.assert_not_called()


class TestOnSpiderClosed:
  """Test _on_spider_closed method."""

  def test_calls_close_backend(self, mocker):
    """Test that _on_spider_closed calls close_backend."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_close = mocker.patch.object(spider, "close_backend")

    spider._on_spider_closed(spider, reason="finished")

    mock_close.assert_called_once()

  def test_swallows_close_backend_error(self, mocker, caplog):
    """R3-H6: a close_backend() failure is swallowed — Scrapy's signal chain stays intact.

    If close_backend raises (network error on disconnect, etc.), the exception
    must NOT propagate through Scrapy's signal dispatcher — other
    spider_closed handlers (stats, extensions, logging) still need to fire.
    Same invariant as the scheduler's ack/nack error-swallow (R64).
    """
    import logging

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mocker.patch.object(spider, "close_backend", side_effect=RuntimeError("close failed"))

    caplog.clear()
    with caplog.at_level(logging.ERROR):
      spider._on_spider_closed(spider, reason="finished")

    # Must NOT propagate; the failure is logged instead.
    assert "close_backend() failed" in caplog.text

  def test_ignores_other_spider_instances(self, mocker):
    """Test that _on_spider_closed ignores other spider instances."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_close = mocker.patch.object(spider, "close_backend")

    other_spider = TestSpider()
    spider._on_spider_closed(other_spider, reason="finished")

    mock_close.assert_not_called()

  def test_reason_parameter_is_optional(self, mocker):
    """Test that _on_spider_closed works without a reason parameter."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_close = mocker.patch.object(spider, "close_backend")

    # Call without reason (default empty string per signature)
    spider._on_spider_closed(spider)

    mock_close.assert_called_once()


class TestGetQueue:
  """Test get_queue method."""

  def test_raises_when_connection_manager_not_setup(self):
    """Test that get_queue raises RuntimeError when setup_backend not called."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="setup_backend\\(\\) must be called"):
      spider.get_queue()

  def test_raises_with_spider_class_name_in_error(self):
    """Test that error message includes the spider class name."""

    class MySpider(BackendSpiderMixin, Spider):
      name = "my_spider"

    spider = MySpider()

    with pytest.raises(RuntimeError, match="MySpider"):
      spider.get_queue()

  def test_caches_queue_instance(self, mocker):
    """Test that get_queue caches the queue instance."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    result1 = spider.get_queue()
    result2 = spider.get_queue()

    assert result1 is result2


class TestGetDupefilter:
  """Test get_dupefilter method."""

  def test_raises_when_connection_manager_not_setup(self):
    """Test that get_dupefilter raises RuntimeError when setup_backend not called."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="setup_backend\\(\\) must be called"):
      spider.get_dupefilter()

  def test_raises_with_spider_class_name_in_error(self):
    """Test that error message includes the spider class name."""

    class MySpider(BackendSpiderMixin, Spider):
      name = "my_spider"

    spider = MySpider()

    with pytest.raises(RuntimeError, match="MySpider"):
      spider.get_dupefilter()

  def test_caches_dupefilter_instance(self, mocker):
    """Test that get_dupefilter caches the dupefilter instance."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    result1 = spider.get_dupefilter()
    result2 = spider.get_dupefilter()

    assert result1 is result2

  def test_mixin_dupefilter_borrows_manager(self, mocker):
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)

    dupefilter = spider.get_dupefilter()

    assert dupefilter._owns_connection_manager is False


class TestGetScheduler:
  """Test get_scheduler method."""

  def test_raises_when_connection_manager_not_setup(self):
    """Test that get_scheduler raises RuntimeError when setup_backend not called."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="setup_backend\\(\\) must be called"):
      spider.get_scheduler()

  def test_raises_with_spider_class_name_in_error(self):
    """Test that error message includes the spider class name."""

    class MySpider(BackendSpiderMixin, Spider):
      name = "my_spider"

    spider = MySpider()

    with pytest.raises(RuntimeError, match="MySpider"):
      spider.get_scheduler()

  def test_caches_scheduler_instance(self, mocker):
    """Test that get_scheduler caches the scheduler instance."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    result1 = spider.get_scheduler()
    result2 = spider.get_scheduler()

    assert result1 is result2

  def test_mixin_scheduler_borrows_manager(self, mocker):
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)

    scheduler = spider.get_scheduler()

    assert scheduler._owns_connection_manager is False


class TestConcurrentComponentGetters:
  """Each lazy component constructor is serialized with backend shutdown."""

  @pytest.mark.parametrize(
    ("getter_name", "constructor_path"),
    (
      ("get_queue", "scrapy_extension.queue.queue.BackendQueue"),
      (
        "get_dupefilter",
        "scrapy_extension.dupefilter.dupefilter.BackendDupeFilter",
      ),
      ("get_scheduler", "scrapy_extension.schedule.scheduler.BackendScheduler"),
    ),
  )
  def test_concurrent_getter_constructs_component_once(
    self,
    mocker,
    getter_name,
    constructor_path,
  ):
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    observed_lock = _ObservedLock()
    spider._lifecycle_lock = observed_lock
    component = mocker.MagicMock()
    constructor_entered = threading.Event()
    release_constructor = threading.Event()

    def construct(**_kwargs):
      constructor_entered.set()
      assert release_constructor.wait(timeout=2.0)
      return component

    constructor = mocker.patch(constructor_path, side_effect=construct)
    results = []
    errors: list[BaseException] = []

    def get_component() -> None:
      try:
        results.append(getattr(spider, getter_name)())
      except BaseException as exc:  # noqa: BLE001 - surface worker failure
        errors.append(exc)

    first = threading.Thread(target=get_component, daemon=True)
    second = threading.Thread(target=get_component, daemon=True)
    first.start()
    assert constructor_entered.wait(timeout=2.0)
    second.start()
    assert observed_lock.second_attempted.wait(timeout=2.0)
    release_constructor.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert errors == []
    assert len(results) == 2
    assert all(result is component for result in results)
    constructor.assert_called_once()

  def test_close_waits_for_inflight_getter_then_closes_component(self, mocker):
    """A component cannot publish itself after its manager was released."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    manager = mocker.MagicMock(spec=ConnectionManager)
    component = mocker.MagicMock()
    observed_lock = _ObservedLock()
    constructor_entered = threading.Event()
    release_constructor = threading.Event()
    getter_finished = threading.Event()
    close_finished = threading.Event()
    errors: list[BaseException] = []
    spider._connection_manager = manager
    spider._lifecycle_lock = observed_lock

    def construct(**_kwargs):
      constructor_entered.set()
      assert release_constructor.wait(timeout=2.0)
      return component

    mocker.patch("scrapy_extension.queue.queue.BackendQueue", side_effect=construct)

    def get_queue() -> None:
      try:
        assert spider.get_queue() is component
        getter_finished.set()
      except BaseException as exc:  # noqa: BLE001 - surface worker failure
        errors.append(exc)

    def close() -> None:
      try:
        spider.close_backend()
        close_finished.set()
      except BaseException as exc:  # noqa: BLE001 - surface worker failure
        errors.append(exc)

    getter_thread = threading.Thread(target=get_queue, daemon=True)
    close_thread = threading.Thread(target=close, daemon=True)
    getter_thread.start()
    assert constructor_entered.wait(timeout=2.0)
    close_thread.start()
    assert observed_lock.second_attempted.wait(timeout=2.0)
    assert close_finished.is_set() is False
    release_constructor.set()
    getter_thread.join(timeout=2.0)
    close_thread.join(timeout=2.0)

    assert errors == []
    assert getter_finished.is_set() is True
    assert close_finished.is_set() is True
    component.close.assert_called_once_with()
    manager.close.assert_called_once_with()
    assert spider._queue is None
    assert spider._connection_manager is None


class TestCloseBackend:
  """Test close_backend method."""

  def test_clears_queue_reference(self, mocker):
    """Test that close_backend clears the _queue reference."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._queue = mocker.MagicMock()

    spider.close_backend()

    assert spider._queue is None

  def test_clears_dupefilter_reference(self, mocker):
    """Test that close_backend clears the _dupefilter reference."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._dupefilter = mocker.MagicMock()

    spider.close_backend()

    assert spider._dupefilter is None

  def test_clears_scheduler_reference(self, mocker):
    """Test that close_backend clears the _scheduler reference."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._scheduler = mocker.MagicMock()

    spider.close_backend()

    assert spider._scheduler is None

  def test_closes_connection_manager(self, mocker):
    """Test that close_backend calls close on the connection manager."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    spider.close_backend()

    mock_manager.close.assert_called_once()

  def test_clears_connection_manager_reference(self, mocker):
    """Test that close_backend clears the _connection_manager reference."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)

    spider.close_backend()

    assert spider._connection_manager is None

  def test_close_backend_when_connection_manager_already_none(self):
    """Test that close_backend works when connection_manager is already None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    spider._connection_manager = None

    # Should not raise
    spider.close_backend()

  def test_close_backend_is_idempotent(self, mocker):
    """Repeated close must release the acquired manager only once."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    spider.close_backend()
    spider.close_backend()

    mock_manager.close.assert_called_once()

  def test_close_backend_closes_components_before_manager(self, mocker):
    """Borrowing components quiesce while their shared manager is still live."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    call_order: list[str] = []
    spider._queue = mocker.MagicMock()
    spider._dupefilter = mocker.MagicMock()
    spider._scheduler = mocker.MagicMock()
    spider._connection_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._queue.close.side_effect = lambda: call_order.append("queue")
    spider._dupefilter.close.side_effect = lambda _reason: call_order.append("dupe")
    spider._scheduler.close.side_effect = lambda _reason: call_order.append("scheduler")
    spider._connection_manager.close.side_effect = lambda: call_order.append("manager")

    spider.close_backend()

    assert call_order == ["scheduler", "queue", "dupe", "manager"]

  def test_close_backend_isolates_each_cleanup_failure(self, mocker, caplog):
    """One failed cleanup must not skip any later component or manager."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    signal_manager = mocker.MagicMock()
    queue = mocker.MagicMock()
    dupefilter = mocker.MagicMock()
    scheduler = mocker.MagicMock()
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connected_signals = signal_manager
    spider._signals_connected = True
    spider._queue = queue
    spider._dupefilter = dupefilter
    spider._scheduler = scheduler
    spider._connection_manager = manager
    signal_manager.disconnect.side_effect = RuntimeError("disconnect failed")
    queue.close.side_effect = RuntimeError("queue failed")
    dupefilter.close.side_effect = RuntimeError("dupefilter failed")
    scheduler.close.side_effect = RuntimeError("scheduler failed")
    manager.close.side_effect = RuntimeError("manager failed")

    spider.close_backend()

    assert spider._connected_signals is None
    assert spider._signals_connected is False
    assert spider._queue is None
    assert spider._dupefilter is None
    assert spider._scheduler is None
    assert spider._connection_manager is None
    assert signal_manager.disconnect.call_count == 2
    scheduler.close.assert_called_once_with("spider-mixin-close")
    queue.close.assert_called_once_with()
    dupefilter.close.assert_called_once_with("spider-mixin-close")
    manager.close.assert_called_once_with()
    assert caplog.text.count("Failed to disconnect backend lifecycle signal") == 2

  def test_close_backend_reentrant_component_close_is_idempotent(self, mocker):
    """A component close hook may safely trigger duplicate spider shutdown."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    scheduler = mocker.MagicMock()
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider._scheduler = scheduler
    spider._connection_manager = manager
    scheduler.close.side_effect = lambda _reason: spider.close_backend()
    finished = threading.Event()

    def close() -> None:
      spider.close_backend()
      finished.set()

    thread = threading.Thread(target=close, daemon=True)
    thread.start()
    assert finished.wait(timeout=2.0)
    thread.join(timeout=2.0)

    scheduler.close.assert_called_once_with("spider-mixin-close")
    manager.close.assert_called_once_with()

  def test_borrowed_components_do_not_release_manager(self, mocker):
    """Only the mixin releases the acquire shared by its real components."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = manager
    spider.get_dupefilter()
    spider.get_scheduler()

    spider.close_backend()

    manager.close.assert_called_once_with()

  def test_close_backend_disconnects_mixin_signals(self, mocker):
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    signal_manager = mocker.MagicMock()
    spider._connected_signals = signal_manager
    spider._signals_connected = True

    spider.close_backend()

    signal_manager.disconnect.assert_any_call(
      spider._on_spider_opened, signals.spider_opened
    )
    signal_manager.disconnect.assert_any_call(
      spider._on_spider_closed, signals.spider_closed
    )
    assert spider._connected_signals is None
    assert spider._signals_connected is False

  def test_close_then_setup_wires_replacement_crawler(self, mocker):
    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    first_manager = mocker.MagicMock(spec=ConnectionManager)
    second_manager = mocker.MagicMock(spec=ConnectionManager)
    acquire = mocker.patch.object(
      ConnectionManager,
      "get_manager",
      side_effect=[first_manager, second_manager],
    )
    first_signals = mocker.MagicMock()
    second_signals = mocker.MagicMock()
    spider = TestSpider()
    spider.crawler = mocker.MagicMock(signals=first_signals)
    spider.setup_backend()
    spider.close_backend()

    spider.crawler = mocker.MagicMock(signals=second_signals)
    assert spider.setup_backend() is second_manager

    assert acquire.call_count == 2
    assert first_signals.connect.call_count == 2
    assert first_signals.disconnect.call_count == 2
    assert second_signals.connect.call_count == 2


class TestConnectionManagerProperty:
  """Test connection_manager property."""

  def test_returns_connection_manager(self, mocker):
    """Test that the property returns the connection manager."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    spider._connection_manager = mock_manager

    result = spider.connection_manager

    assert result is mock_manager

  def test_raises_when_connection_manager_not_setup(self):
    """Test that property raises RuntimeError when manager is None."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="setup_backend\\(\\) must be called"):
      _ = spider.connection_manager

  def test_raises_with_spider_class_name_in_error(self):
    """Test that error message includes the spider class name."""

    class MySpider(BackendSpiderMixin, Spider):
      name = "my_spider"

    spider = MySpider()

    with pytest.raises(RuntimeError, match="MySpider"):
      _ = spider.connection_manager


class TestSpiderMixinHonorsSettings:
  """#29: the convenience getters honor SCRAPY_QUEUE_STRATEGY /
  SCRAPY_DEDUP_STRATEGY from crawler.settings when a crawler is attached,
  falling back to the defaults (passthrough / set) when crawler-less.
  """

  def test_get_queue_honors_queue_strategy_setting(self, mocker) -> None:
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    settings = Mock()
    settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_QUEUE_STRATEGY": "delay"
    }.get(key, default)
    mock_crawler = mocker.MagicMock()
    mock_crawler.settings = settings

    class TestSpider(BackendSpiderMixin, Spider):
      name = "s"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    spider._connection_manager = mock_manager
    spider.crawler = mock_crawler

    q = spider.get_queue()
    from scrapy_extension.queue.strategies.delay import DelayQueueStrategy

    assert isinstance(q._strategy, DelayQueueStrategy)

  def test_get_queue_defaults_to_passthrough_without_crawler(self, mocker) -> None:
    mock_manager = mocker.MagicMock(spec=ConnectionManager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "s"

    spider = TestSpider()
    spider._connection_manager = mock_manager
    object.__setattr__(spider, "crawler", None)

    q = spider.get_queue()
    from scrapy_extension.queue.strategies.passthrough import (
      PassthroughQueueStrategy,
    )

    assert isinstance(q._strategy, PassthroughQueueStrategy)

  def test_get_dupefilter_honors_dedup_strategy_setting(self, mocker) -> None:
    mock_manager = mocker.MagicMock(spec=ConnectionManager)
    settings = Mock()
    settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_DEDUP_STRATEGY": "memory"
    }.get(key, default)
    mock_crawler = mocker.MagicMock()
    mock_crawler.settings = settings

    class TestSpider(BackendSpiderMixin, Spider):
      name = "s"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    spider._connection_manager = mock_manager
    spider.crawler = mock_crawler

    df = spider.get_dupefilter()
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    assert isinstance(df._filter, MemoryMembershipFilter)


class TestIntegration:
  """Integration tests for the full BackendSpiderMixin lifecycle."""

  def test_full_lifecycle_with_redis_backend(self, mocker):
    """Test the full lifecycle: setup_backend -> get_queue -> close_backend."""
    mock_manager = mocker.MagicMock(spec=ConnectionManager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS
      redis_host = "localhost"
      redis_port = 6379

    spider = TestSpider()
    spider._connection_manager = mock_manager

    # get_queue
    queue = spider.get_queue()
    assert queue is not None

    # get_dupefilter
    dupefilter = spider.get_dupefilter()
    assert dupefilter is not None

    # get_scheduler
    scheduler = spider.get_scheduler()
    assert scheduler is not None

    # connection_manager property
    assert spider.connection_manager is mock_manager

    # close_backend
    spider.close_backend()
    assert spider._connection_manager is None
    assert spider._queue is None
    assert spider._dupefilter is None
    assert spider._scheduler is None

  def test_lifecycle_raises_on_each_getter_without_setup(self):
    """Test that each getter raises RuntimeError independently."""

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"

    spider = TestSpider()

    with pytest.raises(RuntimeError, match="setup_backend"):
      spider.get_queue()

    with pytest.raises(RuntimeError, match="setup_backend"):
      spider.get_dupefilter()

    with pytest.raises(RuntimeError, match="setup_backend"):
      spider.get_scheduler()

    with pytest.raises(RuntimeError, match="setup_backend"):
      _ = spider.connection_manager

  def test_build_backend_settings_with_all_backend_types(self):
    """Test _build_backend_settings for all supported backend types."""

    # Redis
    class RedisSpider(BackendSpiderMixin, Spider):
      name = "redis_spider"
      backend_type = BackendType.REDIS
      redis_host = "localhost"

    assert RedisSpider()._build_backend_settings()["host"] == "localhost"

    # MongoDB
    class MongoDBSpider(BackendSpiderMixin, Spider):
      name = "mongodb_spider"
      backend_type = BackendType.MONGODB
      mongodb_uri = "mongodb://localhost"

    assert MongoDBSpider()._build_backend_settings()["uri"] == "mongodb://localhost"

    # Kafka
    class KafkaSpider(BackendSpiderMixin, Spider):
      name = "kafka_spider"
      backend_type = BackendType.KAFKA
      kafka_bootstrap_servers = "localhost:9092"

    assert (
      KafkaSpider()._build_backend_settings()["bootstrap_servers"] == "localhost:9092"
    )

    # RabbitMQ
    class RabbitMQSpider(BackendSpiderMixin, Spider):
      name = "rabbitmq_spider"
      backend_type = BackendType.RABBITMQ
      rabbitmq_url = "amqp://localhost"

    assert RabbitMQSpider()._build_backend_settings()["url"] == "amqp://localhost"

    # ElasticSearch
    class ESSpider(BackendSpiderMixin, Spider):
      name = "es_spider"
      backend_type = BackendType.ELASTICSEARCH
      backend_settings = {"hosts": ["http://localhost:9200"]}

    assert ESSpider()._build_backend_settings()["hosts"] == ["http://localhost:9200"]
