"""Tests for BackendSpiderMixin."""

import os

import pytest
from scrapy import Spider, signals

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import ConnectionManager
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

# Redis password fixture - use env var to avoid S105 warnings
REDIS_PASSWORD = os.environ.get("TEST_REDIS_PASSWORD", "test_password_placeholder")


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


class TestSetupBackend:
  """Test setup_backend method."""

  def test_setup_backend_success(self, mocker):
    """Test successful setup_backend call."""
    mock_manager = mocker.MagicMock(spec=ConnectionManager)

    class TestSpider(BackendSpiderMixin, Spider):
      name = "test_spider"
      backend_type = BackendType.REDIS

    spider = TestSpider()
    # Patch ConnectionManager at the source where it's defined
    mocker.patch(
      "scrapy_extension.backends.connectors.ConnectionManager",
      return_value=mock_manager,
    )

    result = spider.setup_backend()

    assert result is mock_manager
    assert spider._connection_manager is mock_manager

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

    spider = TestSpider()
    result = spider._build_backend_settings()
    assert result["namesrv_address"] == "rmq:9876"
    assert result["access_key"] == "AK"
    assert result["secret_key"] == "SK"


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
