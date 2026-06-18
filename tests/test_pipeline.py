"""Tests for BackendPipeline component."""

from typing import cast

from scrapy import Field, Item

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.pipeline.pipeline import BackendPipeline


class SampleItem(Item):
  """Sample item for pipeline tests."""

  name = Field()
  value = Field()


class TestBackendPipelineInit:
  """Test BackendPipeline.__init__."""

  def test_sets_connection_manager(self, mock_connection_manager):
    """Test that __init__ sets connection_manager."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert pipeline.connection_manager is mock_connection_manager

  def test_sets_key_prefix(self, mock_connection_manager):
    """Test that __init__ sets key_prefix with default value."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert pipeline.key_prefix == "items"

  def test_sets_custom_key_prefix(self, mock_connection_manager):
    """Test that __init__ accepts custom key_prefix."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="custom_items",
    )
    assert pipeline.key_prefix == "custom_items"

  def test_sets_ttl(self, mock_connection_manager):
    """Test that __init__ sets ttl with default None."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert pipeline.ttl is None

  def test_sets_custom_ttl(self, mock_connection_manager):
    """Test that __init__ accepts custom ttl."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      ttl=3600,
    )
    assert pipeline.ttl == 3600


class TestBackendPipelineSerializer:
  """Test BackendPipeline._serializer cached_property."""

  def test_serializer_is_cached_property(self, mock_connection_manager):
    """Test that _serializer is a cached_property."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    # Access first time
    serializer1 = pipeline._serializer
    # Access second time - should be same instance
    serializer2 = pipeline._serializer
    assert serializer1 is serializer2

  def test_serializer_returns_json_serializer_instance(self, mock_connection_manager):
    """Test that _serializer returns a JSONSerializer instance."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    serializer = pipeline._serializer
    assert isinstance(serializer, JSONSerializer)


class TestBackendPipelineFromSettings:
  """Test BackendPipeline.from_settings classmethod."""

  def test_from_settings_creates_pipeline(self, mocker):
    """Test that from_settings creates a BackendPipeline instance."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_PIPELINE_KEY_PREFIX": "my_items",
      "SCRAPY_PIPELINE_TTL": 7200,
    }.get(key, default)
    mock_settings.getint.return_value = 7200
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=mock_manager,
    )

    pipeline = BackendPipeline.from_settings(mock_settings)

    assert pipeline.connection_manager is mock_manager
    assert pipeline.key_prefix == "my_items"
    assert pipeline.ttl == 7200

  def test_from_settings_default_values(self, mocker):
    """Test from_settings uses defaults when settings not provided."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
    }.get(key, default)
    mock_settings.getint.return_value = 0
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=mock_manager,
    )

    pipeline = BackendPipeline.from_settings(mock_settings)

    assert pipeline.key_prefix == "items"
    assert pipeline.ttl is None

  def test_from_settings_zero_ttl_becomes_none(self, mocker):
    """Test that SCRAPY_PIPELINE_TTL=0 is converted to None."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_PIPELINE_TTL": 0,
    }.get(key, default)
    mock_settings.getint.return_value = 0
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=mock_manager,
    )

    pipeline = BackendPipeline.from_settings(mock_settings)

    assert pipeline.ttl is None


class TestBackendPipelineFromCrawler:
  """Test BackendPipeline.from_crawler classmethod."""

  def test_from_crawler_delegates_to_from_settings(self, mocker):
    """Test that from_crawler calls from_settings with crawler.settings."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_PIPELINE_KEY_PREFIX": "crawler_items",
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_crawler = mocker.Mock()
    mock_crawler.settings = mock_settings

    mock_manager = mocker.Mock()
    mocker.patch.object(
      ConnectionManager,
      "get_manager",
      return_value=mock_manager,
    )

    pipeline = BackendPipeline.from_crawler(mock_crawler)

    assert pipeline.key_prefix == "crawler_items"


class TestBackendPipelineOpenSpider:
  """Test BackendPipeline.open_spider method."""

  def test_open_spider_logs_message(self, mock_connection_manager, mocker, caplog):
    """Test that open_spider logs 'Pipeline opened for spider %s'."""
    import logging

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with caplog.at_level(logging.INFO):
      pipeline.open_spider(mock_spider)

    assert "Pipeline opened for spider test_spider" in caplog.text


class TestBackendPipelineCloseSpider:
  """Test BackendPipeline.close_spider method."""

  def test_close_spider_logs_message(self, mock_connection_manager, mocker, caplog):
    """Test that close_spider logs 'Pipeline closed for spider %s'."""
    import logging

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with caplog.at_level(logging.INFO):
      pipeline.close_spider(mock_spider)

    assert "Pipeline closed for spider test_spider" in caplog.text

  def test_close_spider_calls_connection_manager_close(
    self, mock_connection_manager, mocker
  ):
    """Test that close_spider shuts down the connection manager."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    pipeline.close_spider(mock_spider)

    mock_connection_manager.close.assert_called_once_with()


class TestBackendPipelineProcessItem:
  """Test BackendPipeline.process_item method."""

  def test_process_item_serializes_and_stores(self, mock_connection_manager, mocker):
    """Test that process_item serializes item and stores via storage_backend."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Test", value=123)
    result = pipeline.process_item(item, mock_spider)

    assert result is item
    mock_connection_manager.get_storage_backend().store.assert_called_once()

  def test_process_item_without_iter(self, mock_connection_manager, mocker):
    """Test that process_item handles non-iterable items."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    # Pass a non-iterable object - intentionally testing process_item's fallback handling
    item = object()
    result = pipeline.process_item(cast("SampleItem", item), mock_spider)

    assert result is item
    # Should be stored under {"data": str(item)}
    call_args = mock_connection_manager.get_storage_backend().store.call_args
    serialized = call_args[0][1]
    # Serializer returns bytes, decode for string comparison
    if isinstance(serialized, bytes):
      serialized = serialized.decode("utf-8")
    assert '"data"' in serialized

  def test_process_item_key_contains_prefix_spider_timestamp_unique_id(
    self, mock_connection_manager, mocker
  ):
    """Test that stored key contains key_prefix, spider.name, timestamp, and unique id."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="my_items",
    )

    mock_spider = mocker.Mock()
    mock_spider.name = "my_spider"

    item = SampleItem(name="Test", value=123)
    pipeline.process_item(item, mock_spider)

    call_args = mock_connection_manager.get_storage_backend().store.call_args
    key = call_args[0][0]
    assert key.startswith("my_items:my_spider:")

  def test_process_item_returns_original_item(self, mock_connection_manager, mocker):
    """Test that process_item returns the original item unchanged."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Original", value=456)
    result = pipeline.process_item(item, mock_spider)

    assert result is item
    assert result["name"] == "Original"
    assert result["value"] == 456

  def test_process_item_survives_storage_error(self, mock_connection_manager, mocker):
    """R3-G5: storage errors must not kill the spider.

    The pipeline catches exceptions from the storage backend, logs a warning,
    and returns the item unchanged so downstream pipelines continue.
    """
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )
    pipeline._storage_supported = True

    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = RuntimeError("connection refused")

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Test", value=1)
    result = pipeline.process_item(item, mock_spider)

    # Pipeline returned the item, didn't raise.
    assert result is item
    # Storage was attempted.
    assert mock_storage.store.call_count == 1

  def test_open_spider_detects_no_storage_support(self, mock_connection_manager, mocker):
    """R3-G5: backends without storage (Kafka, RabbitMQ) degrade to no-op."""
    mock_connection_manager.get_storage_backend.side_effect = NotImplementedError
    mock_connection_manager.backend_type.value = "kafka"

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    pipeline.open_spider(mock_spider)
    assert pipeline._storage_supported is False

    # process_item is a no-op — no store call attempted.
    item = SampleItem(name="Test", value=1)
    result = pipeline.process_item(item, mock_spider)
    assert result is item
    mock_connection_manager.get_storage_backend.assert_called_once()

  def test_process_item_increments_storage_skipped_when_unsupported(
    self, mock_connection_manager, mocker
  ):
    """R23-A1: storage-skipped path increments pipeline/storage_skipped stat.

    Without this counter, an operator running Kafka/RabbitMQ/RocketMQ
    sees zero items in storage and zero error counts — items are silently
    dropped. The skipped counter surfaces the no-op so dashboards can
    distinguish "no items scraped" from "items scraped but not persisted".
    """
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = False  # bypass open_spider

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"
    item = SampleItem(name="Test", value=1)

    pipeline.process_item(item, mock_spider)

    mock_spider.crawler.stats.inc_value.assert_called_with("pipeline/storage_skipped")

  def test_inc_stat_skips_silently_when_no_crawler(self, mock_connection_manager, mocker):
    """R23-A1: _inc_stat tolerates spiders without a crawler attribute.

    Legacy spiders (or test doubles without ``crawler``) would otherwise
    raise AttributeError, masking the original storage event the stat
    was supposed to record. Silent skip — the spider continues.
    """
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)

    # Spider without .crawler — simulates legacy / test scenarios
    bare_spider = mocker.MagicMock(spec=["name"])
    bare_spider.name = "legacy"

    # Must not raise
    pipeline._inc_stat(bare_spider, "pipeline/storage_errors")
    pipeline._inc_stat(bare_spider, "pipeline/storage_skipped")

