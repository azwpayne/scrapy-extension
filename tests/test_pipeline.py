"""Tests for BackendPipeline component."""

from typing import cast

import pytest
from scrapy import Field, Item

from scrapy_extension.backends.base import JSONSerializer
from scrapy_extension.exceptions import BackendError, SerializationError
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

  def test_close_spider_releases_connection_on_flush_failure(
    self, mock_connection_manager, mocker
  ):
    """Teardown invariant: connection_manager.close() runs even when the final flush raises."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline.storage_strategy = mocker.Mock()
    pipeline.storage_strategy.close.side_effect = RuntimeError("flush failed")
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with pytest.raises(RuntimeError, match="flush failed"):
      pipeline.close_spider(mock_spider)

    mock_connection_manager.close.assert_called_once_with()

  def test_close_spider_flush_error_not_masked_by_connection_close(
    self, mock_connection_manager, mocker, caplog
  ):
    """If both close() calls raise, the original flush error propagates; the connection-close error is logged, not swallowed."""
    import logging

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline.storage_strategy = mocker.Mock()
    pipeline.storage_strategy.close.side_effect = RuntimeError("flush failed")
    mock_connection_manager.close.side_effect = ConnectionError("close failed")
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with caplog.at_level(logging.ERROR):
      with pytest.raises(RuntimeError, match="flush failed"):
        pipeline.close_spider(mock_spider)

    assert "connection_manager.close() failed" in caplog.text


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


class TestBackendPipelineMaxItemBytes:
  """D2: configurable per-item byte cap to prevent DoS via oversize payloads."""

  def test_process_item_oversize_raises_and_increments_stat(
    self, mock_connection_manager, mocker
  ):
    """D2: an oversize serialized item raises SerializationError + bumps stat.

    A hostile target can push arbitrarily large item payloads; storage backends
    with caps (Memcached 1 MB, DynamoDB 400 KB) throw and the item is silently
    dropped. The cap surfaces the oversize condition loudly at store time with
    a stat increment so operators can see it on dashboards.

    Note: unlike a transient storage error (which the pipeline swallows to keep
    the spider alive), an oversize payload is a deterministic validation
    failure — it raises so the operator sees it, not silently dropped.
    """
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      max_item_bytes=32,
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    big_item = SampleItem(name="X" * 200, value=1)

    with pytest.raises(SerializationError, match="exceeds.*max"):
      pipeline.process_item(big_item, mock_spider)

    # Risk 5: renamed ``oversize_dropped`` → ``oversize_rejected`` (canonical);
    # the legacy key is still incremented for one release as a backward-compat
    # alias (mirrors monitor/stats.py ``queue/pop_count`` aliasing). Assert
    # BOTH fire so the rename + alias contract is pinned.
    stats_inc = mock_spider.crawler.stats.inc_value
    stats_inc.assert_any_call("pipeline/oversize_rejected")
    stats_inc.assert_any_call("pipeline/oversize_dropped")
    mock_connection_manager.get_storage_backend().store.assert_not_called()

  def test_process_item_normal_size_succeeds(
    self, mock_connection_manager, mocker
  ):
    """D2: a normal-size item is unaffected by the cap."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      max_item_bytes=1_048_576,
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    item = SampleItem(name="Test", value=123)
    result = pipeline.process_item(item, mock_spider)

    assert result is item
    mock_connection_manager.get_storage_backend().store.assert_called_once()

  def test_default_max_item_bytes_is_one_mib(self, mock_connection_manager):
    """D2: default cap is 1 MiB (matches Memcached's 1 MB ceiling)."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert pipeline.max_item_bytes == 1_048_576

  def test_from_settings_reads_max_item_bytes(self, mocker):
    """D2: from_settings reads SCRAPY_PIPELINE_MAX_ITEM_BYTES."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_PIPELINE_MAX_ITEM_BYTES": 2048,
    }.get(key, default)
    mock_settings.getint.return_value = 2048
    mock_settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    pipeline = BackendPipeline.from_settings(mock_settings)
    assert pipeline.max_item_bytes == 2048



class TestBackendPipelineStorageStrategy:
  """Tier-2: BackendPipeline delegates _store_item to a StorageStrategy."""

  def test_default_strategy_is_passthrough(self, mock_connection_manager):
    """Default strategy is PassthroughStorageStrategy (back-compat)."""
    from scrapy_extension.storage.strategies.passthrough import (
      PassthroughStorageStrategy,
    )

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert isinstance(pipeline.storage_strategy, PassthroughStorageStrategy)

  def test_passthrough_is_byte_identical_to_pre_strategy(
    self, mock_connection_manager, mocker
  ):
    """Default passthrough must call store(key, data, ttl=self.ttl) exactly."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager, ttl=300
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    item = SampleItem(name="x", value=1)
    pipeline.process_item(item, mock_spider)

    store = mock_connection_manager.get_storage_backend().store
    store.assert_called_once()
    args, kwargs = store.call_args
    # Two acceptable shapes: positional (key, data) with ttl= kw, or all-kwargs.
    assert kwargs.get("ttl") == 300
    assert len(args) >= 2
    assert isinstance(args[0], str)
    assert isinstance(args[1], (bytes, bytearray))

  def test_batched_strategy_buffers_until_close(
    self, mock_connection_manager, mocker
  ):
    """A batched strategy buffers items and flushes on close_spider."""
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    strat = BatchedStorageStrategy(threshold=100)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strat,
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    pipeline.process_item(SampleItem(name="a", value=1), mock_spider)
    pipeline.process_item(SampleItem(name="b", value=2), mock_spider)

    store = mock_connection_manager.get_storage_backend().store
    # Buffered — no writes yet.
    assert store.call_count == 0
    assert strat.pending == 2

    pipeline.close_spider(mock_spider)  # drains the buffer
    assert store.call_count == 2
    assert strat.pending == 0

  def test_max_item_bytes_still_rejects_oversize_with_strategy(
    self, mock_connection_manager, mocker
  ):
    """D2 cap still applies per-item BEFORE the strategy sees the bytes."""
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    strat = BatchedStorageStrategy(threshold=100)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      max_item_bytes=32,
      storage_strategy=strat,
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    big_item = SampleItem(name="x" * 100, value=1)
    with pytest.raises(SerializationError):
      pipeline.process_item(big_item, mock_spider)

    # Nothing buffered, nothing stored.
    assert strat.pending == 0
    mock_connection_manager.get_storage_backend().store.assert_not_called()

  def test_from_settings_reads_storage_strategy(self, mocker):
    """from_settings reads SCRAPY_STORAGE_STRATEGY and builds the strategy."""
    from scrapy_extension.backends.connectors import ConnectionManager
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_STORAGE_STRATEGY": "batched",
    }.get(key, default)
    mock_settings.getint.return_value = 0
    mock_settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    pipeline = BackendPipeline.from_settings(mock_settings)
    assert isinstance(pipeline.storage_strategy, BatchedStorageStrategy)

  def test_from_settings_default_strategy_is_passthrough(self, mocker):
    """from_settings defaults to passthrough when SCRAPY_STORAGE_STRATEGY unset."""
    from scrapy_extension.backends.connectors import ConnectionManager
    from scrapy_extension.storage.strategies.passthrough import (
      PassthroughStorageStrategy,
    )

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
    }.get(key, default)
    mock_settings.getint.return_value = 0
    mock_settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    pipeline = BackendPipeline.from_settings(mock_settings)
    assert isinstance(pipeline.storage_strategy, PassthroughStorageStrategy)

  def test_close_spider_calls_strategy_close(
    self, mock_connection_manager, mocker
  ):
    """close_spider flushes the strategy before closing the connection manager."""
    strat = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strat,
    )
    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    pipeline.close_spider(mock_spider)

    strat.close.assert_called_once()
    mock_connection_manager.close.assert_called_once()


class TestBackendPipelineStorageEscalation:
  """C2 (round 2): opt-in loud-fail after N consecutive storage errors.

  Default (``max_storage_errors=None``) preserves the swallow-and-stat
  behavior — zero compat break. When set to an int N, the pipeline tracks
  consecutive storage failures and re-raises (wrapped as ``BackendError``)
  once the consecutive count exceeds N, so a persistent storage outage
  surfaces loudly instead of being silently swallowed as success.
  """

  def test_default_none_preserves_swallow_and_stat(
    self, mock_connection_manager, mocker
  ):
    """Default (None) = current best-effort behavior: raise → item returned + stat."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True

    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = RuntimeError("connection refused")

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    item = SampleItem(name="x", value=1)
    result = pipeline.process_item(item, mock_spider)

    assert result is item
    mock_spider.crawler.stats.inc_value.assert_called_with("pipeline/storage_errors")

  def test_escalation_raises_after_threshold_exceeded(
    self, mock_connection_manager, mocker
  ):
    """N=2: two consecutive raises swallowed; the THIRD raises ``BackendError``.

    Pre-B1 this raises ``AttributeError`` (no ``max_storage_errors`` kwarg) /
    never escalates — RED. Post-B1 the 3rd failure exceeds the threshold and
    re-raises wrapped as ``BackendError``.
    """
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      max_storage_errors=2,
    )
    pipeline._storage_supported = True

    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = RuntimeError("connection refused")

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    # 1st and 2nd consecutive failures: swallowed (count=1, count=2).
    pipeline.process_item(SampleItem(name="a", value=1), mock_spider)
    pipeline.process_item(SampleItem(name="b", value=2), mock_spider)

    # 3rd consecutive failure: count becomes 3 > 2 → escalate.
    with pytest.raises(BackendError, match="consecutive storage"):
      pipeline.process_item(SampleItem(name="c", value=3), mock_spider)

  def test_successful_store_resets_consecutive_counter(
    self, mock_connection_manager, mocker
  ):
    """A successful store between two failures resets the counter.

    With N=2: fail, fail, SUCCESS (reset), fail, fail → the next (3rd in a row
    since reset) would escalate, but only 2 consecutive have happened since
    the reset, so no escalation. Verifies the counter is consecutive, not
    cumulative.
    """
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      max_storage_errors=2,
    )
    pipeline._storage_supported = True

    mock_storage = mock_connection_manager.get_storage_backend()
    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    # Two consecutive failures (swallowed).
    mock_storage.store.side_effect = RuntimeError("connection refused")
    pipeline.process_item(SampleItem(name="a", value=1), mock_spider)
    pipeline.process_item(SampleItem(name="b", value=2), mock_spider)

    # Success — resets the consecutive counter to 0.
    mock_storage.store.side_effect = None
    pipeline.process_item(SampleItem(name="ok", value=3), mock_spider)

    # Two more consecutive failures: only 2 since reset, NOT > 2 → no escalate.
    mock_storage.store.side_effect = RuntimeError("connection refused")
    pipeline.process_item(SampleItem(name="d", value=4), mock_spider)
    result = pipeline.process_item(SampleItem(name="e", value=5), mock_spider)

    # Item returned, not raised — counter was reset by the intervening success.
    assert result is not None
    assert mock_storage.store.call_count == 5


class TestBackendPipelineMonitorWiring:
  """C2/F (round 2): ``on_store`` hook invoked after a successful store.

  Mirrors the dupefilter monitor wiring — an optional ``Monitor`` threaded
  through ``from_crawler``; ``NullMonitor`` default preserves prior behavior.
  The hook fires only on success, never on failure (the failure path has its
  own stat, ``pipeline/storage_errors``).
  """

  def test_on_store_invoked_on_success(self, mock_connection_manager, mocker):
    """A successful store calls ``monitor.on_store(key)`` with the storage key.

    Pre-B2 the pipeline has no ``monitor`` kwarg — RED (AttributeError / hook
    never called). Post-B2 the hook fires exactly once per successful store.
    """
    monitor = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    pipeline.process_item(SampleItem(name="x", value=1), mock_spider)

    monitor.on_store.assert_called_once()
    call_key = monitor.on_store.call_args[0][0]
    assert isinstance(call_key, str)
    assert call_key  # non-empty

  def test_on_store_not_invoked_on_failure(self, mock_connection_manager, mocker):
    """A failed store must NOT call ``on_store`` (failure path has its own stat)."""
    monitor = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True

    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = RuntimeError("connection refused")

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    pipeline.process_item(SampleItem(name="x", value=1), mock_spider)

    monitor.on_store.assert_not_called()

  def test_default_monitor_is_null_when_unset(self, mock_connection_manager):
    """When no monitor is passed, the pipeline holds a ``NullMonitor`` (no-op).

    Preserves prior behavior exactly — calling ``on_store`` on a NullMonitor
    is a no-op, so existing single-call-store tests stay green.
    """
    from scrapy_extension.monitor.base import NullMonitor

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    assert isinstance(pipeline._monitor, NullMonitor)

  def test_from_crawler_wires_scrapy_stats_monitor(self, mocker):
    """from_crawler wires ScrapyStatsMonitor when crawler.stats is available.

    Mirrors the dupefilter pattern — default-on telemetry without an explicit
    ``monitor=`` kwarg. Additive: ``pipeline/store_count`` is a new stat, the
    existing ``pipeline/storage_errors`` stat is untouched.
    """
    from scrapy_extension.backends.connectors import ConnectionManager
    from scrapy_extension.monitor.stats import ScrapyStatsMonitor

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis"
    }.get(key, default)
    mock_settings.getint.return_value = 0
    mock_settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    mock_crawler = mocker.Mock()
    mock_crawler.settings = mock_settings
    mock_crawler.stats = mocker.Mock()

    pipeline = BackendPipeline.from_crawler(mock_crawler)

    assert isinstance(pipeline._monitor, ScrapyStatsMonitor)
