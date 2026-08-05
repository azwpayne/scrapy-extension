"""Tests for BackendPipeline component."""

import logging
import sys
import traceback
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from scrapy import Field, Item
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.pipelines import ItemPipelineManager

from scrapy_extension.backends.base import JSONSerializer, _validate_key_name
from scrapy_extension.exceptions import (
  BackendConnectionError,
  BackendError,
  SerializationError,
  StorageBackpressureError,
  StorageError,
)
from scrapy_extension.pipeline.pipeline import BackendPipeline


def _assert_pipeline_value_is_redacted(
  value: object,
  marker: str,
  seen: set[int] | None = None,
) -> None:
  """Walk bounded builtin/attribute graphs without trusting ``repr``."""
  if seen is None:
    seen = set()
  value_id = id(value)
  if value_id in seen:
    return
  seen.add(value_id)
  if isinstance(value, str):
    assert marker not in value
    return
  if isinstance(value, bytes):
    assert marker.encode() not in value
    return
  if isinstance(value, dict):
    for key, item in value.items():
      _assert_pipeline_value_is_redacted(key, marker, seen)
      _assert_pipeline_value_is_redacted(item, marker, seen)
    return
  if isinstance(value, (tuple, list, set, frozenset)):
    for item in value:
      _assert_pipeline_value_is_redacted(item, marker, seen)
    return
  try:
    attributes = vars(value)
  except TypeError:
    return
  _assert_pipeline_value_is_redacted(attributes, marker, seen)


def _assert_pipeline_public_error_is_redacted(
  error: BaseException,
  marker: str,
) -> None:
  """Assert a process-item terminal error cannot recover private store data."""
  assert marker not in str(error)
  assert marker not in repr(error.args)
  assert marker not in repr(error.__dict__)
  assert error.__cause__ is None
  assert error.__context__ is None
  assert marker not in "".join(traceback.format_exception(error))

  trace = error.__traceback__
  while trace is not None:
    frame = trace.tb_frame
    if "/src/scrapy_extension/" in frame.f_code.co_filename:
      _assert_pipeline_value_is_redacted(frame.f_locals, marker)
    trace = trace.tb_next


class SampleItem(Item):
  """Sample item for pipeline tests."""

  name = Field()
  value = Field()


class _ExceptionContextProbeHandler(logging.Handler):
  """Capture the exception state visible to an application log handler."""

  def __init__(self) -> None:
    super().__init__()
    self.records: list[logging.LogRecord] = []
    self.active_errors: list[BaseException | None] = []

  def emit(self, record: logging.LogRecord) -> None:
    self.records.append(record)
    self.active_errors.append(sys.exc_info()[1])


@contextmanager
def _capture_diagnostics(
  logger_name: str,
  *,
  level: int,
) -> Iterator[_ExceptionContextProbeHandler]:
  """Attach one handler without changing the logger's prior configuration."""
  source_logger = logging.getLogger(logger_name)
  previous_level = source_logger.level
  handler = _ExceptionContextProbeHandler()
  source_logger.addHandler(handler)
  source_logger.setLevel(level)
  try:
    yield handler
  finally:
    source_logger.removeHandler(handler)
    source_logger.setLevel(previous_level)


def _assert_handler_records_are_redacted(
  handler: _ExceptionContextProbeHandler,
  marker: str,
) -> None:
  """Assert a continuation diagnostic exposed no caught failure to a handler."""
  assert handler.records
  assert handler.active_errors == [None] * len(handler.records)
  assert all(marker not in record.getMessage() for record in handler.records)
  assert all(not record.args for record in handler.records)
  assert all(record.exc_info is None for record in handler.records)
  assert all(record.exc_text is None for record in handler.records)


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

  def test_rejects_key_prefix_that_storage_backends_cannot_accept(
    self, mock_connection_manager
  ):
    with pytest.raises(ValueError, match="Invalid key_prefix"):
      BackendPipeline(
        connection_manager=mock_connection_manager,
        key_prefix="bad prefix",
      )

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

  def test_scrapy_manager_registration_has_no_required_spider_warning(
    self, mock_connection_manager, mocker
  ):
    """Current Scrapy must be able to omit the deprecated spider argument."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)

    with warnings.catch_warnings():
      warnings.simplefilter("error", ScrapyDeprecationWarning)
      ItemPipelineManager(pipeline, crawler=mocker.Mock())

  def test_crawler_owned_spider_supports_argumentless_hooks(
    self, mock_connection_manager, mocker
  ):
    """Scrapy's new hook path resolves the spider saved by from_crawler()."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "crawler_spider"
    crawler = mocker.Mock()
    crawler.spider = spider
    crawler.stats = None
    mocker.patch.object(BackendPipeline, "from_settings", return_value=pipeline)

    created = BackendPipeline.from_crawler(crawler)
    created.open_spider()
    item = SampleItem(name="Test", value=123)

    assert created.process_item(item) is item

    created.close_spider()
    mock_connection_manager.get_storage_backend().store.assert_called_once()
    mock_connection_manager.close.assert_called_once_with()

  def test_argumentless_hook_without_crawler_or_opened_spider_fails_clearly(
    self, mock_connection_manager
  ):
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)

    with pytest.raises(RuntimeError, match="has no spider"):
      pipeline.open_spider()


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

  def test_open_spider_transient_error_does_not_abort(
    self, mock_connection_manager, mocker, caplog
  ):
    """A transient connection blip at open must not abort the crawl nor permanently disable storage."""
    import logging

    from scrapy_extension.exceptions import BackendConnectionError

    mock_connection_manager.get_storage_backend.side_effect = BackendConnectionError(
      "connection refused"
    )
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with caplog.at_level(logging.WARNING):
      pipeline.open_spider(mock_spider)  # must NOT raise

    # Neither True (confirmed) nor False (permanently disabled) — left as None
    # so process_item lazily retries storage on each item.
    assert pipeline._storage_supported is None
    assert "not reachable at spider open" in caplog.text

  @pytest.mark.parametrize(
    ("failure_kind", "expected_supported"),
    [
      ("unsupported", False),
      ("unreachable", None),
    ],
  )
  def test_open_fallback_diagnostic_leaves_no_active_exception_for_handler(
    self,
    mock_connection_manager,
    mocker,
    failure_kind: str,
    expected_supported: bool | None,
  ):
    """A caught startup blip must be gone before a warning handler runs."""
    marker = "round47-pipeline-open-private-marker"
    failure: BaseException = (
      NotImplementedError(marker)
      if failure_kind == "unsupported"
      else BackendConnectionError(marker, backend_type=marker)
    )
    mock_connection_manager.get_storage_backend.side_effect = failure
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "safe-spider"

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.WARNING,
    ) as handler:
      pipeline.open_spider(spider)

    assert pipeline._storage_supported is expected_supported
    _assert_handler_records_are_redacted(handler, marker)

  @pytest.mark.parametrize(
    ("failure", "expected_supported"),
    [
      (NotImplementedError, False),
      (BackendConnectionError("connection refused"), None),
    ],
  )
  def test_open_expected_warning_handler_interruption_keeps_pipeline_live(
    self,
    mock_connection_manager,
    mocker,
    failure,
    expected_supported,
  ):
    """R102: expected-open diagnostics cannot roll back a live pipeline."""
    mock_connection_manager.get_storage_backend.side_effect = failure
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.warning",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "test_spider"

    pipeline.open_spider(spider)

    assert pipeline._storage_supported is expected_supported
    assert pipeline._opened is True
    assert pipeline._opened_spider is spider
    mock_connection_manager.close.assert_not_called()

  def test_open_success_log_interruption_keeps_pipeline_live(
    self, mock_connection_manager, mocker
  ):
    """R102: publish precedes info diagnostics and must remain observable."""
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.info",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "test_spider"

    pipeline.open_spider(spider)

    assert pipeline._storage_supported is True
    assert pipeline._opened is True
    assert pipeline._opened_spider is spider
    mock_connection_manager.close.assert_not_called()

  def test_open_spider_programming_error_propagates(
    self, mock_connection_manager, mocker
  ):
    """A non-connection exception (real bug) must still fail fast at open, not be swallowed as transient."""
    mock_connection_manager.get_storage_backend.side_effect = TypeError("bad config")
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    with pytest.raises(TypeError):
      pipeline.open_spider(mock_spider)
    # And storage is NOT marked supported (the bug never confirmed it).
    assert pipeline._storage_supported is None

  def test_open_rollback_preserves_primary_control_error_when_diagnostics_abort(
    self, mock_connection_manager, mocker
  ):
    """R86: cleanup and its logger cannot mask the failed open's cause."""
    strategy = mocker.MagicMock()
    original_error = KeyboardInterrupt("open interrupted")
    cleanup_contexts: list[tuple[object | None, object | None, object | None]] = []

    def fail_open() -> None:
      raise original_error

    def fail_close() -> None:
      cleanup_contexts.append(sys.exc_info())
      raise RuntimeError("release failed")

    strategy.open.side_effect = fail_open
    strategy.close.side_effect = fail_close
    mock_connection_manager.close.side_effect = RuntimeError("manager release failed")
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
    )
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.ERROR,
    ) as handler:
      with pytest.raises(KeyboardInterrupt) as exc_info:
        pipeline.open_spider(spider)

    assert exc_info.value is original_error
    assert cleanup_contexts == [(None, None, None)]
    strategy.open.assert_called_once_with()
    strategy.close.assert_called_once_with()
    mock_connection_manager.close.assert_called_once_with()
    _assert_handler_records_are_redacted(handler, "release failed")

  def test_open_spider_rejects_name_that_cannot_form_a_storage_key(
    self, mock_connection_manager, mocker
  ):
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "bad spider"

    with pytest.raises(ValueError, match="Invalid spider.name"):
      pipeline.open_spider(spider)

    mock_connection_manager.get_storage_backend.assert_not_called()
    mock_connection_manager.close.assert_called_once_with()

  def test_open_spider_opens_storage_strategy_once(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
    )
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    pipeline.open_spider(spider)
    pipeline.open_spider(spider)

    strategy.open.assert_called_once_with()

  def test_open_spider_rejects_a_different_spider(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
    )
    first = mocker.Mock(name="first")
    first.name = "first"
    second = mocker.Mock(name="second")
    second.name = "second"
    pipeline.open_spider(first)

    with pytest.raises(RuntimeError, match="different spider"):
      pipeline.open_spider(second)

    strategy.open.assert_called_once_with()
    mock_connection_manager.close.assert_not_called()


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

  def test_close_spider_releases_resources_when_close_log_interrupts(
    self, mock_connection_manager, mocker
  ):
    """A logging handler interruption cannot skip terminal teardown."""
    strategy = mocker.MagicMock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
    )
    spider = mocker.Mock()
    spider.name = "test_spider"
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.info",
      side_effect=KeyboardInterrupt,
    )

    pipeline.close_spider(spider)

    strategy.close.assert_called_once_with()
    mock_connection_manager.close.assert_called_once_with()
    assert pipeline._closed is True

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

  def test_duplicate_close_closes_strategy_and_manager_once(
    self, mock_connection_manager, mocker
  ):
    strategy = mocker.MagicMock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
    )
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    pipeline.close_spider(spider)
    pipeline.close_spider(spider)

    strategy.close.assert_called_once_with()
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

  def test_process_item_after_close_is_rejected(
    self, mock_connection_manager, mocker
  ):
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.name = "test_spider"
    pipeline.close_spider(spider)

    with pytest.raises(RuntimeError, match="closed"):
      pipeline.process_item(SampleItem(name="late", value=1), spider)

    mock_connection_manager.get_storage_backend.assert_not_called()

  def test_process_item_rejects_unsupported_root_object(
    self, mock_connection_manager, mocker
  ):
    """Unsupported roots must not be silently stringified and persisted."""
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )

    mock_spider = mocker.Mock()
    mock_spider.name = "test_spider"

    item = object()

    with pytest.raises(SerializationError, match="Failed to serialize item"):
      pipeline.process_item(item, mock_spider)  # type: ignore[arg-type]

    mock_connection_manager.get_storage_backend().store.assert_not_called()

  def test_process_item_serializes_dataclass_via_item_adapter(
    self, mock_connection_manager, mocker
  ):
    @dataclass
    class DataclassItem:
      name: str
      value: int

    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix="items",
    )
    spider = mocker.Mock()
    spider.name = "test_spider"
    item = DataclassItem(name="structured", value=7)

    assert pipeline.process_item(item, spider) is item  # type: ignore[arg-type]
    wire = mock_connection_manager.get_storage_backend().store.call_args.args[1]
    assert JSONSerializer().deserialize(wire) == {
      "name": "structured",
      "value": 7,
    }

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
    _validate_key_name(key, "key")

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

  def test_store_warning_handler_interruption_preserves_best_effort_result(
    self, mock_connection_manager, mocker
  ):
    """R102: a warning handler cannot turn an ordinary store failure fatal."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True
    mock_connection_manager.get_storage_backend().store.side_effect = RuntimeError(
      "connection refused"
    )
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.warning",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    spider = mocker.Mock()
    spider.name = "test_spider"
    item = SampleItem(name="Test", value=1)

    assert pipeline.process_item(item, spider) is item
    spider.crawler.stats.inc_value.assert_called_with("pipeline/storage_errors")

  def test_process_item_propagates_deterministic_backend_validation_error(
    self, mock_connection_manager, mocker
  ):
    """A backend's local validation failure must not look like stored data."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True
    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = ValueError("item exceeds backend limit")
    spider = mocker.Mock()
    spider.name = "test_spider"

    with pytest.raises(ValueError, match="backend limit"):
      pipeline.process_item(SampleItem(name="Test", value=1), spider)

    spider.crawler.stats.inc_value.assert_not_called()

  def test_process_item_wraps_item_serialization_failure(
    self, mock_connection_manager, mocker
  ):
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "test_spider"
    item = SampleItem(name="bad", value=object())

    with pytest.raises(SerializationError, match="Failed to serialize item"):
      pipeline.process_item(item, spider)

    mock_connection_manager.get_storage_backend().store.assert_not_called()

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

  def test_inc_stat_keeps_ordinary_failure_nonfatal_when_debug_interrupts(
    self, mock_connection_manager, mocker
  ):
    """R102: stats fallback diagnostics cannot replace an ordinary failure."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.crawler.stats.inc_value.side_effect = RuntimeError("stats unavailable")
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.debug",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )

    pipeline._inc_stat(spider, "pipeline/storage_errors")

  def test_inc_stat_propagates_direct_control_exception(
    self, mock_connection_manager, mocker
  ):
    """R102: only fallback diagnostics are isolated, not stats controls."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    original_error = KeyboardInterrupt("stats interrupted")
    spider.crawler.stats.inc_value.side_effect = original_error

    with pytest.raises(KeyboardInterrupt) as exc_info:
      pipeline._inc_stat(spider, "pipeline/storage_errors")

    assert exc_info.value is original_error


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

    with pytest.raises(SerializationError, match="Failed to serialize item"):
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

  def test_success_debug_handler_interruption_keeps_persisted_result(
    self, mock_connection_manager, mocker
  ):
    """R102: post-store debug is diagnostic-only after a successful write."""
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.debug",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    spider = mocker.Mock()
    spider.name = "test_spider"
    item = SampleItem(name="Test", value=123)

    assert pipeline.process_item(item, spider) is item
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

  def test_shared_batched_strategy_preserves_pipeline_backend_affinity(self, mocker):
    """Each pipeline item drains through the backend accepted with that item."""
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    backend_a = mocker.Mock()
    backend_b = mocker.Mock()
    manager_a = mocker.Mock()
    manager_b = mocker.Mock()
    manager_a.get_storage_backend.return_value = backend_a
    manager_b.get_storage_backend.return_value = backend_b
    strategy = BatchedStorageStrategy(threshold=2)
    pipeline_a = BackendPipeline(
      connection_manager=manager_a,
      ttl=11,
      storage_strategy=strategy,
    )
    pipeline_b = BackendPipeline(
      connection_manager=manager_b,
      ttl=22,
      storage_strategy=strategy,
    )
    pipeline_a._storage_supported = True
    pipeline_b._storage_supported = True
    spider_a = mocker.Mock(name="spider_a")
    spider_b = mocker.Mock(name="spider_b")
    spider_a.name = "alpha"
    spider_b.name = "beta"

    pipeline_a.process_item(SampleItem(name="a", value=1), spider_a)
    pipeline_b.process_item(SampleItem(name="b", value=2), spider_b)

    backend_a.store.assert_called_once()
    backend_b.store.assert_called_once()
    assert backend_a.store.call_args.kwargs == {"ttl": 11}
    assert backend_b.store.call_args.kwargs == {"ttl": 22}
    assert strategy.pending == 0

  def test_batched_monitor_reports_only_durable_flushes(
    self, mock_connection_manager, mocker
  ):
    """Buffered acceptance is not reported as a completed backend write."""
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    monitor = mocker.Mock()
    strategy = BatchedStorageStrategy(threshold=100)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "s"

    pipeline.process_item(SampleItem(name="a", value=1), spider)
    pipeline.process_item(SampleItem(name="b", value=2), spider)

    monitor.on_store.assert_not_called()
    pipeline.close_spider(spider)
    assert monitor.on_store.call_count == 2

  def test_batched_threshold_failure_drives_pipeline_error_guard(
    self, mock_connection_manager, mocker
  ):
    """A volatile retry tail is not reported as a successful persisted item."""
    from scrapy_extension.storage.strategies.batched import (
      BatchedStorageStrategy,
    )

    monitor = mocker.Mock()
    strategy = BatchedStorageStrategy(threshold=1)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
      max_storage_errors=0,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    mock_connection_manager.get_storage_backend().store.side_effect = RuntimeError(
      "backend down"
    )
    spider = mocker.Mock()
    spider.name = "s"

    with pytest.raises(
      BackendError,
      match=r"^Pipeline storage failure threshold exceeded\.$",
    ):
      pipeline.process_item(SampleItem(name="a", value=1), spider)

    assert strategy.pending == 1
    monitor.on_store.assert_not_called()

  def test_batched_backpressure_is_propagated_without_best_effort_swallowing(
    self, mock_connection_manager, mocker
  ):
    """Admission rejection is not a persisted-or-retryable storage outcome."""
    strategy = mocker.Mock()
    strategy.emits_store_events = True
    strategy.store.side_effect = StorageBackpressureError(operation="store")
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      storage_strategy=strategy,
      max_storage_errors=None,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "s"

    with pytest.raises(StorageBackpressureError) as exc_info:
      pipeline.process_item(SampleItem(name="a", value=1), spider)

    assert exc_info.value.operation == "store"
    spider.crawler.stats.inc_value.assert_not_called()

  def test_batched_backpressure_rebuilds_a_terminal_error_after_private_frames(
    self, mock_connection_manager, mocker
  ):
    """Admission rejection must not retain item/key/strategy data in traceback."""
    marker = "round44_pipeline_backpressure_private_marker"

    class _SensitiveBackpressureError(StorageBackpressureError):
      def __init__(self) -> None:
        super().__init__(operation=marker)
        self.private_state = {"marker": marker}

    def reject_store(*_args, **_kwargs):
      private_call_state = {"marker": marker}
      del private_call_state
      raise _SensitiveBackpressureError

    monitor = mocker.Mock()
    strategy = mocker.Mock()
    strategy.emits_store_events = True
    strategy.store.side_effect = reject_store
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix=marker,
      storage_strategy=strategy,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = marker

    with pytest.raises(StorageBackpressureError) as exc_info:
      pipeline.process_item(SampleItem(name=marker, value=marker), spider)

    error = exc_info.value
    assert type(error) is StorageBackpressureError
    assert str(error) == "Batched storage is at capacity."
    assert error.operation == "store"
    assert error.key is None
    _assert_pipeline_public_error_is_redacted(error, marker)
    spider.crawler.stats.inc_value.assert_not_called()
    monitor.on_error.assert_not_called()

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
  consecutive storage failures and re-raises a fixed ``BackendError``
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
    re-raises a fixed ``BackendError``.
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
    with pytest.raises(
      BackendError,
      match=r"^Pipeline storage failure threshold exceeded\.$",
    ):
      pipeline.process_item(SampleItem(name="c", value=3), mock_spider)

  def test_threshold_error_is_terminally_redacted(
    self, mock_connection_manager, mocker, caplog
  ):
    """The opt-in escalation cannot expose its failing item/key/error graph."""
    marker = "round44_pipeline_threshold_private_marker"
    monitor = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix=marker,
      max_storage_errors=0,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    raw_error = RuntimeError(marker)
    mock_connection_manager.get_storage_backend().store.side_effect = raw_error
    spider = mocker.Mock()
    spider.name = marker

    with caplog.at_level(
      logging.WARNING,
      logger="scrapy_extension.pipeline.pipeline",
    ):
      with pytest.raises(BackendError) as exc_info:
        pipeline.process_item(SampleItem(name=marker, value=marker), spider)

    error = exc_info.value
    assert type(error) is BackendError
    assert str(error) == "Pipeline storage failure threshold exceeded."
    _assert_pipeline_public_error_is_redacted(error, marker)
    assert pipeline._consecutive_storage_errors == 1
    spider.crawler.stats.inc_value.assert_called_once_with("pipeline/storage_errors")

    monitor.on_error.assert_called_once()
    reported_error = monitor.on_error.call_args.args[1]
    assert type(reported_error) is StorageError
    _assert_pipeline_public_error_is_redacted(reported_error, marker)
    assert all(marker not in record.getMessage() for record in caplog.records)
    assert all(record.exc_info is None for record in caplog.records)

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

  def test_storage_failure_emits_monitor_on_error(
    self, mock_connection_manager, mocker
  ):
    """A store failure emits one fresh, key-free ``StorageError`` to monitor.

    The event preserves the store error counter while withholding the raw
    backend exception graph from monitor extensions.
    """
    monitor = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True

    sentinel = RuntimeError("connection refused")
    mock_storage = mock_connection_manager.get_storage_backend()
    mock_storage.store.side_effect = sentinel

    mock_spider = mocker.Mock()
    mock_spider.name = "s"

    pipeline.process_item(SampleItem(name="x", value=1), mock_spider)

    monitor.on_error.assert_called_once()
    assert monitor.on_error.call_args[0][0] == "store"
    reported_error = monitor.on_error.call_args[0][1]
    assert isinstance(reported_error, StorageError)
    assert reported_error is not sentinel
    assert str(reported_error) == "Pipeline storage operation failed."
    assert reported_error.operation == "store"
    assert reported_error.key is None

  def test_best_effort_store_diagnostics_and_monitor_are_redacted(
    self, mock_connection_manager, mocker, caplog
  ):
    """The default swallow path logs and reports no key/item/backend details."""
    marker = "round44_pipeline_monitor_private_marker"
    monitor = mocker.Mock()
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      key_prefix=marker,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    raw_error = RuntimeError(marker)
    mock_connection_manager.get_storage_backend().store.side_effect = raw_error
    spider = mocker.Mock()
    spider.name = marker
    item = SampleItem(name=marker, value=marker)

    with caplog.at_level(
      logging.WARNING,
      logger="scrapy_extension.pipeline.pipeline",
    ):
      assert pipeline.process_item(item, spider) is item

    monitor.on_error.assert_called_once()
    reported_error = monitor.on_error.call_args.args[1]
    assert type(reported_error) is StorageError
    assert str(reported_error) == "Pipeline storage operation failed."
    assert reported_error.operation == "store"
    assert reported_error.key is None
    _assert_pipeline_public_error_is_redacted(reported_error, marker)
    spider.crawler.stats.inc_value.assert_called_once_with("pipeline/storage_errors")
    assert all(marker not in record.getMessage() for record in caplog.records)
    assert all(record.exc_info is None for record in caplog.records)

  def test_storage_fallback_diagnostics_leave_no_active_exception_for_handler(
    self, mock_connection_manager, mocker
  ):
    """Store, monitor, and stats fallbacks run after their caught failures."""
    marker = "round47-pipeline-store-private-marker"
    monitor = mocker.Mock()
    monitor.on_error.side_effect = RuntimeError(marker)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    mock_connection_manager.get_storage_backend().store.side_effect = RuntimeError(
      marker
    )
    spider = mocker.Mock()
    spider.name = "safe-spider"
    item = SampleItem(name="safe", value=1)

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.DEBUG,
    ) as handler:
      assert pipeline.process_item(item, spider) is item

    monitor.on_error.assert_called_once()
    _assert_handler_records_are_redacted(handler, marker)

  def test_stats_fallback_diagnostic_leaves_no_active_exception_for_handler(
    self, mock_connection_manager, mocker
  ):
    """A custom handler cannot inspect a swallowed stats-collector failure."""
    marker = "round47-pipeline-stats-private-marker"
    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    spider = mocker.Mock()
    spider.crawler.stats.inc_value.side_effect = RuntimeError(marker)

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.DEBUG,
    ) as handler:
      pipeline._inc_stat(spider, "pipeline/storage_errors")

    _assert_handler_records_are_redacted(handler, marker)

  def test_serialization_monitor_fallback_handler_has_no_active_exception(
    self, mock_connection_manager, mocker
  ):
    """Monitor-fallback logging in the terminal boundary is also isolated."""
    marker = "round47-pipeline-serialization-monitor-private-marker"
    monitor = mocker.Mock()
    monitor.on_error.side_effect = RuntimeError(marker)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "safe-spider"

    def fail_serialization(_item: object) -> bytes:
      raise RuntimeError(marker)

    pipeline._serialize_item = fail_serialization

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.DEBUG,
    ) as handler:
      with pytest.raises(SerializationError):
        pipeline.process_item(SampleItem(name="safe", value=1), spider)

    monitor.on_error.assert_called_once()
    _assert_handler_records_are_redacted(handler, marker)

  def test_serialization_stats_callback_runs_after_raw_error_unwinds(
    self, mock_connection_manager
  ):
    """A custom stats extension cannot inspect the serializer failure."""
    marker = "round48-pipeline-serialization-stats-marker"
    observed_contexts: list[tuple[object | None, object | None, object | None]] = []

    class StatsProbe:
      def inc_value(self, _stat_name: str) -> None:
        observed_contexts.append(sys.exc_info())

    pipeline = BackendPipeline(connection_manager=mock_connection_manager)
    pipeline._storage_supported = True
    spider = type(
      "Spider",
      (),
      {"name": "safe-spider", "crawler": type("Crawler", (), {"stats": StatsProbe()})()},
    )()

    def fail_serialization(_item: object) -> bytes:
      raise RuntimeError(marker)

    pipeline._serialize_item = fail_serialization

    with pytest.raises(SerializationError, match="Failed to serialize item"):
      pipeline.process_item(SampleItem(name="safe", value=1), spider)

    assert observed_contexts == [(None, None, None)]

  def test_on_store_failure_does_not_fail_already_persisted_item(
    self, mock_connection_manager, mocker
  ):
    """Observability callbacks cannot reverse a successful storage write."""
    monitor = mocker.Mock()
    monitor.on_store.side_effect = RuntimeError("monitor boom")
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "s"
    item = SampleItem(name="x", value=1)

    assert pipeline.process_item(item, spider) is item
    mock_connection_manager.get_storage_backend().store.assert_called_once()

  def test_store_success_monitor_fallback_handler_has_no_active_exception(
    self, mock_connection_manager, mocker
  ):
    """A successful-store monitor fallback also logs after its catch ends."""
    marker = "round47-pipeline-store-success-monitor-private-marker"
    monitor = mocker.Mock()
    monitor.on_store.side_effect = RuntimeError(marker)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "safe-spider"

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.DEBUG,
    ) as handler:
      assert pipeline.process_item(SampleItem(name="safe", value=1), spider)

    observations = [
      (record, active_error)
      for record, active_error in zip(
        handler.records,
        handler.active_errors,
        strict=True,
      )
      if record.getMessage() == "Pipeline store-success monitor callback raised; ignored."
    ]
    assert len(observations) == 1
    record, active_error = observations[0]
    assert active_error is None
    assert marker not in record.getMessage()
    assert not record.args
    assert record.exc_info is None
    assert record.exc_text is None

  def test_monitor_failure_debug_interruption_keeps_best_effort_result(
    self, mock_connection_manager, mocker
  ):
    """R102: a logger handler cannot replace an ordinary monitor failure."""
    monitor = mocker.Mock()
    monitor.on_error.side_effect = RuntimeError("monitor boom")
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    mock_connection_manager.get_storage_backend().store.side_effect = RuntimeError(
      "connection refused"
    )
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.debug",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    spider = mocker.Mock()
    spider.name = "s"
    item = SampleItem(name="x", value=1)

    assert pipeline.process_item(item, spider) is item
    monitor.on_error.assert_called_once()

  def test_on_store_failure_debug_interruption_keeps_persisted_result(
    self, mock_connection_manager, mocker
  ):
    """R102: on_store fallback logging cannot reverse a successful write."""
    monitor = mocker.Mock()
    monitor.on_store.side_effect = RuntimeError("monitor boom")
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    mocker.patch(
      "scrapy_extension.pipeline.pipeline.logger.debug",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    spider = mocker.Mock()
    spider.name = "s"
    item = SampleItem(name="x", value=1)

    assert pipeline.process_item(item, spider) is item
    monitor.on_store.assert_called_once()

  def test_scrapy_stats_monitor_debug_interruption_keeps_persisted_result(
    self, mock_connection_manager, mocker
  ):
    """R102: the concrete default monitor cannot make a store appear failed."""
    from scrapy_extension.monitor import ScrapyStatsMonitor

    stats = mocker.Mock()
    stats.inc_value.side_effect = RuntimeError("stats unavailable")
    monitor = ScrapyStatsMonitor(stats)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    mocker.patch(
      "scrapy_extension.monitor.stats.logger.debug",
      side_effect=KeyboardInterrupt("logger interrupted"),
    )
    spider = mocker.Mock()
    spider.name = "s"
    item = SampleItem(name="x", value=1)

    assert pipeline.process_item(item, spider) is item
    mock_connection_manager.get_storage_backend().store.assert_called_once()

  def test_scrapy_stats_monitor_fallback_handler_has_no_active_exception(
    self, mock_connection_manager, mocker
  ):
    """The concrete stats monitor exits its catch before logging a fallback."""
    from scrapy_extension.monitor import ScrapyStatsMonitor

    marker = "round47-pipeline-monitor-stats-private-marker"
    stats = mocker.Mock()
    stats.inc_value.side_effect = RuntimeError(marker)
    monitor = ScrapyStatsMonitor(stats)
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "safe-spider"

    with _capture_diagnostics(
      "scrapy_extension.monitor.stats",
      level=logging.DEBUG,
    ) as handler:
      assert pipeline.process_item(SampleItem(name="safe", value=1), spider)

    _assert_handler_records_are_redacted(handler, marker)

  def test_monitor_control_exception_remains_observable(
    self, mock_connection_manager, mocker
  ):
    """R102: direct monitor control exceptions are not telemetry fallback."""
    monitor = mocker.Mock()
    original_error = KeyboardInterrupt("monitor interrupted")
    monitor.on_store.side_effect = original_error
    pipeline = BackendPipeline(
      connection_manager=mock_connection_manager,
      monitor=monitor,
    )
    pipeline._storage_supported = True
    spider = mocker.Mock()
    spider.name = "s"

    with pytest.raises(KeyboardInterrupt) as exc_info:
      pipeline.process_item(SampleItem(name="x", value=1), spider)

    assert exc_info.value is original_error
    mock_connection_manager.get_storage_backend().store.assert_called_once()

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


class TestBackendPipelineCloseBaseException:
  """R20-B: _close_locked must not swallow a BaseException from
  connection_manager.close() when storage_strategy.close() succeeded."""

  def test_close_locked_reraises_baseexception_when_no_primary_error(self, mocker) -> None:
    """A Ctrl+C during connection_manager.close() (after the strategy flush
    succeeded) must propagate, not be swallowed.

    Pre-R20-B the manager close was wrapped in 'except BaseException:
    logger.exception(...)' with no raise, so a KeyboardInterrupt during the
    blocking backend disconnect was silently discarded — the operator could not
    break a hung shutdown. Mirror the dupefilter primary_error pattern: when
    manager close is the ONLY failure, re-raise it.
    """
    manager = mocker.MagicMock()
    manager.close.side_effect = KeyboardInterrupt
    strategy = mocker.MagicMock()  # close() is a no-op (succeeds)
    pipeline = BackendPipeline(connection_manager=manager, storage_strategy=strategy)

    with pytest.raises(KeyboardInterrupt):
      pipeline._close_locked()

    strategy.close.assert_called_once()
    manager.close.assert_called_once()

  def test_close_locked_preserves_strategy_error_over_manager_baseexception(
    self, mocker
  ) -> None:
    """A strategy close error is the primary_error; a BaseException from the
    later manager close must not mask it."""
    manager = mocker.MagicMock()
    manager.close.side_effect = SystemExit
    strategy = mocker.MagicMock()
    strategy.close.side_effect = RuntimeError("flush failed")
    pipeline = BackendPipeline(connection_manager=manager, storage_strategy=strategy)

    # The strategy RuntimeError is the primary error; the manager SystemExit is logged, not raised.
    with pytest.raises(RuntimeError, match="flush failed"):
      pipeline._close_locked()

    strategy.close.assert_called_once()
    manager.close.assert_called_once()

  def test_close_locked_secondary_manager_diagnostic_has_no_active_exception(
    self, mocker
  ) -> None:
    """A handler cannot recover the secondary teardown failure.

    The primary strategy failure must still win, and manager teardown must run
    after it.  The distinct markers ensure the logging record and handler
    context expose neither failure from the secondary diagnostic path.
    """
    primary_marker = "round50-pipeline-primary-close-marker"
    secondary_marker = "round50-pipeline-secondary-close-marker"
    call_order: list[str] = []
    manager = mocker.MagicMock()
    strategy = mocker.MagicMock()

    def fail_strategy_close() -> None:
      call_order.append("strategy")
      raise RuntimeError(primary_marker)

    def fail_manager_close() -> None:
      call_order.append("manager")
      raise SystemExit(secondary_marker)

    strategy.close.side_effect = fail_strategy_close
    manager.close.side_effect = fail_manager_close
    pipeline = BackendPipeline(connection_manager=manager, storage_strategy=strategy)

    with _capture_diagnostics(
      "scrapy_extension.pipeline.pipeline",
      level=logging.ERROR,
    ) as handler:
      with pytest.raises(RuntimeError, match=primary_marker) as exc_info:
        pipeline._close_locked()

    assert str(exc_info.value) == primary_marker
    assert call_order == ["strategy", "manager"]
    _assert_handler_records_are_redacted(handler, secondary_marker)
    assert all(primary_marker not in record.getMessage() for record in handler.records)


def test_from_crawler_wires_monitor_into_connection_manager(mocker) -> None:
  """R25-F: from_crawler threads the ScrapyStatsMonitor into the pipeline's
  ConnectionManager so backend lifecycle counters cover the storage backend in
  multi-backend deployments (queue!=storage). Pre-fix only the scheduler's
  manager was wired."""
  mock_cm = mocker.MagicMock()
  pipeline = BackendPipeline(connection_manager=mock_cm)
  pipeline.storage_strategy = mocker.MagicMock()  # ensure attr present for from_crawler
  mocker.patch.object(BackendPipeline, "from_settings", return_value=pipeline)
  crawler = mocker.MagicMock()
  result = BackendPipeline.from_crawler(crawler)
  assert result is pipeline
  mock_cm.set_monitor.assert_called_once()
  # R26-C: assert the wired monitor is a ScrapyStatsMonitor (not a NullMonitor
  # left by a refactor mistake) — assert_called_once() alone would pass either.
  from scrapy_extension.monitor.stats import ScrapyStatsMonitor

  assert isinstance(
    mock_cm.set_monitor.call_args[0][0], ScrapyStatsMonitor
  )
