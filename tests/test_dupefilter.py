"""Tests for BackendDupeFilter component."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from scrapy.http import Request

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter


class TestBackendDupeFilterInit:
  """Test BackendDupeFilter __init__ method."""

  def test_init_with_defaults(self, mock_connection_manager):
    """Test initialization with default values."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    assert dupefilter.connection_manager is mock_connection_manager
    assert dupefilter.key == "dupefilter"
    assert dupefilter.debug is False

  def test_init_with_custom_key(self, mock_connection_manager):
    """Test initialization with custom key."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="custom:dupefilter",
    )

    assert dupefilter.key == "custom:dupefilter"

  def test_init_with_debug_true(self, mock_connection_manager):
    """Test initialization with debug=True."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      debug=True,
    )

    assert dupefilter.debug is True

  def test_init_with_all_params(self, mock_connection_manager):
    """Test initialization with all parameters."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="my:filter",
      debug=True,
    )

    assert dupefilter.connection_manager is mock_connection_manager
    assert dupefilter.key == "my:filter"
    assert dupefilter.debug is True


class TestBackendDupeFilterClassMethods:
  """Test BackendDupeFilter class methods."""

  def test_from_settings_defaults(self, mocker):
    """Test from_settings with default values."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)

    assert dupefilter.connection_manager is mock_manager
    assert dupefilter.key == "dupefilter"
    assert dupefilter.debug is False

  def test_from_settings_custom_values(self, mocker):
    """Test from_settings with custom values."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "mongodb",
      "SCRAPY_DUPEFILTER_KEY": "my:filter",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": True,
    }.get(key, default)
    mock_settings.getdict.return_value = {"uri": "mongodb://localhost:27017"}

    mock_manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)

    assert dupefilter.connection_manager is mock_manager
    assert dupefilter.key == "my:filter"
    assert dupefilter.debug is True

  def test_from_settings_redis_backend_type(self, mocker):
    """Test from_settings infers Redis backend type."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    BackendDupeFilter.from_settings(mock_settings)

    # Verify the manager was created
    assert mock_manager is not None

  def test_from_crawler(self, mocker):
    """Test from_crawler delegates to from_settings."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_crawler = mocker.Mock()
    mock_crawler.settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "crawler:filter",
    }.get(key, default)
    mock_crawler.settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": True,
    }.get(key, default)
    mock_crawler.settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_crawler(mock_crawler)

    assert dupefilter.key == "crawler:filter"
    assert dupefilter.debug is True

  def test_from_crawler_threads_request_fingerprinter(self, mocker):
    """R45: from_crawler wires crawler.request_fingerprinter into the dupefilter."""
    from scrapy_extension.backends.connectors import ConnectionManager

    sentinel_fp = mocker.MagicMock(name="custom-fingerprinter")
    mock_crawler = mocker.Mock()
    mock_crawler.request_fingerprinter = sentinel_fp
    mock_crawler.settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
    }.get(key, default)
    mock_crawler.settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_crawler.settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    dupefilter = BackendDupeFilter.from_crawler(mock_crawler)
    assert dupefilter._fingerprinter is sentinel_fp

  def test_from_crawler_falls_back_when_no_fingerprinter(self, mocker):
    """R45: a crawler without request_fingerprinter degrades to the default fn."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_crawler = mocker.Mock(spec=["settings"])  # no request_fingerprinter attr
    mock_crawler.settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
    }.get(key, default)
    mock_crawler.settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_crawler.settings.getdict.return_value = {}

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mocker.Mock())

    dupefilter = BackendDupeFilter.from_crawler(mock_crawler)
    assert dupefilter._fingerprinter is None


class TestBackendDupeFilterOpenClose:
  """Test BackendDupeFilter open and close methods."""

  def test_open_is_noop(self, mock_connection_manager):
    """Test open method does nothing."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    # Should not raise
    dupefilter.open()

  def test_close_is_noop(self, mock_connection_manager):
    """Test close method does nothing."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    # Should not raise
    dupefilter.close("finished")
    dupefilter.close("closed")
    dupefilter.close("")

  def test_close_calls_connection_manager_close(self, mock_connection_manager):
    """Test close shuts down the connection manager."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    dupefilter.close("finished")

    mock_connection_manager.close.assert_called_once_with()

  def test_close_releases_connection_manager_only_once(self, mock_connection_manager):
    """Duplicate close notifications must not over-release a shared manager."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    dupefilter.close("finished")
    dupefilter.close("duplicate")

    mock_connection_manager.close.assert_called_once_with()

  def test_duplicate_close_closes_membership_filter_once(
    self, mock_connection_manager, mocker
  ):
    membership_filter = mocker.MagicMock()
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )

    dupefilter.close("finished")
    dupefilter.close("duplicate")

    membership_filter.close.assert_called_once_with()

  def test_duplicate_open_is_idempotent(self, mock_connection_manager, mocker):
    membership_filter = mocker.MagicMock()
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
      clear_on_open=True,
    )
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    dupefilter.open(spider)
    dupefilter.open(spider)

    membership_filter.open.assert_called_once_with()
    membership_filter.clear.assert_called_once_with()

  def test_open_for_different_spider_is_rejected(
    self, mock_connection_manager, mocker
  ):
    membership_filter = mocker.MagicMock()
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    first = mocker.Mock(name="first")
    first.name = "first"
    second = mocker.Mock(name="second")
    second.name = "second"
    dupefilter.open(first)

    with pytest.raises(RuntimeError, match="different spider"):
      dupefilter.open(second)

    membership_filter.open.assert_called_once_with()

  def test_open_failure_closes_filter_and_manager(
    self, mock_connection_manager, mocker
  ):
    membership_filter = mocker.MagicMock()
    membership_filter.open.side_effect = RuntimeError("filter open failed")
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )

    with pytest.raises(RuntimeError, match="filter open failed"):
      dupefilter.open()

    membership_filter.close.assert_called_once_with()
    mock_connection_manager.close.assert_called_once_with()
    with pytest.raises(RuntimeError, match="closed"):
      dupefilter.open()

  @pytest.mark.parametrize("operation", ["open", "clear", "request_seen", "forget"])
  def test_operations_after_close_are_rejected(
    self, operation, mock_connection_manager
  ):
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)
    dupefilter.close("finished")
    request = Request("https://example.com")

    with pytest.raises(RuntimeError, match="closed"):
      if operation in {"request_seen", "forget"}:
        getattr(dupefilter, operation)(request)
      else:
        getattr(dupefilter, operation)()

  def test_filter_close_error_is_not_masked_by_manager_close_error(
    self, mock_connection_manager, mocker
  ):
    membership_filter = mocker.MagicMock()
    membership_filter.close.side_effect = RuntimeError("filter close failed")
    mock_connection_manager.close.side_effect = ConnectionError(
      "manager close failed"
    )
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )

    with pytest.raises(RuntimeError, match="filter close failed"):
      dupefilter.close("finished")

    membership_filter.close.assert_called_once_with()
    mock_connection_manager.close.assert_called_once_with()

  def test_from_crawler_rethreads_resolved_monitor_to_memory_filter(
    self, mock_connection_manager, mocker
  ):
    from scrapy_extension.dupefilter.filters.memory_filter import (
      MemoryMembershipFilter,
    )

    membership_filter = MemoryMembershipFilter(maxsize=1)
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    mocker.patch.object(BackendDupeFilter, "from_settings", return_value=dupefilter)
    crawler = mocker.MagicMock()
    crawler.stats = mocker.MagicMock()
    crawler.request_fingerprinter = None

    resolved = BackendDupeFilter.from_crawler(crawler)

    assert membership_filter._monitor is resolved._monitor


class TestBackendDupeFilterLog:
  """Test BackendDupeFilter log method."""

  def test_log_does_nothing_when_debug_false(self, mock_connection_manager, caplog):
    """Test log does nothing when debug is False."""
    caplog.clear()
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      debug=False,
    )

    request = Request(url="https://example.com")
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"

    dupefilter.log(request, mock_spider)

    # No debug log should be emitted
    assert "Filtered duplicate request" not in caplog.text

  def test_log_emits_debug_message_when_debug_true(
    self, mock_connection_manager, caplog
  ):
    """Test log emits debug message when debug is True."""
    import logging

    caplog.clear()
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      debug=True,
    )

    request = Request(url="https://example.com")
    mock_spider = mock_connection_manager.get_queue_backend()
    mock_spider.name = "test_spider"

    with caplog.at_level(logging.DEBUG):
      dupefilter.log(request, mock_spider)

    assert "Filtered duplicate request: https://example.com" in caplog.text
    # spider in extra dict appears in record, not in text
    assert len(caplog.records) == 1
    assert caplog.records[0].spider is mock_spider


class TestBackendDupeFilterRequestSeen:
  """Test BackendDupeFilter request_seen method."""

  def test_request_seen_new_request(self, mock_connection_manager):
    """Test seeing a new request that was not seen before."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.add.return_value = True  # Item was newly added (not duplicate)

    request = Request(url="https://example.com")
    result = dupefilter.request_seen(request)

    assert result is False  # Not a duplicate
    mock_set_backend.add.assert_called_once()
    # Verify the key and encoded fingerprint were passed
    call_args = mock_set_backend.add.call_args
    assert call_args[0][0] == "test:dupefilter"
    assert isinstance(call_args[0][1], bytes)

  def test_request_seen_duplicate_request(self, mock_connection_manager):
    """Test seeing a request that was already seen."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.add.return_value = False  # Item already existed (duplicate)

    request = Request(url="https://example.com")
    result = dupefilter.request_seen(request)

    assert result is True  # Is a duplicate
    mock_set_backend.add.assert_called_once()

  def test_request_seen_backend_not_implemented(self, mock_connection_manager):
    """Test request_seen raises when backend does not support sets."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_connection_manager.get_set_backend.side_effect = NotImplementedError(
      "Backend does not support set operations"
    )

    request = Request(url="https://example.com")

    with pytest.raises(RuntimeError, match="does not support set/duplicate filtering"):
      dupefilter.request_seen(request)

    mock_connection_manager.get_set_backend.assert_called_once()

  def test_request_seen_get_set_backend_raises_not_implemented_error(
    self, mock_connection_manager
  ):
    """Test request_seen raises clear guidance when set operations unsupported."""

    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_connection_manager.get_set_backend.side_effect = NotImplementedError(
      "Backend does not support set operations"
    )

    request = Request(url="https://example.com")

    with pytest.raises(RuntimeError) as exc_info:
      dupefilter.request_seen(request)

    message = str(exc_info.value)
    assert "does not support set/duplicate filtering" in message
    assert "use a backend with SetBackend" in message
    assert "disable BackendDupeFilter" in message

  def test_request_seen_uses_fingerprint(self, mock_connection_manager):
    """Test request_seen uses request_fingerprint for fingerprinting."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="test:dupefilter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.add.return_value = True

    request = Request(url="https://unique-url.com")
    dupefilter.request_seen(request)

    # Verify add was called with bytes of fingerprint
    call_args = mock_set_backend.add.call_args
    fingerprint_bytes = call_args[0][1]
    assert isinstance(fingerprint_bytes, bytes)
    # Fingerprint should be hex string encoded to bytes
    assert len(fingerprint_bytes) > 0


class TestBackendDupeFilterForget:
  """Compensate request_seen reservations when the later queue push fails."""

  def test_removable_filter_forgets_reservation(self, mock_connection_manager):
    membership_filter = MemoryMembershipFilter(maxsize=None)
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    request = Request("https://example.com/removable")

    assert dupefilter.request_seen(request) is False
    assert len(membership_filter) == 1

    dupefilter.forget(request)

    assert len(membership_filter) == 0
    assert dupefilter.request_seen(request) is False

  def test_bloom_filter_grants_exactly_one_retry_allowance(
    self, mock_connection_manager
  ):
    membership_filter = BloomMembershipFilter(capacity=100, error_rate=1e-9)
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    request = Request("https://example.com/bloom")

    assert dupefilter.request_seen(request) is False
    dupefilter.forget(request)

    assert dupefilter.request_seen(request) is False
    assert dupefilter.request_seen(request) is True

  def test_retry_allowance_has_single_linearized_concurrent_consumer(
    self, mock_connection_manager
  ):
    membership_filter = BloomMembershipFilter(capacity=100, error_rate=1e-9)
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    request = Request("https://example.com/concurrent-allowance")
    assert dupefilter.request_seen(request) is False
    dupefilter.forget(request)
    worker_count = 8
    barrier = threading.Barrier(worker_count)

    def request_seen_together() -> bool:
      barrier.wait()
      return dupefilter.request_seen(request)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
      results = list(executor.map(lambda _: request_seen_together(), range(worker_count)))

    assert results.count(False) == 1
    assert results.count(True) == worker_count - 1

  def test_retry_allowances_evict_oldest_at_fixed_bound(
    self, mock_connection_manager
  ):
    membership_filter = BloomMembershipFilter(capacity=100, error_rate=1e-9)
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      membership_filter=membership_filter,
    )
    dupefilter._retry_allowance_limit = 2
    requests = [Request(f"https://example.com/bounded/{i}") for i in range(3)]

    for request in requests:
      assert dupefilter.request_seen(request) is False
      dupefilter.forget(request)

    assert len(dupefilter._retry_allowances) == 2
    assert dupefilter.request_seen(requests[0]) is True
    assert dupefilter.request_seen(requests[1]) is False
    assert dupefilter.request_seen(requests[2]) is False


class TestBackendDupeFilterRequestFingerprint:
  """Test BackendDupeFilter request_fingerprint method."""

  def test_request_fingerprint_returns_string(self, mock_connection_manager):
    """Test request_fingerprint returns a string."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    request = Request(url="https://example.com")
    result = dupefilter.request_fingerprint(request)

    assert isinstance(result, str)
    assert len(result) > 0

  def test_request_fingerprint_same_request_same_fingerprint(
    self, mock_connection_manager
  ):
    """Test same request produces same fingerprint."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    request1 = Request(url="https://example.com")
    request2 = Request(url="https://example.com")

    fp1 = dupefilter.request_fingerprint(request1)
    fp2 = dupefilter.request_fingerprint(request2)

    assert fp1 == fp2

  def test_request_fingerprint_different_urls_different_fingerprints(
    self, mock_connection_manager
  ):
    """Test different URLs produce different fingerprints."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2")

    fp1 = dupefilter.request_fingerprint(request1)
    fp2 = dupefilter.request_fingerprint(request2)

    assert fp1 != fp2

  def test_request_fingerprint_is_hex_string(self, mock_connection_manager):
    """Test fingerprint is a valid hex string."""
    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)

    request = Request(url="https://example.com")
    fp = dupefilter.request_fingerprint(request)

    # Should be valid hex
    int(fp, 16)

  def test_request_fingerprint_uses_injected_fingerprinter(
    self, mock_connection_manager, mocker
  ):
    """R45: an injected fingerprinter is used instead of the default module function.

    BackendDupeFilter previously hardcoded ``scrapy.utils.request.fingerprint``,
    silently ignoring any configured ``REQUEST_FINGERPRINTER_CLASS``. Now it
    delegates to the injected fingerprinter (threaded from
    ``crawler.request_fingerprinter`` via ``from_crawler``) when present.
    """
    mock_fp = mocker.MagicMock()
    mock_fp.fingerprint.return_value = b"\xde\xad\xbe\xef"

    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      fingerprinter=mock_fp,
    )

    request = Request(url="https://example.com")
    result = dupefilter.request_fingerprint(request)

    mock_fp.fingerprint.assert_called_once_with(request)
    assert result == b"\xde\xad\xbe\xef".hex()  # "deadbeef"

  def test_request_fingerprint_falls_back_when_no_fingerprinter(
    self, mock_connection_manager
  ):
    """R45: without an injected fingerprinter, behavior is unchanged (default fn)."""
    from scrapy.utils.request import fingerprint as scrapy_fingerprint

    dupefilter = BackendDupeFilter(connection_manager=mock_connection_manager)
    assert dupefilter._fingerprinter is None

    request = Request(url="https://example.com")
    assert (
      dupefilter.request_fingerprint(request) == scrapy_fingerprint(request).hex()
    )


class TestBackendDupeFilterIntegration:
  """Integration tests for BackendDupeFilter."""

  def test_full_dupefilter_lifecycle(self, mock_connection_manager):
    """Test full lifecycle: new request, duplicate request, open, close."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="lifecycle:filter",
      debug=True,
    )

    # open and close should not raise
    dupefilter.open()
    dupefilter.close("finished")

  def test_multiple_unique_requests(self, mock_connection_manager):
    """Test multiple unique requests are all not filtered."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="multi:filter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    mock_set_backend.add.return_value = True  # All new

    urls = [
      "https://example.com/1",
      "https://example.com/2",
      "https://example.com/3",
    ]

    for url in urls:
      request = Request(url=url)
      result = dupefilter.request_seen(request)
      assert result is False

    # Should have 3 add calls
    assert mock_set_backend.add.call_count == 3

  def test_mixed_unique_and_duplicate_requests(self, mock_connection_manager):
    """Test mix of unique and duplicate requests."""
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="mixed:filter",
    )

    mock_set_backend = mock_connection_manager.get_set_backend()
    # First call returns True (new), second returns False (duplicate)
    mock_set_backend.add.side_effect = [True, False, True]

    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page1")  # Duplicate
    request3 = Request(url="https://example.com/page2")

    assert dupefilter.request_seen(request1) is False
    assert dupefilter.request_seen(request2) is True  # Duplicate
    assert dupefilter.request_seen(request3) is False

  def test_from_settings_then_request_seen(self, mocker):
    """Test creating via from_settings and then using request_seen."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "settings:filter",
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mock_set_backend = mocker.Mock()
    mock_set_backend.add.return_value = True
    mock_manager.get_set_backend.return_value = mock_set_backend

    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)

    request = Request(url="https://example.com")
    result = dupefilter.request_seen(request)

    assert result is False
    mock_set_backend.add.assert_called_once()


class TestBackendDupeFilterClearOnOpen:
  """D1 (C5 HIGH): SCRAPY_DUPEFILTER_CLEAR_ON_OPEN resets the dedup set at open()."""

  def test_clear_on_open_resets_seen_fingerprints(self, mocker):
    """clear_on_open=True: add fp → seen True → open(spider) → same request seen False.

    Pre-fix (RED): ``open(spider)`` is a no-op for the set filter, so the
    previously-seen fingerprint stays and the same request is reported
    seen=True on the second ``request_seen`` call. Post-fix (GREEN):
    ``open(spider)`` calls ``self.clear()`` and the second call returns False.
    """
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
      "SCRAPY_DEDUP_STRATEGY": "set",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
      "SCRAPY_DUPEFILTER_CLEAR_ON_OPEN": True,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mock_set_backend = mocker.Mock()
    # First add → newly added (True); after clear, the second add must also be
    # newly added (True) — proving the set was cleared.
    mock_set_backend.add.side_effect = [True, True]
    mock_manager.get_set_backend.return_value = mock_set_backend
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    request = Request(url="https://example.com/page")
    # First sighting: newly added → not a duplicate.
    assert dupefilter.request_seen(request) is False
    # Same request again: backend side_effect returns True on the 2nd call,
    # but without clear-on-open the dupefilter's own dedup state would still
    # consider it seen. After the fix, open(spider) clears the backend set.
    dupefilter.open(spider)
    mock_set_backend.clear_set.assert_called_once()
    # After clear, the same request must be newly added again (not seen).
    assert dupefilter.request_seen(request) is False

  def test_clear_on_open_default_is_false_preserves_behavior(self, mocker):
    """Default (clear_on_open=False): open(spider) does NOT clear the set.

    Ensures the new opt-in is additive — zero compat break when the setting
    is not explicitly enabled.
    """
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter",
      "SCRAPY_DEDUP_STRATEGY": "set",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mock_set_backend = mocker.Mock()
    mock_manager.get_set_backend.return_value = mock_set_backend
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)
    spider = mocker.Mock(name="spider")
    spider.name = "test_spider"

    dupefilter.open(spider)
    mock_set_backend.clear_set.assert_not_called()


class TestBackendDupeFilterSpiderKeyTemplating:
  """D2 (C8 HIGH): {spider} placeholder substituted in the dedup key at open()."""

  def test_spider_placeholder_substituted_in_key(self, mocker):
    """Key 'dupefilter:{spider}' + spider.name 'foo' → backend key 'dupefilter:foo'.

    Pre-fix (RED): the key is passed verbatim ('dupefilter:{spider}') to the
    backend — the literal placeholder is sent as the set name. Post-fix
    (GREEN): ``open(spider)`` substitutes ``spider.name`` so the resolved
    backend key contains 'foo'.
    """
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "dupefilter:{spider}",
      "SCRAPY_DEDUP_STRATEGY": "set",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mock_set_backend = mocker.Mock()
    mock_set_backend.add.return_value = True
    mock_manager.get_set_backend.return_value = mock_set_backend
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)
    spider = mocker.Mock(name="spider")
    spider.name = "foo"

    dupefilter.open(spider)

    request = Request(url="https://example.com/page")
    dupefilter.request_seen(request)

    # The resolved backend key passed to SetBackend.add must contain the
    # substituted spider name, not the literal placeholder.
    call_args = mock_set_backend.add.call_args
    resolved_key = call_args[0][0]
    assert "foo" in resolved_key
    assert "{spider}" not in resolved_key

  def test_no_placeholder_keeps_key_verbatim(self, mocker):
    """Keys without '{spider}' are passed through unchanged at open(spider)."""
    from scrapy_extension.backends.connectors import ConnectionManager

    mock_settings = mocker.Mock()
    mock_settings.get.side_effect = lambda key, default=None: {
      "SCRAPY_BACKEND_TYPE": "redis",
      "SCRAPY_DUPEFILTER_KEY": "static:dupefilter",
      "SCRAPY_DEDUP_STRATEGY": "set",
    }.get(key, default)
    mock_settings.getbool.side_effect = lambda key, default=False: {
      "DUPEFILTER_DEBUG": False,
    }.get(key, default)
    mock_settings.getdict.return_value = {}

    mock_manager = mocker.Mock()
    mock_set_backend = mocker.Mock()
    mock_set_backend.add.return_value = True
    mock_manager.get_set_backend.return_value = mock_set_backend
    mocker.patch.object(ConnectionManager, "get_manager", return_value=mock_manager)

    dupefilter = BackendDupeFilter.from_settings(mock_settings)
    spider = mocker.Mock(name="spider")
    spider.name = "irrelevant"

    dupefilter.open(spider)

    request = Request(url="https://example.com/page")
    dupefilter.request_seen(request)

    call_args = mock_set_backend.add.call_args
    assert call_args[0][0] == "static:dupefilter"


class TestBackendDupeFilterCuckooFilterFullDegradation:
  """R7-A (Theme C HIGH): cuckoo filter full → graceful degradation, no crash.

  Pre-fix (RED): ``CuckooMembershipFilter.add`` raises ``RuntimeError`` once
  the filter exhausts ``_MAX_KICKS`` (filter full). The dupefilter layer only
  caught ``NotImplementedError``, so the ``RuntimeError`` propagated through
  ``scheduler.enqueue_request``'s hot path and crashed the spider the first
  time the filter filled past capacity. Post-fix (GREEN): the dupefilter
  catches ``RuntimeError`` in a separate arm, logs a warn-once, bumps
  ``dupefilter/filter_full``, and treats the item as NOT-seen (degrade by
  allowing the enqueue — dedup stays effective within capacity, overflow
  items pass through).
  """

  @pytest.fixture(autouse=True)
  def _reset_filter_full_warned(self):
    """Reset the module-level warn-once flag before each test (isolation).

    ``_filter_full_warned`` is process-global (mirrors factory.py ``_warned``);
    without a reset, the first test to trip it would pre-arm the rest and
    hide a broken warn-once contract.
    """
    from scrapy_extension.dupefilter import dupefilter as dupefilter_module

    original = dupefilter_module._filter_full_warned
    dupefilter_module._filter_full_warned = False
    yield
    dupefilter_module._filter_full_warned = original

  def _make_tiny_cuckoo_dupefilter(self, mock_connection_manager, mocker):
    """Build a dupefilter wrapping a TINY cuckoo filter (capacity=4).

    Tuned so ``_MAX_KICKS`` exhausts after a handful of distinct inserts past
    capacity — reproduces the filter-full ``FilterFull`` signal reliably
    without a huge insert loop. Returns ``(dupefilter, monitor)``; the monitor
    is a Mock so ``monitor.on_filter_full`` is assertable.
    """
    cuckoo = CuckooMembershipFilter(capacity=4, error_rate=0.01)
    monitor = mocker.Mock(name="monitor")
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="cuckoo:full",
      membership_filter=cuckoo,
      monitor=monitor,
    )
    return dupefilter, monitor

  def test_filter_full_does_not_crash(self, mock_connection_manager, mocker):
    """RED/GREEN: distinct inserts past capacity must not raise.

    Pre-fix: the cuckoo ``RuntimeError`` propagates → test fails RED.
    Post-fix: dupefilter swallows it and treats the request as not-seen.
    """
    dupefilter, _monitor = self._make_tiny_cuckoo_dupefilter(
      mock_connection_manager, mocker
    )
    dupefilter.open()

    # Insert well past capacity (capacity=4, bucket=4, ~85% load target →
    # _MAX_KICKS exhausts after a modest number of distinct items).
    for i in range(50):
      request = Request(url=f"https://example.com/page/{i}")
      # Must not raise — degradation treats overflow as not-seen.
      result = dupefilter.request_seen(request)
      assert isinstance(result, bool)

  def test_filter_full_increments_stat(self, mock_connection_manager, mocker):
    """The monitor's ``on_filter_full`` hook fires when degradation triggers.

    The dupefilter emits ``monitor.on_filter_full()`` (the monitor contract),
    not a private-attribute stat bump — ``ScrapyStatsMonitor`` translates it
    to ``dupefilter/filter_full`` (covered in ``test_monitor.py``).
    """
    dupefilter, monitor = self._make_tiny_cuckoo_dupefilter(
      mock_connection_manager, mocker
    )
    dupefilter.open()

    # Force the filter past capacity so the FilterFull signal fires.
    for i in range(50):
      dupefilter.request_seen(Request(url=f"https://example.com/page/{i}"))

    # The monitor hook fired at least once — proving the degradation path ran.
    monitor.on_filter_full.assert_called()

  def test_filter_full_warns_once(self, mock_connection_manager, mocker, caplog):
    """Warn-once contract: filter-full triggered twice logs exactly once.

    Mirrors the factory ``_warned`` pattern — a long-running crawl must not
    have its log spammed by per-request filter-full signals.
    """
    dupefilter, _monitor = self._make_tiny_cuckoo_dupefilter(
      mock_connection_manager, mocker
    )
    dupefilter.open()

    caplog.clear()
    with caplog.at_level(logging.WARNING):
      for i in range(100):
        dupefilter.request_seen(Request(url=f"https://example.com/p/{i}"))

    warning_records = [
      r for r in caplog.records if r.levelno == logging.WARNING
    ]
    filter_full_warnings = [
      r for r in warning_records if "filter_full" in r.getMessage()
    ]
    assert len(filter_full_warnings) == 1, (
      f"expected exactly one filter_full warning, got {len(filter_full_warnings)}"
    )

  def test_no_false_negative_within_capacity(self, mock_connection_manager, mocker):
    """Green-path sanity: within capacity, dedup still works (seen=True on repeat).

    Ensures the degradation does not accidentally fire early — cuckoo's
    never-false-negative-within-capacity contract is preserved.
    """
    cuckoo = CuckooMembershipFilter(capacity=200, error_rate=0.01)
    monitor = mocker.Mock(name="monitor")
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="cuckoo:green",
      membership_filter=cuckoo,
      monitor=monitor,
    )
    dupefilter.open()

    # Within capacity — first sight is not-seen, second is seen.
    request = Request(url="https://example.com/within-cap")
    assert dupefilter.request_seen(request) is False
    assert dupefilter.request_seen(request) is True
    # filter_full hook must NOT have fired in the green path.
    monitor.on_filter_full.assert_not_called()

  def test_filter_full_treats_item_as_not_seen(self, mock_connection_manager, mocker):
    """On filter-full, the overflowing request is treated as NOT-seen.

    Dedup stays effective within capacity; overflow items are allowed
    through (may re-fetch) — strictly better than crashing the crawl.
    """
    dupefilter, monitor = self._make_tiny_cuckoo_dupefilter(
      mock_connection_manager, mocker
    )
    dupefilter.open()

    # Drive the filter decisively past capacity so the FilterFull arm fires.
    for i in range(50):
      req = Request(url=f"https://example.com/seed/{i}")
      dupefilter.request_seen(req)

    # An overflow request must be reported as NOT-seen (allowed through) —
    # AND monitor.on_filter_full must have fired at least once along the way,
    # proving the degradation path actually fired before we got here.
    overflow_req = Request(url="https://example.com/overflow/unique")
    result = dupefilter.request_seen(overflow_req)
    assert result is False
    monitor.on_filter_full.assert_called()


class TestBackendDupeFilterTransientBackendError:
  """Risk 4: a transient BackendConnectionError from the SetBackend degrades.

  Pre-fix (RED): ``request_seen`` caught only ``NotImplementedError`` and
  ``FilterFull`` — a transient ``BackendConnectionError`` (Redis/MongoDB/ES
  outage during dedup) propagated to the Scrapy engine and crashed the crawl,
  contradicting the codebase's "a dead spider is worse than a duplicate fetch"
  philosophy. Post-fix (GREEN): a dedicated arm catches it, warns once per
  process, emits ``monitor.on_error("dedup", exc)``, and degrades to not-seen.
  """

  @pytest.fixture(autouse=True)
  def _reset_backend_error_warned(self):
    """Reset the module-level warn-once flag (Risk 4) before each test."""
    from scrapy_extension.dupefilter import dupefilter as dupefilter_module

    original = dupefilter_module._backend_error_warned
    dupefilter_module._backend_error_warned = False
    yield
    dupefilter_module._backend_error_warned = original

  def _make_dupefilter_with_raising_filter(self, mock_connection_manager, mocker):
    """Build a dupefilter whose membership filter raises BackendConnectionError.

    Returns ``(dupefilter, monitor)``; the monitor is a Mock so
    ``monitor.on_error`` is assertable. The membership filter's ``add`` raises
    a transient ``BackendConnectionError`` on every call (simulating a
    sustained backend outage).
    """
    from scrapy_extension.exceptions.base import BackendConnectionError

    membership_filter = mocker.Mock(name="membership_filter")
    membership_filter.add.side_effect = BackendConnectionError(
      "transient redis outage"
    )
    # saturation is read via getattr in request_seen — set to None so the
    # saturation hook is skipped (keeps the test focused on the error arm).
    membership_filter.saturation = None
    monitor = mocker.Mock(name="monitor")
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="transient",
      membership_filter=membership_filter,
      monitor=monitor,
    )
    return dupefilter, monitor

  def test_open_circuit_degrades_to_not_seen(
    self,
    mock_connection_manager,
    mocker,
  ):
    """A circuit-breaker rejection follows the transient-outage envelope."""
    from scrapy_extension.backends.circuit_breaker import CircuitBreakerOpenError

    membership_filter = mocker.Mock(name="membership_filter")
    membership_filter.add.side_effect = CircuitBreakerOpenError("redis-set")
    membership_filter.saturation = None
    monitor = mocker.Mock(name="monitor")
    dupefilter = BackendDupeFilter(
      connection_manager=mock_connection_manager,
      key="open-circuit",
      membership_filter=membership_filter,
      monitor=monitor,
    )
    dupefilter.open()

    assert dupefilter.request_seen(Request("https://example.com/circuit")) is False
    monitor.on_error.assert_called_once()
    assert monitor.on_error.call_args.args[0] == "dedup"

  def test_transient_error_does_not_crash(self, mock_connection_manager, mocker):
    """RED/GREEN: a transient backend error must not propagate.

    Pre-fix: the BackendConnectionError propagated and crashed the crawl.
    Post-fix: the dupefilter degrades and treats the request as not-seen.
    """
    dupefilter, _monitor = self._make_dupefilter_with_raising_filter(
      mock_connection_manager, mocker
    )
    dupefilter.open()
    result = dupefilter.request_seen(Request(url="https://example.com/x"))
    assert result is False  # degrade to not-seen (allow the request through)

  def test_transient_error_emits_on_error(self, mock_connection_manager, mocker):
    """The monitor's ``on_error("dedup", exc)`` hook fires on degradation.

    A wired ScrapyStatsMonitor translates this to ``errors/dedup`` — the
    operability signal that distinguishes a transient outage from silence.
    """
    dupefilter, monitor = self._make_dupefilter_with_raising_filter(
      mock_connection_manager, mocker
    )
    dupefilter.open()
    dupefilter.request_seen(Request(url="https://example.com/x"))
    monitor.on_error.assert_called_once()
    assert monitor.on_error.call_args[0][0] == "dedup"

  def test_transient_error_warns_once(self, mock_connection_manager, mocker, caplog):
    """Warn-once contract: two transient errors log exactly one WARNING.

    Mirrors the FilterFull warn-once — a long-running crawl must not have its
    log spammed by per-request outage signals.
    """
    dupefilter, _monitor = self._make_dupefilter_with_raising_filter(
      mock_connection_manager, mocker
    )
    dupefilter.open()
    with caplog.at_level(
      logging.WARNING, logger="scrapy_extension.dupefilter.dupefilter"
    ):
      dupefilter.request_seen(Request(url="https://example.com/x"))
      dupefilter.request_seen(Request(url="https://example.com/y"))
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "transiently unavailable" in warnings[0].message
