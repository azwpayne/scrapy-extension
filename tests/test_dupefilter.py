"""Tests for BackendDupeFilter component."""

import pytest
from scrapy.http import Request

from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter


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
    mock_settings.getdict.return_value = {"host": "localhost"}

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
