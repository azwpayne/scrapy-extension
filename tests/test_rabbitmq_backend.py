"""Tests for RabbitMQ backend implementation."""

import ssl

import pika.exceptions
import pytest

from scrapy_extension.backends.rabbitmq import RabbitMQBackend
from scrapy_extension.exceptions import (
  BackendConnectionError,
  ConfigurationError,
  QueueError,
)
from scrapy_extension.settings import RabbitMQMode, RabbitMQSettings


def test_rabbitmq_backend_connect(mocker):
  """Test RabbitMQ backend connection."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()

  assert backend.is_connected()
  mock_instance.channel.assert_called_once()


def test_rabbitmq_backend_warns_when_ssl_disabled(mocker, caplog):
  """R2-B3: default ssl_enabled=False triggers a one-shot cleartext warning.

  Operators running across datacenters / clouds need to know their
  RabbitMQ credentials traverse the network in cleartext. The warning
  fires on first connect (not on every reconnect) so logs stay readable.
  """
  import logging

  config = RabbitMQSettings()  # ssl_enabled defaults to False
  assert config.ssl_enabled is False
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_instance.channel.return_value = mocker.MagicMock()
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  with caplog.at_level(logging.WARNING, logger="scrapy_extension.backends.rabbitmq"):
    backend.connect()

  assert any(
    "without SSL" in rec.message and "cleartext" in rec.message for rec in caplog.records
  ), "expected cleartext-credential warning when ssl_enabled=False"
  assert backend._ssl_warning_emitted is True


def test_rabbitmq_backend_ssl_warning_debounces_across_reconnects(mocker, caplog):
  """R2-B3: warning fires once per backend instance — not on every connect.

  Reconnect cycles (network blip, broker restart) would otherwise flood
  the logs with the same warning. The debounce flag persists for the
  instance lifetime; a fresh backend (new settings, new process) re-warns.
  """
  import logging

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_instance.channel.return_value = mocker.MagicMock()
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  with caplog.at_level(logging.WARNING, logger="scrapy_extension.backends.rabbitmq"):
    backend.connect()
    backend.disconnect()
    backend.connect()  # second connect — should NOT re-warn

  ssl_warnings = [
    rec for rec in caplog.records if "without SSL" in rec.message
  ]
  assert len(ssl_warnings) == 1, "SSL warning must fire exactly once per instance"


def test_rabbitmq_backend_no_warning_when_ssl_enabled(mocker, caplog):
  """R2-B3: ssl_enabled=True produces no cleartext warning."""
  import logging

  config = RabbitMQSettings(ssl_enabled=True)
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_instance.channel.return_value = mocker.MagicMock()
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  with caplog.at_level(logging.WARNING, logger="scrapy_extension.backends.rabbitmq"):
    backend.connect()

  assert not any("without SSL" in rec.message for rec in caplog.records), (
    "ssl_enabled=True must not emit the cleartext warning"
  )


def test_rabbitmq_backend_push(mocker):
  """Test RabbitMQ backend push."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend.push("test_queue", b"test_item", priority=5)

  mock_channel.queue_declare.assert_called_once()
  mock_channel.basic_publish.assert_called_once()
  call_kwargs = mock_channel.basic_publish.call_args[1]
  assert call_kwargs["routing_key"] == "test_queue"
  assert call_kwargs["body"] == b"test_item"


def test_rabbitmq_backend_push_clamps_negative_priority_to_zero(mocker):
  """Negative priorities must map to the lowest valid RabbitMQ priority."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend.push("test_queue", b"test_item", priority=-2)

  properties = mock_channel.basic_publish.call_args[1]["properties"]
  assert properties.priority == 0


def test_rabbitmq_backend_pop_does_not_auto_ack_after_round_12(mocker):
  """Round 12: pop no longer auto-acks — ack is driven by Scrapy signals.

  The delivery tag is tracked but basic_ack isn't called until the
  scheduler's response_received signal fires.
  """
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  mock_method = mocker.MagicMock()
  mock_method.delivery_tag = "tag123"
  mock_channel.basic_get.return_value = (mock_method, None, b"test_item")

  backend.connect()
  result = backend.pop("test_queue")

  assert result == b"test_item"
  assert backend._last_delivery_tag == "tag123"
  # No auto-ack — the scheduler signal handler calls ack() after download.
  mock_channel.basic_ack.assert_not_called()


def test_rabbitmq_backend_ack_calls_basic_ack(mocker):
  """R1-P1-14 Phase 1: ack() invokes basic_ack with the tracked delivery tag."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._last_delivery_tag = 42

  backend.ack("test_queue")

  mock_channel.basic_ack.assert_called_once_with(delivery_tag=42)
  assert backend._last_delivery_tag is None


def test_rabbitmq_backend_nack_calls_basic_nack_with_requeue(mocker):
  """R1-P1-14 Phase 1: nack() requeues the message for retry."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._last_delivery_tag = 99

  backend.nack("test_queue")

  mock_channel.basic_nack.assert_called_once_with(
    delivery_tag=99,
    requeue=True,
  )
  assert backend._last_delivery_tag is None


def test_rabbitmq_backend_ack_idempotent_when_no_pending(mocker):
  """ack() with no tracked delivery tag is a no-op."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend.ack("test_queue")
  backend.ack("test_queue")


def test_rabbitmq_backend_ack_raises_queue_error_on_amqp_error(mocker):
  """R11: ack() wraps a basic_ack AMQPError as QueueError (lines 483-485).

  Pins the error-wrapping contract — callers catch QueueError, never the
  raw AMQPError.
  """
  from pika.exceptions import AMQPError

  from scrapy_extension.exceptions import QueueError

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._last_delivery_tag = 42
  mock_channel.basic_ack.side_effect = AMQPError("ack failed")

  with pytest.raises(QueueError, match="Failed to ack RabbitMQ message"):
    backend.ack("test_queue")
  # finally still clears the tracked tag even on failure
  assert backend._last_delivery_tag is None


def test_rabbitmq_backend_nack_raises_queue_error_on_amqp_error(mocker):
  """R11: nack() wraps a basic_nack AMQPError as QueueError (lines 498-500)."""
  from pika.exceptions import AMQPError

  from scrapy_extension.exceptions import QueueError

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._last_delivery_tag = 99
  mock_channel.basic_nack.side_effect = AMQPError("nack failed")

  with pytest.raises(QueueError, match="Failed to nack RabbitMQ message"):
    backend.nack("test_queue")
  assert backend._last_delivery_tag is None


def test_rabbitmq_backend_pop_does_not_warn_on_concurrent_pops(mocker, caplog):
  """Tier-2 Unit H: the single-slot defect warning is GONE.

  Previously pop() warned about CONCURRENT_REQUESTS>1 because the single
  _last_delivery_tag slot would be overwritten. With the in-flight-set fix
  (pop_with_ack tracks every delivery tag), concurrent pops no longer warn
  — they're correct. This pins the warning's absence to catch regressions.
  """
  import logging

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  method_frame = mocker.MagicMock(delivery_tag=1)
  mock_channel.basic_get.return_value = (method_frame, None, b"body")

  caplog.clear()
  with caplog.at_level(logging.WARNING):
    backend.pop("test_queue")  # sets _last_delivery_tag
    backend.pop("test_queue")  # concurrent pop — no longer a defect

  assert "pop() called while previous message is unacked" not in caplog.text
  assert "CONCURRENT_REQUESTS>1" not in caplog.text

  assert mock_channel.basic_ack.call_count == 0


def test_rabbitmq_backend_pop_empty(mocker):
  """Test RabbitMQ backend pop with empty queue."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  # Mock empty queue (None returned)
  mock_channel.basic_get.return_value = (None, None, None)

  backend.connect()
  result = backend.pop("test_queue")

  assert result is None


def test_rabbitmq_backend_queue_len(mocker):
  """Test RabbitMQ backend queue length."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  # Mock queue_declare return with message_count
  mock_method = mocker.MagicMock()
  mock_method.method.message_count = 5
  mock_channel.queue_declare.return_value = mock_method

  backend.connect()
  length = backend.queue_len("test_queue")

  assert length == 5


def test_rabbitmq_backend_clear_queue(mocker):
  """Test RabbitMQ backend clear queue."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend.clear_queue("test_queue")

  mock_channel.queue_purge.assert_called_once_with(queue="test_queue")


def test_rabbitmq_backend_disconnect(mocker):
  """Test RabbitMQ backend disconnect."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend.is_connected()

  backend.disconnect()
  assert not backend.is_connected()
  mock_instance.close.assert_called_once()


def test_rabbitmq_backend_ping(mocker):
  """Test RabbitMQ backend ping returns True when connection is open."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend.ping() is True


def test_rabbitmq_backend_only_implements_queuebackend():
  """Test that RabbitMQBackend only implements QueueBackend protocol."""
  from scrapy_extension.backends.base import Backend, QueueBackend

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  # Should implement Backend and QueueBackend
  assert isinstance(backend, Backend)
  assert isinstance(backend, QueueBackend)


def test_rabbitmq_backend_connection_error(mocker):
  """Test RabbitMQ backend connection error handling."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_conn = mocker.patch("pika.BlockingConnection")
  mock_conn.side_effect = pika.exceptions.AMQPError("Connection failed")

  with pytest.raises(BackendConnectionError):
    backend.connect()


def test_rabbitmq_backend_push_error(mocker):
  """Test RabbitMQ backend push error handling."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  mock_channel.basic_publish.side_effect = pika.exceptions.AMQPError("Publish failed")

  backend.connect()
  with pytest.raises(QueueError):
    backend.push("test_queue", b"test_item")


def test_rabbitmq_backend_pop_error(mocker):
  """Test RabbitMQ backend pop error handling."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  mock_channel.basic_get.side_effect = pika.exceptions.AMQPError("Get failed")

  backend.connect()
  with pytest.raises(QueueError):
    backend.pop("test_queue")


# ---------------------------------------------------------------------------
# Coverage gap tests
# ---------------------------------------------------------------------------


def test_rabbitmq_backend_connect_non_amqp_error(mocker):
  """Test connect handles non-AMQPError exceptions (lines 97-99)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mocker.patch("pika.BlockingConnection").side_effect = OSError("Network unreachable")

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert "Network unreachable" in str(exc_info.value)


def test_rabbitmq_backend_get_ssl_verify_mode_cert_none():
  """Test _get_ssl_verify_mode returns CERT_NONE for CERT_NONE mode (line 111)."""
  config = RabbitMQSettings()
  config.ssl_verify_mode = "CERT_NONE"
  backend = RabbitMQBackend(config)

  result = backend._get_ssl_verify_mode()
  assert result == ssl.CERT_NONE


def test_rabbitmq_backend_build_common_parameters_cert_none_disables_hostname_check(
  mocker,
):
  """CERT_NONE must disable hostname checks before verify_mode is lowered."""
  config = RabbitMQSettings(
    ssl_enabled=True,
    ssl_verify_mode="CERT_NONE",
  )
  backend = RabbitMQBackend(config)

  ssl_context = ssl.create_default_context()
  create_default_context = mocker.patch(
    "ssl.create_default_context",
    return_value=ssl_context,
  )

  parameters = backend._build_common_parameters()

  create_default_context.assert_called_once_with(cafile=config.ssl_cafile)
  assert parameters.ssl_options is not None
  assert ssl_context.verify_mode == ssl.CERT_NONE
  assert ssl_context.check_hostname is False


def test_rabbitmq_backend_get_ssl_verify_mode_cert_optional():
  """Test _get_ssl_verify_mode returns CERT_OPTIONAL (line 113)."""
  config = RabbitMQSettings()
  config.ssl_verify_mode = "CERT_OPTIONAL"
  backend = RabbitMQBackend(config)

  result = backend._get_ssl_verify_mode()
  assert result == ssl.CERT_OPTIONAL


def test_rabbitmq_backend_get_ssl_verify_mode_default():
  """Test _get_ssl_verify_mode returns CERT_REQUIRED as default (line 114)."""
  config = RabbitMQSettings()
  config.ssl_verify_mode = "UNKNOWN_MODE"
  backend = RabbitMQBackend(config)

  result = backend._get_ssl_verify_mode()
  assert result == ssl.CERT_REQUIRED


def test_rabbitmq_backend_guest_credentials_non_standalone(mocker):
  """Test BackendConnectionError (wrapping ConfigurationError) when guest credentials in non-standalone mode (lines 133-141)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.CLUSTER
  config.username = "guest"
  config.password = "guest"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  # The original ConfigurationError is chained as __cause__
  assert isinstance(exc_info.value.__cause__, ConfigurationError)
  assert "guest/guest" in str(exc_info.value.__cause__)


def test_rabbitmq_backend_mirrored_queues_ha_mode(mocker):
  """Test _connect_mirrored_queues proceeds when ha_mode is set (lines 215-238)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = "all"
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()

  assert backend._channel is not None


def test_rabbitmq_backend_mirrored_queues_ha_sync_mode(mocker):
  """Test _connect_mirrored_queues includes ha_sync_mode in definition (line 228-229)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = "nodes"
  config.ha_sync_mode = "manual"
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend._channel is not None


def test_rabbitmq_backend_mirrored_queues_amqp_error(mocker):
  """Test _connect_mirrored_queues logs warning on AMQPError during HA setup (lines 237-238)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = "all"
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  # Simulate AMQPError when exchange_declare (HA policy step)
  mock_channel.exchange_declare.side_effect = pika.exceptions.AMQPError("Policy error")

  # Should not raise, just log warning
  backend.connect()
  assert backend._channel is not None


def test_rabbitmq_backend_setup_qos(mocker):
  """Test _setup_qos calls basic_qos with prefetch settings (lines 244-248)."""
  config = RabbitMQSettings()
  config.prefetch_count = 10
  config.prefetch_size = 5
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()

  mock_channel.basic_qos.assert_called_once_with(
    prefetch_count=10,
    prefetch_size=5,
  )


def test_rabbitmq_backend_disconnect_channel_amqp_error(mocker):
  """Test disconnect suppresses AMQPError when closing channel (lines 257-259)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.close.side_effect = pika.exceptions.AMQPError("Close error")

  # Should not raise
  backend.disconnect()


def test_rabbitmq_backend_disconnect_connection_amqp_error(mocker):
  """Test disconnect suppresses AMQPError when closing connection (lines 261-264)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_instance.close.side_effect = pika.exceptions.AMQPError("Connection close error")

  # Should not raise
  backend.disconnect()


def test_rabbitmq_backend_ensure_queue_exists_no_channel():
  """Test _ensure_queue_exists raises QueueError when channel is None (lines 311-317)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  # _channel is None without connecting
  with pytest.raises(QueueError) as exc_info:
    backend._ensure_queue_exists("test_queue")
  assert "Not connected" in str(exc_info.value)


def test_rabbitmq_backend_ensure_queue_exists_amqp_error(mocker):
  """Test _ensure_queue_exists raises QueueError on AMQPError (lines 325-327)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.queue_declare.side_effect = pika.exceptions.AMQPError("Declare failed")

  with pytest.raises(QueueError) as exc_info:
    backend._ensure_queue_exists("test_queue")
  assert "Declare failed" in str(exc_info.value)


def test_rabbitmq_backend_ensure_queue_exists_skips_redeclare(mocker):
  """After the first successful declare, subsequent calls must skip queue_declare.

  Regression for R1-P0-7: re-declaring an existing queue with different
  args raises PRECONDITION_FAILED and kills the channel. The backend tracks
  declared queues in-session to avoid this.
  """
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._ensure_queue_exists("test_queue")
  backend._ensure_queue_exists("test_queue")
  backend._ensure_queue_exists("test_queue")

  assert mock_channel.queue_declare.call_count == 1


def test_rabbitmq_backend_ensure_queue_exists_precondition_failed(mocker):
  """PRECONDITION_FAILED error message must include recovery guidance."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.queue_declare.side_effect = pika.exceptions.AMQPError(
    "PRECONDITION_FAILED - inequivalent arg 'x-max-priority'"
  )

  with pytest.raises(QueueError) as exc_info:
    backend._ensure_queue_exists("test_queue")
  assert "incompatible arguments" in str(exc_info.value)
  assert "Drop the queue" in str(exc_info.value)


def test_rabbitmq_backend_disconnect_clears_declared_queues(mocker):
  """disconnect() must clear the declared-queue cache so reconnect re-declares."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  backend._ensure_queue_exists("test_queue")
  assert "test_queue" in backend._declared_queues

  backend.disconnect()
  assert len(backend._declared_queues) == 0


def test_rabbitmq_backend_push_no_channel():
  """Test push raises QueueError when channel is None (lines 345-351)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with pytest.raises(QueueError) as exc_info:
    backend.push("test_queue", b"item")
  assert "Not connected" in str(exc_info.value)


def test_rabbitmq_backend_pop_no_channel():
  """Test pop raises QueueError when channel is None (lines 392-398)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with pytest.raises(QueueError) as exc_info:
    backend.pop("test_queue")
  assert "Not connected" in str(exc_info.value)


def test_rabbitmq_backend_queue_len_no_channel():
  """Test queue_len raises QueueError when channel is None."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  with pytest.raises(QueueError) as exc_info:
    backend.queue_len("test_queue")
  assert "Not connected" in str(exc_info.value)


def test_rabbitmq_backend_queue_len_amqp_error(mocker):
  """Test queue_len raises QueueError when AMQPError is raised."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.queue_declare.side_effect = pika.exceptions.AMQPError("Queue error")

  with pytest.raises(QueueError) as exc_info:
    backend.queue_len("test_queue")
  assert "Failed to get queue length" in str(exc_info.value)


def test_rabbitmq_backend_clear_queue_no_channel():
  """Test clear_queue returns early when channel is None (lines 447-448)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  # Should not raise
  backend.clear_queue("test_queue")


def test_rabbitmq_backend_clear_queue_amqp_error(mocker):
  """Test clear_queue logs warning on AMQPError (lines 451-452)."""
  from scrapy_extension.backends import rabbitmq

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.queue_purge.side_effect = pika.exceptions.AMQPError("Purge failed")

  mock_log = mocker.patch.object(rabbitmq.logger, "warning")
  backend.clear_queue("test_queue")
  mock_log.assert_called_once()
  assert "Purge failed" in str(mock_log.call_args[0][2])


# ---------------------------------------------------------------------------
# Additional coverage gap tests
# ---------------------------------------------------------------------------


def test_rabbitmq_backend_import_error():
  """Test ImportError includes helpful install message (lines 23-24)."""
  import subprocess
  import sys

  # Use subprocess to avoid corrupting the current process's module state
  result = subprocess.run(
    [
      sys.executable,
      "-c",
      (
        "import sys\n"
        "# Block pika from being imported\n"
        "sys.modules['pika'] = None\n"
        "sys.modules['pika.exceptions'] = None\n"
        "try:\n"
        "    import scrapy_extension.backends.rabbitmq\n"
        "    print('ERROR: No ImportError raised')\n"
        "    sys.exit(1)\n"
        "except ImportError as e:\n"
        "    msg = str(e)\n"
        '    if "pip install scrapy-extension[rabbitmq]" in msg:\n'
        "        print('PASS')\n"
        "    else:\n"
        "        print(f'ERROR: Wrong message: {msg}')\n"
        "        sys.exit(1)\n"
      ),
    ],
    capture_output=True,
    text=True,
  )
  assert result.returncode == 0, f"subprocess failed: {result.stderr}\n{result.stdout}"
  assert "PASS" in result.stdout


def test_rabbitmq_backend_validate_key_name_empty():
  """Test _validate_key_name raises ValueError for empty name (line 56)."""
  from scrapy_extension.backends.rabbitmq import _validate_key_name

  with pytest.raises(ValueError, match="Invalid name"):
    _validate_key_name("")


def test_rabbitmq_backend_connect_unsupported_mode_repr(mocker):
  """Test connect handles unsupported mode where str() fails (lines 99-104)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  # Create a mode object that raises TypeError on str() and has no .value
  class BadMode:
    def __str__(self):
      raise TypeError("cannot convert")

  backend.config.mode = BadMode()

  with pytest.raises(ConfigurationError) as exc_info:
    backend.connect()
  assert "Unsupported RabbitMQ mode" in str(exc_info.value)


def test_rabbitmq_backend_ssl_disabled(mocker):
  """Test SSL is disabled when ssl_enabled=False (default)."""
  config = RabbitMQSettings()
  config.ssl_enabled = False
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend.is_connected()
  # SSL should not be configured
  assert backend._connection is not None


def test_rabbitmq_backend_get_ssl_verify_mode(mocker):
  """Test _get_ssl_verify_mode returns correct ssl.VerifyMode values."""
  from scrapy_extension.backends.rabbitmq import RabbitMQBackend
  from scrapy_extension.settings import RabbitMQSettings

  config = RabbitMQSettings()
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  # Test CERT_NONE
  config.ssl_verify_mode = "CERT_NONE"
  assert backend._get_ssl_verify_mode() == ssl.CERT_NONE

  # Test CERT_OPTIONAL
  config.ssl_verify_mode = "CERT_OPTIONAL"
  assert backend._get_ssl_verify_mode() == ssl.CERT_OPTIONAL

  # Test default (CERT_REQUIRED) for unknown values
  config.ssl_verify_mode = "UNKNOWN"
  assert backend._get_ssl_verify_mode() == ssl.CERT_REQUIRED


def test_rabbitmq_backend_connect_cluster_with_nodes(mocker):
  """Test _connect_cluster passes failover parameters with primary first."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.CLUSTER
  config.host = "node1"
  config.port = 5672
  config.cluster_nodes = ["node2:5672", "node3:5673"]
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mock_parameters = [mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock()]
  mock_connection_parameters = mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.ConnectionParameters",
    side_effect=mock_parameters,
  )
  mock_blocking_connection = mocker.patch(
    "pika.BlockingConnection",
    return_value=mock_instance,
  )

  backend.connect()

  assert mock_connection_parameters.call_args_list[0].kwargs["host"] == "node1"
  assert mock_connection_parameters.call_args_list[0].kwargs["port"] == 5672
  assert mock_connection_parameters.call_args_list[1].kwargs["host"] == "node2"
  assert mock_connection_parameters.call_args_list[1].kwargs["port"] == 5672
  assert mock_connection_parameters.call_args_list[2].kwargs["host"] == "node3"
  assert mock_connection_parameters.call_args_list[2].kwargs["port"] == 5673
  mock_blocking_connection.assert_called_once_with(mock_parameters)
  assert backend.is_connected()


def test_rabbitmq_backend_mirrored_queues_no_ha_mode(mocker):
  """Test _connect_mirrored_queues skips HA setup when ha_mode is None (lines 242->exit)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = None
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend._channel is not None


def test_rabbitmq_backend_mirrored_queues_ha_params_non_digit(mocker):
  """Test _connect_mirrored_queues with non-digit ha_params (line 250)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = "nodes"
  config.ha_params = "node1,node2"
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend._channel is not None


def test_rabbitmq_backend_mirrored_queues_ha_params_digit(mocker):
  """Test _connect_mirrored_queues with digit ha_params (line 250-251)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = "exactly"
  config.ha_params = "2"
  config.ha_sync_mode = "automatic"
  config.username = "user"
  config.password = "pass"
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend._channel is not None


def test_rabbitmq_backend_setup_qos_no_prefetch(mocker):
  """Test _setup_qos skips basic_qos when prefetch settings are zero (line 316)."""
  config = RabbitMQSettings()
  config.prefetch_count = 0
  config.prefetch_size = 0
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  # basic_qos should NOT be called when both prefetch values are 0
  mock_channel.basic_qos.assert_not_called()


def test_rabbitmq_backend_disconnect_only_channel(mocker):
  """Test disconnect closes channel but not connection when connection is None (lines 283->287)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_channel = mocker.MagicMock()
  backend._channel = mock_channel
  backend._connection = None

  backend.disconnect()
  mock_channel.close.assert_called_once()
  assert backend._channel is None


def test_rabbitmq_backend_disconnect_only_connection(mocker):
  """Test disconnect closes connection when channel is None (lines 287->exit)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  backend._channel = None
  backend._connection = mock_instance

  backend.disconnect()
  mock_instance.close.assert_called_once()
  assert backend._connection is None


class TestRabbitMQBackendPopWithAckConcurrency:
  """Tier-2 Unit H: pop_with_ack + ack(token) correctness under CONCURRENT_REQUESTS>1.

  Proves N concurrent pops return N distinct delivery tags and ack(token=tag)
  basic_acks the RIGHT tag regardless of pop/ack order.
  """

  @staticmethod
  def _make_backend(mocker, delivery_tags):
    """Build a connected RabbitMQBackend whose basic_get yields the given tags in order."""
    config = RabbitMQSettings()
    backend = RabbitMQBackend(config)
    mock_instance = mocker.MagicMock()
    mock_channel = mocker.MagicMock()
    mock_instance.channel.return_value = mock_channel
    mocker.patch("pika.BlockingConnection", return_value=mock_instance)
    backend.connect()

    method_frames = [mocker.MagicMock(delivery_tag=tag) for tag in delivery_tags]
    bodies = [bytes([tag]) for tag in delivery_tags]
    mock_channel.basic_get.side_effect = [
      (method_frames[i], None, bodies[i]) for i in range(len(delivery_tags))
    ] + [(None, None, None)] * 3  # subsequent gets return empty
    return backend, mock_channel

  def test_concurrent_pops_return_distinct_delivery_tags(self, mocker):
    """(a) N concurrent pops return N distinct delivery tags."""
    backend, _channel = self._make_backend(mocker, [10, 20, 30])

    results = [backend.pop_with_ack("q") for _ in range(3)]
    tags = [token for _body, token in results]

    assert all(t is not None for t in tags)
    assert len(set(tags)) == 3
    assert sorted(tags) == [10, 20, 30]

  def test_ack_token_acks_right_tag_regardless_of_order(self, mocker):
    """(b) ack(token=tag_i) calls basic_ack(delivery_tag=tag_i) for the RIGHT tag, any order."""
    backend, mock_channel = self._make_backend(mocker, [10, 20, 30])

    _b0, t0 = backend.pop_with_ack("q")  # tag 10
    _b1, t1 = backend.pop_with_ack("q")  # tag 20
    _b2, t2 = backend.pop_with_ack("q")  # tag 30

    # Ack in reverse order — each must ack its own tag.
    backend.ack("q", token=t2)
    backend.ack("q", token=t1)
    backend.ack("q", token=t0)

    acked_tags = {
      call.kwargs.get("delivery_tag") for call in mock_channel.basic_ack.call_args_list
    }
    assert acked_tags == {10, 20, 30}
    # multiple=False — never bulk-ack (would skip an unacked peer).
    for call in mock_channel.basic_ack.call_args_list:
      assert call.kwargs.get("multiple") is False

  def test_in_flight_set_empties_as_each_acked(self, mocker):
    """(c) The in-flight delivery-tag set drains as each message is acked."""
    backend, _channel = self._make_backend(mocker, [10, 20, 30])

    _b0, t0 = backend.pop_with_ack("q")
    _b1, t1 = backend.pop_with_ack("q")
    _b2, t2 = backend.pop_with_ack("q")

    assert backend._in_flight_tags == {10, 20, 30}
    backend.ack("q", token=t1)
    assert backend._in_flight_tags == {10, 30}
    backend.ack("q", token=t0)
    assert backend._in_flight_tags == {30}
    backend.ack("q", token=t2)
    assert backend._in_flight_tags == set()

  def test_nack_token_calls_basic_nack_with_requeue(self, mocker):
    """(d) nack(token=tag) calls basic_nack(delivery_tag=tag, requeue=True)."""
    backend, mock_channel = self._make_backend(mocker, [42])

    _body, token = backend.pop_with_ack("q")

    backend.nack("q", token=token)

    mock_channel.basic_nack.assert_called_once_with(delivery_tag=42, requeue=True)
    assert 42 not in backend._in_flight_tags

  def test_ack_token_none_legacy_fallback(self, mocker):
    """(e) ack(token=None) legacy fallback basic_acks the last-popped tag."""
    config = RabbitMQSettings()
    backend = RabbitMQBackend(config)
    mock_instance = mocker.MagicMock()
    mock_channel = mocker.MagicMock()
    mock_instance.channel.return_value = mock_channel
    mocker.patch("pika.BlockingConnection", return_value=mock_instance)
    backend.connect()
    backend._last_delivery_tag = 99  # legacy single-slot set by old pop()

    backend.ack("q", token=None)

    mock_channel.basic_ack.assert_called_once_with(delivery_tag=99)
    assert backend._last_delivery_tag is None


# ---------------------------------------------------------------------------
# SEC-1 (round-6): RabbitMQ password redaction in PlainCredentials.
# ---------------------------------------------------------------------------


def test_rabbitmq_password_redacted_in_credentials_repr(mocker):
  """SEC-1: the password handed to pika.PlainCredentials is wrapped in
  _RedactedStr so ``repr(credentials)`` / Sentry captures of locals don't
  leak it. The str VALUE is preserved so pika still authenticates.
  """
  from scrapy_extension.backends._redaction import _RedactedStr
  from scrapy_extension.backends.rabbitmq import RabbitMQBackend
  from scrapy_extension.settings.rabbitmq import RabbitMQSettings

  config = RabbitMQSettings(
    username="alice",
    password="top-secret-rmq-pwd",
  )
  backend = RabbitMQBackend(config)

  captured: dict[str, object] = {}

  class _FakePlainCredentials:
    def __init__(self, username: str, password: object) -> None:
      captured["username"] = username
      captured["password"] = password

  mocker.patch("scrapy_extension.backends.rabbitmq.pika.PlainCredentials", _FakePlainCredentials)
  # ConnectionParameters validates the credentials type; patch it to a stub
  # so we can capture the password before pika's type-check rejects the fake.
  mocker.patch("scrapy_extension.backends.rabbitmq.pika.ConnectionParameters", dict)
  # Avoid touching real SSL / connection paths.
  mocker.patch.object(RabbitMQBackend, "_connect_standalone", lambda self: None)

  backend._build_common_parameters()

  password = captured["password"]
  # Value still usable as a normal string for pika auth.
  assert str(password) == "top-secret-rmq-pwd"
  # But repr of the credential object hides it.
  assert "top-secret-rmq-pwd" not in repr(password)
  assert isinstance(password, _RedactedStr)


# ===========================================================================
# R14-E — Lifecycle bounds: RabbitMQ QoS null-on-failure
# ===========================================================================


def test_rabbitmq_qos_failure_nulls_connection(mocker):
  """R14-E HIGH: a QoS failure during connect must null ``_connection``/``_channel``.

  Before R14-E, ``_setup_qos`` ran AFTER ``self._connection``/``self._channel``
  were assigned. An ``AMQPError`` from ``basic_qos`` left the half-init state
  in place, so ``is_connected()`` returned True on a broken channel. The fix
  runs QoS on a local channel before committing to instance state and nulls
  both attrs on failure (mirrors R25-A1's connect-path cleanup).
  """
  # prefetch_count > 0 so _apply_qos actually invokes basic_qos.
  config = RabbitMQSettings(prefetch_count=10)
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_instance.is_open = True
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  # basic_qos raises AMQPError — the half-init bug.
  mock_channel.basic_qos.side_effect = pika.exceptions.AMQPError("QoS failed")

  # connect() wraps the AMQPError as BackendConnectionError.
  with pytest.raises(BackendConnectionError):
    backend.connect()

  # The load-bearing assertion: is_connected() must be False, not True.
  assert backend.is_connected() is False, (
    "is_connected() returned True on a half-init channel — the QoS "
    "failure did not null _connection/_channel"
  )
  assert backend._connection is None
  assert backend._channel is None


def test_rabbitmq_in_flight_set_bounded(mocker, caplog):
  """R14-E MED: the diagnostic ``_in_flight_tags`` set is capped.

  A long-running process with slow acks would grow the set unbounded; we
  cap at ``_MAX_IN_FLIGHT`` and warn-once on overflow. The pop itself is
  never dropped — the message is still returned to the caller.
  """
  from scrapy_extension.backends.rabbitmq import _MAX_IN_FLIGHT

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)
  # Simulate an already-saturated set.
  backend._in_flight_tags = set(range(_MAX_IN_FLIGHT))
  assert not backend._in_flight_overflow_warned

  # Pop a fresh message — the new tag must NOT grow the set past the cap.
  mock_channel = mocker.MagicMock()
  backend._channel = mock_channel
  mock_method = mocker.MagicMock(delivery_tag=999_999)
  mock_channel.basic_get.return_value = (mock_method, mocker.MagicMock(), b"body")

  import logging

  with caplog.at_level(logging.WARNING):
    body, _tag = backend.pop_with_ack("q")

  # The pop succeeded — message returned, not dropped.
  assert body == b"body"
  # The set stayed at the cap (the new tag was not added).
  assert len(backend._in_flight_tags) == _MAX_IN_FLIGHT
  # The one-shot warning fired.
  assert backend._in_flight_overflow_warned is True
  assert any("at cap" in r.message for r in caplog.records)


def test_disconnect_clears_delivery_tag_state_and_in_flight_set():
  """R-mq-reconnect: ``disconnect()`` must clear ``_last_delivery_tag`` AND the
  ``_in_flight_tags`` set so a reconnect cannot (a) reuse a stale delivery tag
  on the new channel via the legacy ack path (spurious QueueError /
  PRECONDITION_FAILED on basic_ack) or (b) leak the in-flight set across
  reconnect cycles (unbounded ``set[int]`` growth for a long-running crawler
  that reconnects repeatedly). At-least-once is preserved either way — the
  broker requeues unacked messages on consumer disconnect — so this is
  correctness/hygiene, not a data-loss fix.
  """
  from unittest.mock import MagicMock

  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)
  # Simulate a connected backend with state accumulated by pops.
  backend._channel = MagicMock()
  backend._connection = MagicMock()
  backend._last_delivery_tag = 42
  backend._in_flight_tags = {10, 20, 30}

  backend.disconnect()

  # State from the closed channel must not survive to the next channel.
  assert backend._last_delivery_tag is None
  assert backend._in_flight_tags == set()
  assert backend._channel is None
  assert backend._connection is None
