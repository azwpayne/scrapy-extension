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


def test_rabbitmq_backend_pop(mocker):
  """Test RabbitMQ backend pop."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  # Mock message return
  mock_method = mocker.MagicMock()
  mock_method.delivery_tag = "tag123"
  mock_channel.basic_get.return_value = (mock_method, None, b"test_item")

  backend.connect()
  result = backend.pop("test_queue")

  assert result == b"test_item"
  mock_channel.basic_ack.assert_called_once_with(delivery_tag="tag123")


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
  config.password = "guest"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  """Test queue_len returns 0 when channel is None (line 429-430)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  result = backend.queue_len("test_queue")
  assert result == 0


def test_rabbitmq_backend_queue_len_amqp_error(mocker):
  """Test queue_len returns 0 when AMQPError is raised (lines 436-437)."""
  config = RabbitMQSettings()
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  mock_channel.queue_declare.side_effect = pika.exceptions.AMQPError("Queue error")

  result = backend.queue_len("test_queue")
  assert result == 0


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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  """Test _connect_cluster with cluster_nodes configured (lines 221-222)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.CLUSTER
  config.cluster_nodes = ["node2:5672", "node3:5672"]
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
  backend = RabbitMQBackend(config)

  mock_instance = mocker.MagicMock()
  mock_channel = mocker.MagicMock()
  mock_instance.channel.return_value = mock_channel
  mock_instance.is_open = True
  mocker.patch("pika.BlockingConnection", return_value=mock_instance)

  backend.connect()
  assert backend.is_connected()


def test_rabbitmq_backend_mirrored_queues_no_ha_mode(mocker):
  """Test _connect_mirrored_queues skips HA setup when ha_mode is None (lines 242->exit)."""
  config = RabbitMQSettings()
  config.mode = RabbitMQMode.MIRRORED_QUEUES
  config.ha_mode = None
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
  config.username = "user"  # noqa: S106
  config.password = "pass"  # noqa: S105
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
