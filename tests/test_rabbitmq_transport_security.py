"""Security contracts for RabbitMQ transport and connection construction."""

from __future__ import annotations

import ssl
import traceback

import pytest
from pydantic import SecretStr

from scrapy_extension.backends.rabbitmq import RabbitMQBackend
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RabbitMQMode, RabbitMQSettings


@pytest.mark.parametrize("host", ["localhost", "localhost.", "127.0.0.1", "::1"])
def test_plaintext_is_limited_to_loopback_hosts(host: str) -> None:
  settings = RabbitMQSettings(host=host)

  assert settings.ssl_enabled is False


def test_remote_plaintext_connection_is_rejected() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      host="rabbit.internal",
      username="crawler",
      password="secret",
    )

  assert exc_info.value.setting_name == "ssl_enabled"


def test_remote_cluster_node_requires_tls_even_with_loopback_primary() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      mode=RabbitMQMode.CLUSTER,
      host="localhost",
      cluster_nodes=["rabbit-2.internal:5672"],
      username="crawler",
      password="secret",
    )

  assert exc_info.value.setting_name == "ssl_enabled"


def test_cluster_nodes_parse_ipv4_and_bracketed_ipv6() -> None:
  settings = RabbitMQSettings(
    mode=RabbitMQMode.CLUSTER,
    cluster_nodes=["127.0.0.2:5673", "[::1]:5674"],
  )

  snapshot = RabbitMQBackend(settings)._capture_connection_snapshot()

  assert snapshot.cluster_nodes == (("127.0.0.2", 5673), ("::1", 5674))


def test_malformed_cluster_node_is_rejected_before_connection() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      mode=RabbitMQMode.CLUSTER,
      cluster_nodes=["localhost:not-a-port"],
    )

  assert exc_info.value.setting_name == "cluster_nodes"


def test_remote_plaintext_url_is_rejected() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      url="amqp://rabbit.internal/vhost",
      username="crawler",
      password="secret",
    )

  assert exc_info.value.setting_name == "ssl_enabled"


def test_url_userinfo_is_rejected_without_retaining_password(monkeypatch) -> None:
  monkeypatch.delenv("SCRAPY_RABBITMQ_USERNAME", raising=False)
  monkeypatch.delenv("SCRAPY_RABBITMQ_PASSWORD", raising=False)

  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(  # type: ignore[call-arg]
      url="amqps://crawler:do-not-leak@rabbit.internal/vhost"  # type: ignore[arg-type]
    )

  assert exc_info.value.setting_name == "url"
  assert "do-not-leak" not in str(exc_info.value)
  assert "do-not-leak" not in repr(exc_info.value.__dict__)
  assert exc_info.value.__cause__ is None


def test_amqps_url_cannot_be_explicitly_downgraded() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      url="amqps://rabbit.internal/vhost",
      username="crawler",
      password="secret",
      ssl_enabled=False,
    )

  assert exc_info.value.setting_name == "ssl_enabled"


def test_amqps_url_cannot_be_downgraded_by_environment_text(monkeypatch) -> None:
  monkeypatch.setenv("SCRAPY_RABBITMQ_URL", "amqps://localhost/vhost")
  monkeypatch.setenv("SCRAPY_RABBITMQ_SSL_ENABLED", "false")

  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings()

  assert exc_info.value.setting_name == "ssl_enabled"


@pytest.mark.parametrize("verify_mode", ["CERT_NONE", "CERT_OPTIONAL"])
def test_tls_requires_certificate_and_hostname_verification(verify_mode: str) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      host="rabbit.internal",
      username="crawler",
      password="secret",
      ssl_enabled=True,
      ssl_verify_mode=verify_mode,
    )

  assert exc_info.value.setting_name == "ssl_verify_mode"


@pytest.mark.parametrize(
  ("certfile", "keyfile", "missing_name"),
  [
    ("/tls/client.pem", None, "ssl_keyfile"),
    (None, "/tls/client.key", "ssl_certfile"),
  ],
)
def test_tls_client_certificate_must_be_a_pair(
  certfile: str | None, keyfile: str | None, missing_name: str
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      ssl_enabled=True,
      ssl_certfile=certfile,
      ssl_keyfile=keyfile,
    )

  assert exc_info.value.setting_name == missing_name


def test_remote_guest_user_is_rejected() -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(
      host="rabbit.internal",
      username="guest",
      password="not-the-default-password",
      ssl_enabled=True,
    )

  assert exc_info.value.setting_name == "username"


@pytest.mark.parametrize(
  ("username", "password", "setting_name"),
  [
    ("   ", "secret", "username"),
    ("crawler", "   ", "password"),
  ],
)
def test_blank_explicit_credentials_are_rejected_without_retention(
  username: str, password: str, setting_name: str
) -> None:
  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQSettings(username=username, password=password)

  assert exc_info.value.setting_name == setting_name
  assert password not in repr(exc_info.value.__dict__)


def test_tls_binds_hostname_to_the_actual_connection_node() -> None:
  settings = RabbitMQSettings(
    host="rabbit.internal",
    username="crawler",
    password="secret",
    ssl_enabled=True,
  )

  parameters = RabbitMQBackend(settings)._build_common_parameters()

  assert parameters.ssl_options is not None
  assert parameters.ssl_options.server_hostname == "rabbit.internal"
  assert parameters.ssl_options.context.verify_mode == ssl.CERT_REQUIRED
  assert parameters.ssl_options.context.check_hostname is True


def test_connect_revalidates_mutated_transport_before_sdk_io(mocker) -> None:
  settings = RabbitMQSettings()
  settings.host = "rabbit.internal"
  blocking_connection = mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection"
  )

  with pytest.raises(ConfigurationError) as exc_info:
    RabbitMQBackend(settings).connect()

  assert exc_info.value.setting_name == "ssl_enabled"
  blocking_connection.assert_not_called()


def test_cluster_connect_uses_one_preconstruction_snapshot(mocker) -> None:
  settings = RabbitMQSettings(
    mode=RabbitMQMode.CLUSTER,
    host="rabbit-1.internal",
    port=5671,
    cluster_nodes=["rabbit-2.internal:5671"],
    username="crawler",
    password="original-secret",
    ssl_enabled=True,
    ssl_cafile="/tls/original-ca.pem",
    prefetch_count=7,
  )
  captured_credentials: list[tuple[str, object]] = []
  captured_parameters: list[dict[str, object]] = []
  captured_tls_hosts: list[str] = []

  def credentials_factory(username: str, password: object):
    captured_credentials.append((username, password))
    return object()

  def ssl_options_factory(_context: object, server_hostname: str):
    captured_tls_hosts.append(server_hostname)
    return object()

  def parameters_factory(**kwargs):
    captured_parameters.append(kwargs)
    if len(captured_parameters) == 1:
      settings.mode = RabbitMQMode.STANDALONE
      settings.host = "attacker.internal"
      settings.cluster_nodes = []
      settings.password = SecretStr("attacker-secret")
      settings.ssl_enabled = False
      settings.ssl_cafile = "/tls/attacker-ca.pem"
      settings.prefetch_count = 0
    return object()

  ssl_context = mocker.MagicMock(spec=ssl.SSLContext)
  create_context = mocker.patch(
    "scrapy_extension.backends.rabbitmq.ssl.create_default_context",
    return_value=ssl_context,
  )
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.PlainCredentials",
    side_effect=credentials_factory,
  )
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.SSLOptions",
    side_effect=ssl_options_factory,
  )
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.ConnectionParameters",
    side_effect=parameters_factory,
  )
  connection = mocker.MagicMock()
  channel = mocker.MagicMock()
  connection.channel.return_value = channel
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    return_value=connection,
  )

  RabbitMQBackend(settings).connect()

  assert [params["host"] for params in captured_parameters] == [
    "rabbit-1.internal",
    "rabbit-2.internal",
  ]
  assert captured_tls_hosts == ["rabbit-1.internal", "rabbit-2.internal"]
  assert create_context.call_count == 2
  assert all(
    call.kwargs["cafile"] == "/tls/original-ca.pem"
    for call in create_context.call_args_list
  )
  assert all(str(password) == "original-secret" for _, password in captured_credentials)
  assert all("original-secret" not in repr(password) for _, password in captured_credentials)
  channel.basic_qos.assert_called_once_with(prefetch_count=7, prefetch_size=0)


def test_startup_error_traceback_does_not_echo_driver_secrets(mocker) -> None:
  settings = RabbitMQSettings(
    host="rabbit.internal",
    username="crawler",
    password="rabbit-secret",
    ssl_enabled=True,
  )
  mocker.patch(
    "scrapy_extension.backends.rabbitmq.pika.BlockingConnection",
    side_effect=RuntimeError("connection dump contained rabbit-secret"),
  )

  with pytest.raises(BackendConnectionError) as exc_info:
    RabbitMQBackend(settings).connect()

  rendered = "".join(traceback.format_exception(exc_info.value))
  assert "rabbit-secret" not in str(exc_info.value)
  assert "rabbit-secret" not in rendered
  assert exc_info.value.__cause__ is None
