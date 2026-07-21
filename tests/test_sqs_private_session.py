"""Real-SDK seam tests for SQS private Session ownership."""

from __future__ import annotations

import subprocess
import sys


def test_real_private_session_client_works_with_botocore_stubber() -> None:
  """Exercise the real botocore request model in an unpolluted process."""
  script = "\n".join(
    (
      "from unittest.mock import patch",
      "import boto3",
      "from botocore.stub import Stubber",
      "from scrapy_extension.backends.sqs import SqsBackend",
      "from scrapy_extension.settings import SqsSettings",
      "real_session = boto3.session.Session(",
      "  aws_access_key_id='x', aws_secret_access_key='y',",
      "  region_name='us-east-1',",
      ")",
      "real_client = real_session.client(",
      "  'sqs', endpoint_url='http://localhost:4566',",
      ")",
      "class PoisonDefault:",
      "  def client(self, *_args, **_kwargs):",
      "    raise AssertionError('module-level boto3.client was used')",
      "poison = PoisonDefault()",
      "boto3.DEFAULT_SESSION = poison",
      "queue_url = 'http://localhost:4566/000000000000/scrapy-q'",
      "with Stubber(real_client) as stubber:",
      "  stubber.add_response(",
      "    'get_queue_url', {'QueueUrl': queue_url},",
      "    {'QueueName': 'scrapy-q'},",
      "  )",
      "  stubber.add_response(",
      "    'send_message', {'MessageId': 'message-id'},",
      "    {'QueueUrl': queue_url, 'MessageBody': 'eA=='},",
      "  )",
      "  with patch(",
      "    'scrapy_extension.backends.sqs.boto3.session.Session',",
      "    return_value=real_session,",
      "  ) as session_factory, patch.object(",
      "    real_session, 'client', return_value=real_client,",
      "  ) as client_factory, patch.object(",
      "    real_client, 'close', wraps=real_client.close,",
      "  ) as close:",
      "    backend = SqsBackend(SqsSettings())",
      "    backend.connect()",
      "    backend.push('q', b'x')",
      "    stubber.assert_no_pending_responses()",
      "    assert backend._generation is not None",
      "    assert backend._generation.session is real_session",
      "    assert backend._generation.client is real_client",
      "    assert boto3.DEFAULT_SESSION is poison",
      "    session_factory.assert_called_once_with()",
      "    client_factory.assert_called_once()",
      "    args, kwargs = client_factory.call_args",
      "    assert args == ('sqs',)",
      "    config = kwargs.pop('config')",
      "    assert config.ignore_configured_endpoint_urls is True",
      "    assert kwargs == {",
      "      'region_name': 'us-east-1',",
      "      'endpoint_url': 'http://localhost:4566',",
      "    }",
      "    backend.disconnect()",
      "    close.assert_called_once_with()",
    )
  )

  result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True,
    check=False,
  )

  assert result.returncode == 0, result.stderr or result.stdout


def test_private_sessions_refresh_ambient_credentials_between_generations() -> None:
  """A reconnect must not inherit credentials cached by a default Session."""
  script = "\n".join(
    (
      "import os",
      "import boto3",
      "from scrapy_extension.backends.sqs import SqsBackend",
      "from scrapy_extension.settings import SqsSettings",
      "os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'",
      "os.environ['AWS_ACCESS_KEY_ID'] = 'generation-a-key'",
      "os.environ['AWS_SECRET_ACCESS_KEY'] = 'generation-a-secret'",
      "boto3.DEFAULT_SESSION = None",
      "backend = SqsBackend(SqsSettings())",
      "backend.connect()",
      "assert backend._generation is not None",
      "first = backend._generation.session.get_credentials()",
      "assert first is not None and first.access_key == 'generation-a-key'",
      "backend.disconnect()",
      "os.environ['AWS_ACCESS_KEY_ID'] = 'generation-b-key'",
      "os.environ['AWS_SECRET_ACCESS_KEY'] = 'generation-b-secret'",
      "backend.connect()",
      "assert backend._generation is not None",
      "second = backend._generation.session.get_credentials()",
      "assert second is not None and second.access_key == 'generation-b-key'",
      "assert boto3.DEFAULT_SESSION is None",
      "backend.disconnect()",
    )
  )

  result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True,
    check=False,
  )

  assert result.returncode == 0, result.stderr or result.stdout


def test_cloud_mode_ignores_ambient_configured_endpoint_urls() -> None:
  """Ambient HTTP endpoint overrides must not bypass the cloud TLS guard."""
  script = "\n".join(
    (
      "import os",
      "from urllib.parse import urlsplit",
      "import boto3",
      "from scrapy_extension.backends.sqs import SqsBackend",
      "from scrapy_extension.settings import SqsMode, SqsSettings",
      "os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'",
      "os.environ['AWS_ACCESS_KEY_ID'] = 'cloud-key'",
      "os.environ['AWS_SECRET_ACCESS_KEY'] = 'cloud-secret'",
      "for name in ('AWS_ENDPOINT_URL_SQS', 'AWS_ENDPOINT_URL'):",
      "  os.environ.pop('AWS_ENDPOINT_URL_SQS', None)",
      "  os.environ.pop('AWS_ENDPOINT_URL', None)",
      "  configured = 'http://example.invalid:9876'",
      "  os.environ[name] = configured",
      "  boto3.DEFAULT_SESSION = None",
      "  backend = SqsBackend(SqsSettings(mode=SqsMode.CLOUD))",
      "  assert backend.config.endpoint_url is None",
      "  backend.connect()",
      "  assert backend._generation is not None",
      "  actual = backend._generation.client.meta.endpoint_url",
      "  assert actual != configured, (name, actual)",
      "  assert urlsplit(actual).scheme == 'https', (name, actual)",
      "  assert boto3.DEFAULT_SESSION is None",
      "  backend.disconnect()",
    )
  )

  result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True,
    check=False,
  )

  assert result.returncode == 0, result.stderr or result.stdout
