"""Collection-order contracts for optional SDK-backed test modules."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SDK_TEST_MODULES = (
  "tests/test_connectors.py",
  "tests/test_backend_coverage2.py",
  "tests/test_scheduler_ack_gate.py",
  "tests/test_pulsar_backend.py",
  "tests/test_pulsar_coverage.py",
  "tests/test_dynamodb_backend.py",
  "tests/test_dynamodb_coverage.py",
  "tests/test_sqs_backend.py",
  "tests/test_sqs_coverage.py",
  "tests/test_memcached_backend.py",
  "tests/test_memcached_coverage.py",
)


@pytest.mark.parametrize(
  "test_modules", [_SDK_TEST_MODULES, tuple(reversed(_SDK_TEST_MODULES))],
  ids=["forward", "reverse"],
)
@pytest.mark.parametrize("preload", [False, True], ids=["cold", "preloaded"])
def test_sdk_modules_are_collection_order_independent(
  test_modules: tuple[str, ...], preload: bool
) -> None:
  """Collection must neither replace nor split installed optional SDKs."""
  lines = ["import importlib", "import runpy"]
  if preload:
    lines.extend(
      (
        "import boto3, pulsar, pymemcache",
        "from pymemcache.client.base import Client as PymemcacheClient",
        "before = (pulsar, pulsar.Client, pulsar.Timeout, boto3, "
        "boto3.session.Session, pymemcache, PymemcacheClient)",
      )
    )
  lines.extend(f"runpy.run_path({module!r})" for module in test_modules)
  lines.extend(
    (
      "import boto3, pulsar, pymemcache",
      "from pymemcache.client.base import Client as PymemcacheClient",
      "pulsar_backend = importlib.import_module('scrapy_extension.backends.pulsar')",
      "sqs_backend = importlib.import_module('scrapy_extension.backends.sqs')",
      "ddb_backend = importlib.import_module('scrapy_extension.backends.dynamodb')",
      "memcached_backend = importlib.import_module('scrapy_extension.backends.memcached')",
      "for sdk in (pulsar, boto3, pymemcache):",
      "  assert isinstance(getattr(sdk, '__file__', None), str)",
      "  assert getattr(sdk, '__spec__', None) is not None",
      "assert pulsar_backend.pulsar is pulsar",
      "assert sqs_backend.boto3 is boto3",
      "assert ddb_backend.boto3 is boto3",
      "assert memcached_backend.MemcachedClient is PymemcacheClient",
    )
  )
  if preload:
    lines.append(
      "assert before == (pulsar, pulsar.Client, pulsar.Timeout, boto3, "
      "boto3.session.Session, pymemcache, PymemcacheClient)"
    )
  script = "\n".join(lines)

  result = subprocess.run(
    [sys.executable, "-c", script],
    cwd=_ROOT,
    capture_output=True,
    text=True,
    timeout=30,
    check=False,
  )

  assert result.returncode == 0, result.stderr or result.stdout
