"""Import-error behavior for the subsystem-③ backends + lazy __getattr__ paths.

These run in a subprocess (mirroring test_rabbitmq_import.py) so the main
process's module state isn't corrupted. They verify the helpful install-hint
ImportError is raised when a backend's dependency is missing.

Note: subprocess execution isn't counted toward the main process's coverage
figures; these are behavior assertions, part of all-around verification.
"""

from __future__ import annotations

import subprocess
import sys


def _assert_import_error(code: str) -> None:
  result = subprocess.run(
    [sys.executable, "-c", code], capture_output=True, text=True
  )
  assert result.returncode == 0, f"subprocess failed: {result.stderr}\n{result.stdout}"
  assert "PASS" in result.stdout, f"no PASS: {result.stdout}\n{result.stderr}"


def test_pulsar_import_error() -> None:
  _assert_import_error(
    "import sys\n"
    "sys.modules['pulsar'] = None\n"
    "try:\n"
    "    import scrapy_extension.backends.pulsar\n"
    "    print('ERROR: no ImportError'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'scrapy-extension[pulsar]' in str(e) else (print(f'ERR: {e}'), sys.exit(1))\n"
  )


def test_memcached_import_error() -> None:
  _assert_import_error(
    "import sys\n"
    "for m in ('pymemcache', 'pymemcache.client', 'pymemcache.client.base'):\n"
    "    sys.modules[m] = None\n"
    "try:\n"
    "    import scrapy_extension.backends.memcached\n"
    "    print('ERROR'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'scrapy-extension[memcached]' in str(e) else (print(f'ERR: {e}'), sys.exit(1))\n"
  )


def test_sqs_import_error() -> None:
  _assert_import_error(
    "import sys\n"
    "sys.modules['boto3'] = None\n"
    "try:\n"
    "    import scrapy_extension.backends.sqs\n"
    "    print('ERROR'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'scrapy-extension[sqs]' in str(e) else (print(f'ERR: {e}'), sys.exit(1))\n"
  )


def test_dynamodb_import_error() -> None:
  _assert_import_error(
    "import sys\n"
    "sys.modules['boto3'] = None\n"
    "try:\n"
    "    import scrapy_extension.backends.dynamodb\n"
    "    print('ERROR'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'scrapy-extension[dynamodb]' in str(e) else (print(f'ERR: {e}'), sys.exit(1))\n"
  )


def test_top_level_getattr_import_error() -> None:
  """Accessing scrapy_extension.RedisBackend with redis missing -> helpful ImportError."""
  _assert_import_error(
    "import sys\n"
    "sys.modules['redis'] = None\n"
    "import scrapy_extension\n"
    "try:\n"
    "    scrapy_extension.RedisBackend\n"
    "    print('ERROR'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'redis' in str(e).lower() else (print(f'ERR: {e}'), sys.exit(1))\n"
  )


def test_backends_getattr_import_error() -> None:
  """Accessing scrapy_extension.backends.RedisBackend with redis missing -> helpful ImportError."""
  _assert_import_error(
    "import sys\n"
    "sys.modules['redis'] = None\n"
    "import scrapy_extension.backends\n"
    "try:\n"
    "    scrapy_extension.backends.RedisBackend\n"
    "    print('ERROR'); sys.exit(1)\n"
    "except ImportError as e:\n"
    "    print('PASS') if 'redis' in str(e).lower() else (print(f'ERR: {e}'), sys.exit(1))\n"
  )
