"""Final coverage push: lazy __getattr__ import-error paths + default ack/nack."""

from __future__ import annotations

import builtins
import sys

import pytest


def _block_dep(mocker, dep: str) -> None:
  original = builtins.__import__

  def blocking(name, *args, **kwargs):
    if name == dep or name.startswith(dep + "."):
      raise ImportError(f"No module named '{dep}' (mocked)")
    return original(name, *args, **kwargs)

  mocker.patch.object(builtins, "__import__", side_effect=blocking)


def test_top_level_getattr_import_error(mocker) -> None:
  """Accessing scrapy_extension.RedisBackend with redis missing -> __getattr__ ImportError."""
  _block_dep(mocker, "redis")
  sys.modules.pop("scrapy_extension.backends.redis", None)
  import scrapy_extension

  with pytest.raises(ImportError):
    scrapy_extension.RedisBackend  # noqa: B018


def test_backends_getattr_import_error(mocker) -> None:
  """Accessing backends.RedisBackend with redis missing -> backends __getattr__ ImportError."""
  _block_dep(mocker, "redis")
  sys.modules.pop("scrapy_extension.backends.redis", None)
  import scrapy_extension.backends

  with pytest.raises(ImportError):
    scrapy_extension.backends.RedisBackend  # noqa: B018


def test_default_ack_nack_are_noops() -> None:
  """Cover the QueueBackend default ack/nack no-op bodies (del queue_name)."""
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.settings import RedisSettings

  backend = RedisBackend(RedisSettings())
  backend.ack("q")  # default no-op
  backend.nack("q")  # default no-op
