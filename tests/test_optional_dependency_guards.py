from __future__ import annotations

import builtins
import importlib
import sys
from collections.abc import Callable, Mapping, Sequence

import pytest

_MODULE_GUARDS = [
  ("scrapy_extension.backends.redis", "redis", "redis"),
  ("scrapy_extension.backends.mongodb", "pymongo", "mongodb"),
  ("scrapy_extension.backends.kafka", "kafka", "kafka"),
  ("scrapy_extension.backends.rabbitmq", "pika", "rabbitmq"),
  ("scrapy_extension.backends.elasticsearch", "elasticsearch", "elasticsearch"),
  ("scrapy_extension.backends.pulsar", "pulsar", "pulsar"),
  ("scrapy_extension.backends.memcached", "pymemcache", "memcached"),
  ("scrapy_extension.backends.sqs", "boto3", "sqs"),
  ("scrapy_extension.backends.dynamodb", "boto3", "dynamodb"),
]


def _with_blocked_import(
  monkeypatch: pytest.MonkeyPatch,
  dependency: str,
  failure_factory: Callable[[], ImportError],
) -> None:
  real_import = builtins.__import__

  def guarded_import(
    name: str,
    globals_: Mapping[str, object] | None = None,
    locals_: Mapping[str, object] | None = None,
    fromlist: Sequence[str] | None = (),
    level: int = 0,
  ) -> object:
    if name == dependency or name.startswith(f"{dependency}."):
      raise failure_factory()
    return real_import(name, globals_, locals_, fromlist, level)

  monkeypatch.setattr(builtins, "__import__", guarded_import)


@pytest.mark.parametrize("module_path,dependency,extra", _MODULE_GUARDS)
def test_module_guard_hints_only_when_dependency_is_missing(
  monkeypatch: pytest.MonkeyPatch,
  module_path: str,
  dependency: str,
  extra: str,
) -> None:
  missing = ModuleNotFoundError(
    f"No module named {dependency!r}",
    name=dependency,
  )
  _with_blocked_import(monkeypatch, dependency, lambda: missing)
  cached = sys.modules.pop(module_path, None)
  try:
    with pytest.raises(ImportError) as exc_info:
      importlib.import_module(module_path)
    assert f"scrapy-extension[{extra}]" in str(exc_info.value)
    assert exc_info.value.__cause__ is missing
  finally:
    if cached is not None:
      sys.modules[module_path] = cached


@pytest.mark.parametrize("module_path,dependency,extra", _MODULE_GUARDS)
def test_module_guard_preserves_dependency_internal_import_error(
  monkeypatch: pytest.MonkeyPatch,
  module_path: str,
  dependency: str,
  extra: str,
) -> None:
  failure = ImportError(f"{dependency} internal ABI/import failure")
  _with_blocked_import(monkeypatch, dependency, lambda: failure)
  cached = sys.modules.pop(module_path, None)
  try:
    with pytest.raises(ImportError) as exc_info:
      importlib.import_module(module_path)
    assert exc_info.value is failure
    assert f"scrapy-extension[{extra}]" not in str(exc_info.value)
  finally:
    if cached is not None:
      sys.modules[module_path] = cached


def test_rocketmq_connect_wraps_a_genuinely_missing_dependency(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  from scrapy_extension.backends.rocketmq import RocketMQBackend
  from scrapy_extension.exceptions import BackendConnectionError
  from scrapy_extension.settings import RocketMQSettings

  missing = ModuleNotFoundError("No module named 'rocketmq'", name="rocketmq")
  _with_blocked_import(monkeypatch, "rocketmq", lambda: missing)

  with pytest.raises(BackendConnectionError) as exc_info:
    RocketMQBackend(RocketMQSettings()).connect()
  assert "not installed" in str(exc_info.value)
  assert exc_info.value.__cause__ is missing


def test_rocketmq_connect_preserves_dependency_internal_import_error(
  monkeypatch: pytest.MonkeyPatch,
) -> None:
  from scrapy_extension.backends.rocketmq import RocketMQBackend
  from scrapy_extension.settings import RocketMQSettings

  failure = ImportError("rocketmq internal ABI/import failure")
  _with_blocked_import(monkeypatch, "rocketmq", lambda: failure)

  with pytest.raises(ImportError) as exc_info:
    RocketMQBackend(RocketMQSettings()).connect()
  assert exc_info.value is failure
