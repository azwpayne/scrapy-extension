"""Tests for per-component backend configuration (multi-backend coexistence).

Feature: ``SCRAPY_QUEUE_BACKEND_TYPE`` / ``SCRAPY_SET_BACKEND_TYPE`` /
``SCRAPY_STORAGE_BACKEND_TYPE`` let the three independent components —
``BackendScheduler`` (queue), ``BackendDupeFilter`` (set), ``BackendPipeline``
(storage) — each bind to a *different* backend type, with independent settings.

Backward compatibility: when a per-component key is absent, the component
falls back to ``SCRAPY_BACKEND_TYPE`` / ``SCRAPY_BACKEND_SETTINGS`` so
existing single-backend configurations keep working unchanged.

Canonical use case: queue seeds in Redis-Cluster, dedup fingerprints in
MongoDB, scraped data in MongoDB (or ElasticSearch) — three backends,
one spider.
"""

from __future__ import annotations

from typing import Any

import pytest

from scrapy_extension.backends.base import BackendType
from scrapy_extension.backends.connectors import (
  ConnectionManager,
  resolve_backend_config,
)
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.schedule.scheduler import BackendScheduler


def _mock_settings(
  mocker,
  gets: dict[str, Any] | None = None,
  getdicts: dict[str, dict] | None = None,
  getfloats: dict[str, float] | None = None,
  getints: dict[str, int] | None = None,
  getbools: dict[str, bool] | None = None,
):
  """Build a mock Scrapy Settings with per-key get/getdict/getfloat/getint/getbool.

  Centralizes the lambda wiring so each test declares only the keys it cares
  about; unconfigured accessors return sane defaults (empty dict / 0 / False)
  mirroring Scrapy's real Settings semantics.
  """
  gets = gets or {}
  getdicts = getdicts or {}
  getfloats = getfloats or {}
  getints = getints or {}
  getbools = getbools or {}

  mock = mocker.Mock()
  mock.get.side_effect = lambda key, default=None: gets.get(key, default)
  mock.getdict.side_effect = lambda key, default=None: getdicts.get(
    key, default if default is not None else {}
  )
  mock.getfloat.side_effect = lambda key, default=0.0: getfloats.get(key, default)
  mock.getint.side_effect = lambda key, default=0: getints.get(key, default)
  mock.getbool.side_effect = lambda key, default=False: getbools.get(key, default)
  return mock


def _patch_get_manager(mocker):
  """Patch ConnectionManager.get_manager to return a fresh Mock and capture calls."""
  mock_manager = mocker.Mock()
  return mock_manager, mocker.patch.object(
    ConnectionManager, "get_manager", return_value=mock_manager
  )


class TestSchedulerPerComponentBackend:
  """BackendScheduler.from_settings honors SCRAPY_QUEUE_BACKEND_TYPE."""

  def test_uses_queue_backend_type_when_set(self, mocker):
    """SCRAPY_QUEUE_BACKEND_TYPE overrides SCRAPY_BACKEND_TYPE for the queue."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_BACKEND_TYPE": "mongodb",
        "SCRAPY_QUEUE_KEY": "scheduler:queue",
        "SCRAPY_QUEUE_STRATEGY": "passthrough",
      },
      getdicts={"SCRAPY_QUEUE_BACKEND_SETTINGS": {"uri": "mongodb://queue:27017"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendScheduler.from_settings(settings)

    patched.assert_called_once()
    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.MONGODB
    assert kwargs["settings"] == {"uri": "mongodb://queue:27017"}

  def test_falls_back_to_backend_type_when_queue_not_set(self, mocker):
    """Backward compat: no SCRAPY_QUEUE_BACKEND_TYPE → uses SCRAPY_BACKEND_TYPE."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_KEY": "scheduler:queue",
        "SCRAPY_QUEUE_STRATEGY": "passthrough",
      },
    )
    _, patched = _patch_get_manager(mocker)

    BackendScheduler.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.REDIS

  def test_falls_back_to_backend_settings_when_queue_settings_not_set(self, mocker):
    """No SCRAPY_QUEUE_BACKEND_SETTINGS → falls back to SCRAPY_BACKEND_SETTINGS."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_KEY": "scheduler:queue",
        "SCRAPY_QUEUE_STRATEGY": "passthrough",
      },
      getdicts={"SCRAPY_BACKEND_SETTINGS": {"host": "shared-redis"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendScheduler.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["settings"] == {"host": "shared-redis"}


class TestDupeFilterPerComponentBackend:
  """BackendDupeFilter.from_settings honors SCRAPY_SET_BACKEND_TYPE."""

  def test_uses_set_backend_type_when_set(self, mocker):
    """SCRAPY_SET_BACKEND_TYPE overrides SCRAPY_BACKEND_TYPE for dedup."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_SET_BACKEND_TYPE": "mongodb",
        "SCRAPY_DUPEFILTER_KEY": "dupefilter",
        "SCRAPY_DEDUP_STRATEGY": "set",
      },
      getdicts={"SCRAPY_SET_BACKEND_SETTINGS": {"uri": "mongodb://set:27017"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendDupeFilter.from_settings(settings)

    patched.assert_called_once()
    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.MONGODB
    assert kwargs["settings"] == {"uri": "mongodb://set:27017"}

  def test_falls_back_to_backend_type_when_set_not_set(self, mocker):
    """Backward compat: no SCRAPY_SET_BACKEND_TYPE → uses SCRAPY_BACKEND_TYPE."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_DUPEFILTER_KEY": "dupefilter",
        "SCRAPY_DEDUP_STRATEGY": "set",
      },
    )
    _, patched = _patch_get_manager(mocker)

    BackendDupeFilter.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.REDIS

  def test_falls_back_to_backend_settings_when_set_settings_not_set(self, mocker):
    """No SCRAPY_SET_BACKEND_SETTINGS → falls back to SCRAPY_BACKEND_SETTINGS."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_DUPEFILTER_KEY": "dupefilter",
        "SCRAPY_DEDUP_STRATEGY": "set",
      },
      getdicts={"SCRAPY_BACKEND_SETTINGS": {"host": "shared-redis"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendDupeFilter.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["settings"] == {"host": "shared-redis"}


class TestPipelinePerComponentBackend:
  """BackendPipeline.from_settings honors SCRAPY_STORAGE_BACKEND_TYPE."""

  def test_uses_storage_backend_type_when_set(self, mocker):
    """SCRAPY_STORAGE_BACKEND_TYPE overrides SCRAPY_BACKEND_TYPE for storage."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_STORAGE_BACKEND_TYPE": "mongodb",
      },
      getdicts={"SCRAPY_STORAGE_BACKEND_SETTINGS": {"uri": "mongodb://storage:27017"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendPipeline.from_settings(settings)

    patched.assert_called_once()
    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.MONGODB
    assert kwargs["settings"] == {"uri": "mongodb://storage:27017"}

  def test_falls_back_to_backend_type_when_storage_not_set(self, mocker):
    """Backward compat: no SCRAPY_STORAGE_BACKEND_TYPE → uses SCRAPY_BACKEND_TYPE."""
    settings = _mock_settings(
      mocker,
      gets={"SCRAPY_BACKEND_TYPE": "redis"},
    )
    _, patched = _patch_get_manager(mocker)

    BackendPipeline.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["backend_type"] == BackendType.REDIS

  def test_falls_back_to_backend_settings_when_storage_settings_not_set(self, mocker):
    """No SCRAPY_STORAGE_BACKEND_SETTINGS → falls back to SCRAPY_BACKEND_SETTINGS."""
    settings = _mock_settings(
      mocker,
      gets={"SCRAPY_BACKEND_TYPE": "redis"},
      getdicts={"SCRAPY_BACKEND_SETTINGS": {"host": "shared-redis"}},
    )
    _, patched = _patch_get_manager(mocker)

    BackendPipeline.from_settings(settings)

    _, kwargs = patched.call_args
    assert kwargs["settings"] == {"host": "shared-redis"}


class TestMultiBackendCoexistence:
  """End-to-end: queue/set/storage bind to three different backends from one config.

    This is the canonical scenario the feature unlocks — queue seeds in Redis,
    dedup fingerprints in MongoDB, scraped data in ElasticSearch — all wired
    from a single Scrapy settings dict, each component independently.
  """

  def test_three_backends_coexist_from_one_settings(self, mocker):
    """One settings dict → three components → three distinct backend types."""
    settings = _mock_settings(
      mocker,
      gets={
        # Default (used by anything without a per-component override)
        "SCRAPY_BACKEND_TYPE": "redis",
        # Per-component overrides — the three backends diverge here
        "SCRAPY_QUEUE_BACKEND_TYPE": "redis",
        "SCRAPY_SET_BACKEND_TYPE": "mongodb",
        "SCRAPY_STORAGE_BACKEND_TYPE": "elasticsearch",
        # Scheduler keys
        "SCRAPY_QUEUE_KEY": "scheduler:queue",
        "SCRAPY_QUEUE_STRATEGY": "passthrough",
        # DupeFilter keys
        "SCRAPY_DUPEFILTER_KEY": "dupefilter",
        "SCRAPY_DEDUP_STRATEGY": "set",
      },
      getdicts={
        "SCRAPY_QUEUE_BACKEND_SETTINGS": {"host": "redis-cluster"},
        "SCRAPY_SET_BACKEND_SETTINGS": {"uri": "mongodb://mongo:27017"},
        "SCRAPY_STORAGE_BACKEND_SETTINGS": {"hosts": ["http://es:9200"]},
      },
    )
    _, patched = _patch_get_manager(mocker)

    BackendScheduler.from_settings(settings)
    BackendDupeFilter.from_settings(settings)
    BackendPipeline.from_settings(settings)

    # Three independent ConnectionManager.get_manager calls — one per component.
    assert patched.call_count == 3

    called_types = [c.kwargs["backend_type"] for c in patched.call_args_list]
    assert called_types == [
      BackendType.REDIS,
      BackendType.MONGODB,
      BackendType.ELASTICSEARCH,
    ]

    called_settings = [c.kwargs["settings"] for c in patched.call_args_list]
    assert called_settings[0] == {"host": "redis-cluster"}
    assert called_settings[1] == {"uri": "mongodb://mongo:27017"}
    assert called_settings[2] == {"hosts": ["http://es:9200"]}

  def test_mixed_override_and_fallback(self, mocker):
    """Queue overrides to mongodb; set/storage fall back to SCRAPY_BACKEND_TYPE=redis."""
    settings = _mock_settings(
      mocker,
      gets={
        "SCRAPY_BACKEND_TYPE": "redis",
        "SCRAPY_QUEUE_BACKEND_TYPE": "mongodb",  # only queue diverges
        "SCRAPY_QUEUE_KEY": "scheduler:queue",
        "SCRAPY_QUEUE_STRATEGY": "passthrough",
        "SCRAPY_DUPEFILTER_KEY": "dupefilter",
        "SCRAPY_DEDUP_STRATEGY": "set",
      },
      getdicts={
        "SCRAPY_QUEUE_BACKEND_SETTINGS": {"uri": "mongodb://queue:27017"},
        "SCRAPY_BACKEND_SETTINGS": {"host": "default-redis"},
      },
    )
    _, patched = _patch_get_manager(mocker)

    BackendScheduler.from_settings(settings)
    BackendDupeFilter.from_settings(settings)
    BackendPipeline.from_settings(settings)

    called_types = [c.kwargs["backend_type"] for c in patched.call_args_list]
    assert called_types == [
      BackendType.MONGODB,  # queue overridden
      BackendType.REDIS,  # set falls back
      BackendType.REDIS,  # storage falls back
    ]

    called_settings = [c.kwargs["settings"] for c in patched.call_args_list]
    assert called_settings[0] == {"uri": "mongodb://queue:27017"}  # queue-specific
    assert called_settings[1] == {"host": "default-redis"}  # fallback
    assert called_settings[2] == {"host": "default-redis"}  # fallback


class TestResolveBackendConfig:
  """Direct unit tests for the resolve_backend_config helper.

  Locks edge-case behavior of the shared config resolver so future
  refactors can't silently break the per-component vs fallback semantics.
  These are characterization tests — the behavior already exists; the
  tests pin it against regressions.
  """

  def test_per_component_type_without_settings_returns_empty_dict(self, mocker):
    """Edge case: per-component type set but settings key absent → empty dict.

    The component then uses the backend's default settings (e.g.
    MongoDBSettings defaults). Intended — not all components need custom
    connection params.
    """
    settings = _mock_settings(mocker, gets={"SCRAPY_QUEUE_BACKEND_TYPE": "mongodb"})

    backend_type, backend_settings = resolve_backend_config(
      settings,
      type_key="SCRAPY_QUEUE_BACKEND_TYPE",
      settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
    )

    assert backend_type == BackendType.MONGODB
    assert backend_settings == {}

  def test_invalid_backend_type_raises_value_error(self, mocker):
    """Edge case: invalid backend type string → ValueError from BackendType().

    Surfaces a clear error at config time rather than a confusing failure
    deep in connection setup.
    """
    settings = _mock_settings(
      mocker, gets={"SCRAPY_QUEUE_BACKEND_TYPE": "not-a-real-backend"}
    )

    with pytest.raises(ValueError):
      resolve_backend_config(
        settings,
        type_key="SCRAPY_QUEUE_BACKEND_TYPE",
        settings_key="SCRAPY_QUEUE_BACKEND_SETTINGS",
      )
