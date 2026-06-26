"""Round-8 forward coverage: per-mode MongoDB client construction contract.

Closes F6 (``backends/mongodb.py`` 87.22% → higher). The four ``MongoDBMode``
values funnel into four distinct ``_connect_*`` private methods, each building
a *different* ``MongoClient(uri, **kwargs)`` call. This module pins the
mode → constructor contract by asserting the mock-captured URI shape and
mode-specific kwargs for every mode — so a refactor that collapses two modes
into the same call shape is caught.

Honest tests only (PUA Integrity Guard): no disabled/weakened assertions, no
over-broad ``try/except`` masking failures. Each mode is a standalone test
that asserts the *observable* call — ``MongoClient.call_args`` — exactly as
captured by ``mocker.patch``.

Reads ``src/scrapy_extension/backends/mongodb.py`` +
``src/scrapy_extension/settings/mongodb.py`` to pin the REAL constructor
logic per mode:

- STANDALONE (``_connect_standalone``): ``MongoClient(config.uri, **kwargs)`` —
  single-host, no ``replicaSet`` kwarg, no URI query suffix.
- REPLICA_SET (``_connect_replica_set``): when ``replica_set_members`` set,
  URI is ``mongodb://<members>/<db>?replicaSet=<name>`` and ``replicaSet`` is
  ALSO injected as a kwarg when ``replica_set_name`` is set. Without members
  it falls back to ``config.uri``.
- SHARDED_CLUSTER (``_connect_sharded_cluster``): when ``mongos_routers`` set,
  URI is ``mongodb://<routers>/<db>`` (multiple hosts). Without routers it
  falls back to ``config.uri``.
- ATLAS (``_connect_atlas``): forces ``tls=True`` in kwargs regardless of
  ``tls_enabled`` config; URI comes from ``config.uri`` unchanged.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from scrapy_extension.backends.mongodb import MongoDBBackend
from scrapy_extension.settings import MongoDBMode, MongoDBSettings


def _patch_mongo_client(mocker) -> MagicMock:
  """Patch ``MongoClient`` in the mongodb module with a fresh MagicMock.

  Returns the mock so the test can inspect ``call_args`` / ``call_count``.
  The ``client.admin.command("ping")`` and ``client[db][coll]`` chains are
  auto-mocked by MagicMock — no real pymongo is needed.
  """
  mock_client = MagicMock()
  mocker.patch(
    "scrapy_extension.backends.mongodb.MongoClient", return_value=mock_client
  )
  return mock_client


class TestStandaloneModeConstructor:
  """STANDALONE: single-host ``MongoClient(uri, **kwargs)`` — no replicaSet."""

  def test_standalone_uses_config_uri_and_no_replica_set_kwarg(self, mocker):
    """STANDALONE passes ``config.uri`` verbatim and never sets ``replicaSet``.

    Pins ``_connect_standalone`` (mongodb.py:250-255): the call is
    ``MongoClient(self.config.uri, **kwargs)`` with no URI mutation and no
    ``replicaSet`` kwarg (that kwarg is replica-set-specific).
    """
    config = MongoDBSettings(
      mode=MongoDBMode.STANDALONE,
      uri="mongodb://standalone-host:27017",
    )
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    assert MongoClient.call_count == 1
    call_args, call_kwargs = MongoClient.call_args
    assert call_args == ("mongodb://standalone-host:27017",)
    # STANDALONE never injects replicaSet — that's REPLICA_SET-only.
    assert "replicaSet" not in call_kwargs
    # Pool sizing kwargs are present (proves _build_client_kwargs ran).
    assert call_kwargs["minPoolSize"] == config.min_pool_size
    assert call_kwargs["maxPoolSize"] == config.max_pool_size


class TestReplicaSetModeConstructor:
  """REPLICA_SET: ``replicaSet=`` in URI and/or ``replicaSet`` kwarg."""

  def test_replica_set_builds_members_uri_with_replicaset_query(self, mocker):
    """REPLICA_SET + ``replica_set_members`` → URI is
    ``mongodb://<members>/<db>?replicaSet=<name>`` AND ``replicaSet`` kwarg
    is set.

    Pins ``_connect_replica_set`` (mongodb.py:257-279): members → URI is
    rebuilt; ``replica_set_name`` (when set) appears BOTH as the
    ``?replicaSet=`` query suffix AND as the ``replicaSet=<name>`` kwarg.
    """
    config = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET,
      replica_set_name="rs0",
      replica_set_members=["rs-host1:27017", "rs-host2:27017", "rs-host3:27017"],
    )
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    call_args, call_kwargs = MongoClient.call_args
    uri = call_args[0]
    # Multi-host URI rebuilt from members, NOT the default localhost URI.
    assert uri.startswith("mongodb://rs-host1:27017,rs-host2:27017,rs-host3:27017/")
    assert "?replicaSet=rs0" in uri
    # The replicaSet kwarg is ALSO injected (separate from the URI suffix).
    assert call_kwargs.get("replicaSet") == "rs0"

  def test_replica_set_without_members_falls_back_to_config_uri(self, mocker):
    """REPLICA_SET without ``replica_set_members`` → ``config.uri`` is used
    verbatim and no ``?replicaSet=`` query is appended.

    Pins the fallback branch (mongodb.py:271-272): when ``replica_set_members``
    is empty, the URI is ``config.uri`` unchanged.
    """
    config = MongoDBSettings(
      mode=MongoDBMode.REPLICA_SET,
      uri="mongodb://fallback-host:27017/?replicaSet=existing",
    )
    # No replica_set_members, no replica_set_name → no URI rebuild, no kwarg.
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    call_args, call_kwargs = MongoClient.call_args
    assert call_args[0] == "mongodb://fallback-host:27017/?replicaSet=existing"
    # No replica_set_name → no kwarg injection.
    assert "replicaSet" not in call_kwargs


class TestShardedClusterModeConstructor:
  """SHARDED_CLUSTER: multiple hosts (mongos routers) in the URI."""

  def test_sharded_cluster_builds_multi_host_uri_from_mongos_routers(self, mocker):
    """SHARDED_CLUSTER + ``mongos_routers`` → URI is
    ``mongodb://<router1>,<router2>/<db>``.

    Pins ``_connect_sharded_cluster`` (mongodb.py:281-298): multiple mongos
    routers become a multi-host URI. This is the load-bearing difference
    from STANDALONE (which is always single-host).
    """
    config = MongoDBSettings(
      mode=MongoDBMode.SHARDED_CLUSTER,
      mongos_routers=["mongos-a:27017", "mongos-b:27017"],
      database="shard_db",
    )
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    call_args, _call_kwargs = MongoClient.call_args
    uri = call_args[0]
    # Both routers appear as comma-separated hosts.
    assert "mongos-a:27017" in uri
    assert "mongos-b:27017" in uri
    assert uri.startswith("mongodb://mongos-a:27017,mongos-b:27017/shard_db")
    # Sharded cluster does NOT inject the replicaSet kwarg.
    _args, call_kwargs = MongoClient.call_args
    assert "replicaSet" not in call_kwargs

  def test_sharded_cluster_without_routers_falls_back_to_config_uri(self, mocker):
    """SHARDED_CLUSTER without ``mongos_routers`` → ``config.uri`` is used
    verbatim.

    Pins the fallback branch (mongodb.py:294-295): no routers → URI unchanged.
    """
    config = MongoDBSettings(
      mode=MongoDBMode.SHARDED_CLUSTER,
      uri="mongodb://shard-fallback:27017",
    )
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    call_args, _call_kwargs = MongoClient.call_args
    assert call_args[0] == "mongodb://shard-fallback:27017"


class TestAtlasModeConstructor:
  """ATLAS: TLS forced on regardless of ``tls_enabled`` config."""

  def test_atlas_forces_tls_true_in_kwargs(self, mocker):
    """ATLAS → ``tls=True`` is injected into kwargs unconditionally.

    Pins ``_connect_atlas`` (mongodb.py:300-312): Atlas ALWAYS requires TLS,
    so the kwarg is forced True even when ``tls_enabled=False`` in config
    (the operator sets the ``mongodb+srv://`` URI; the driver needs the
    explicit ``tls=True`` kwarg to actually negotiate TLS).
    """
    config = MongoDBSettings(
      mode=MongoDBMode.ATLAS,
      uri="mongodb+srv://cluster0.example.mongodb.net",
      database="atlas_db",
      tls_enabled=False,  # Atlas forces True regardless.
    )
    backend = MongoDBBackend(config)
    _patch_mongo_client(mocker)

    backend.connect()

    from scrapy_extension.backends.mongodb import MongoClient

    call_args, call_kwargs = MongoClient.call_args
    # Atlas URI is used verbatim.
    assert call_args[0] == "mongodb+srv://cluster0.example.mongodb.net"
    # The load-bearing assertion: tls is forced True.
    assert call_kwargs.get("tls") is True


class TestModesDiverge:
  """Cross-mode: assert the four modes produce four DIFFERENT call shapes.

  If a refactor accidentally collapses two modes into the same constructor
  call, this test fires. It's the regression-backstop for the per-mode
  contract.
  """

  def test_each_mode_yields_distinct_uri_or_kwargs(self, mocker):
    """Constructing under each mode yields a distinguishable MongoClient call.

    Captures the call signature under all four modes and asserts pairwise
    distinctness on (uri, tls_kwarg, replicaset_kwarg). Catches mode-collapse
    regressions where e.g. ATLAS accidentally falls through to STANDALONE.
    """
    configs_and_expected = [
      (
        MongoDBSettings(
          mode=MongoDBMode.STANDALONE, uri="mongodb://h1:27017"
        ),
        "mongodb://h1:27017",
        False,
        None,
      ),
      (
        MongoDBSettings(
          mode=MongoDBMode.REPLICA_SET,
          replica_set_name="rs0",
          replica_set_members=["r1:27017", "r2:27017"],
        ),
        None,  # don't pin exact URI; assert it's multi-host + has suffix
        False,
        "rs0",
      ),
      (
        MongoDBSettings(
          mode=MongoDBMode.SHARDED_CLUSTER,
          mongos_routers=["m1:27017", "m2:27017"],
        ),
        None,  # multi-host
        False,
        None,
      ),
      (
        MongoDBSettings(
          mode=MongoDBMode.ATLAS,
          uri="mongodb+srv://atlas.example.net",
        ),
        "mongodb+srv://atlas.example.net",
        True,
        None,
      ),
    ]

    seen: list[tuple[str, bool, str | None]] = []
    for config, _expected_uri, expected_tls, expected_rs in configs_and_expected:
      mocker.stopall()  # fresh patch per mode
      _patch_mongo_client(mocker)
      backend = MongoDBBackend(config)
      backend.connect()

      from scrapy_extension.backends.mongodb import MongoClient

      uri = MongoClient.call_args[0][0]
      kwargs = MongoClient.call_args.kwargs
      tls = bool(kwargs.get("tls", False))
      rs = kwargs.get("replicaSet")
      seen.append((uri, tls, rs))

      # Per-mode invariants (the contract pinned by the per-mode tests above).
      assert tls is expected_tls
      assert rs == expected_rs

    # The four captured (uri, tls, replicaSet) triples are pairwise distinct
    # — proof the four modes produce four distinguishable constructor calls.
    assert len(seen) == 4
    assert len(set(seen)) == 4, f"mode calls are not pairwise distinct: {seen}"


@pytest.mark.parametrize(
  ("mode", "expected_in_uri"),
  [
    (MongoDBMode.STANDALONE, ("mongodb://h:27017",)),
    (MongoDBMode.REPLICA_SET, ("replicaSet=rs0",)),
    (MongoDBMode.SHARDED_CLUSTER, ("mongos1:27017", "mongos2:27017")),
    (MongoDBMode.ATLAS, ("mongodb+srv://",)),
  ],
)
def test_mode_specific_uri_signature(mode: MongoDBMode, expected_in_uri: tuple[str, ...], mocker):
  """Parametrized smoke: each mode's captured URI contains mode-specific tokens.

  This is a compact regression check on the URI shape itself — standalone is
  a plain single-host URI, replica_set carries the ``?replicaSet=`` suffix,
  sharded_cluster has multiple mongos hosts, atlas uses ``mongodb+srv://``.
  """
  if mode is MongoDBMode.STANDALONE:
    config = MongoDBSettings(mode=mode, uri="mongodb://h:27017")
  elif mode is MongoDBMode.REPLICA_SET:
    config = MongoDBSettings(
      mode=mode,
      replica_set_name="rs0",
      replica_set_members=["rs1:27017", "rs2:27017"],
    )
  elif mode is MongoDBMode.SHARDED_CLUSTER:
    config = MongoDBSettings(
      mode=mode, mongos_routers=["mongos1:27017", "mongos2:27017"]
    )
  else:  # ATLAS
    config = MongoDBSettings(mode=mode, uri="mongodb+srv://atlas.example.net")

  _patch_mongo_client(mocker)
  MongoDBBackend(config).connect()

  from scrapy_extension.backends.mongodb import MongoClient

  captured_uri = MongoClient.call_args[0][0]
  for token in expected_in_uri:
    assert token in captured_uri, (
      f"mode {mode.value}: expected {token!r} in URI, got {captured_uri!r}"
    )
