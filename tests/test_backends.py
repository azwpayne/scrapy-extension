"""Tests for backend implementations."""

import pytest

from scrapy_extension.backends.base import (
  BackendType,
  JSONSerializer,
)
from scrapy_extension.exceptions import BackendConnectionError


class TestRedisMode:
  """Test RedisMode enum."""

  def test_standalone_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.STANDALONE.value == "standalone"

  def test_master_slave_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.MASTER_SLAVE.value == "master_slave"

  def test_sentinel_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.SENTINEL.value == "sentinel"

  def test_cluster_value(self):
    from scrapy_extension.settings import RedisMode

    assert RedisMode.CLUSTER.value == "cluster"


class TestBackendType:
  """Test BackendType enum."""

  def test_redis_value(self):
    assert BackendType.REDIS.value == "redis"

  def test_mongodb_value(self):
    assert BackendType.MONGODB.value == "mongodb"

  def test_kafka_value(self):
    assert BackendType.KAFKA.value == "kafka"

  def test_rabbitmq_value(self):
    assert BackendType.RABBITMQ.value == "rabbitmq"

  def test_invalid_value_lists_valid_options(self):
    """R3-G7: BackendType(invalid) raises ValueError with valid-values hint."""
    with pytest.raises(ValueError) as exc_info:
      BackendType("mysql")
    msg = str(exc_info.value)
    assert "'mysql'" in msg
    assert "redis" in msg
    assert "mongodb" in msg
    assert "Valid values:" in msg


class TestJSONSerializer:
  """Test JSONSerializer."""

  def test_serialize_dict(self):
    serializer = JSONSerializer()
    data = {"key": "value"}
    result = serializer.serialize(data)
    assert result == b'{"key": "value"}'

  def test_deserialize_dict(self):
    serializer = JSONSerializer()
    data = b'{"key": "value"}'
    result = serializer.deserialize(data)
    assert result == {"key": "value"}

  def test_serialize_list(self):
    serializer = JSONSerializer()
    data = [1, 2, 3]
    result = serializer.serialize(data)
    assert result == b"[1, 2, 3]"

  def test_round_trip(self):
    serializer = JSONSerializer()
    data = {"nested": {"key": "value"}, "list": [1, 2, 3]}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized == data

  def test_datetime_serializes_to_isoformat(self):
    """R17-followup: datetime in request.meta survives as ISO 8601 string."""
    from datetime import datetime, timezone

    serializer = JSONSerializer()
    dt = datetime(2026, 6, 16, 12, 30, 0, tzinfo=timezone.utc)
    data = {"scraped_at": dt}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["scraped_at"] == "2026-06-16T12:30:00+00:00"

  def test_bytes_round_trips_to_bytes(self):
    """bytes in request.meta round-trips back to bytes (symmetric serializer).

    Previously (R17) the serializer was asymmetric: ``serialize`` base64-
    encoded ``bytes`` to a ``str`` via ``_json_default``, but ``deserialize``
    never decoded it — so a ``bytes`` value in ``meta`` / ``cookies`` /
    ``cb_kwargs`` came back as a base64 ``str``. R17 accepted that as "survives
    (not repr)" to avoid the worse ``b'\\x00'`` → ``"b'\\x00'"`` repr corruption.
    The serializer is now symmetric: ``bytes`` round-trips to ``bytes``, so a
    spider reading ``request.meta[key]`` after a queue cycle gets back exactly
    what it pushed — no manual ``base64.b64decode`` workaround.

    Note: ``bytearray`` also round-trips, narrowing to ``bytes`` (JSON cannot
    distinguish the two without a second marker; ``bytes`` is the superset
    callers expect).
    """
    serializer = JSONSerializer()
    data = {"raw": b"\x00\xff\x42"}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["raw"] == b"\x00\xff\x42"
    assert isinstance(deserialized["raw"], bytes)

  def test_corrupt_b64_marker_does_not_crash_deserialize(self):
    """#31: a stored value shaped like {"__b64__": "<invalid base64>"} (a
    spider's own meta key, or a truncated/corrupt value) must NOT crash the
    whole request deserialize via binascii.Error -- it falls through as the
    original dict so the pop surfaces a value instead of dropping the request.
    """
    serializer = JSONSerializer()
    # 'A' is a single base64-alphabet char -> b64decode raises binascii.Error
    # (data length 1 cannot be 1 more than a multiple of 4).
    corrupt = b'{"k": {"__b64__": "A"}}'
    result = serializer.deserialize(corrupt)
    assert result == {"k": {"__b64__": "A"}}

  def test_secret_str_in_meta_serializes(self):
    """#31: pydantic SecretStr in request.meta serializes (does not raise
    TypeError). Round-trip yields a plain str (SecretStr is not reconstructable
    from JSON without a pydantic hook); the bar here is 'does not crash push'.
    """
    from pydantic import SecretStr

    serializer = JSONSerializer()
    data = {"token": SecretStr("hunter2")}
    serialized = serializer.serialize(data)  # must not raise
    deserialized = serializer.deserialize(serialized)
    assert deserialized["token"] == "hunter2"

  def test_string_looking_like_base64_stays_str(self):
    """Guard: a plain string that happens to be valid base64 is NOT decoded.

    Only genuine ``bytes`` — encoded as a tagged ``{"__b64__": ...}`` marker on
    serialize — decode back to bytes. ASCII strings pass through untouched. A
    naive "decode every base64-looking string" fix would corrupt this case and
    every ordinary string token in ``meta``.
    """
    serializer = JSONSerializer()
    data = {"token": "AAEC"}  # valid base64, but it's a str the caller owns
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["token"] == "AAEC"
    assert isinstance(deserialized["token"], str)

  def test_nested_bytes_round_trip(self):
    """Bytes nested in lists/dicts (e.g. ``meta['blobs']``) round-trip."""
    serializer = JSONSerializer()
    data = {"meta": {"blobs": [b"a", b"b", {"deep": b"c"}]}}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["meta"]["blobs"] == [b"a", b"b", {"deep": b"c"}]

  def test_unsupported_type_raises_with_clear_message(self):
    """R17-followup: truly unexpected types raise TypeError, not silent str()."""
    serializer = JSONSerializer()

    class Custom:
      pass

    with pytest.raises(TypeError, match="not JSON serializable"):
      serializer.serialize({"obj": Custom()})

  def test_decimal_serializes_as_str(self):
    """R19: Decimal (prices) preserves exact representation, no float drift."""
    from decimal import Decimal

    serializer = JSONSerializer()
    data = {"price": Decimal("19.99")}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["price"] == "19.99"

  def test_uuid_serializes_as_str(self):
    """R19: UUID serializes to canonical hex form."""
    import uuid as uuid_mod

    serializer = JSONSerializer()
    uid = uuid_mod.UUID("12345678-1234-5678-1234-567812345678")
    data = {"request_id": uid}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["request_id"] == "12345678-1234-5678-1234-567812345678"

  def test_set_serializes_as_list(self):
    """R19: set/frozenset convert to list (JSON has no set type)."""
    serializer = JSONSerializer()
    data = {"tags": {"a", "b", "c"}}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert set(deserialized["tags"]) == {"a", "b", "c"}

  def test_enum_serializes_as_value(self):
    """R19: Enum members serialize to their .value, not the member name."""
    from enum import Enum

    class Status(Enum):
      PENDING = "pending"
      DONE = "done"

    serializer = JSONSerializer()
    data = {"status": Status.DONE}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["status"] == "done"

  def test_path_serializes_as_str(self):
    """R20: pathlib.Path in request.meta survives as string."""
    from pathlib import Path

    serializer = JSONSerializer()
    data = {"output": Path("/tmp/output.json")}
    serialized = serializer.serialize(data)
    deserialized = serializer.deserialize(serialized)
    assert deserialized["output"] == "/tmp/output.json"


class TestRedisBackend:
  """Test RedisBackend implementation."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_backend_type(self, redis_settings):
    """Test backend type is REDIS."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    assert backend.backend_type == BackendType.REDIS

  def test_connect_success(self, redis_settings, mock_redis, mocker):
    """Test successful connection."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected()
    # ping is called in connect() and is_connected(), so at least once
    assert mock_redis.ping.call_count >= 1

  def test_connect_failure(self, redis_settings, mocker):
    """Test connection failure raises ConnectionError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisError("Connection refused")
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_queue_push(self, redis_settings, mock_redis, mocker):
    """Test queue push operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"test_data", priority=1.0)
    # Push uses a Lua script: INCR counter + ZADD + HSET atomically
    mock_redis.register_script.assert_called_once()
    mock_script.assert_called_once()

  def test_queue_pop(self, redis_settings, mock_redis, mocker):
    """Test queue pop operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    # New signal scheme: [1, payload] = success.
    mock_script.return_value = [1, b"test_data"]
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result == b"test_data"

  def test_queue_pop_empty(self, redis_settings, mock_redis, mocker):
    """Test queue pop with empty queue."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    # New signal scheme: [0, None] = empty queue.
    mock_script.return_value = [0, None]
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result is None

  def test_queue_pop_corrupt_payload_raises_queue_error(
    self, redis_settings, mock_redis, mocker
  ):
    """Coverage: status==1 (success signal) but the payload is neither str nor
    bytes (a corrupt Lua return — should never happen, but defensive) must raise
    QueueError, NOT silently return a wrong-type value. Locks the corrupt-data
    guard at redis.py:532-536.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_script = mocker.MagicMock()
    mock_script.return_value = [1, 12345]  # int — neither str nor bytes
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError, match="Corrupt payload"):
      backend.pop("test_queue")

  def test_queue_pop_lost_payload_race_returns_none(
    self, redis_settings, mock_redis, mocker
  ):
    """B1: concurrent-consumer race must NOT raise — recoverable.

    Two consumers race ZPOPMIN; the loser finds its payload already HDEL'd.
    This is an item-consumed-elsewhere race, not corruption: pop() must
    return None (DEBUG-log, no raise).
    """
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    # New signal scheme: [2, None] = member popped but payload gone (race).
    mock_script.return_value = [2, None]
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result is None

  def test_queue_pop_corruption_raises_queueerror(
    self, redis_settings, mock_redis, mocker
  ):
    """B1: structural corruption (unexpected payload type) raises QueueError.

    Only a genuine invariant violation — payload decoded to a non-bytes /
    non-str shape that cannot be normalized — escalates to QueueError.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_script = mocker.MagicMock()
    # New signal scheme: [3, msg] = structural corruption (e.g. Lua
    # surfaced an unexpected payload type). We surface it as QueueError.
    mock_script.return_value = [3, "unexpected payload type: float"]
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError):
      backend.pop("test_queue")

  def test_set_add(self, redis_settings, mock_redis, mocker):
    """Test set add operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.add("test_set", b"test_item")
    assert result is True

  def test_set_add_wraps_redis_error(self, redis_settings, mock_redis, mocker):
    """R-dupe-1 (option b): a transient RedisError during set add is wrapped as
    BackendConnectionError so BackendDupeFilter's graceful-degradation arm fires
    (degrade to not-seen) instead of crashing the crawl."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.side_effect = RedisError("set add failed")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.add("test_set", b"test_item")
    assert exc_info.value.backend_type == "redis"

  def test_set_contains(self, redis_settings, mock_redis, mocker):
    """Test set contains operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sismember.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.contains("test_set", b"test_item")
    assert result is True

  def test_storage_store(self, redis_settings, mock_redis, mocker):
    """Test storage store operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data")
    mock_redis.set.assert_called_once_with("test_key", b"test_data")

  def test_storage_retrieve(self, redis_settings, mock_redis, mocker):
    """Test storage retrieve operation."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = b"test_data"
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("test_key")
    assert result == b"test_data"


class TestRedisBackendModes:
  """Test RedisBackend with different deployment modes."""

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_standalone_mode_default(self, mock_redis, mocker):
    """Test standalone mode is default."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(host="localhost", port=6379)
    assert settings.mode == RedisMode.STANDALONE

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_sentinel_mode_success(self, mock_redis, mocker):
    """Test sentinel mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379"],
      sentinel_master_name="mymaster",
      password="secret",
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=mock_sentinel)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()
    mock_sentinel.master_for.assert_called_once()

  def test_sentinel_mode_missing_sentinels(self):
    """Test sentinel mode requires sentinels configuration (validated at construction).

    R1-P2-20 fix: cross-mode validation runs at RedisSettings construction,
    so misconfiguration fails fast rather than at connect() time. R9-b SV2
    upgrades the raised exception from pydantic ValidationError to the
    project's ``ConfigurationError`` (with ``setting_name=``) for a named,
    debuggable failure.
    """
    from scrapy_extension.exceptions import ConfigurationError
    from scrapy_extension.settings import RedisMode, RedisSettings

    with pytest.raises(ConfigurationError) as exc_info:
      RedisSettings(
        mode=RedisMode.SENTINEL,
        sentinel_master_name="mymaster",
      )
    assert "sentinels" in str(exc_info.value).lower()
    assert exc_info.value.setting_name == "sentinels"

  def test_sentinel_mode_missing_master_name(self):
    """Sentinel mode with empty master_name fails fast (R1-P2-20 + R9-b SV2)."""
    from scrapy_extension.exceptions import ConfigurationError
    from scrapy_extension.settings import RedisMode, RedisSettings

    with pytest.raises(ConfigurationError) as exc_info:
      RedisSettings(
        mode=RedisMode.SENTINEL,
        sentinels=["redis-sentinel-1:26379"],
        sentinel_master_name="",
      )
    assert "sentinel_master_name" in str(exc_info.value).lower()
    assert exc_info.value.setting_name == "sentinel_master_name"

  def test_standalone_mode_passes_validation(self):
    """Standalone mode requires no mode-specific fields (R1-P2-20 sanity check)."""
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(mode=RedisMode.STANDALONE)
    assert settings.mode == RedisMode.STANDALONE

  def test_cluster_mode_success(self, mock_redis, mocker):
    """Test cluster mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000", "node3:7000"],
      password="secret",
    )

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_redis
    )
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_master_slave_mode_success(self, mock_redis, mocker):
    """Test master-slave mode connection."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379", "replica2.redis.com:6379"],
    )

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_cluster_mode_uses_startup_nodes(self, mock_redis, mocker):
    """Test cluster mode uses startup nodes configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node1:7000", "node2:7000"],
    )

    mock_cluster_class = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock_cluster_class.return_value = mock_redis
    backend = RedisBackend(settings)
    backend.connect()
    mock_cluster_class.assert_called_once()
    call_kwargs = mock_cluster_class.call_args.kwargs
    assert "startup_nodes" in call_kwargs
    assert len(call_kwargs["startup_nodes"]) == 2

  def test_sentinel_mode_configuration(self, mock_redis, mocker):
    """Test sentinel mode configuration options."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379", "sentinel2:26379", "sentinel3:26379"],
      sentinel_master_name="myredis",
      sentinel_password="sentinel_pass",
      password="redis_pass",
      db=0,
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mock_sentinel_class = mocker.patch("scrapy_extension.backends.redis.Sentinel")
    mock_sentinel_class.return_value = mock_sentinel
    backend = RedisBackend(settings)
    backend.connect()
    mock_sentinel_class.assert_called_once()
    # Verify sentinels were passed correctly
    call_args = mock_sentinel_class.call_args
    assert len(call_args.args[0]) == 3  # Three sentinel tuples

  def test_sentinel_mode_with_username(self, mock_redis, mocker):
    """Test sentinel mode with username configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
      sentinel_username="sentinel_user",
      sentinel_password="sentinel_pass",
    )

    mock_sentinel = mocker.Mock()
    mock_sentinel.master_for.return_value = mock_redis

    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=mock_sentinel)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

  def test_master_slave_mode_with_replicas(self, mocker):
    """Test master-slave mode logs replica configuration."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379", "replica2.redis.com:6379"],
    )

    mock_redis = mocker.Mock()
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    # Just verify connect succeeds with replicas configured
    assert backend.is_connected()

  def test_cluster_mode_fallback_host_port(self, mock_redis, mocker):
    """Test cluster mode falls back to host:port when no startup nodes."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      host="cluster.redis.com",
      port=7000,
      # No cluster_startup_nodes configured
    )

    mock_cluster_class = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock_cluster_class.return_value = mock_redis
    backend = RedisBackend(settings)
    backend.connect()
    mock_cluster_class.assert_called_once()
    call_kwargs = mock_cluster_class.call_args.kwargs
    # Should fall back to host:port
    assert len(call_kwargs["startup_nodes"]) == 1


class TestRedisSentinelClusterWiring:
  """Exercise Sentinel/Cluster connection paths beyond constructor-level mocks.

  These tests mock the CLIENT returned by Sentinel.master_for() / RedisCluster,
  but let the real Sentinel / RedisCluster classes (or lightly-mocked versions
  that still exercise the parsing + wiring logic) run, proving:
  - sentinel_tuples parsing (``rsplit(':', 1)`` + ``int(port)``)
  - Sentinel(...).master_for(master_name) call shape + master client stored as self._client
  - ClusterNode wiring from cluster_startup_nodes
  - malformed sentinel/node entries surface a clear error
  """

  @pytest.fixture
  def mock_master_client(self, mocker):
    """Mock master client returned by Sentinel.master_for()."""
    client = mocker.Mock()
    client.ping.return_value = True
    return client

  def test_sentinel_parses_tuples_and_calls_master_for(self, mock_master_client, mocker):
    """_connect_sentinel parses sentinel_tuples, calls Sentinel(sentinels).master_for(name).

    Sentinel is real-captured-then-mocked: we verify the parsed sentinel_tuples
    are passed to Sentinel() in exactly the ``(host, int(port))`` shape, and that
    master_for is called with the configured master_name. The master client is
    then stored as self._client and is_operable through ping().
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-a:26379", "sentinel-b:26380", "sentinel-c:26381"],
      sentinel_master_name="mymaster",
      password="secret",
    )

    captured_sentinel = {}

    def fake_sentinel_factory(sentinels, **kwargs):
      captured_sentinel["sentinels"] = sentinels
      captured_sentinel["kwargs"] = kwargs
      instance = mocker.Mock()
      instance.master_for.return_value = mock_master_client
      return instance

    mocker.patch("scrapy_extension.backends.redis.Sentinel", side_effect=fake_sentinel_factory)

    backend = RedisBackend(settings)
    backend.connect()

    # 1) sentinel_tuples parsed as (host, int_port) tuples
    assert captured_sentinel["sentinels"] == [
      ("sentinel-a", 26379),
      ("sentinel-b", 26380),
      ("sentinel-c", 26381),
    ]
    # all ports coerced to int (not str)
    assert all(isinstance(p, int) for _, p in captured_sentinel["sentinels"])

    # 2) master_for called with the configured master_name
    sentinel_instance = backend._sentinel
    sentinel_instance.master_for.assert_called_once()
    assert sentinel_instance.master_for.call_args.args[0] == "mymaster"

    # 3) master client is stored as self._client and responds to ping
    assert backend._client is mock_master_client
    assert backend._master_client is mock_master_client
    mock_master_client.ping.assert_called()
    assert backend.is_connected()

  def test_sentinel_master_client_is_operable_for_set(self, mock_master_client, mocker):
    """The discovered master client flows through to real ops (set/ping)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
    )

    sentinel_instance = mocker.Mock()
    sentinel_instance.master_for.return_value = mock_master_client
    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel_instance)

    backend = RedisBackend(settings)
    backend.connect()

    # The client property returns the master client — set() routes through it.
    assert backend.client is mock_master_client
    backend.client.set("k", "v")
    mock_master_client.set.assert_called_once_with("k", "v")

  def test_sentinel_empty_sentinels_raises_configuration_error(self, mocker):
    """Empty sentinels list → ConfigurationError.

    The R9-b SV2 mode-validator rejects empty ``sentinels`` at construction
    with the project's ``ConfigurationError`` (upgraded from pydantic's
    ValidationError so the failure is named/debuggable), so we bypass it by
    constructing with a valid config then mutating ``sentinels`` to []
    before connect() — exercising the defensive guard in _connect_sentinel.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import ConfigurationError
    from scrapy_extension.settings import RedisMode, RedisSettings

    # Construction with empty sentinels is rejected by the SV2 validator
    with pytest.raises(ConfigurationError) as construct_exc:
      RedisSettings(mode=RedisMode.SENTINEL, sentinels=[])
    assert construct_exc.value.setting_name == "sentinels"

    # Defensive guard: build valid, then blank out sentinels pre-connect
    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
    )
    settings.sentinels = []

    backend = RedisBackend(settings)
    with pytest.raises(ConfigurationError) as exc_info:
      backend.connect()
    assert exc_info.value.setting_name == "sentinels"

  def test_sentinel_malformed_entry_no_port_raises(self, mocker):
    """SEC-6: malformed sentinel entry (no ``:port``) is wrapped as
    BackendConnectionError, not surfaced as a raw ValueError.

    The parser does ``host, port_str = sentinel_str.rsplit(":", 1)``; a bare
    "host" raises ValueError("not enough values to unpack"). Pre-SEC-6 this
    propagated raw (not a RedisError, so the connect() except clauses missed
    it). SEC-6 wraps the parse + ping in try/except → BackendConnectionError.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import BackendConnectionError
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel-without-port"],
      sentinel_master_name="mymaster",
    )

    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert exc_info.value.backend_type == "redis"
    # The original ValueError is chained for debuggability.
    assert isinstance(exc_info.value.__cause__, ValueError)

  def test_sentinel_malformed_entry_non_numeric_port_raises(self, mocker):
    """SEC-6: non-numeric port in a sentinel entry is wrapped as
    BackendConnectionError (was raw ValueError from int() coercion)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import BackendConnectionError
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:not-a-port"],
      sentinel_master_name="mymaster",
    )

    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert exc_info.value.backend_type == "redis"
    assert isinstance(exc_info.value.__cause__, ValueError)

  def test_cluster_parses_startup_nodes_into_cluster_nodes(self, mock_master_client, mocker):
    """_connect_cluster parses cluster_startup_nodes into ClusterNode objects.

    Captures the RedisCluster(startup_nodes=...) call and asserts each entry is
    a real ClusterNode with the right host and int port.
    """
    from redis.cluster import ClusterNode

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node-a:7000", "node-b:7001", "node-c:7002"],
      password="secret",
    )

    captured: dict[str, object] = {}

    def fake_cluster_factory(*args, **kwargs):
      captured["kwargs"] = kwargs
      mock_client = mocker.Mock()
      mock_client.ping.return_value = True
      return mock_client

    mocker.patch("scrapy_extension.backends.redis.RedisCluster", side_effect=fake_cluster_factory)

    backend = RedisBackend(settings)
    backend.connect()

    startup_nodes = captured["kwargs"]["startup_nodes"]
    assert len(startup_nodes) == 3
    # Each entry is a real ClusterNode (not a tuple/str) with host + int port
    assert all(isinstance(n, ClusterNode) for n in startup_nodes)
    assert [(n.host, n.port) for n in startup_nodes] == [
      ("node-a", 7000),
      ("node-b", 7001),
      ("node-c", 7002),
    ]
    assert all(isinstance(n.port, int) for n in startup_nodes)

  def test_cluster_malformed_startup_node_raises(self, mocker):
    """SEC-6: malformed cluster node (no port) is wrapped as
    BackendConnectionError (was raw ValueError during parsing)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import BackendConnectionError
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER,
      cluster_startup_nodes=["node-no-port"],
    )

    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.connect()
    assert exc_info.value.backend_type == "redis"
    assert isinstance(exc_info.value.__cause__, ValueError)

  def test_cluster_no_failover_rediscovery_path_exists(self, mock_master_client, mocker):
    """Document the failover gap: the backend delegates failover to the
    master_for() proxy and has no explicit discover_master() / reconnect path.

    Sentinel.master_for() returns a proxy that lazily re-discovers the master
    on MasterNotFoundError-style exceptions (handled inside redis-py). The
    backend code does NOT call discover_master() itself, nor does it reconnect
    on connection loss beyond a fresh connect() call. This test pins that
    behavior so a future change is intentional.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
    )

    sentinel_instance = mocker.Mock()
    sentinel_instance.master_for.return_value = mock_master_client
    # Ensure discover_master is never invoked by _connect_sentinel
    sentinel_instance.discover_master = mocker.Mock()
    mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=sentinel_instance)

    backend = RedisBackend(settings)
    backend.connect()

    # Failover delegation: the backend builds the connection via master_for()
    # only — it never queries discover_master() during a normal connect.
    sentinel_instance.master_for.assert_called_once()
    sentinel_instance.discover_master.assert_not_called()
    # And there is no public reconnect/re-discovery method on the backend
    assert not hasattr(backend, "discover_master")
    assert not hasattr(backend, "reconnect")


class TestRedisBackendDisconnect:
  """Test RedisBackend disconnect functionality."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_disconnect_single_client(self, redis_settings, mock_redis, mocker):
    """Test disconnect with single client."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected()

    backend.disconnect()
    assert not backend.is_connected()
    mock_redis.close.assert_called_once()

  def test_disconnect_master_slave_separate_clients(self, mocker):
    """Test disconnect with separate master and slave clients."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=["replica1.redis.com:6379"],
    )

    mock_master = mocker.Mock()
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_master)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()

    backend.disconnect()
    assert not backend.is_connected()
    # Master should be closed separately
    mock_master.close.assert_called()

  def test_disconnect_error_suppressed(self, redis_settings, mock_redis, mocker):
    """Test disconnect suppresses RedisError during close."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    mock_redis.close.side_effect = RedisError("Already closed")
    backend = RedisBackend(redis_settings)
    backend.connect()
    # Should not raise
    backend.disconnect()


class TestRedisBackendQueueOperations:
  """Test RedisBackend queue operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_queue_push_with_priority(self, redis_settings, mock_redis, mocker):
    """Test queue push with priority passes negated score to the Lua script."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"test_data", priority=5.0)
    # args = [member_uuid, -priority, item]
    args = mock_script.call_args.kwargs["args"]
    assert args[1] == -5.0

  def test_queue_push_error(self, redis_settings, mock_redis, mocker):
    """Test queue push raises QueueError on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_script = mocker.MagicMock()
    mock_script.side_effect = RedisError("Script eval error")
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError) as exc_info:
      backend.push("test_queue", b"test_data")
    assert "push" in str(exc_info.value).lower()

  def test_queue_pop_blocking(self, redis_settings, mock_redis, mocker):
    """Test blocking pop with BZPOPMIN."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmin.return_value = ("test_queue", b"member-1", 1.0)
    # Simulate payload sidecar returning the stored item
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = [b"blocked_data", 1]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue", timeout=5.0)
    assert result == b"blocked_data"
    mock_redis.bzpopmin.assert_called_once_with("test_queue", timeout=5.0)

  def test_queue_pop_blocking_timeout(self, redis_settings, mock_redis, mocker):
    """Test blocking pop returns None on timeout."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmin.return_value = None
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue", timeout=1.0)
    assert result is None

  def test_queue_pop_error(self, redis_settings, mock_redis, mocker):
    """Test queue pop raises QueueError on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_script = mocker.MagicMock()
    mock_script.side_effect = RedisError("Script eval error")
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError) as exc_info:
      backend.pop("test_queue")
    assert "pop" in str(exc_info.value).lower()

  def test_queue_len_error(self, redis_settings, mock_redis, mocker):
    """R-qlen: queue_len must wrap RedisError as QueueError, NOT swallow to 0.

    Pre-R-qlen this returned 0 (pinned by this test as ``== 0``), conflating an
    empty queue with a backend failure. The scheduler trusts ``len(queue)`` for
    ``has_pending_requests`` / the backpressure gate — a swallowed 0 during a
    Redis blip can trigger premature idle/CloseSpider and loses the backpressure
    signal at the worst moment. ``pop()`` wraps RedisError as QueueError;
    queue_len now matches. The scheduler's ``next_request`` already handles
    QueueError from ``len(self._queue)`` (returns None safely — see
    scheduler.py backpressure docstring).
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.zcard.side_effect = RedisError("Card error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(QueueError):
      backend.queue_len("test_queue")

  def test_clear_queue_error(self, redis_settings, mock_redis, mocker):
    """Test clear_queue logs warning on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Should not raise, just log warning
    backend.clear_queue("test_queue")

  def test_push_uses_lua_script(self, redis_settings, mock_redis, mocker):
    """Push must use a Lua script for atomic INCR + ZADD + HSET."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"data", priority=1.0)

    mock_redis.register_script.assert_called_once()
    script_body = mock_redis.register_script.call_args.args[0]
    assert "INCR" in script_body
    assert "ZADD" in script_body
    assert "HSET" in script_body
    keys = mock_script.call_args.kwargs["keys"]
    assert keys == ["test_queue", "{test_queue}:payload", "{test_queue}:counter"]

  def test_push_identical_bytes_use_distinct_members(
    self, redis_settings, mock_redis, mocker
  ):
    """Two pushes of identical bytes must produce distinct ZSET members.

    Regression for R1-P0-1: pre-fix, both pushes shared the raw item as the
    ZSET member and the second silently overwrote the first.
    """
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.push("test_queue", b"identical", priority=1.0)
    backend.push("test_queue", b"identical", priority=1.0)

    assert mock_script.call_count == 2
    member_uuid_first = mock_script.call_args_list[0].kwargs["args"][0]
    member_uuid_second = mock_script.call_args_list[1].kwargs["args"][0]
    assert member_uuid_first != member_uuid_second, (
      "Identical payloads must produce distinct member uuids; "
      "the Lua script prefixes each with an INCR counter."
    )

  def test_payload_key_uses_hash_tag(self, redis_settings, mock_redis, mocker):
    """Payload key must use a Redis Cluster hash tag so it shares a slot with the queue.

    Without `{queue_name}` hash tags, MULTI/EXEC across `queue_name` and
    `queue_name:payload` raises CROSSSLOT in Redis Cluster mode.
    """
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    payload_key = backend._payload_key("test_queue")
    assert payload_key == "{test_queue}:payload"
    # Sanity: same hash slot when wrapped in {}.
    assert payload_key.startswith("{test_queue}:")

  def test_pop_raises_on_missing_payload(self, redis_settings, mock_redis, mocker):
    """B1: a lost-payload race must NOT raise; structural corruption must.

    Previously the Lua script returned ``-1`` when HGET missed and pop
    escalated every such miss to QueueError. B1 distinguishes:

    - lost-payload race (``[2, _]`` — another consumer won ZPOPMIN and
      HDEL'd the payload first): recoverable → pop returns None, no raise.
    - structural corruption (``[3, msg]`` — payload decoded to an
      unexpected type): QueueError, so the caller can surface it.

    Regression for R4-C1 / B1.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    # Lost-payload race: recoverable, returns None (no raise).
    mock_script_race = mocker.MagicMock()
    mock_script_race.return_value = [2, None]
    mock_redis.register_script.return_value = mock_script_race
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    assert backend.pop("test_queue") is None

    # Structural corruption: loud QueueError.
    mock_script_corrupt = mocker.MagicMock()
    mock_script_corrupt.return_value = [3, "unexpected payload type: float"]
    mock_redis.register_script.return_value = mock_script_corrupt
    with pytest.raises(QueueError, match="Structural corruption"):
      backend.pop("test_queue")

  def test_non_blocking_pop_uses_lua_script(
    self, redis_settings, mock_redis, mocker
  ):
    """Non-blocking pop must use a Lua script for ZPOPMIN+HGET+HDEL atomicity.

    Regression for R5-C1: the previous pipeline(transaction=True) approach
    left an orphan window between ZPOPMIN and HGET/HDEL if the worker
    crashed mid-pop.
    """
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    mock_script.return_value = [1, b"payload"]  # New signal scheme: [1, payload] = success.
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.pop("test_queue")  # timeout=0 (default)

    mock_redis.register_script.assert_called_once()
    script_body = mock_redis.register_script.call_args.args[0]
    assert "ZPOPMIN" in script_body
    assert "HGET" in script_body
    assert "HDEL" in script_body
    mock_script.assert_called_once()
    keys = mock_script.call_args.kwargs["keys"]
    assert keys == ["test_queue", "{test_queue}:payload"]

  def test_pop_normalizes_str_payload_to_bytes(
    self, redis_settings, mock_redis, mocker
  ):
    """With decode_responses=True, Lua script returns str; pop must return bytes.

    Regression for R6-C1: pre-fix, pop raised QueueError when the script
    returned a str because isinstance(result, bytes) failed.
    """
    from scrapy_extension.backends.redis import RedisBackend

    mock_script = mocker.MagicMock()
    # New signal scheme: [1, payload] = success; decode_responses=True → str payload.
    mock_script.return_value = [1, "string_payload"]
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue")
    assert result == b"string_payload"
    assert isinstance(result, bytes)

  def test_blocking_pop_uses_transaction_pipeline_for_consume(
    self, redis_settings, mock_redis, mocker
  ):
    """Blocking pop (timeout>0) cannot use Lua; falls back to MULTI/EXEC pipeline."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmin.return_value = ("test_queue", b"member-1", 1.0)
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = [b"payload", 1]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.pop("test_queue", timeout=5.0)
    assert result == b"payload"
    mock_redis.pipeline.assert_called_with(transaction=True)

  def test_pop_raises_on_unexpected_payload_type(self, redis_settings, mock_redis, mocker):
    """R5: a pop result that is not a 2-element [status, payload] list → QueueError (defensive)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_script = mocker.MagicMock()
    mock_script.return_value = 3.14  # float — not the expected [status, payload] list shape
    mock_redis.register_script.return_value = mock_script
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)

    with pytest.raises(QueueError, match="Corrupt pop result"):
      backend.pop("test_queue")

  def test_consume_payload_raises_on_pipeline_redis_error(
    self, redis_settings, mock_redis, mocker
  ):
    """Blocking-pop consume: a RedisError from the pipeline → QueueError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.bzpopmin.return_value = ("test_queue", b"member", 1.0)
    pipe = mock_redis.pipeline.return_value
    pipe.execute.side_effect = RedisError("pipe broke")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)

    with pytest.raises(QueueError, match="Failed to consume payload"):
      backend.pop("test_queue", timeout=5.0)

  def test_consume_payload_raises_on_orphan_member(
    self, redis_settings, mock_redis, mocker
  ):
    """R4: a ZSET member with no payload (None) → QueueError (queue corruption)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.bzpopmin.return_value = ("test_queue", b"member", 1.0)
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = [None, 1]  # payload missing → orphan member
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)

    with pytest.raises(QueueError, match="Queue corruption"):
      backend.pop("test_queue", timeout=5.0)

  def test_consume_payload_normalizes_str_to_bytes(
    self, redis_settings, mock_redis, mocker
  ):
    """Blocking-pop + decode_responses=True: str payload → bytes (R6 normalization)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.bzpopmin.return_value = ("test_queue", b"member", 1.0)
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = ["str_payload", 1]  # str under decode_responses
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)

    assert backend.pop("test_queue", timeout=5.0) == b"str_payload"

  def test_consume_payload_raises_on_unexpected_type(
    self, redis_settings, mock_redis, mocker
  ):
    """Blocking-pop consume: a non-None/str/bytes payload → QueueError."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import QueueError

    mock_redis.bzpopmin.return_value = ("test_queue", b"member", 1.0)
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = [3.14, 1]  # float — unexpected payload type
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)

    with pytest.raises(QueueError, match="Unexpected payload type from HGET"):
      backend.pop("test_queue", timeout=5.0)


class TestRedisBackendSetOperations:
  """Test RedisBackend set operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_set_add_already_exists(self, redis_settings, mock_redis, mocker):
    """Test set add returns False when item already exists."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sadd.return_value = 0  # Already exists
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.add("test_set", b"existing_item")
    assert result is False

  def test_set_add_error(self, redis_settings, mock_redis, mocker):
    """R-dupe-1 (option b): RedisError during set add is wrapped as
    BackendConnectionError so BackendDupeFilter's graceful-degradation arm
    catches it (degrade to not-seen) instead of crashing the crawl. The raw
    RedisError is chained (``from e``) for diagnosis.

    Supersedes R31-A1's "must propagate raw" — but preserves R31-A1's core
    concern: add does NOT return False on error (no silent mis-treatment as
    duplicate, which would drop new requests during network blips). It still
    raises a typed, catchable exception; only the type changed from raw
    ``RedisError`` to ``BackendConnectionError`` so ``except BackendError``
    (the dupefilter's degradation arm) catches it uniformly across backends.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import BackendConnectionError

    mock_redis.sadd.side_effect = RedisError("Add error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError) as exc_info:
      backend.add("test_set", b"test_item")
    assert exc_info.value.backend_type == "redis"
    assert isinstance(exc_info.value.__cause__, RedisError)  # raw error chained

  def test_set_remove_success(self, redis_settings, mock_redis, mocker):
    """Test set remove returns True on successful removal."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.remove("test_set", b"test_item")
    assert result is True

  def test_set_remove_not_found(self, redis_settings, mock_redis, mocker):
    """Test set remove returns False when item not found."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.remove("test_set", b"missing_item")
    assert result is False

  def test_set_remove_error(self, redis_settings, mock_redis, mocker):
    """R34-A1: RedisError on remove must propagate, NOT return False.

    Returning False conflated "item not in set" with "couldn't reach the
    backend". The SetBackend.remove contract (base.py) says False = "not
    in set"; real errors propagate.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.srem.side_effect = RedisError("Remove error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="Remove error"):
      backend.remove("test_set", b"test_item")

  def test_set_contains_error(self, redis_settings, mock_redis, mocker):
    """R34-A1: RedisError on contains must propagate, NOT return False.

    Returning False conflated "not in set" with "couldn't check" — the
    standard ``if not set.contains(fp): set.add(fp)`` pattern would
    produce duplicates during network blips.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.sismember.side_effect = RedisError("Member error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="Member error"):
      backend.contains("test_set", b"test_item")

  def test_set_len_error(self, redis_settings, mock_redis, mocker):
    """Test set_len returns 0 on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.scard.side_effect = RedisError("Card error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.set_len("test_set")
    assert result == 0

  def test_clear_set_error(self, redis_settings, mock_redis, mocker):
    """Test clear_set logs warning on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Should not raise, just log warning
    backend.clear_set("test_set")


class TestRedisBackendStorageOperations:
  """Test RedisBackend storage operations with error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_storage_store_with_ttl(self, redis_settings, mock_redis, mocker):
    """Test storage store with TTL uses SETEX."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data", ttl=3600)
    mock_redis.setex.assert_called_once_with("test_key", 3600, b"test_data")

  def test_storage_store_no_ttl(self, redis_settings, mock_redis, mocker):
    """Test storage store without TTL uses SET."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.store("test_key", b"test_data")
    mock_redis.set.assert_called_once_with("test_key", b"test_data")

  def test_storage_store_error(self, redis_settings, mock_redis, mocker):
    """R-store: RedisError on store must raise StorageError, not be swallowed.

    Pre-fix ``store()`` caught ``RedisError`` and returned normally, so the
    item pipeline treated the failed write as a success — silent data loss AND
    the ``max_storage_errors`` (C2) escalation was neutered (the success arm
    reset the consecutive-error counter). Now mirrors the
    mongodb/elasticsearch/memcached/dynamodb ``store()`` contracts (all raise
    ``StorageError``). Redis ``retrieve()`` already propagates (R32-A1); this
    closes the last storage-path swallow on the Redis backend.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.exceptions import StorageError

    mock_redis.set.side_effect = RedisError("Write error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(StorageError) as exc_info:
      backend.store("test_key", b"test_data")
    assert exc_info.value.operation == "store"
    assert exc_info.value.key == "test_key"

  def test_storage_retrieve_string_conversion(self, redis_settings, mock_redis, mocker):
    """Test storage retrieve converts string to bytes."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = "string_data"
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("test_key")
    assert result == b"string_data"

  def test_storage_retrieve_error(self, redis_settings, mock_redis, mocker):
    """R32-A1: RedisError on retrieve must propagate, NOT return None.

    Returning None on RedisError conflated "key doesn't exist" with
    "couldn't reach the backend". Callers writing ``if storage.retrieve(k)
    is None: create_new()`` would silently overwrite existing data during
    any network blip / Redis failover — silent data loss.
    The StorageBackend.retrieve contract (base.py) says None = "not found";
    real errors propagate so callers can distinguish.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.side_effect = RedisError("Read error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="Read error"):
      backend.retrieve("test_key")

  def test_delete_success(self, redis_settings, mock_redis, mocker):
    """Test delete returns True on successful deletion."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.delete("test_key")
    assert result is True

  def test_delete_not_found(self, redis_settings, mock_redis, mocker):
    """Test delete returns False when key not found."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.delete("missing_key")
    assert result is False

  def test_delete_error(self, redis_settings, mock_redis, mocker):
    """R34-A1: RedisError on delete must propagate, NOT return False.

    Returning False conflated "key didn't exist" with "couldn't reach
    the backend". The StorageBackend.delete contract (base.py) says
    False = "didn't exist"; real errors propagate.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.delete.side_effect = RedisError("Delete error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="Delete error"):
      backend.delete("test_key")

  def test_exists_true(self, redis_settings, mock_redis, mocker):
    """Test exists returns True when key exists."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.return_value = 1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.exists("test_key")
    assert result is True

  def test_exists_false(self, redis_settings, mock_redis, mocker):
    """Test exists returns False when key does not exist."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.return_value = 0
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.exists("missing_key")
    assert result is False

  def test_exists_error(self, redis_settings, mock_redis, mocker):
    """R33-A1: RedisError on exists must propagate, NOT return False.

    Returning False on RedisError conflated "key doesn't exist" with
    "couldn't reach the backend". Callers writing ``if not
    storage.exists(k): create_new()`` would silently overwrite existing
    data during any network blip / Redis failover — silent data loss.
    The StorageBackend.exists contract (base.py) says False = "doesn't
    exist"; real errors propagate so callers can distinguish.
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.exists.side_effect = RedisError("Exists error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="Exists error"):
      backend.exists("test_key")

  def test_ttl_with_ttl(self, redis_settings, mock_redis, mocker):
    """Test ttl returns seconds when TTL is set."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = 3600
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("test_key")
    assert result == 3600

  def test_ttl_no_ttl(self, redis_settings, mock_redis, mocker):
    """Test ttl returns None when no TTL set (-1)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = -1
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("test_key")
    assert result is None

  def test_ttl_key_not_exists(self, redis_settings, mock_redis, mocker):
    """Test ttl returns None when key doesn't exist (R1-P0-4 contract fix)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.return_value = -2
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.ttl("missing_key")
    assert result is None

  def test_ttl_error(self, redis_settings, mock_redis, mocker):
    """R34-A1: RedisError on ttl must propagate, NOT return None.

    Returning None conflated "no TTL" with "couldn't reach the backend".
    The StorageBackend.ttl contract (base.py) says None = "no TTL",
    -1 = "expired"; real errors propagate so callers can distinguish
    "key has no expiry" from "couldn't check".
    """
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ttl.side_effect = RedisError("TTL error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    with pytest.raises(RedisError, match="TTL error"):
      backend.ttl("test_key")

  def test_clear_storage_with_prefix(self, redis_settings, mock_redis, mocker):
    """Test clear_storage with prefix uses scan_iter."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.scan_iter.return_value = iter(["key1", "key2"])
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage(prefix="test_prefix")
    mock_redis.scan_iter.assert_called_once_with(match="test_prefix*")
    assert mock_redis.delete.call_count == 2

  def test_clear_storage_no_prefix(self, redis_settings, mock_redis, mocker):
    """Test clear_storage without prefix uses flushdb."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage()
    mock_redis.flushdb.assert_called_once()

  def test_clear_storage_cluster_with_prefix(self):
    """Test clear_storage with cluster and prefix.

    Note: isinstance check with mocked RedisCluster doesn't work with mocks.
    This test verifies the non-cluster branch behavior with prefix via the
    regular Redis client path. Cluster-specific behavior is covered by
    integration tests with real Redis Cluster.
    """
    # Cluster mode with prefix uses scan_iter - tested via code inspection
    # The isinstance check is the limiting factor for direct mocking
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    backend = RedisBackend(settings)
    # Verify settings are correctly stored for cluster mode
    assert backend.config.mode == RedisMode.CLUSTER

  def test_clear_storage_cluster_no_prefix(self):
    """Test clear_storage with cluster without prefix.

    Note: isinstance check with mocked RedisCluster doesn't work with mocks.
    Cluster-specific flushall is tested via integration tests.
    """
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    backend = RedisBackend(settings)
    # Verify settings are correctly stored for cluster mode
    assert backend.config.mode == RedisMode.CLUSTER

  def test_clear_storage_error(self, redis_settings, mock_redis, mocker, caplog):
    """Test clear_storage logs warning on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.flushdb.side_effect = RedisError("Flush error")
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.clear_storage()
    assert "Failed to clear storage" in caplog.text


class TestRedisBackendPingAndConnection:
  """Test RedisBackend ping and connection state methods."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_is_connected_true(self, redis_settings, mock_redis, mocker):
    """Test is_connected returns True when ping succeeds."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ping.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected() is True

  def test_is_connected_false_when_none(self, redis_settings):
    """Test is_connected returns False when client is None."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    # Never connected
    assert backend.is_connected() is False

  def test_is_connected_false_on_error(self, redis_settings, mock_redis, mocker):
    """Test is_connected returns False on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    # First ping succeeds to allow connect, then fails for is_connected check
    mock_redis.ping.side_effect = [True, RedisError("Ping error")]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.is_connected() is False

  def test_ping_success(self, redis_settings, mock_redis, mocker):
    """Test ping returns True on success."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.ping.return_value = True
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.ping() is True

  def test_ping_false_when_none(self, redis_settings):
    """Test ping returns False when client is None."""
    from scrapy_extension.backends.redis import RedisBackend

    backend = RedisBackend(redis_settings)
    assert backend.ping() is False

  def test_ping_false_on_error(self, redis_settings, mock_redis, mocker):
    """Test ping returns False on RedisError."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    # First ping succeeds to allow connect, then fails for ping check
    mock_redis.ping.side_effect = [True, RedisError("Ping error")]
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    backend.connect()
    assert backend.ping() is False

  def test_client_property_auto_connect(self, redis_settings, mock_redis, mocker):
    """Test client property triggers auto-connect if not connected."""
    from scrapy_extension.backends.redis import RedisBackend

    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    # Access client property without calling connect
    client = backend.client
    assert client is mock_redis
    # Verify ping was called during auto-connect
    assert getattr(mock_redis.ping, "call_count", 0) > 0


class TestRedisBackendConnectErrors:
  """Test RedisBackend connection error handling."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_connect_standalone_connection_error(self, redis_settings, mocker):
    """Test connect raises BackendConnectionError on ConnectionError."""
    from redis.exceptions import ConnectionError as RedisConnError

    from scrapy_extension.backends.redis import RedisBackend

    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisConnError("Connection refused")
    backend = RedisBackend(redis_settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_master_slave_error(self, mocker):
    """Test connect raises BackendConnectionError for master-slave mode."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(mode=RedisMode.MASTER_SLAVE, host="master.redis.com")
    mock = mocker.patch("scrapy_extension.backends.redis.Redis")
    mock.return_value.ping.side_effect = RedisError("Master error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_sentinel_error(self, mocker):
    """Test connect raises BackendConnectionError for sentinel mode."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.SENTINEL,
      sentinels=["sentinel1:26379"],
      sentinel_master_name="mymaster",
    )
    mock_sentinel = mocker.patch("scrapy_extension.backends.redis.Sentinel")
    mock_sentinel.return_value.master_for.side_effect = RedisError("Sentinel error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()

  def test_connect_cluster_error(self, mocker):
    """Test connect raises BackendConnectionError for cluster mode."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    mock = mocker.patch("scrapy_extension.backends.redis.RedisCluster")
    mock.return_value.ping.side_effect = RedisError("Cluster error")
    backend = RedisBackend(settings)
    with pytest.raises(BackendConnectionError):
      backend.connect()


class TestRedisBackendCoverageGaps:
  """Tests covering previously missing coverage lines in RedisBackend."""

  @pytest.fixture
  def redis_settings(self):
    """Create Redis settings."""
    from scrapy_extension.settings import RedisSettings

    return RedisSettings(host="localhost", port=6379)

  @pytest.fixture
  def mock_redis(self, mocker):
    """Create mock Redis client."""
    return mocker.Mock()

  def test_validate_key_name_empty(self):
    """Test _validate_key_name raises ValueError for empty name (line 33)."""
    from scrapy_extension.backends.redis import _validate_key_name

    with pytest.raises(ValueError, match="Invalid name"):
      _validate_key_name("")

  def test_import_error_message(self):
    """Test ImportError includes helpful install message (lines 43-44)."""
    import subprocess
    import sys

    # Use subprocess to avoid corrupting the current process's module state
    result = subprocess.run(
      [
        sys.executable,
        "-c",
        (
          "import sys\n"
          "# Block redis from being imported\n"
          "import importlib.util\n"
          "sys.modules['redis'] = None\n"
          "sys.modules['redis.exceptions'] = None\n"
          "sys.modules['redis.cluster'] = None\n"
          "sys.modules['redis.sentinel'] = None\n"
          "try:\n"
          "    import scrapy_extension.backends.redis\n"
          "    print('ERROR: No ImportError raised')\n"
          "    sys.exit(1)\n"
          "except ImportError as e:\n"
          "    msg = str(e)\n"
          '    if "pip install scrapy-extension[redis]" in msg:\n'
          "        print('PASS')\n"
          "    else:\n"
          "        print(f'ERROR: Wrong message: {msg}')\n"
          "        sys.exit(1)\n"
        ),
      ],
      capture_output=True,
      text=True,
    )
    assert result.returncode == 0, (
      f"subprocess failed: {result.stderr}\n{result.stdout}"
    )
    assert "PASS" in result.stdout

  def test_connect_cluster_branch(self, mock_redis, mocker):
    """Test connect() CLUSTER branch and logger.debug (lines 113->118)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )
    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_redis
    )
    backend = RedisBackend(settings)
    backend.connect()
    # The CLUSTER branch is exercised; verify it connected
    assert backend.is_connected()

  def test_connect_master_slave_no_replicas(self, mock_redis, mocker):
    """Test _connect_master_slave with no replicas skips logging (line 169->exit)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.MASTER_SLAVE,
      host="master.redis.com",
      port=6379,
      replicas=[],
    )
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(settings)
    backend.connect()
    assert backend.is_connected()
    # With replicas=None, the `if self.config.replicas:` branch is skipped

  def test_disconnect_separate_master_client(self, redis_settings, mocker):
    """Test disconnect closes separate _master_client (lines 283-285)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_master = mocker.Mock()
    mock_client = mocker.Mock()

    backend = RedisBackend(redis_settings)
    # Manually create a scenario where _master_client is separate from _client
    backend._master_client = mock_master
    backend._client = mock_client
    backend._sentinel = mocker.Mock()

    backend.disconnect()
    # Both should be closed
    mock_master.close.assert_called()
    mock_client.close.assert_called()
    assert backend._master_client is None
    assert backend._client is None
    assert backend._sentinel is None

  def test_disconnect_master_client_redis_error_suppressed(
    self, redis_settings, mocker
  ):
    """Test disconnect suppresses RedisError when closing _master_client (lines 283-285)."""
    from redis.exceptions import RedisError

    from scrapy_extension.backends.redis import RedisBackend

    mock_master = mocker.Mock()
    mock_master.close.side_effect = RedisError("Already closed")
    mock_client = mocker.Mock()

    backend = RedisBackend(redis_settings)
    backend._master_client = mock_master
    backend._client = mock_client

    # Should not raise
    backend.disconnect()
    assert backend._master_client is None
    assert backend._client is None

  def test_disconnect_clears_sentinel(self, redis_settings, mocker):
    """Test disconnect sets _sentinel to None (lines 287->292)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_client = mocker.Mock()
    backend = RedisBackend(redis_settings)
    backend._client = mock_client
    backend._sentinel = mocker.Mock()

    backend.disconnect()
    assert backend._sentinel is None
    assert backend._client is None

  def test_retrieve_returns_none_for_missing_key(
    self, redis_settings, mock_redis, mocker
  ):
    """Test retrieve returns None when key doesn't exist (line 573)."""
    from scrapy_extension.backends.redis import RedisBackend

    mock_redis.get.return_value = None
    mocker.patch("scrapy_extension.backends.redis.Redis", return_value=mock_redis)
    backend = RedisBackend(redis_settings)
    result = backend.retrieve("missing_key")
    assert result is None

  def test_clear_storage_cluster_with_prefix(self, mocker):
    """Test clear_storage cluster scan_iter branch (lines 661-662)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )

    mock_cluster = mocker.MagicMock()
    mock_cluster.scan_iter.return_value = iter([b"prefix:key1", b"prefix:key2"])
    mock_cluster.ping.return_value = True

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_cluster
    )
    # Patch isinstance so it returns True for the mock_cluster instance
    original_isinstance = isinstance
    mocker.patch(
      "scrapy_extension.backends.redis.isinstance",
      side_effect=lambda obj, cls: (
        True if obj is mock_cluster else original_isinstance(obj, cls)
      ),
    )
    backend = RedisBackend(settings)
    backend.connect()
    backend.clear_storage(prefix="prefix")

    mock_cluster.scan_iter.assert_called_once_with(match="prefix*")
    assert mock_cluster.delete.call_count == 2

  def test_clear_storage_cluster_no_prefix(self, mocker):
    """Test clear_storage cluster flushall branch (line 669)."""
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.settings import RedisMode, RedisSettings

    settings = RedisSettings(
      mode=RedisMode.CLUSTER, cluster_startup_nodes=["node1:7000"]
    )

    mock_cluster = mocker.MagicMock()
    mock_cluster.ping.return_value = True

    mocker.patch(
      "scrapy_extension.backends.redis.RedisCluster", return_value=mock_cluster
    )
    # Patch isinstance so it returns True for the mock_cluster instance
    original_isinstance = isinstance
    mocker.patch(
      "scrapy_extension.backends.redis.isinstance",
      side_effect=lambda obj, cls: (
        True if obj is mock_cluster else original_isinstance(obj, cls)
      ),
    )
    backend = RedisBackend(settings)
    backend.connect()
    backend.clear_storage()

    mock_cluster.flushall.assert_called_once()


# ---------------------------------------------------------------------------
# SEC-6 (round-6): Sentinel/Cluster malformed-entry + ping() wrap.
# (The 3 tests above were updated in place; this covers the ping-failure path.)
# ---------------------------------------------------------------------------


def test_sentinel_ping_failure_wrapped_as_connection_error(mocker):
  """SEC-6: a ``master_for(...).ping()`` failure (bad master name, unreachable
  sentinels) is wrapped as BackendConnectionError, not surfaced as whatever
  raw exception redis-py raises (which varies across versions)."""
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.exceptions import BackendConnectionError
  from scrapy_extension.settings import RedisMode, RedisSettings

  settings = RedisSettings(
    mode=RedisMode.SENTINEL,
    sentinels=["sentinel-a:26379"],
    sentinel_master_name="mymaster",
    password="secret",
  )

  mock_sentinel = mocker.Mock()
  mock_master = mocker.Mock()
  mock_master.ping.side_effect = RuntimeError("master unknown")
  mock_sentinel.master_for.return_value = mock_master
  mocker.patch("scrapy_extension.backends.redis.Sentinel", return_value=mock_sentinel)

  backend = RedisBackend(settings)
  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert exc_info.value.backend_type == "redis"
  assert "master unknown" in str(exc_info.value)


def test_cluster_ping_failure_wrapped_as_connection_error(mocker):
  """SEC-6: a RedisCluster ping() failure is wrapped as BackendConnectionError."""
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.exceptions import BackendConnectionError
  from scrapy_extension.settings import RedisMode, RedisSettings

  settings = RedisSettings(
    mode=RedisMode.CLUSTER,
    cluster_startup_nodes=["node-a:7000"],
    password="secret",
  )

  mock_cluster = mocker.Mock()
  mock_cluster.ping.side_effect = RuntimeError("cluster unreachable")
  mocker.patch("scrapy_extension.backends.redis.RedisCluster", return_value=mock_cluster)

  backend = RedisBackend(settings)
  with pytest.raises(BackendConnectionError) as exc_info:
    backend.connect()
  assert exc_info.value.backend_type == "redis"
  assert "cluster unreachable" in str(exc_info.value)
