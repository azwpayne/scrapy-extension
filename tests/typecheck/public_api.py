from typing import Literal

from twisted.internet.defer import Deferred
from typing_extensions import assert_type

from scrapy_extension import (
    BackendDupeFilter,
    BackendSpiderMixin,
    KafkaTopicNameGeneration,
    RedisBackend,
    RedisMode,
    RedisSettings,
    StorageError,
)
from scrapy_extension.backends import RedisBackend as BackendsRedisBackend
from scrapy_extension.backends.redis import RedisBackend as ConcreteRedisBackend
from scrapy_extension.dupefilter.filters.memory_filter import (
    MemoryMembershipFilter as ConcreteMemoryMembershipFilter,
)
from scrapy_extension.exceptions import StorageError as ConcreteStorageError
from scrapy_extension.settings.kafka import (
    KafkaTopicNameGeneration as ConcreteKafkaTopicNameGeneration,
)
from scrapy_extension.settings.redis import RedisMode as ConcreteRedisMode
from scrapy_extension.settings.redis import RedisSettings as ConcreteRedisSettings

assert_type(KafkaTopicNameGeneration, type[ConcreteKafkaTopicNameGeneration])
assert_type(RedisBackend, type[ConcreteRedisBackend])
assert_type(RedisSettings, type[ConcreteRedisSettings])
assert_type(RedisMode, type[ConcreteRedisMode])
assert_type(StorageError, type[ConcreteStorageError])
assert_type(RedisBackend(RedisSettings()), ConcreteRedisBackend)
assert_type(BackendsRedisBackend(RedisSettings()), ConcreteRedisBackend)
assert_type(RedisMode.CLUSTER, Literal[ConcreteRedisMode.CLUSTER])

# Async dupefilter lifecycle adapters expose precise Deferred results so
# downstream strict code keeps addCallback/addErrback signature checking.
_dupefilter = BackendDupeFilter(
    connection_manager=None,
    membership_filter=ConcreteMemoryMembershipFilter(),
)
assert_type(_dupefilter.open_async(), Deferred[None])
assert_type(_dupefilter.clear_async(), Deferred[None])
assert_type(_dupefilter.release_async(object(), "closed"), Deferred[None])


class PluginSpider(BackendSpiderMixin):
    name = "plugin-spider"
    backend_type = "mybackend"
