from typing import Literal

from typing_extensions import assert_type

from scrapy_extension import RedisBackend, RedisMode, RedisSettings, StorageError
from scrapy_extension.backends import RedisBackend as BackendsRedisBackend
from scrapy_extension.backends.redis import RedisBackend as ConcreteRedisBackend
from scrapy_extension.exceptions import StorageError as ConcreteStorageError
from scrapy_extension.settings.redis import RedisMode as ConcreteRedisMode
from scrapy_extension.settings.redis import RedisSettings as ConcreteRedisSettings

assert_type(RedisBackend, type[ConcreteRedisBackend])
assert_type(RedisSettings, type[ConcreteRedisSettings])
assert_type(RedisMode, type[ConcreteRedisMode])
assert_type(StorageError, type[ConcreteStorageError])
assert_type(RedisBackend(RedisSettings()), ConcreteRedisBackend)
assert_type(BackendsRedisBackend(RedisSettings()), ConcreteRedisBackend)
assert_type(RedisMode.CLUSTER, Literal[ConcreteRedisMode.CLUSTER])
