"""Scrapy extension for distributed crawling with multiple backend support.

This package provides distributed crawling capabilities for Scrapy with support
for multiple backends: Redis, MongoDB, Kafka, RabbitMQ, ElasticSearch, RocketMQ, Pulsar, SQS, Memcached, and DynamoDB.
"""

from __future__ import annotations

import importlib
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING

from scrapy_extension.backends._optional import _is_missing_optional_dependency
from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    JSONSerializer,
    QueueBackend,
    Serializer,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.connectors import (
    ConnectionManager,
    resolve_backend_config,
)
from scrapy_extension.dupefilter.dupefilter import BackendDupeFilter
from scrapy_extension.dupefilter.filters.base import FilterFull, MembershipFilter
from scrapy_extension.dupefilter.filters.bloom_filter import BloomMembershipFilter
from scrapy_extension.dupefilter.filters.cuckoo_filter import CuckooMembershipFilter
from scrapy_extension.dupefilter.filters.factory import (
    DedupeStrategy,
    build_membership_filter,
)
from scrapy_extension.dupefilter.filters.memory_filter import MemoryMembershipFilter
from scrapy_extension.dupefilter.filters.set_filter import SetMembershipFilter
from scrapy_extension.exceptions import (
    BackendConnectionError,
    BackendError,
    ConfigurationError,
    QueueError,
    SerializationError,
    StorageError,
)
from scrapy_extension.monitor import Monitor, NullMonitor, ScrapyStatsMonitor
from scrapy_extension.pipeline.pipeline import BackendPipeline
from scrapy_extension.queue.queue import BackendQueue
from scrapy_extension.schedule.scheduler import BackendScheduler
from scrapy_extension.settings.base import Settings
from scrapy_extension.spider.spider_mixin import BackendSpiderMixin

if TYPE_CHECKING:
    from scrapy_extension.backends.dynamodb import DynamoDBBackend
    from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
    from scrapy_extension.backends.kafka import KafkaBackend
    from scrapy_extension.backends.memcached import MemcachedBackend
    from scrapy_extension.backends.mongodb import MongoDBBackend
    from scrapy_extension.backends.pulsar import PulsarBackend
    from scrapy_extension.backends.rabbitmq import RabbitMQBackend
    from scrapy_extension.backends.redis import RedisBackend
    from scrapy_extension.backends.rocketmq import RocketMQBackend
    from scrapy_extension.backends.sqs import SqsBackend
    from scrapy_extension.settings.dynamodb import DynamoDBMode, DynamoDBSettings
    from scrapy_extension.settings.elasticsearch import (
        ElasticSearchMode,
        ElasticSearchSettings,
    )
    from scrapy_extension.settings.kafka import KafkaMode, KafkaSettings
    from scrapy_extension.settings.memcached import MemcachedMode, MemcachedSettings
    from scrapy_extension.settings.mongodb import MongoDBMode, MongoDBSettings
    from scrapy_extension.settings.pulsar import PulsarMode, PulsarSettings
    from scrapy_extension.settings.rabbitmq import RabbitMQMode, RabbitMQSettings
    from scrapy_extension.settings.redis import RedisMode, RedisSettings
    from scrapy_extension.settings.rocketmq import RocketMQMode, RocketMQSettings
    from scrapy_extension.settings.sqs import SqsMode, SqsSettings

try:
    __version__ = _pkg_version("scrapy-extension")
except PackageNotFoundError:
    __version__ = "0.0.0"


# Optional imports: (module_path, attribute_name)
# These are lazily loaded via __getattr__ so that backend-specific dependencies
# (e.g. redis, pymongo, kafka-python, pika, elasticsearch, rocketmq) are only
# required when the corresponding class is actually used.
_OPTIONAL_IMPORTS: dict[str, tuple[str, str]] = {
    # Backend classes
    "DynamoDBBackend": ("scrapy_extension.backends.dynamodb", "DynamoDBBackend"),
    "ElasticSearchBackend": ("scrapy_extension.backends.elasticsearch", "ElasticSearchBackend"),
    "KafkaBackend": ("scrapy_extension.backends.kafka", "KafkaBackend"),
    "MemcachedBackend": ("scrapy_extension.backends.memcached", "MemcachedBackend"),
    "MongoDBBackend": ("scrapy_extension.backends.mongodb", "MongoDBBackend"),
    "PulsarBackend": ("scrapy_extension.backends.pulsar", "PulsarBackend"),
    "RabbitMQBackend": ("scrapy_extension.backends.rabbitmq", "RabbitMQBackend"),
    "RedisBackend": ("scrapy_extension.backends.redis", "RedisBackend"),
    "RocketMQBackend": ("scrapy_extension.backends.rocketmq", "RocketMQBackend"),
    "SqsBackend": ("scrapy_extension.backends.sqs", "SqsBackend"),
    # Settings classes
    "DynamoDBMode": ("scrapy_extension.settings.dynamodb", "DynamoDBMode"),
    "DynamoDBSettings": ("scrapy_extension.settings.dynamodb", "DynamoDBSettings"),
    "ElasticSearchMode": ("scrapy_extension.settings.elasticsearch", "ElasticSearchMode"),
    "ElasticSearchSettings": ("scrapy_extension.settings.elasticsearch", "ElasticSearchSettings"),
    "KafkaMode": ("scrapy_extension.settings.kafka", "KafkaMode"),
    "KafkaSettings": ("scrapy_extension.settings.kafka", "KafkaSettings"),
    "MemcachedMode": ("scrapy_extension.settings.memcached", "MemcachedMode"),
    "MemcachedSettings": ("scrapy_extension.settings.memcached", "MemcachedSettings"),
    "MongoDBMode": ("scrapy_extension.settings.mongodb", "MongoDBMode"),
    "MongoDBSettings": ("scrapy_extension.settings.mongodb", "MongoDBSettings"),
    "PulsarMode": ("scrapy_extension.settings.pulsar", "PulsarMode"),
    "PulsarSettings": ("scrapy_extension.settings.pulsar", "PulsarSettings"),
    "RabbitMQMode": ("scrapy_extension.settings.rabbitmq", "RabbitMQMode"),
    "RabbitMQSettings": ("scrapy_extension.settings.rabbitmq", "RabbitMQSettings"),
    "RedisMode": ("scrapy_extension.settings.redis", "RedisMode"),
    "RedisSettings": ("scrapy_extension.settings.redis", "RedisSettings"),
    "RocketMQMode": ("scrapy_extension.settings.rocketmq", "RocketMQMode"),
    "RocketMQSettings": ("scrapy_extension.settings.rocketmq", "RocketMQSettings"),
    "SqsMode": ("scrapy_extension.settings.sqs", "SqsMode"),
    "SqsSettings": ("scrapy_extension.settings.sqs", "SqsSettings"),
}

# Extra name -> pip extras mapping for helpful error messages
_BACKEND_EXTRAS: dict[str, str] = {
    "DynamoDBBackend": "dynamodb",
    "DynamoDBMode": "dynamodb",
    "DynamoDBSettings": "dynamodb",
    "ElasticSearchBackend": "elasticsearch",
    "ElasticSearchMode": "elasticsearch",
    "ElasticSearchSettings": "elasticsearch",
    "KafkaBackend": "kafka",
    "KafkaMode": "kafka",
    "KafkaSettings": "kafka",
    "MemcachedBackend": "memcached",
    "MemcachedMode": "memcached",
    "MemcachedSettings": "memcached",
    "MongoDBBackend": "mongodb",
    "MongoDBMode": "mongodb",
    "MongoDBSettings": "mongodb",
    "PulsarBackend": "pulsar",
    "PulsarMode": "pulsar",
    "PulsarSettings": "pulsar",
    "RabbitMQBackend": "rabbitmq",
    "RabbitMQMode": "rabbitmq",
    "RabbitMQSettings": "rabbitmq",
    "RedisBackend": "redis",
    "RedisMode": "redis",
    "RedisSettings": "redis",
    "RocketMQBackend": "rocketmq",
    "RocketMQMode": "rocketmq",
    "RocketMQSettings": "rocketmq",
    "SqsBackend": "sqs",
    "SqsMode": "sqs",
    "SqsSettings": "sqs",
}

# Backend module path -> the set of top-level optional-dep module names that
# backend declares at module level. Used by __getattr__ to decide whether an
# ImportError is "genuine missing optional dep" (→ wrap as the install hint)
# vs. a real bug inside the backend module (→ re-raise the original so the
# user sees the real traceback). R14-H.
#
# Settings modules pull no optional dep at module level, so only backend
# module paths are listed; a settings-class path that fails for a non-dep
# reason correctly falls through to "re-raise original".
_OPTIONAL_DEP_MODULES: dict[str, frozenset[str]] = {
    "scrapy_extension.backends.dynamodb": frozenset({"boto3"}),
    "scrapy_extension.backends.elasticsearch": frozenset({"elasticsearch"}),
    "scrapy_extension.backends.kafka": frozenset({"kafka"}),
    "scrapy_extension.backends.memcached": frozenset({"pymemcache"}),
    "scrapy_extension.backends.mongodb": frozenset({"pymongo"}),
    "scrapy_extension.backends.pulsar": frozenset({"pulsar"}),
    "scrapy_extension.backends.rabbitmq": frozenset({"pika"}),
    "scrapy_extension.backends.redis": frozenset({"redis"}),
    # RocketMQ imports its dep inside connect(), not at module level, so a
    # module-level ImportError there is never a "missing dep" signal.
    "scrapy_extension.backends.rocketmq": frozenset(),
    "scrapy_extension.backends.sqs": frozenset({"boto3"}),
}


def _is_missing_optional_dep(exc: ImportError, module_path: str) -> bool:
    """Decide whether an ``ImportError`` from ``module_path`` is a genuine
    missing-optional-dep signal (→ wrap as the install hint) or a real bug
    inside the backend module (→ re-raise to surface the real chain).

    R14-H. Rule:

    * The error must be a ``ModuleNotFoundError`` (the CPython subclass set
      when the failure is "no module named X").
    * Its ``name`` attribute (the missing top-level module) must be one of
      the documented optional-dep modules for this backend (looked up in
      ``_OPTIONAL_DEP_MODULES``); the import may also be a submodule
      ``"<dep>.foo"`` of one of those names.

    Any other ``ImportError`` — including a non-ModuleNotFoundError raised
    mid-import by the backend module's own code, or a ModuleNotFoundError
    whose ``name`` is *not* the backend's optional dep (e.g. the backend
    imports some third-party helper that itself went missing) — is treated
    as a real bug and surfaced as-is.
    """
    dep_modules = _OPTIONAL_DEP_MODULES.get(module_path, frozenset())
    return any(
        _is_missing_optional_dependency(exc, dependency)
        for dependency in dep_modules
    )


def __getattr__(name: str) -> object:
    """Lazily import optional backend classes and settings (PEP 562).

    Core classes (Backend, ConnectionManager, Settings, etc.) are always
    available. Backend-specific classes require their respective optional
    dependencies and are imported on first access.
    """
    if name in _OPTIONAL_IMPORTS:
        module_path, attr_name = _OPTIONAL_IMPORTS[name]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, attr_name)
        except ImportError as e:
            # R14-H: only re-wrap as the install hint when this is a *genuine*
            # missing optional dep. A bare ``except ImportError`` would mask a
            # real bug inside the backend module (whose dep IS installed) as
            # "install scrapy-extension[X]", hiding the actual traceback.
            # ``ModuleNotFoundError.name`` is set by CPython precisely when the
            # failure is "no module named X"; we check it against this
            # backend's documented optional-dep module set.
            if _is_missing_optional_dep(e, module_path):
                extra = _BACKEND_EXTRAS.get(name, name)
                raise ImportError(
                    f"{name} requires additional dependencies. "
                    f"Install with: pip install scrapy-extension[{extra}]"
                ) from e
            # Real bug (or a non-dep ImportError) — surface the original chain.
            raise
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """PEP 562 companion — expose lazily-imported names to dir() and autocomplete.

    Without this, ``dir(scrapy_extension)`` / ``pydoc`` / IDE autocomplete see
    only eagerly-imported names; the 30 lazily-imported ``__all__`` members
    (backends, Mode enums, Settings classes) are invisible despite importing
    successfully on access. Returns eager globals union the lazy
    ``_OPTIONAL_IMPORTS`` keys — no optional dep is imported (dict keys only).
    """
    return sorted(set(globals()) | set(_OPTIONAL_IMPORTS))


__all__ = [
    # Backends
    "Backend",
    "BackendConnectionError",
    "BackendDupeFilter",
    # Exceptions
    "BackendError",
    "BackendPipeline",
    # Components
    "BackendQueue",
    "BackendScheduler",
    # Spider
    "BackendSpiderMixin",
    "BackendType",
    # Dedup strategy (subsystem ①)
    "BloomMembershipFilter",
    "ConfigurationError",
    # Connection
    "ConnectionManager",
    "CuckooMembershipFilter",
    "DedupeStrategy",
    "DynamoDBBackend",
    "DynamoDBMode",
    "DynamoDBSettings",
    "ElasticSearchBackend",
    "ElasticSearchMode",
    "ElasticSearchSettings",
    "FilterFull",
    "JSONSerializer",
    "KafkaBackend",
    "KafkaMode",
    "KafkaSettings",
    "MembershipFilter",
    "MemcachedBackend",
    "MemcachedMode",
    "MemcachedSettings",
    "MemoryMembershipFilter",
    "MongoDBBackend",
    "MongoDBMode",
    "MongoDBSettings",
    "Monitor",
    "NullMonitor",
    "PulsarBackend",
    "PulsarMode",
    "PulsarSettings",
    "QueueBackend",
    "QueueError",
    "RabbitMQBackend",
    "RabbitMQMode",
    "RabbitMQSettings",
    "RedisBackend",
    "RedisMode",
    "RedisSettings",
    "RocketMQBackend",
    "RocketMQMode",
    "RocketMQSettings",
    "ScrapyStatsMonitor",
    "SerializationError",
    # Serialization
    "Serializer",
    "SetBackend",
    "SetMembershipFilter",
    # Configuration
    "Settings",
    "SqsBackend",
    "SqsMode",
    "SqsSettings",
    "StorageBackend",
    "StorageError",
    "build_membership_filter",
    "resolve_backend_config",
]
