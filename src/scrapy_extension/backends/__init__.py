"""Backend implementations for scrapy-extension.

This module provides abstract base classes and concrete implementations
for different backend types (Redis, MongoDB, Kafka, RabbitMQ).

Concrete backend classes are lazily loaded via PEP 562 __getattr__ to avoid
importing optional dependencies at module level.
"""

import importlib

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    JSONSerializer,
    QueueBackend,
    Serializer,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.backends.connectors import ConnectionManager

_BACKEND_MODULES = {
    "RedisBackend": ("scrapy_extension.backends.redis", "RedisBackend"),
    "MongoDBBackend": ("scrapy_extension.backends.mongodb", "MongoDBBackend"),
    "KafkaBackend": ("scrapy_extension.backends.kafka", "KafkaBackend"),
    "RabbitMQBackend": ("scrapy_extension.backends.rabbitmq", "RabbitMQBackend"),
    "ElasticSearchBackend": (
        "scrapy_extension.backends.elasticsearch",
        "ElasticSearchBackend",
    ),
    "RocketMQBackend": ("scrapy_extension.backends.rocketmq", "RocketMQBackend"),
    "PulsarBackend": ("scrapy_extension.backends.pulsar", "PulsarBackend"),
    "MemcachedBackend": ("scrapy_extension.backends.memcached", "MemcachedBackend"),
    "SqsBackend": ("scrapy_extension.backends.sqs", "SqsBackend"),
    "DynamoDBBackend": ("scrapy_extension.backends.dynamodb", "DynamoDBBackend"),
}

# Backend module path -> the top-level optional-dep module name that backend
# declares at module level. Used by __getattr__ to decide whether an
# ImportError is "genuine missing optional dep" (→ wrap as the install hint)
# vs. a real bug inside the backend module (→ re-raise the original).
# R14-H. RocketMQ's dep is imported inside connect(), not at module level, so
# a module-level ImportError there is never a "missing dep" signal.
_BACKEND_DEP_MODULES: dict[str, frozenset[str]] = {
    "scrapy_extension.backends.redis": frozenset({"redis"}),
    "scrapy_extension.backends.mongodb": frozenset({"pymongo"}),
    "scrapy_extension.backends.kafka": frozenset({"kafka"}),
    "scrapy_extension.backends.rabbitmq": frozenset({"pika"}),
    "scrapy_extension.backends.elasticsearch": frozenset({"elasticsearch"}),
    "scrapy_extension.backends.rocketmq": frozenset(),
    "scrapy_extension.backends.pulsar": frozenset({"pulsar"}),
    "scrapy_extension.backends.memcached": frozenset({"pymemcache"}),
    "scrapy_extension.backends.sqs": frozenset({"boto3"}),
    "scrapy_extension.backends.dynamodb": frozenset({"boto3"}),
}

# Backend class name (e.g. ``RedisBackend``) -> pip extras name. Used to build
# the install hint when a genuine missing-dep ImportError is caught. The extras
# name does NOT always equal the dep module name (e.g. MongoDBBackend →
# ``[mongodb]`` extra but ``pymongo`` dep module), so it's listed separately.
_BACKEND_EXTRAS: dict[str, str] = {
    "RedisBackend": "redis",
    "MongoDBBackend": "mongodb",
    "KafkaBackend": "kafka",
    "RabbitMQBackend": "rabbitmq",
    "ElasticSearchBackend": "elasticsearch",
    "RocketMQBackend": "rocketmq",
    "PulsarBackend": "pulsar",
    "MemcachedBackend": "memcached",
    "SqsBackend": "sqs",
    "DynamoDBBackend": "dynamodb",
}


def _is_missing_optional_dep(exc: ImportError, module_path: str) -> bool:
    """Decide whether an ``ImportError`` from ``module_path`` is a genuine
    missing-optional-dep signal (→ wrap as the install hint) or a real bug
    inside the backend module (→ re-raise to surface the real chain).

    R14-H. See ``scrapy_extension._is_missing_optional_dep`` for the full
    rationale. Rule: must be a ``ModuleNotFoundError`` whose ``name`` is (or is
    a submodule of) one of this backend's documented optional-dep modules.
    """
    if not isinstance(exc, ModuleNotFoundError):
        return False
    missing_name = getattr(exc, "name", None)
    if not missing_name:
        return False
    dep_modules = _BACKEND_DEP_MODULES.get(module_path, frozenset())
    if not dep_modules:
        return False
    return (
        missing_name in dep_modules
        or missing_name.split(".", 1)[0] in dep_modules
    )


def __getattr__(name: str) -> object:
    if name in _BACKEND_MODULES:
        module_path, attr_name = _BACKEND_MODULES[name]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, attr_name)
        except ImportError as e:
            # R14-H: only re-wrap as the install hint for a genuine missing
            # optional dep. A real bug inside the backend module (whose dep IS
            # installed) must surface its real chain, not the install hint.
            if _is_missing_optional_dep(e, module_path):
                extra = _BACKEND_EXTRAS.get(name, name.replace("Backend", "").lower())
                raise ImportError(
                    f"{name} requires the '{extra}' extra. "
                    f"Install with: pip install scrapy-extension[{extra}]"
                ) from e
            raise
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """PEP 562 companion — expose lazily-imported backends to dir() and autocomplete.

    Without this, ``dir(scrapy_extension.backends)`` / ``pydoc`` / IDE
    autocomplete see only eagerly-imported names; the lazily-imported
    ``_BACKEND_MODULES`` backends are invisible despite importing successfully
    on access. Returns eager globals union the lazy ``_BACKEND_MODULES`` keys
    — no optional dep is imported (dict keys only).
    """
    return sorted(set(globals()) | set(_BACKEND_MODULES))


__all__ = [
    "Backend",
    "BackendType",
    "ConnectionManager",
    "DynamoDBBackend",
    "ElasticSearchBackend",
    "JSONSerializer",
    "KafkaBackend",
    "MemcachedBackend",
    "MongoDBBackend",
    "PulsarBackend",
    "QueueBackend",
    "RabbitMQBackend",
    "RedisBackend",
    "RocketMQBackend",
    "Serializer",
    "SetBackend",
    "SqsBackend",
    "StorageBackend",
]
