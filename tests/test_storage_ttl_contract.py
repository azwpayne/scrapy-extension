"""Cross-backend StorageBackend TTL input contract."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest


def _storage_backends() -> list[Callable[[], Any]]:
  from scrapy_extension.backends.dynamodb import DynamoDBBackend
  from scrapy_extension.backends.elasticsearch import ElasticSearchBackend
  from scrapy_extension.backends.memcached import MemcachedBackend
  from scrapy_extension.backends.mongodb import MongoDBBackend
  from scrapy_extension.backends.redis import RedisBackend
  from scrapy_extension.settings import (
    DynamoDBSettings,
    ElasticSearchSettings,
    MemcachedSettings,
    MongoDBSettings,
    RedisSettings,
  )

  return [
    lambda: RedisBackend(RedisSettings()),
    lambda: MongoDBBackend(MongoDBSettings()),
    lambda: ElasticSearchBackend(ElasticSearchSettings()),
    lambda: DynamoDBBackend(DynamoDBSettings()),
    lambda: MemcachedBackend(MemcachedSettings()),
  ]


@pytest.mark.parametrize("factory", _storage_backends())
@pytest.mark.parametrize("ttl", [0, -1, 1.5, True])
def test_store_rejects_non_positive_or_non_integer_ttl(
  factory: Callable[[], Any], ttl: Any
) -> None:
  backend = factory()

  with pytest.raises(ValueError, match="ttl must be a positive integer"):
    backend.store("key", b"value", ttl=ttl)
