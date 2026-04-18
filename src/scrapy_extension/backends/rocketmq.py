"""RocketMQ backend implementation."""

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends.base import (
    Backend,
    BackendType,
    QueueBackend,
    SetBackend,
    StorageBackend,
)
from scrapy_extension.exceptions import BackendConnectionError, ConfigurationError
from scrapy_extension.settings import RocketMQMode

if TYPE_CHECKING:
    from scrapy_extension.settings import RocketMQSettings

logger = logging.getLogger(__name__)


class RocketMQBackend(Backend, QueueBackend, SetBackend, StorageBackend):
    """RocketMQ backend implementation."""

    def __init__(self, config: RocketMQSettings) -> None:
        self.config = config
        self._producer = None
        self._consumer = None

    def connect(self) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    def is_connected(self) -> bool:
        raise NotImplementedError

    def ping(self) -> bool:
        raise NotImplementedError

    @property
    def backend_type(self) -> BackendType:
        return BackendType.ROCKETMQ
