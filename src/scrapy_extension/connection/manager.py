"""Connection manager for backend connections.

This module provides a lazy singleton connection manager with retry logic
for all backend types.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from scrapy_extension.backends.base import BackendType
from scrapy_extension.config.settings import RedisSettings
from scrapy_extension.exceptions import ConnectionError

if TYPE_CHECKING:
    from scrapy_extension.backends.base import (
        Backend,
        QueueBackend,
        SetBackend,
        StorageBackend,
    )

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Lazy singleton connection manager for backends.

    This class manages backend connections with:
    - Lazy initialization (connects on first use)
    - Thread-safe singleton pattern
    - Automatic retry with exponential backoff
    - Connection pooling

    Attributes:
        backend_type: The type of backend to manage.
        settings: Backend-specific settings.
        _backend: The backend instance (None until connected).
        _lock: Threading lock for thread safety.
    """

    # Class-level registry of managers
    _managers: dict[str, "ConnectionManager"] = {}
    _registry_lock = threading.Lock()

    def __init__(
        self,
        backend_type: BackendType,
        settings: dict[str, Any] | None = None,
    ) -> None:
        """Initialize connection manager.

        Args:
            backend_type: The type of backend to manage.
            settings: Backend-specific settings dictionary.
        """
        self.backend_type = backend_type
        self.settings = settings or {}
        self._backend: Backend | None = None
        self._lock = threading.Lock()

    @classmethod
    def get_manager(
        cls,
        backend_type: BackendType,
        settings: dict[str, Any] | None = None,
    ) -> ConnectionManager:
        """Get or create a connection manager.

        Args:
            backend_type: The type of backend.
            settings: Backend-specific settings.

        Returns:
            A ConnectionManager instance for the given backend.
        """
        key = f"{backend_type.value}:{hash(str(settings))}"

        with cls._registry_lock:
            if key not in cls._managers:
                cls._managers[key] = cls(backend_type, settings)
            return cls._managers[key]

    def _create_backend(self) -> Backend:
        """Create a backend instance based on type.

        Returns:
            A new backend instance.

        Raises:
            ValueError: If the backend type is not supported.
        """
        if self.backend_type == BackendType.REDIS:
            from scrapy_extension.backends.redis_backend import RedisBackend

            config = RedisSettings(**self.settings)
            return RedisBackend(config)
        else:
            raise ValueError(f"Unsupported backend type: {self.backend_type}")

    def connect(self) -> None:
        """Establish connection with retry logic.

        Attempts to connect with exponential backoff based on
        retry_attempts and retry_delay settings.

        Raises:
            ConnectionError: If all retry attempts fail.
        """
        retry_attempts = self.settings.get("retry_attempts", 3)
        retry_delay = self.settings.get("retry_delay", 1.0)

        for attempt in range(retry_attempts):
            try:
                self._backend = self._create_backend()
                self._backend.connect()
                logger.debug(f"Connected to {self.backend_type.value}")
                return
            except Exception as e:
                logger.warning(
                    f"Connection attempt {attempt + 1}/{retry_attempts} failed: {e}"
                )
                if attempt < retry_attempts - 1:
                    time.sleep(retry_delay * (2**attempt))
                else:
                    raise ConnectionError(
                        f"Failed to connect after {retry_attempts} attempts: {e}",
                        backend_type=self.backend_type.value,
                    ) from e

    def close(self) -> None:
        """Close the backend connection.

        Closes the connection and cleans up resources.
        """
        with self._lock:
            if self._backend:
                try:
                    self._backend.disconnect()
                    logger.debug(f"Disconnected from {self.backend_type.value}")
                except Exception as e:
                    logger.warning(f"Error during disconnect: {e}")
                finally:
                    self._backend = None

    @property
    def backend(self) -> Backend:
        """Get the backend instance, connecting if necessary.

        Returns:
            The backend instance.

        Raises:
            ConnectionError: If connection fails.
        """
        if self._backend is None:
            with self._lock:
                if self._backend is None:
                    self.connect()
        return self._backend

    def is_connected(self) -> bool:
        """Check if backend is connected.

        Returns:
            True if connected, False otherwise.
        """
        if self._backend is None:
            return False
        return self._backend.is_connected()

    def get_queue_backend(self) -> QueueBackend:
        """Get the queue backend interface.

        Returns:
            The QueueBackend interface of the backend.
        """
        return self.backend  # type: ignore[return-value]

    def get_set_backend(self) -> SetBackend:
        """Get the set backend interface.

        Returns:
            The SetBackend interface of the backend.
        """
        return self.backend  # type: ignore[return-value]

    def get_storage_backend(self) -> StorageBackend:
        """Get the storage backend interface.

        Returns:
            The StorageBackend interface of the backend.
        """
        return self.backend  # type: ignore[return-value]
