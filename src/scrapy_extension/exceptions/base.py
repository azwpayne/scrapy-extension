"""Custom exceptions for scrapy-extension.

This module defines the exception hierarchy used throughout the package.
"""

from __future__ import annotations

_REDACTED = "***REDACTED***"
_SENSITIVE_NAME_FRAGMENTS = ("password", "secret", "api_key", "apikey", "token", "credential")


def _is_sensitive_name(name: object) -> bool:
  """Heuristic: does this setting name suggest the value is secret?"""
  if not isinstance(name, str):
    return False
  lowered = name.lower()
  return any(frag in lowered for frag in _SENSITIVE_NAME_FRAGMENTS)


def _is_secret_value(value: object) -> bool:
  """Detect SecretStr / SecretBytes from pydantic without importing pydantic."""
  return type(value).__name__ in {"SecretStr", "SecretBytes"}


class BackendError(Exception):
  """Base exception for all backend-related errors.

  All custom exceptions in this package inherit from BackendError,
  allowing users to catch all backend-related errors with a single
  except clause.
  """


class BackendConnectionError(BackendError):
  """Exception raised for connection-related errors.

  This includes failures to establish initial connections, lost
  connections during operation, and authentication failures.

  Attributes:
      backend_type: The type of backend that failed to connect.
      message: Explanation of the error.
  """

  def __init__(self, message: str, backend_type: str | None = None) -> None:
    super().__init__(message)
    self.backend_type = backend_type
    self.message = message


class QueueError(BackendError):
  """Exception raised for queue operation errors.

  This includes failures to push/pop items, queue full conditions,
  and serialization errors for queue items.

  Attributes:
      queue_name: The name of the queue where the error occurred.
      operation: The operation being performed (push, pop, etc.).
  """

  def __init__(
    self,
    message: str,
    queue_name: str | None = None,
    operation: str | None = None,
  ) -> None:
    super().__init__(message)
    self.queue_name = queue_name
    self.operation = operation


class SerializationError(BackendError):
  """Exception raised for serialization/deserialization errors.

  This includes JSON encoding/decoding errors and other data
  transformation failures.

  Attributes:
      data: The data that failed to serialize/deserialize.
      serializer: The serializer that failed.
  """

  def __init__(
    self,
    message: str,
    data: object = None,
    serializer: str | None = None,
  ) -> None:
    super().__init__(message)
    self.data = data
    self.serializer = serializer


class ConfigurationError(BackendError):
  """Exception raised for configuration errors.

  This includes invalid settings, missing required parameters,
  and validation failures.

  The ``setting_value`` attribute is automatically redacted when either:
  - The value is a pydantic ``SecretStr`` / ``SecretBytes`` (detected by type
    name, no pydantic import required)
  - The ``setting_name`` contains a sensitive fragment (``password``,
    ``secret``, ``api_key``, ``apikey``, ``token``, ``credential``)

  Redaction prevents accidental secret leaks via ``repr(exc)`` or
  debug-logging the exception. The raw value is never retained on the
  exception object once redacted.

  Attributes:
      setting_name: The name of the setting that caused the error.
      setting_value: The invalid value (or ``***REDACTED***`` if sensitive).
  """

  def __init__(
    self,
    message: str,
    setting_name: str | None = None,
    setting_value: object = None,
  ) -> None:
    super().__init__(message)
    self.setting_name = setting_name
    if _is_sensitive_name(setting_name) or _is_secret_value(setting_value):
      self.setting_value = _REDACTED
    else:
      self.setting_value = setting_value
