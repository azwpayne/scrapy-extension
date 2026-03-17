"""Custom exceptions for scrapy-extension.

This module defines the exception hierarchy used throughout the package.
"""


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

  Attributes:
      setting_name: The name of the setting that caused the error.
      setting_value: The invalid value that was provided.
  """

  def __init__(
    self,
    message: str,
    setting_name: str | None = None,
    setting_value: object = None,
  ) -> None:
    super().__init__(message)
    self.setting_name = setting_name
    self.setting_value = setting_value
