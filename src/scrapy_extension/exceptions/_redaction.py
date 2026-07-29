"""Static error boundaries for untrusted configuration and backend inputs."""

from __future__ import annotations

from collections.abc import Callable, Collection
from functools import wraps
from typing import ParamSpec, TypeVar, cast

from scrapy_extension.exceptions.base import (
  BackendConnectionError,
  BackendError,
  ConfigurationError,
  QueueError,
  SerializationError,
  StorageError,
)

_P = ParamSpec("_P")
_T = TypeVar("_T")


def sanitize_backend_error(
  error: BackendError,
  *,
  message: str,
  safe_queue_operations: Collection[str] = (),
  safe_storage_operations: Collection[str] = (),
  fallback_queue_operation: str | None = None,
  fallback_storage_operation: str | None = None,
) -> BackendError:
  """Rebuild a backend error without its traceback graph or user data.

  Public operation boundaries cannot safely re-raise an error from a client
  library: its traceback can retain credentials, endpoints, queue identifiers,
  payloads, and opaque delivery tokens.  The known package exception classes
  are reconstructed with only static metadata.  Unknown subclasses retain
  their class when they can be allocated without invoking custom initializers;
  otherwise they collapse to :class:`BackendError` rather than preserving an
  untrusted object graph.
  """
  error_type = type(error)
  if error_type is QueueError:
    queue_error = cast(QueueError, error)
    operation = queue_error.operation
    safe_operation = (
      operation
      if type(operation) is str and operation in safe_queue_operations
      else (
        fallback_queue_operation
        if fallback_queue_operation in safe_queue_operations
        else None
      )
    )
    return QueueError(message, operation=safe_operation)
  if error_type is StorageError:
    storage_error = cast(StorageError, error)
    operation = storage_error.operation
    safe_operation = (
      operation
      if type(operation) is str and operation in safe_storage_operations
      else (
        fallback_storage_operation
        if fallback_storage_operation in safe_storage_operations
        else None
      )
    )
    return StorageError(message, operation=safe_operation)
  if error_type is BackendConnectionError:
    return BackendConnectionError(message)
  if error_type is SerializationError:
    return SerializationError(message)
  if error_type is ConfigurationError:
    return ConfigurationError(message)
  if error_type is BackendError:
    return BackendError(message)

  # A plugin may define a thin ``BackendError`` subclass.  Preserve that
  # public type without calling arbitrary ``__init__`` code or copying its
  # attributes.  ``BaseException.__init__`` gives it a single static arg; an
  # exotic allocator that cannot support this safely falls back to the base.
  try:
    replacement = error_type.__new__(error_type)
    BaseException.__init__(replacement, message)
  except Exception:  # noqa: BLE001 - custom exception allocation is untrusted
    return BackendError(message)
  if isinstance(replacement, BackendError):
    return replacement
  return BackendError(message)


def queue_operation_error_boundary(
  operation: str,
  message: str,
  *,
  safe_messages: Collection[str] = (),
  validator: Callable[..., None] | None = None,
  handled_exception_types: tuple[type[Exception], ...] | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Make one public queue operation publish a redacted ``QueueError``.

  ``validator`` deliberately runs before the protected call so established
  ``ValueError`` input contracts remain visible to callers.  Once I/O begins,
  every regular exception is reconstructed after all implementation frames
  have unwound.  ``handled_exception_types`` may narrow that behavior for a
  backend whose known driver failures have already been normalized to package
  exceptions; unlisted exceptions then retain their established behavior.
  The replacement has a fixed operation, no logical queue identifier, and no
  exception chain.  ``BaseException`` control flow remains untouched.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      if validator is not None:
        validator(*args, **kwargs)
      caught_error: Exception | None = None
      try:
        return function(*args, **kwargs)
      except Exception as error:  # noqa: BLE001 - terminal public boundary
        if (
          handled_exception_types is not None
          and type(error) not in handled_exception_types
        ):
          del args
          del kwargs
          raise
        caught_error = error
      except BaseException:
        del args
        del kwargs
        raise

      replacement_message = message
      raw_args: object = None
      if type(caught_error) is QueueError:
        raw_args = caught_error.args
        if (
          type(raw_args) is tuple
          and len(raw_args) == 1
          and type(raw_args[0]) is str
          and raw_args[0] in safe_messages
        ):
          replacement_message = raw_args[0]
      sanitized_error = QueueError(replacement_message, operation=operation)
      del args
      del kwargs
      del caught_error
      del raw_args
      del replacement_message
      raise sanitized_error

    return wrapped

  return decorate


def set_operation_error_boundary(
  message: str,
  backend_type: str,
  *,
  safe_messages: Collection[str] = (),
  validator: Callable[..., None] | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Rebuild a known direct set connection failure without private state.

  Direct set implementations normalize expected client failures to the exact
  :class:`BackendConnectionError` class so callers can make their documented
  graceful-degradation decision.  Its original traceback can still retain a
  logical set name, payload, client object, or mutable settings graph.  This
  boundary may preserve an exact approved static message through
  ``safe_messages``; all other messages use ``message``.  It publishes a new
  error only after implementation frames have unwound.  It deliberately leaves
  subclasses and unknown exceptions alone: a bundled operation must not
  silently redefine plugin/custom error contracts just because it is protected
  by this terminal boundary.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      if validator is not None:
        validator(*args, **kwargs)
      caught_connection_error: BackendConnectionError | None = None
      try:
        return function(*args, **kwargs)
      except BackendConnectionError as error:
        if type(error) is not BackendConnectionError:
          del args
          del kwargs
          raise
        caught_connection_error = error
      except BaseException:
        del args
        del kwargs
        raise

      assert caught_connection_error is not None
      replacement_message = message
      raw_args: object = caught_connection_error.args
      if (
        type(raw_args) is tuple
        and len(raw_args) == 1
        and type(raw_args[0]) is str
        and raw_args[0] in safe_messages
      ):
        replacement_message = raw_args[0]
      sanitized_error = BackendConnectionError(
        replacement_message, backend_type=backend_type
      )
      del args
      del kwargs
      del caught_connection_error
      del replacement_message
      del raw_args
      raise sanitized_error

    return wrapped

  return decorate


def storage_operation_error_boundary(
  operation: str,
  message: str,
  backend_type: str,
  *,
  safe_messages: Collection[str] = (),
  safe_connection_messages: Collection[str] = (),
  validator: Callable[..., None] | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Publish a terminal direct-storage error without key or backend graphs.

  The wrapped backend may expose either the exact ``StorageError`` operational
  contract or the exact ``BackendConnectionError`` not-connected contract.
  Both are reconstructed from static caller-selected metadata.  Storage
  failures retain their fixed operation but never the logical key; connection
  failures retain a trusted bundled backend type.  A caller may preserve a
  known fixed storage or connection message through ``safe_messages`` and
  ``safe_connection_messages``; all other messages use ``message``.  Input
  validation runs outside the protected call.  Custom exception subclasses
  and unknown implementation failures are intentionally not converted.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      if validator is not None:
        validator(*args, **kwargs)
      caught_storage_error: StorageError | None = None
      caught_connection_error: BackendConnectionError | None = None
      try:
        return function(*args, **kwargs)
      except StorageError as error:
        if type(error) is not StorageError:
          del args
          del kwargs
          raise
        caught_storage_error = error
      except BackendConnectionError as error:
        if type(error) is not BackendConnectionError:
          del args
          del kwargs
          raise
        caught_connection_error = error
      except BaseException:
        del args
        del kwargs
        raise

      replacement_message = message
      raw_args: object = None
      if caught_storage_error is not None:
        raw_args = caught_storage_error.args
        if (
          type(raw_args) is tuple
          and len(raw_args) == 1
          and type(raw_args[0]) is str
          and raw_args[0] in safe_messages
        ):
          replacement_message = raw_args[0]
        sanitized_error: BackendError = StorageError(
          replacement_message,
          operation=operation,
          key=None,
        )
      else:
        assert caught_connection_error is not None
        raw_args = caught_connection_error.args
        if (
          type(raw_args) is tuple
          and len(raw_args) == 1
          and type(raw_args[0]) is str
          and raw_args[0] in safe_connection_messages
        ):
          replacement_message = raw_args[0]
        sanitized_error = BackendConnectionError(
          replacement_message, backend_type=backend_type
        )
      del args
      del kwargs
      del caught_storage_error
      del caught_connection_error
      del replacement_message
      del raw_args
      raise sanitized_error

    return wrapped

  return decorate


def not_implemented_error_boundary(
  message: str,
  *,
  validator: Callable[..., None] | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Rebuild one documented static capability error outside private frames.

  Some public backend operations intentionally report an unavailable broker
  capability with a fixed :class:`NotImplementedError`.  The literal itself is
  safe, but raising it inside a backend method leaves that method's traceback
  holding the backend object and caller-controlled operation arguments.  This
  boundary keeps the documented concrete exception and exact fixed message
  while releasing those frames first.

  ``validator`` runs before the protected call so existing input-validation
  failures retain their normal public contract.  Only the exact built-in
  ``NotImplementedError`` is reconstructed: subclasses and every other
  exception are unknown backend/plugin behavior and intentionally propagate
  unchanged.  ``BaseException`` control flow is likewise never converted.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      if validator is not None:
        validator(*args, **kwargs)
      caught_error: NotImplementedError | None = None
      try:
        return function(*args, **kwargs)
      except NotImplementedError as error:
        if type(error) is not NotImplementedError:
          del args
          del kwargs
          raise
        caught_error = error
      except BaseException:
        del args
        del kwargs
        raise

      assert caught_error is not None
      sanitized_error = NotImplementedError(message)
      del args
      del kwargs
      del caught_error
      raise sanitized_error

    return wrapped

  return decorate


def sanitize_configuration_error(
  error: ConfigurationError,
  allowed_setting_names: Collection[str],
  *,
  message: str,
  fallback_setting_name: str = "configuration",
) -> ConfigurationError:
  """Rebuild an error with static text and a verified setting name.

  Validation helpers frequently receive values and field labels from callers.
  The original exception can retain both through its traceback or attributes,
  so public boundaries must never re-raise it.  Exact built-in strings are
  the only names safe to retain; every other value collapses to ``fallback``.
  """
  candidate = error.setting_name
  setting_name = (
    candidate
    if type(candidate) is str and candidate in allowed_setting_names
    else fallback_setting_name
  )
  return ConfigurationError(message, setting_name=setting_name)


def configuration_error_boundary(
  message: str,
  allowed_setting_names: Collection[str],
  *,
  fallback_setting_name: str = "configuration",
  preserve_static_message: bool = False,
  safe_messages: Collection[str] = (),
  safe_message_predicate: Callable[[str], bool] | None = None,
  sanitize_exception_types: tuple[type[Exception], ...] = (),
  pass_through_exception_types: tuple[type[Exception], ...] = (),
  catch_unexpected: bool = True,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Make a direct validation helper publish only a static config error.

  The replacement is intentionally raised after the ``except`` suite.  That
  removes the original traceback and leaves both ``__cause__`` and
  ``__context__`` unset.  Explicit deletion also avoids retaining raw
  arguments in the wrapper frame when diagnostic tooling captures locals.

  ``pass_through_exception_types`` is deliberately narrow: callers may use
  it only under an outer boundary that rebuilds the escaped exception after
  this wrapper returns.  It lets that outer boundary preserve an operational
  exception family without exposing the inner validation frames.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      caught_error: ConfigurationError | None = None
      pass_through_error: Exception | None = None
      unexpected_failure = False
      original_message: object = None
      message_is_safe = False
      predicate_matches = False
      safe_message = message
      try:
        return function(*args, **kwargs)
      except ConfigurationError as error:
        # A subclass can override attribute access and turn the redaction
        # handler itself into a disclosure path. Only the exact base class is
        # safe to inspect; subclasses collapse to the static fallback.
        if type(error) is ConfigurationError:
          caught_error = error
        else:
          unexpected_failure = True
      except Exception as error:  # noqa: BLE001 - validation input methods are untrusted
        error_type = type(error)
        if any(
          issubclass(error_type, exception_type)
          for exception_type in pass_through_exception_types
        ):
          pass_through_error = error
        elif any(
          issubclass(error_type, exception_type)
          for exception_type in sanitize_exception_types
        ):
          unexpected_failure = True
        elif not catch_unexpected:
          raise
        else:
          unexpected_failure = True
      if pass_through_error is not None:
        escaped_error = pass_through_error
        del args
        del kwargs
        del caught_error
        del pass_through_error
        del original_message
        del message_is_safe
        del predicate_matches
        del safe_message
        raise escaped_error
      if caught_error is not None:
        original_message = caught_error.args[0] if caught_error.args else None
        if type(original_message) is str and safe_message_predicate is not None:
          try:
            predicate_matches = safe_message_predicate(original_message)
          except Exception:  # noqa: BLE001 - safe-message check must fail closed
            predicate_matches = False
        message_is_safe = type(original_message) is str and (
          original_message in safe_messages
          or predicate_matches
        )
        if preserve_static_message and message_is_safe:
          assert type(original_message) is str
          safe_message = original_message
        sanitized_error = sanitize_configuration_error(
          caught_error,
          allowed_setting_names,
          message=safe_message,
          fallback_setting_name=fallback_setting_name,
        )
      else:
        assert unexpected_failure
        sanitized_error = ConfigurationError(
          message,
          setting_name=fallback_setting_name,
        )
      del args
      del kwargs
      del caught_error
      del pass_through_error
      del original_message
      del message_is_safe
      del predicate_matches
      del safe_message
      raise sanitized_error

    return wrapped

  return decorate


def import_error_traceback_boundary(
  function: Callable[_P, _T],
) -> Callable[_P, _T]:
  """Keep an import failure's public identity without retaining config frames.

  Optional dependencies distinguish a genuinely missing package from an
  internal ABI/import failure. The latter remains the original ``ImportError``
  for compatibility, but its accumulated traceback can retain backend
  configuration. Strip that graph only after the wrapped function has exited,
  then re-raise from a wrapper whose arguments have been released.
  """

  @wraps(function)
  def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
    caught_error: ImportError | None = None
    try:
      return function(*args, **kwargs)
    except ImportError as error:
      caught_error = error
    except BaseException:
      del args
      del kwargs
      raise
    assert caught_error is not None
    caught_error.__traceback__ = None
    caught_error.__cause__ = None
    caught_error.__context__ = None
    caught_error.__suppress_context__ = True
    sanitized_error = caught_error
    del args
    del kwargs
    del caught_error
    raise sanitized_error

  return wrapped


def backend_connection_error_boundary(
  message: str,
  backend_type: str,
  *,
  safe_messages: Collection[str] = (),
  safe_message_predicate: Callable[[str], bool] | None = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Rebuild public startup failures without retaining connection inputs.

  A backend's direct ``connect`` method commonly retains mutable endpoint and
  credential snapshots in its stack while a driver failure propagates.  The
  public error must keep the operational exception family, but not that
  traceback graph. A caller may preserve an exact static message through a
  literal allowlist or a fail-closed structural predicate; every other
  message falls back to ``message``. Non-connection exceptions keep their
  established semantics after this wrapper drops its own argument references.
  """

  def decorate(function: Callable[_P, _T]) -> Callable[_P, _T]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
      caught_error: BackendConnectionError | None = None
      try:
        return function(*args, **kwargs)
      except BackendConnectionError as error:
        caught_error = error
      except BaseException:
        del args
        del kwargs
        raise
      connection_message = message
      raw_args: object = None
      raw_message: object = None
      predicate_matches = False
      if type(caught_error) is BackendConnectionError:
        raw_args = caught_error.args
        if type(raw_args) is tuple and len(raw_args) == 1:
          raw_message = raw_args[0]
        if type(raw_message) is str and safe_message_predicate is not None:
          try:
            predicate_matches = safe_message_predicate(raw_message)
          except Exception:  # noqa: BLE001 - diagnostic predicates fail closed
            predicate_matches = False
        if type(raw_message) is str and (
          raw_message in safe_messages or predicate_matches
        ):
          connection_message = raw_message
      sanitized_error = BackendConnectionError(
        connection_message, backend_type=backend_type
      )
      del args
      del kwargs
      del caught_error
      del raw_args
      del raw_message
      del predicate_matches
      del connection_message
      raise sanitized_error

    return wrapped

  return decorate
