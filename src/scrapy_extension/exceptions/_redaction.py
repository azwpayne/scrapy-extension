"""Static configuration-error boundaries for untrusted validation inputs."""

from __future__ import annotations

from collections.abc import Callable, Collection
from functools import wraps
from typing import ParamSpec, TypeVar

from scrapy_extension.exceptions.base import BackendConnectionError, ConfigurationError

_P = ParamSpec("_P")
_T = TypeVar("_T")


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


def backend_connection_error_boundary(
  message: str,
  backend_type: str,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
  """Rebuild public startup failures without retaining connection inputs.

  A backend's direct ``connect`` method commonly retains mutable endpoint and
  credential snapshots in its stack while a driver failure propagates.  The
  public error must keep the operational exception family, but not that
  traceback graph.  Non-connection exceptions keep their established
  semantics after this wrapper drops its own argument references.
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
      sanitized_error = BackendConnectionError(message, backend_type=backend_type)
      del args
      del kwargs
      del caught_error
      raise sanitized_error

    return wrapped

  return decorate
