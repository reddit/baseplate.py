"""Internal library helpers."""

from __future__ import annotations

import inspect
import warnings
from typing import Any, Callable, Generic, TypeVar, overload

from typing_extensions import Self


def warn_deprecated(message: str) -> None:
    """Emit a deprecation warning from the caller.

    The stacktrace for the warning will point to the place where the function
    calling this was called, rather than in baseplate. This allows the user to
    most easily see where in _their_ code the deprecation is coming from.

    """
    warnings.warn(message, DeprecationWarning, stacklevel=3)


T = TypeVar("T")  # Type of the class instance which the property is attached to.
R = TypeVar("R")  # Return type of the wrapped method.


# cached_property is a renamed copy of pyramid.decorator.reify
# see COPYRIGHT for full license information
class cached_property(Generic[T, R]):
    """Like @property but the function will only be called once per instance.

    When used as a method decorator, this will act like @property but instead
    of calling the function each time the attribute is accessed, instead it
    will only call it on first access and then replace itself on the instance
    with the return value of the first call.
    """

    wrapped: Callable[[T], R]

    def __init__(self, wrapped: Callable[[T], R]) -> None:
        self.wrapped = wrapped
        self.__doc__ = wrapped.__doc__
        self.__name__ = wrapped.__name__

    @overload
    def __get__(self, instance: T, owner: type[Any]) -> R: ...

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> Self: ...

    def __get__(self, instance: T | None, owner: type[Any]) -> R | Self:
        if instance is None:
            return self
        ret = self.wrapped(instance)
        setattr(instance, self.wrapped.__name__, ret)
        return ret


class UnknownCallerError(Exception):
    def __init__(self) -> None:
        super().__init__("Could not determine calling module's name")


def get_calling_module_name() -> str:
    module = inspect.getmodule(inspect.stack()[2].frame)
    if not module:
        raise UnknownCallerError
    return module.__name__
