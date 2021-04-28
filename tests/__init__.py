try:
    # nullcontext is only available in Python 3.7+
    from contextlib import nullcontext as does_not_raise
except ImportError:
    from contextlib import contextmanager

    @contextmanager
    def does_not_raise():
        yield


__all__ = [
    "does_not_raise",
]
