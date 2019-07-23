from typing import Any


class Targeting:
    """Base targeting interface for experiment targeting."""

    def evaluate(self, **kwargs: Any) -> bool:
        """Evaluate whether the provided kwargs match the expected values for targeting."""
        raise NotImplementedError
