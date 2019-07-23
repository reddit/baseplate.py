from typing import Any
from typing import Optional


class Experiment:
    """Base interface for experiment objects."""

    def get_unique_id(self, **kwargs: Any) -> Optional[str]:  # pylint: disable=unused-argument
        """Generate a unique ID for this experiment with the given inputs.

        Used to determine if a bucketing event has alread been fired for a
        given experiment and bucketing value pair.  Returns None by default.
        If None is returned, we will not mark that a bucketing event has been
        logged even if we do log a bucketing event. The kwargs should be the
        same values passed to the call to Experiment.variant.  If your
        experiment does log bucketing events, you must implement this function.

        """
        if self.should_log_bucketing():
            raise NotImplementedError
        return None

    def variant(self, **kwargs: Any) -> Optional[str]:
        """Determine which variant, if any, of this experiment is active.

        All arguments needed for bucketing, targeting, and variant overrides
        should be passed in as kwargs.  The parameter names are determined by
        the specific implementation of the Experiment interface.

        :returns: The name of the enabled variant as a string if any variant is
        enabled.  If no variant is enabled, return None.
        """
        raise NotImplementedError

    def should_log_bucketing(self) -> bool:
        """Return whether this experiment should log bucketing events to the event pipeline."""
        raise NotImplementedError
