

class Targeting(object):
    """Base targeting interface for experiment targeting.
    """

    def evaluate(self, **kwargs):
        """Evaluate whether the provided kwargs match the expected values
        for targeting.
        """
        raise NotImplementedError
