

class VariantSet(object):
    """ Base interface for variant sets.

    A VariantSet contains a set of experimental variants, as well as
    their distributions. It is used by experiments to track which
    bucket a variant is assigned to.
    """

    def __contains__(self, item):
        """Return true if the variant name provided exists
        in this variant set.
        """
        raise NotImplementedError

    def choose_variant(self, bucket):
        raise NotImplementedError
