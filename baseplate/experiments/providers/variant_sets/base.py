

class VariantSet(object):
    """ Base interface for variant sets """

    def __contains__(self, item):
        """Return true if the variant name provided exists
        in this variant set.
        """
        raise NotImplementedError

    def choose_variant(self, bucket):
        raise NotImplementedError
