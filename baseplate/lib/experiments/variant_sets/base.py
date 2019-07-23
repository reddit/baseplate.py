from typing import Any
from typing import Dict
from typing import List
from typing import Optional


class VariantSet:
    """Base interface for variant sets.

    A VariantSet contains a set of experimental variants, as well as
    their distributions. It is used by experiments to track which
    bucket a variant is assigned to.
    """

    def __init__(self, variants: List[Dict[str, Any]], num_buckets: int = 1000):
        raise NotImplementedError

    def __contains__(self, item: str) -> bool:
        """Return true if the variant name provided exists in this variant set."""
        raise NotImplementedError

    def choose_variant(self, bucket: int) -> Optional[str]:
        raise NotImplementedError
