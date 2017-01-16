"""Extensions to the standard library `random` module."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import bisect
import random

from . import _compat


class WeightedLottery(object):
    """A lottery where items can have different chances of selection.

    Items will be picked with chance proportional to their weight relative to
    the sum of all weights, so the higher the weight, the higher the chance of
    being picked.

    :param items: A sequence of items to choose from.
    :param weight_key: A function that takes an
        item in ``items`` and returns a non-negative integer
        weight for that item.
    :raises: :py:exc:`ValueError` if any weights are negative or there are no
        items.

    .. testsetup::

        import random
        from baseplate.random import WeightedLottery
        random.seed(12345)

    An example of usage:

    .. doctest::

        >>> words = ["apple", "banana", "cantelope"]
        >>> lottery = WeightedLottery(words, weight_key=len)
        >>> lottery.pick()
        'banana'
        >>> lottery.sample(2)
        ['apple', 'cantelope']

    """

    def __init__(self, items, weight_key):
        self.weights = []
        self.items = list(items)
        if not self.items:
            raise ValueError("items must not be empty")

        # Build a running list of current total weight.  This allows us to do a
        # binary search for a random weight to get a choice quickly.
        accumulated_weight = 0
        for item in self.items:
            weight = weight_key(item)
            if weight < 0:
                raise ValueError("weight for %r must be non-negative" % item)
            accumulated_weight += weight
            self.weights.append(accumulated_weight)

        if accumulated_weight <= 0:
            raise ValueError("at least one item must have weight")

    def _pick_index(self):
        winning_ticket = random.random() * self.weights[-1]
        return bisect.bisect(self.weights, winning_ticket)

    def pick(self):
        """Pick a random element from the lottery."""
        winning_idx = self._pick_index()
        return self.items[winning_idx]

    def sample(self, sample_size):
        """Sample elements from the lottery without replacement.

        :param int sample_size: The number of items to sample from the lottery.

        """
        if not 0 <= sample_size < len(self.items):
            raise ValueError("sample size is negative or larger than the population")

        already_picked = set()
        results = [None] * sample_size

        # we use indexes in the set so we don't add a hashability requirement
        # to the items in the population.
        for i in _compat.range(sample_size):
            picked_index = self._pick_index()
            while picked_index in already_picked:
                picked_index = self._pick_index()
            results[i] = self.items[picked_index]
            already_picked.add(picked_index)
        return results
