"""Extensions to the standard library `random` module."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import bisect
import random


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

    def pick(self):
        """Pick a random element from the lottery."""
        winning_ticket = random.random() * self.weights[-1]
        winning_idx = bisect.bisect(self.weights, winning_ticket)
        return self.items[winning_idx]
