"""Extensions to the standard library `random` module."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

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
        self.tickets = 0
        self.items_with_weights = []

        for item in items:
            weight = weight_key(item)
            if weight < 0:
                raise ValueError("weight for %r must be non-negative" % item)
            self.tickets += weight
            self.items_with_weights.append((item, weight))

        if self.tickets == 0:
            raise ValueError("at least one item must have weight")

    def pick(self):
        """Pick a random element from the lottery."""
        winning_ticket = random.random() * self.tickets
        current_ticket = 0
        for item, weight in self.items_with_weights:
            current_ticket += weight
            if current_ticket > winning_ticket:
                return item
        else:  # pragma: nocover
            raise RuntimeError("weighted_choice failed unexpectedly")
