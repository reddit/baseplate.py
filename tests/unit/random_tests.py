import collections
import unittest

from unittest import mock

from baseplate.lib import random


class WeightedChoiceTests(unittest.TestCase):
    def test_empty_list(self):
        with self.assertRaises(ValueError):
            random.WeightedLottery([], lambda i: i)

    def test_negative_weight(self):
        with self.assertRaises(ValueError):
            random.WeightedLottery(["a"], lambda i: -1)

    def test_no_weight(self):
        with self.assertRaises(ValueError):
            random.WeightedLottery(["a"], lambda i: 0)

    @mock.patch("random.random")
    def test_iterator(self, mock_random):
        iterator = (n for n in range(10))

        mock_random.return_value = 0.0
        lottery = random.WeightedLottery(iterator, lambda i: i)
        choice = lottery.pick()
        self.assertEqual(choice, 1)

    @mock.patch("random.random")
    def test_choice(self, mock_random):
        def weight_fn(i):
            if i == "c":
                return 3
            else:
                return 1

        items = ["a", "b", "c", "d"]  # 1  # 1  # 3  # 1

        mock_random.return_value = 0.5
        lottery = random.WeightedLottery(items, weight_fn)
        choice = lottery.pick()
        self.assertEqual(choice, "c")

    def test_distribution(self):
        def weight_fn(i):
            return ord(i) - 96

        items = ["a", "b", "c", "d"]  # 1  # 2  # 3  # 4
        lottery = random.WeightedLottery(items, weight_fn)

        choices = collections.Counter()
        for _ in range(10000):
            choice = lottery.pick()
            choices[choice] += 1

        # we give a bit of fuzz factor here since we're
        # allowing true randomness in this test and don't
        # want spurious failures.
        self.assertLess(abs(1000 - choices["a"]), 150)
        self.assertLess(abs(2000 - choices["b"]), 150)
        self.assertLess(abs(3000 - choices["c"]), 150)
        self.assertLess(abs(4000 - choices["d"]), 150)

    def test_sample_errors(self):
        def weight_fn(i):
            if i == "c":
                return 3
            else:
                return 1

        items = ["a", "b", "c", "d"]  # 1  # 1  # 3  # 1
        lottery = random.WeightedLottery(items, weight_fn)

        with self.assertRaises(ValueError):
            lottery.sample(-1)

        with self.assertRaises(ValueError):
            lottery.sample(5)

    def test_sample(self):
        def weight_fn(i):
            if i == "c":
                return 3
            else:
                return 1

        items = ["a", "b", "c", "d"]  # 1  # 1  # 3  # 1
        lottery = random.WeightedLottery(items, weight_fn)

        for k in range(len(items)):
            samples = lottery.sample(k)
            # we got the right number
            self.assertEqual(len(samples), k)
            # there are no duplicates
            self.assertEqual(len(samples), len(set(samples)))
