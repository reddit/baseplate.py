import unittest

from baseplate.thing_id import is_valid_id
from baseplate.thing_id import ThingPrefix


class ThingIdTests(unittest.TestCase):
    def test_validate_ids(self):
        valid_ids = ["t3_1", "t3_asdf", "t5_1"]
        for valid_id in valid_ids:
            self.assertTrue(is_valid_id(valid_id, (ThingPrefix.LINK, ThingPrefix.SUBREDDIT)))

        invalid_ids = ["asdf", "t5__bbbbbbork", "t3_.b.o.r.k."]
        for invalid_id in invalid_ids:
            self.assertFalse(is_valid_id(invalid_id, (ThingPrefix.LINK, ThingPrefix.SUBREDDIT)))
