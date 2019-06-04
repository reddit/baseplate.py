from datetime import datetime
import unittest
import pytz

from baseplate import chronos

EXAMPLE_DATETIME = datetime.utcnow().replace(tzinfo=pytz.UTC, microsecond=0)


class ChronosTests(unittest.TestCase):
    def test_datetime_conversions(self):
        epoch_sec = chronos.datetime_to_epoch_seconds(EXAMPLE_DATETIME)
        epoch_ms = chronos.datetime_to_epoch_milliseconds(EXAMPLE_DATETIME)
        self.assertEqual(EXAMPLE_DATETIME, chronos.epoch_seconds_to_datetime(epoch_sec))
        self.assertEqual(epoch_sec * 1000, epoch_ms)
