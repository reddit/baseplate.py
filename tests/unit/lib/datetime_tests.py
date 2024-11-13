import unittest

from datetime import datetime
from datetime import timezone

import pytz

from baseplate.lib.datetime import datetime_to_epoch_milliseconds
from baseplate.lib.datetime import datetime_to_epoch_seconds
from baseplate.lib.datetime import epoch_milliseconds_to_datetime
from baseplate.lib.datetime import epoch_seconds_to_datetime
from baseplate.lib.datetime import get_utc_now


EXAMPLE_DATETIME = datetime.utcnow().replace(tzinfo=timezone.utc, microsecond=0)


class DatetimeTests(unittest.TestCase):
    def test_datetime_conversions(self):
        epoch_sec = datetime_to_epoch_seconds(EXAMPLE_DATETIME)
        epoch_ms = datetime_to_epoch_milliseconds(EXAMPLE_DATETIME)
        self.assertEqual(EXAMPLE_DATETIME, epoch_seconds_to_datetime(epoch_sec))
        self.assertEqual(epoch_sec, int(epoch_ms / 1000))
        self.assertEqual(EXAMPLE_DATETIME, epoch_milliseconds_to_datetime(epoch_ms))

    def test_timezone_equivalence(self):
        pytz_datetime = EXAMPLE_DATETIME.replace(tzinfo=pytz.UTC)
        self.assertEqual(
            datetime_to_epoch_milliseconds(pytz_datetime),
            datetime_to_epoch_milliseconds(EXAMPLE_DATETIME),
        )
        self.assertEqual(
            datetime_to_epoch_seconds(pytz_datetime), datetime_to_epoch_seconds(EXAMPLE_DATETIME)
        )

    def test_get_utc_now(self):
        now = get_utc_now()
        self.assertIsNotNone(now.tzinfo)
