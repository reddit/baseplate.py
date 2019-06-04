"""Extensions to the standard library `datetime` module."""

from datetime import datetime

import pytz


def datetime_to_epoch_seconds(dt):
    """Convert datetime object to epoch seconds."""
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=pytz.UTC)
    return int(dt.timestamp())


def datetime_to_epoch_milliseconds(dt):
    """Convert datetime object to epoch milliseconds."""
    return datetime_to_epoch_seconds(dt) * 1000


def epoch_seconds_to_datetime(sec):
    """Convert epoch seconds to UTC datetime."""
    return datetime.utcfromtimestamp(sec).replace(tzinfo=pytz.UTC)
