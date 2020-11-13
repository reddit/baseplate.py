"""Extensions to the standard library `datetime` module."""
from datetime import datetime
from datetime import timezone


def datetime_to_epoch_milliseconds(dt: datetime) -> int:
    """Convert datetime object to epoch milliseconds."""
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def datetime_to_epoch_seconds(dt: datetime) -> int:
    """Convert datetime object to epoch seconds."""
    return datetime_to_epoch_milliseconds(dt) // 1000


def epoch_milliseconds_to_datetime(ms: int) -> datetime:
    """Convert epoch milliseconds to UTC datetime."""
    return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=timezone.utc)


def epoch_seconds_to_datetime(sec: int) -> datetime:
    """Convert epoch seconds to UTC datetime."""
    return datetime.utcfromtimestamp(sec).replace(tzinfo=timezone.utc)


def get_utc_now() -> datetime:
    """Get current offset-aware datetime which has timezone information."""
    return datetime.now(tz=timezone.utc)
