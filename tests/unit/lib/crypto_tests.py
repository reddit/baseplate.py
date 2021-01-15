import datetime

from unittest import mock

import pytest

from baseplate.lib import crypto
from baseplate.lib.secrets import VersionedSecret


TEST_SECRET = VersionedSecret(previous=b"one", current=b"two", next=b"three")
MESSAGE = "test message"
VALID_TIL_1030 = b"AQAABgQAAOMD6M5zvQU0-GK_uKvPdKH7NOeRAq5Jdlkjwq67BzLt"


@mock.patch("time.time")
def test_make_signature(mock_time):
    mock_time.return_value = 1000

    signature = crypto.make_signature(TEST_SECRET, MESSAGE, max_age=datetime.timedelta(seconds=30))

    assert signature == VALID_TIL_1030


@pytest.mark.parametrize(
    "signature",
    [
        b"totally bogus",
        b"Ym9ndXM=",  # base64, but "bogus" content
        b"AgAA0gQAAGFzZGZhc2Rm",  # v2 header
        b"AQAABgQAAOMD6M5zvQU0",  # wrong length
    ],
)
def test_bogus_signature(signature):
    with pytest.raises(crypto.UnreadableSignatureError):
        crypto.validate_signature(TEST_SECRET, MESSAGE, signature)


@mock.patch("time.time")
def test_expired(mock_time):
    mock_time.return_value = 2000

    with pytest.raises(crypto.ExpiredSignatureError) as exc:
        crypto.validate_signature(TEST_SECRET, MESSAGE, VALID_TIL_1030)

    assert exc.value.expiration == 1030


@mock.patch("time.time")
@pytest.mark.parametrize(
    "rotated_secret",
    [
        VersionedSecret(previous=TEST_SECRET.current, current=b"new", next=b"new"),
        VersionedSecret(previous=b"old", current=TEST_SECRET.current, next=b"new"),
        VersionedSecret(previous=b"old", current=b"old", next=TEST_SECRET.current),
    ],
)
def test_secret_rotation(mock_time, rotated_secret):
    mock_time.return_value = 1000

    result = crypto.validate_signature(rotated_secret, MESSAGE, VALID_TIL_1030)

    assert result.version == 1
    assert result.expiration == 1030


@mock.patch("time.time")
def test_bad_signature(mock_time):
    mock_time.return_value = 1000

    with pytest.raises(crypto.IncorrectSignatureError):
        crypto.validate_signature(TEST_SECRET, "SNEAKY DIFFERENT MESSAGE", VALID_TIL_1030)
