import unittest

import fakeredis

from baseplate.clients.redis_cluster import HotKeyTracker


class HotKeyTrackerTests(unittest.TestCase):
    def setUp(self):
        self.rc = fakeredis.FakeStrictRedis()

    def test_increment_reads_once(self):
        tracker = HotKeyTracker(self.rc, 1, 1)
        tracker.increment_keys_read_counter(["foo"], ignore_errors=False)
        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-reads", "-inf", "+inf", withscores=True
            ),
            [(b"foo", float(1))],
        )

    def test_increment_several_reads(self):
        tracker = HotKeyTracker(self.rc, 1, 1)
        for _ in range(5):
            tracker.increment_keys_read_counter(["foo"], ignore_errors=False)

        tracker.increment_keys_read_counter(["bar"], ignore_errors=False)

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-reads", "-inf", "+inf", withscores=True
            ),
            [(b"bar", float(1)), (b"foo", float(5))],
        )

    def test_reads_disabled_tracking(self):
        tracker = HotKeyTracker(self.rc, 0, 0)
        for _ in range(5):
            tracker.maybe_track_key_usage(["GET", "foo"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-reads", "-inf", "+inf", withscores=True
            ),
            [],
        )

    def test_reads_enabled_tracking(self):
        tracker = HotKeyTracker(self.rc, 1, 1)
        for _ in range(5):
            tracker.maybe_track_key_usage(["GET", "foo"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-reads", "-inf", "+inf", withscores=True
            ),
            [(b"foo", float(5))],
        )

    def test_writes_enabled_tracking(self):
        tracker = HotKeyTracker(self.rc, 1, 1)
        for _ in range(5):
            tracker.maybe_track_key_usage(["SET", "foo", "bar"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-writes", "-inf", "+inf", withscores=True
            ),
            [(b"foo", float(5))],
        )

    def test_writes_disabled_tracking(self):
        tracker = HotKeyTracker(self.rc, 0, 0)
        for _ in range(5):
            tracker.maybe_track_key_usage(["SET", "foo", "bar"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-writes", "-inf", "+inf", withscores=True
            ),
            [],
        )

    def test_write_multikey_commands(self):
        tracker = HotKeyTracker(self.rc, 1, 1)

        tracker.maybe_track_key_usage(["DEL", "foo", "bar"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-writes", "-inf", "+inf", withscores=True
            ),
            [(b"bar", float(1)), (b"foo", float(1))],
        )

    def test_write_batchkey_commands(self):
        tracker = HotKeyTracker(self.rc, 1, 1)

        tracker.maybe_track_key_usage(["MSET", "foo", "bar", "baz", "wednesday"])

        self.assertEqual(
            tracker.redis_client.zrangebyscore(
                "baseplate-hot-key-tracker-writes", "-inf", "+inf", withscores=True
            ),
            [(b"baz", float(1)), (b"foo", float(1))],
        )
