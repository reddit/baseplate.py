import os
import unittest

from unittest import mock

import fakeredis
import pytest

from prometheus_client import REGISTRY
from rediscluster.exceptions import RedisClusterException

from baseplate.clients.redis_cluster import ACTIVE_REQUESTS
from baseplate.clients.redis_cluster import cluster_pool_from_config
from baseplate.clients.redis_cluster import HotKeyTracker
from baseplate.clients.redis_cluster import LATENCY_SECONDS
from baseplate.clients.redis_cluster import MonitoredRedisClusterConnection
from baseplate.clients.redis_cluster import REQUESTS_TOTAL


class DummyConnection(object):
    description_format = "DummyConnection<>"

    def __init__(self, host="localhost", port=7000, socket_timeout=None, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout

    def get_connection(self):
        pass

    def connect(self):
        pass

    def can_read(self):
        return False

    def send_command(self, command):
        pass

    def read_response(self):
        # Must return same number as test_pipeline_instrumentation calls
        return ["OK", "OK"]

    def pack_commands(self, *args):
        pass

    def send_packed_command(self, *args):
        pass

    def disconnect(self):
        pass


class TestMonitoredRedisConnection:
    def setup(self):
        ACTIVE_REQUESTS.clear()
        REQUESTS_TOTAL.clear()
        LATENCY_SECONDS.clear()

    @pytest.fixture
    def app_config(self):
        yield {
            "redis.url": "redis://localhost:1234/0",
        }

    @pytest.fixture
    def expected_labels(self):
        yield {
            "redis_client_name": "test_client",
            "redis_type": "cluster",
            "redis_command": "some_command",
            "redis_database": "0",
        }

    @pytest.fixture
    def connection(self):
        yield {
            "connection_class": DummyConnection,
        }

    @pytest.fixture
    def connection_pool(self, app_config, connection):
        return cluster_pool_from_config(
            app_config=app_config,
            prefix="redis.",
            client_name="test_client",
            init_slot_cache=False,
            startup_nodes=[
                {
                    "host": "127.0.0.1",
                    "port": 7000,
                }
            ],
            **connection,
        )

    @pytest.fixture
    def context(self):
        yield mock.MagicMock()

    @pytest.fixture
    def span(self):
        yield mock.MagicMock()

    @pytest.fixture
    @mock.patch("rediscluster.RedisCluster", new=mock.MagicMock())
    def monitored_redis_connection(self, span, connection_pool):
        return MonitoredRedisClusterConnection("redis_context_name", span, connection_pool)

    # NOTE: a successful execute_command() call is difficult to mock
    def test_execute_command_exc_redis_err(
        self, monitored_redis_connection, expected_labels, app_config
    ):
        with pytest.raises(RedisClusterException):
            monitored_redis_connection.execute_command("some_command")
        assert REGISTRY.get_sample_value(f"{ACTIVE_REQUESTS._name}", expected_labels) == 0
        expected_labels["redis_success"] = "false"
        assert (
            REGISTRY.get_sample_value(
                f"{LATENCY_SECONDS._name}_bucket", {**expected_labels, **{"le": "+Inf"}}
            )
            == 1
        )
        assert REGISTRY.get_sample_value(f"{REQUESTS_TOTAL._name}_total", expected_labels) == 1

    def test_pipeline_instrumentation(self, monitored_redis_connection, expected_labels):
        active_labels = {**expected_labels, "redis_command": "pipeline"}
        mock_manager = mock.Mock()
        with mock.patch.object(
            ACTIVE_REQUESTS.labels(**active_labels),
            "inc",
            wraps=ACTIVE_REQUESTS.labels(**active_labels).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                ACTIVE_REQUESTS.labels(**active_labels),
                "dec",
                wraps=ACTIVE_REQUESTS.labels(**active_labels).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")

                # This KeyError is the same problem as the RedisClusterException in `test_execute_command_exc_redis_err` above
                with pytest.raises(KeyError):
                    monitored_redis_connection.pipeline("test").set("hello", 42).set(
                        "goodbye", 23
                    ).execute()
                labels = {**active_labels, "redis_success": "false"}
                assert (
                    REGISTRY.get_sample_value(f"{REQUESTS_TOTAL._name}_total", labels) == 1.0
                ), "Unexpected value for REQUESTS_TOTAL metric. Expected one 'pipeline' command"
                assert (
                    REGISTRY.get_sample_value(
                        f"{LATENCY_SECONDS._name}_bucket", {**labels, "le": "+Inf"}
                    )
                    == 1.0
                ), "Expected one 'pipeline' latency request"
                assert mock_manager.mock_calls == [
                    mock.call.inc(),
                    mock.call.dec(),
                ], "Instrumentation should increment and then decrement active requests exactly once"
                print(list(REGISTRY.collect()))
                assert (
                    REGISTRY.get_sample_value(ACTIVE_REQUESTS._name, active_labels) == 0.0
                ), "Should have 0 (and not None) active requests"


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
