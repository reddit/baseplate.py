import unittest
import uuid

from io import BytesIO
from unittest import mock

try:
    from kazoo.client import KazooClient
    from kazoo.exceptions import NoNodeError
except ImportError:
    raise unittest.SkipTest("kazoo is not installed")

from baseplate.lib.live_data.writer import (
    NodeDoesNotExistError,
    UnexpectedChangeError,
    write_file_to_zookeeper,
)

from .. import get_endpoint_or_skip_container


zookeeper_endpoint = get_endpoint_or_skip_container("zookeeper", 2181)


# randomize the node path so simultaneously running test suites don't clobber one another
TEST_NODE_PATH = f"/writer-test-{uuid.uuid4()}"


class LiveDataWriterTests(unittest.TestCase):
    def setUp(self):
        self.zookeeper = KazooClient(hosts="%s:%d" % zookeeper_endpoint.address)
        self.zookeeper.start()

        try:
            self.zookeeper.delete(TEST_NODE_PATH)
        except NoNodeError:
            pass

        self.zookeeper.create(TEST_NODE_PATH, b"")

    def tearDown(self):
        self.zookeeper.stop()

    def test_exits_when_node_does_not_exist(self):
        input = BytesIO()

        with self.assertRaises(NodeDoesNotExistError):
            write_file_to_zookeeper(self.zookeeper, input, "/does_not_exist")

    def test_current_data_matches_new_data(self):
        data = b"data"
        self.zookeeper.set(TEST_NODE_PATH, data)

        input = BytesIO(data)
        did_write = write_file_to_zookeeper(self.zookeeper, input, TEST_NODE_PATH)
        self.assertFalse(did_write)

    def test_successful_set(self):
        self.assertEqual(self.zookeeper.get(TEST_NODE_PATH)[0], b"")

        input = BytesIO(b"new_data")
        did_write = write_file_to_zookeeper(self.zookeeper, input, TEST_NODE_PATH)
        self.assertTrue(did_write)

        self.assertEqual(self.zookeeper.get(TEST_NODE_PATH)[0], b"new_data")

    def test_version_changed(self):
        # this is a horrible hack. we want to test what happens if the contents
        # of the znode change between when write_file_to_zookeeper reads and
        # when it writes (so that the diff is trustworthy). this mock side
        # effect allows us to inject some behavior into the middle of the
        # function.
        mock_file = mock.Mock()

        def mock_read():
            self.zookeeper.set(TEST_NODE_PATH, b"I changed!")
            return b"contents of file"

        mock_file.read.side_effect = mock_read

        with self.assertRaises(UnexpectedChangeError):
            write_file_to_zookeeper(self.zookeeper, mock_file, TEST_NODE_PATH)
