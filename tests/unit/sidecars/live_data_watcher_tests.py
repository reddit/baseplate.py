import grp
import os
import pwd
import tempfile
import unittest

from pathlib import Path
from unittest.mock import Mock, patch

from baseplate.sidecars.live_data_watcher import NodeWatcher


class NodeWatcherTests(unittest.TestCase):
    def run(self, result: unittest.TestResult = None) -> unittest.TestResult:
        with tempfile.TemporaryDirectory(prefix=self.id()) as loc:
            self.output_dir = Path(loc)
            return super().run(result)

    @patch("requests.get")
    def test_http_load_type_on_change(self, request_mock: Mock):
        # Mock the returned value from the request (bytes).
        response_mock = Mock(status_code=200)
        request_mock.return_value = response_mock
        response_mock.content = b"{\"foo\":\"bar\"}"

        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b"{\"live_data_watcher_load_type\":\"http\",\"data\":\"http://someurl.com/test.json\",\"md5_hashed_data\":\"hashed_data\"}"
        expected_content = b"{\"foo\":\"bar\"}"
        inst.on_change(new_content, None)
        self.assertEqual(expected_content, dest.read_bytes())
        self.assertEqual(dest.owner(), pwd.getpwuid(os.getuid()).pw_name)
        self.assertEqual(dest.group(), grp.getgrgid(os.getgid()).gr_name)

    def test_on_change(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())
        self.assertEqual(dest.owner(), pwd.getpwuid(os.getuid()).pw_name)
        self.assertEqual(dest.group(), grp.getgrgid(os.getgid()).gr_name)

    def test_on_change_new_dir(self):
        dest = self.output_dir.joinpath("data/output.json")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())

    def test_on_change_deep_new_dir(self):
        dest = self.output_dir.joinpath("data/foo/bar.json")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())
