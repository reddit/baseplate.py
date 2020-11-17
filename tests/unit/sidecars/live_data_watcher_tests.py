import grp
import os
import pwd
import tempfile
import unittest

from pathlib import Path

from baseplate.sidecars.live_data_watcher import NodeWatcher


class NodeWatcherTests(unittest.TestCase):
    def run(self, result: unittest.TestResult = None) -> unittest.TestResult:
        with tempfile.TemporaryDirectory(prefix=self.id()) as loc:
            self.output_dir = Path(loc)
            return super().run(result)

    def test_on_change(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777,)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())
        self.assertEqual(dest.owner(), pwd.getpwuid(os.getuid()).pw_name)
        self.assertEqual(dest.group(), grp.getgrgid(os.getgid()).gr_name)

    def test_on_change_new_dir(self):
        dest = self.output_dir.joinpath("data/output.json")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777,)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())

    def test_on_change_deep_new_dir(self):
        dest = self.output_dir.joinpath("data/foo/bar.json")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777,)

        new_content = b"foobar"
        inst.on_change(new_content, None)
        self.assertEqual(new_content, dest.read_bytes())
