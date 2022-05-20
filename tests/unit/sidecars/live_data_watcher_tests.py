import grp
import json
import os
import pwd
import tempfile
import unittest

from pathlib import Path

import boto3

from moto import mock_s3

from baseplate.sidecars.live_data_watcher import NodeWatcher


class NodeWatcherTests(unittest.TestCase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        bucket_name = "test_bucket"
        s3_data = {"foo_encrypted": "bar_encrypted"}
        s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
        )
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(
            Bucket=bucket_name,
            Key="test_file_key",
            Body=json.dumps(s3_data).encode(),
            SSECustomerKey="test_decryption_key",
            SSECustomerAlgorithm="AES256",
        )

    def tearDown(self):
        self.mock_s3.stop()

    def run(self, result: unittest.TestResult = None) -> unittest.TestResult:
        with tempfile.TemporaryDirectory(prefix=self.id()) as loc:
            self.output_dir = Path(loc)
            return super().run(result)

    def test_s3_load_type_on_change(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b'{"live_data_watcher_load_type":"S3","bucket_name":"test_bucket","file_key":"test_file_key","sse_key":"test_decryption_key","region_name":"us-east-1"}'
        expected_content = b'{"foo_encrypted": "bar_encrypted"}'
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

    def test_on_change_no_data(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = None
        inst.on_change(new_content, None)
        self.assertEqual(False, os.path.exists(dest))

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
