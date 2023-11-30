import grp
import json
import logging
import os
import pwd
import tempfile
import unittest

from pathlib import Path

import boto3

from moto import mock_s3

from baseplate.sidecars.live_data_watcher import _generate_sharded_file_key
from baseplate.sidecars.live_data_watcher import NodeWatcher

NUM_FILE_SHARDS = 6


logger = logging.getLogger(__name__)


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
        default_file_key = "test_file_key"
        for file_shard_num in range(NUM_FILE_SHARDS):
            if file_shard_num == 0:
                # The first copy should just be the original file.
                sharded_file_key = default_file_key
            else:
                # All other copies should include the sharded prefix.
                sharded_file_key = str(file_shard_num) + "/" + default_file_key
            s3_client.put_object(
                Bucket=bucket_name,
                Key=sharded_file_key,
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

    def test_generate_sharded_file_key_no_sharding(self):
        original_file_key = "test_file_key"
        expected_sharded_file_key = "test_file_key"
        possible_no_sharding_values = [-2, -1, 0, 1, None]
        for values in possible_no_sharding_values:
            actual_sharded_file_key = _generate_sharded_file_key(values, original_file_key)
            self.assertEqual(actual_sharded_file_key, expected_sharded_file_key)

    def test_generate_sharded_file_key_sharding(self):
        original_file_key = "test_file_key"
        possible_sharded_file_keys = set(
            [
                "1/test_file_key",
                "2/test_file_key",
                "3/test_file_key",
                "4/test_file_key",
                "5/test_file_key",
            ]
        )
        for i in range(50):
            actual_sharded_file_key = _generate_sharded_file_key(NUM_FILE_SHARDS, original_file_key)
            # If num_file_shards is provided, the generated file key MUST have a prefix.
            self.assertTrue(actual_sharded_file_key in possible_sharded_file_keys)
            # Make sure we aren't generating a file without the prefix.
            self.assertFalse(actual_sharded_file_key == original_file_key)

    def test_s3_load_type_on_change_no_sharding(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b'{"live_data_watcher_load_type":"S3","bucket_name":"test_bucket","file_key":"test_file_key","sse_key":"test_decryption_key","region_name":"us-east-1"}'
        expected_content = b'{"foo_encrypted": "bar_encrypted"}'
        inst.on_change(new_content, None)
        self.assertEqual(expected_content, dest.read_bytes())
        self.assertEqual(dest.owner(), pwd.getpwuid(os.getuid()).pw_name)
        self.assertEqual(dest.group(), grp.getgrgid(os.getgid()).gr_name)

    def test_s3_load_type_on_change_sharding(self):
        dest = self.output_dir.joinpath("data.txt")
        inst = NodeWatcher(str(dest), os.getuid(), os.getgid(), 777)

        new_content = b'{"live_data_watcher_load_type":"S3","bucket_name":"test_bucket","file_key":"test_file_key","sse_key":"test_decryption_key","region_name":"us-east-1", "num_file_shards": 5}'
        expected_content = b'{"foo_encrypted": "bar_encrypted"}'

        # For safe measure, run this 50 times. It should succeed every time.
        # We've uploaded 5 files to S3 in setUp() and num_file_shards=5 in the
        # ZK node so we should be fetching one of these 5 files randomly (and successfully)
        # and all should have the same content.
        for i in range(50):
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
