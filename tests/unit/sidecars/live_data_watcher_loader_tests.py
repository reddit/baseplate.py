import os
from unittest import mock
import pytest
import json

import boto3
from botocore.client import ClientError
from moto import mock_s3

from baseplate.sidecars.live_data_watcher import NodeWatcher
from baseplate.sidecars.live_data_watcher import _load_from_s3
from baseplate.sidecars.live_data_watcher import _parse_loader_type
from baseplate.sidecars.live_data_watcher import LoaderType

@pytest.fixture()
def start_mock_s3():
    with mock_s3():
        yield

@pytest.mark.parametrize('data,return_value', (
    # Non-RAW configs
    (json.dumps({'live_data_watcher_load_type': 'S3'}).encode('utf-8'), LoaderType.S3),

    # Everything else should be RAW
    (b'', LoaderType.RAW),
    (json.dumps({'key': 'value'}).encode('utf-8'), LoaderType.RAW),
    (json.dumps([]).encode('utf-8'), LoaderType.RAW),
    (json.dumps(1234).encode('utf-8'), LoaderType.RAW),
    (json.dumps(None).encode('utf-8'), LoaderType.RAW),
    # TODO: fuzzing
))
def test_parse_loader_type(data, return_value):
    assert _parse_loader_type(data) == return_value


@pytest.mark.parametrize("missing_config", (
    "region_name",
    "bucket_name",
    "file_key",
    "sse_key",
))
def test_load_from_s3_missing_config(start_mock_s3, missing_config):
    data = {
        "region_name": "some_region",
        "bucket_name": "some_bucket",
        "file_key": "some_file",
        "sse_key": "some_key",
    }
    data.pop(missing_config)

    assert _load_from_s3(json.dumps(data).encode('utf-8')) is None


def test_successful_load_from_s3(start_mock_s3):
    bucket = 'my-test-bucket'
    key = 'my-test-key'
    contents = b'my-test-contents'

    s3 = boto3.resource('s3')
    s3.Bucket(bucket).create()
    obj = s3.Object(bucket, key)
    obj.put(Body=contents)

    data = _load_from_s3(json.dumps({
        "region_name": "us-east-1",
        "bucket_name": bucket,
        "file_key": key,
        "sse_key": "key"
    }).encode('utf-8'))

    assert data == contents


@pytest.mark.parametrize('exc', (
    ValueError,
    ClientError({'Error': {'Code': 'error', 'Message': 'msg'}}, 'operation'),
))
def test_unsuccessful_load_from_s3(start_mock_s3, exc):
    with mock.patch('baseplate.sidecars.live_data_watcher.boto3.client') as client:
        client.return_value.get_object.side_effect = exc
        data = _load_from_s3(json.dumps({
            "region_name": "us-east-1",
            "bucket_name": 'bucket',
            "file_key": 'key',
            "sse_key": "key"
        }).encode('utf-8'))

        assert data is None


@pytest.mark.parametrize('load_type,should_call_s3', (
    (LoaderType.S3, True),
    (LoaderType.RAW, False),
))
def test_loader_type_calls(tmp_path, load_type, should_call_s3):
    loader_path = 'baseplate.sidecars.live_data_watcher._load_from_s3'
    parse_path = 'baseplate.sidecars.live_data_watcher._parse_loader_type'
    with mock.patch(loader_path) as loader, mock.patch(parse_path) as parser:
        parser.return_value = load_type
        loader.return_value = b''
        watcher = NodeWatcher(str(tmp_path / 'tmp'), os.getuid(), os.getgid(), 777)
        watcher.on_change(b'', None)

        assert loader.called == should_call_s3
