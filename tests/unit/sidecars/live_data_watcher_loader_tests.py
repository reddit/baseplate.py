import io
import json
import os

from unittest import mock

import botocore.session
import pytest

from botocore.response import StreamingBody
from botocore.stub import Stubber
from moto import mock_s3

from baseplate.sidecars.live_data_watcher import _load_from_s3
from baseplate.sidecars.live_data_watcher import _parse_loader_type
from baseplate.sidecars.live_data_watcher import LoaderException
from baseplate.sidecars.live_data_watcher import LoaderType
from baseplate.sidecars.live_data_watcher import NodeWatcher


@pytest.fixture()
def start_mock_s3():
    with mock_s3():
        yield


@pytest.fixture()
def s3_stub(start_mock_s3):
    s3 = botocore.session.get_session().create_client("s3")
    with Stubber(s3) as stubber:
        with mock.patch("baseplate.sidecars.live_data_watcher.boto3.client") as client:
            client.return_value = s3
            yield stubber


@pytest.mark.parametrize(
    "data,return_value",
    (
        # Non-obviously-PASSTHROUGH configs
        (json.dumps({"live_data_watcher_load_type": "S3"}).encode("utf-8"), LoaderType.S3),
        (
            json.dumps({"live_data_watcher_load_type": "NOTS3"}).encode("utf-8"),
            LoaderType.PASSTHROUGH,
        ),
        # Everything else should be PASSTHROUGH
        (b"", LoaderType.PASSTHROUGH),
        (json.dumps({"key": "value"}).encode("utf-8"), LoaderType.PASSTHROUGH),
        (json.dumps([]).encode("utf-8"), LoaderType.PASSTHROUGH),
        (json.dumps(1234).encode("utf-8"), LoaderType.PASSTHROUGH),
        (json.dumps(None).encode("utf-8"), LoaderType.PASSTHROUGH),
        (b"hey{look}at--this//ugly**string@of(ugly^^chars", LoaderType.PASSTHROUGH),
    ),
)
def test_parse_loader_type(data, return_value):
    assert _parse_loader_type(data) == return_value


@pytest.mark.parametrize(
    "missing_config",
    (
        "region_name",
        "bucket_name",
        "file_key",
        "sse_key",
    ),
)
def test_load_from_s3_missing_config(start_mock_s3, missing_config):
    data = {
        "region_name": "some_region",
        "bucket_name": "some_bucket",
        "file_key": "some_file",
        "sse_key": "some_key",
    }
    data.pop(missing_config)

    with pytest.raises(LoaderException):
        assert _load_from_s3(json.dumps(data).encode("utf-8"))


@pytest.mark.parametrize(
    "anon",
    (
        True,
        False,
    ),
)
def test_successful_load_from_s3(s3_stub, anon):
    contents = b"my-test-contents"
    bucket_name = "my-test-bucket"
    file_key = "my-test-key"
    sse_key = "key"

    s3_stub.add_response(
        "get_object",
        service_response={
            "Body": StreamingBody(io.BytesIO(contents), len(contents)),
        },
        expected_params={
            "Bucket": bucket_name,
            "Key": file_key,
            "SSECustomerKey": sse_key,
            "SSECustomerAlgorithm": "AES256",
        },
    )

    data = _load_from_s3(
        json.dumps(
            {
                "region_name": "us-east-1",
                "bucket_name": bucket_name,
                "file_key": file_key,
                "sse_key": sse_key,
                "anon": anon,
            }
        ).encode("utf-8")
    )

    assert data == contents


def test_unsuccessful_load_from_s3_client_error(s3_stub):
    s3_stub.add_client_error("get_object")

    with pytest.raises(LoaderException):
        _load_from_s3(
            json.dumps(
                {
                    "region_name": "us-east-1",
                    "bucket_name": "my-test-bucket",
                    "file_key": "my-object-key",
                    "sse_key": "my-sse-key",
                }
            ).encode("utf-8")
        )


@pytest.mark.parametrize(
    "load_type,should_call_s3",
    [(LoaderType.S3, True)] + [(lt, False) for lt in LoaderType if lt != LoaderType.S3],
)
def test_loader_type_calls(tmp_path, load_type, should_call_s3):
    loader_path = "baseplate.sidecars.live_data_watcher._load_from_s3"
    parse_path = "baseplate.sidecars.live_data_watcher._parse_loader_type"
    with mock.patch(loader_path) as loader, mock.patch(parse_path) as parser:
        parser.return_value = load_type
        loader.return_value = b""
        watcher = NodeWatcher(str(tmp_path / "tmp"), os.getuid(), os.getgid(), 777)
        watcher.on_change(b"", None)

        assert loader.called == should_call_s3
