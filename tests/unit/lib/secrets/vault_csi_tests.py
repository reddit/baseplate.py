import datetime
import json
import shutil
import string
import tempfile
import typing
import unittest

from pathlib import Path

import gevent
import pytest
import typing_extensions

from baseplate.lib.secrets import SecretNotFoundError
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.lib.secrets import SecretsStore
from baseplate.lib.secrets import VaultCSISecretsStore

SecretType: typing_extensions.TypeAlias = typing.Dict[str, any]


def write_secrets(secrets_data_path: Path, data: typing.Dict[str, SecretType]) -> None:
    """Write secrets to the current data directory."""
    for key, value in data.items():
        secret_path = secrets_data_path.joinpath(key)
        secret_path.parent.mkdir(parents=True, exist_ok=True)
        with open(secret_path, "w") as fp:
            json.dump(value, fp)


def write_symlinks(data_path: Path) -> None:
    csi_path = data_path.parent
    # This path can be monitored for changes
    # https://github.com/kubernetes-sigs/secrets-store-csi-driver/blob/c697863c35d5431ec048b440d36550eb3ceb338f/pkg/util/fileutil/atomic_writer.go#L60-L62
    data_link = Path(csi_path, "..data")
    if data_link.exists():
        # Simulate atomic update
        new_data_link = Path(csi_path, "..data-new")
        new_data_link.symlink_to(data_path)
        new_data_link.rename(data_link)
    else:
        data_link.symlink_to(data_path)
    human_path = Path(csi_path, "secret")
    if not human_path.exists():
        human_path.symlink_to(csi_path.joinpath("..data/secret"))


def new_fake_csi(data: typing.Dict[str, SecretType]) -> Path:
    """Creates a simulated CSI directory with data and symlinks.
    Note that this would already be configured before the pod starts."""
    csi_dir = Path(tempfile.mkdtemp())
    # Closely resembles but doesn't precisely match the actual CSI plugin
    data_path = Path(csi_dir, f'..{datetime.datetime.today().strftime("%Y_%m_%d_%H_%M_%S.%f")}')
    write_secrets(data_path, data)
    write_symlinks(data_path)
    return csi_dir


def simulate_secret_update(
    csi_dir: Path, updated_data: typing.Optional[typing.Dict[str, SecretType]] = None
) -> None:
    """Simulates either TTL expiry / a secret update."""
    old_data_path = csi_dir.joinpath("..data").resolve()
    # Clone the data directory
    new_data_path = Path(csi_dir, f'..{datetime.datetime.today().strftime("%Y_%m_%d_%H_%M_%S.%f")}')
    # Update the secret
    if updated_data:
        write_secrets(new_data_path, updated_data)
    else:
        shutil.copytree(old_data_path, new_data_path)
    write_symlinks(new_data_path)
    shutil.rmtree(old_data_path)


def get_secrets_store(csi_dir: str) -> SecretsStore:
    store = secrets_store_from_config({"secrets.path": csi_dir, "secrets.provider": "vault_csi"})
    assert isinstance(store, VaultCSISecretsStore)
    return store


EXAMPLE_SECRETS_DATA = {
    "secret/example-service/example-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"password": "password", "type": "credential", "username": "reddit"},
        "warnings": None,
    },
    "secret/example-service/nested/example-nested-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"password": "password", "type": "credential", "username": "reddit"},
        "warnings": None,
    },
    "secret/bare-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"password": "password", "type": "credential", "username": "reddit"},
        "warnings": None,
    },
    "secret/simple-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"type": "simple", "value": "simply a secret"},
        "warnings": None,
    },
    "secret/simple-encoded-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"type": "simple", "encoding": "base64", "value": "MTMzNw=="},
        "warnings": None,
    },
    "secret/versioned-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {"type": "versioned", "current": "current value", "previous": "previous value"},
        "warnings": None,
    },
    "secret/versioned-encoded-secret": {
        "request_id": "8487d906-2154-0151-d07e-57f41447326a",
        "lease_id": "",
        "lease_duration": 2764800,
        "renewable": False,
        "data": {
            "type": "versioned",
            "encoding": "base64",
            "current": "Y3VycmVudCBlbmNvZGVkIHZhbHVl",
            "previous": "cHJldmlvdXMgZW5jb2RlZCB2YWx1ZQ==",
        },
        "warnings": None,
    },
}

EXAMPLE_UPDATED_SECRETS = EXAMPLE_SECRETS_DATA.copy()
EXAMPLE_UPDATED_SECRETS.update(
    {
        "secret/example-service/example-secret": {
            "request_id": "8487d906-2154-0151-d07e-57f41447326a",
            "lease_id": "",
            "lease_duration": 2764800,
            "renewable": False,
            "data": {
                "password": "new_password",
                "type": "credential",
                "username": "new_reddit",
            },
            "warnings": None,
        },
    }
)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.csi_dir = new_fake_csi(EXAMPLE_SECRETS_DATA)

    def tearDown(self):
        shutil.rmtree(self.csi_dir)

    def test_can_load_credential_secret(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_credentials("secret/example-service/example-secret")
        assert data.username == "reddit"
        assert data.password == "password"

    def test_get_raw_secret(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_raw("secret/example-service/nested/example-nested-secret")
        assert data == {"password": "password", "type": "credential", "username": "reddit"}

    def test_get_simple_secret(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_simple("secret/simple-secret")
        assert data == b"simply a secret"

    def test_get_versioned_secret(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_versioned("secret/versioned-secret")
        assert data.current == b"current value"
        assert data.previous == b"previous value"

    def test_simple_encoding(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_simple("secret/simple-encoded-secret")
        assert data == b"1337"

    def test_versioned_encoding(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_versioned("secret/versioned-encoded-secret")
        assert data.current == b"current encoded value"
        assert data.previous == b"previous encoded value"

    def test_symlink_updated(self):
        original_data_path = self.csi_dir.joinpath("..data").resolve()
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_credentials("secret/bare-secret")
        gevent.sleep(0.1)  # prevent gevent from making execution out-of-order
        assert data.username == "reddit"
        assert data.password == "password"
        simulate_secret_update(self.csi_dir)
        assert original_data_path != self.csi_dir.joinpath("..data").resolve()
        data = secrets_store.get_credentials("secret/bare-secret")
        assert data.username == "reddit"
        assert data.password == "password"

    def test_secret_updated(self):
        secrets_store = get_secrets_store(str(self.csi_dir))
        data = secrets_store.get_credentials("secret/example-service/example-secret")
        gevent.sleep(0.1)  # prevent gevent from making execution out-of-order

        assert data.username == "reddit"
        assert data.password == "password"

        printable_ascii = list(string.printable)
        for i in range(0, len(printable_ascii), 6):
            chars = printable_ascii[i : i + 6]
            expected_username = "".join(chars[:3])
            expected_password = "".join(chars[3:])
            new_secrets = EXAMPLE_UPDATED_SECRETS.copy()
            new_secrets["secret/example-service/example-secret"]["data"][
                "username"
            ] = expected_username
            new_secrets["secret/example-service/example-secret"]["data"][
                "password"
            ] = expected_password
            simulate_secret_update(
                self.csi_dir,
                updated_data=EXAMPLE_UPDATED_SECRETS,
            )
            data = secrets_store.get_credentials("secret/example-service/example-secret")
            assert data.username == expected_username, f"{data.username} != {expected_username}"
            assert data.password == expected_password, f"{data.password} != {expected_password}"

    def test_multiple_requests_during_symlink_update(self):
        original_data_path = self.csi_dir.joinpath("..data").resolve()
        secrets_store = get_secrets_store(str(self.csi_dir))
        # Populate the cache
        secrets_store.get_credentials("secret/example-service/example-secret")
        gevent.sleep(0.1)  # prevent gevent from making execution out-of-order
        original_raw_secret_callable = secrets_store._raw_secret

        def mock_raw_secret(*args):
            """Inverts control back to the test during the symlink update."""
            second_request_result = secrets_store.get_credentials(
                "secret/example-service/example-secret"
            )
            # We should get stale data back from the store
            assert second_request_result.username == "reddit"
            assert second_request_result.password == "password"
            return original_raw_secret_callable(*args)

        secrets_store._raw_secret = mock_raw_secret
        simulate_secret_update(
            self.csi_dir,
            EXAMPLE_UPDATED_SECRETS,
        )
        first_request_result = secrets_store.get_credentials(
            "secret/example-service/example-secret"
        )
        gevent.sleep(0.1)  # prevent gevent from making execution out-of-order
        assert first_request_result.username == "new_reddit"
        assert first_request_result.password == "new_password"
        assert original_data_path != self.csi_dir.joinpath("..data").resolve()

    def test_invalid_secret_raises(self):
        self.csi_dir.joinpath("..data").resolve()
        secrets_store = get_secrets_store(str(self.csi_dir))
        with pytest.raises(SecretNotFoundError):
            secrets_store.get_credentials("secret/example-service/does-not-exist")
        # While cache is updating we should still fail
        with pytest.raises(SecretNotFoundError):
            secrets_store.get_credentials("secret/example-service/does-not-exist")
