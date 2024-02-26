import datetime
import json
import shutil
import tempfile
import unittest

from pathlib import Path

import typing_extensions

from baseplate.lib.secrets import DirectorySecretsStore, SecretsStore
from baseplate.lib.secrets import secrets_store_from_config

SecretType: typing_extensions.TypeAlias = dict[str, any]


def write_secrets(csi_path: Path, data: dict[str, SecretType]) -> None:
    secrets_path = Path(csi_path, "..data/secret").resolve()
    for key, value in data.items():
        secret_path = secrets_path.joinpath(key)
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


def new_fake_csi(data: dict[str, SecretType]) -> Path:
    """Creates a simulated CSI directory with data and symlinks.
    Note that this would already be configured before the pod starts."""
    csi_dir = Path(tempfile.mkdtemp())
    # Closely resembles but doesn't precisely match the actual CSI plugin
    data_path = Path(csi_dir, f'..{datetime.datetime.today().strftime("%Y_%m_%d_%H_%M_%S.%f")}')
    data_path.joinpath("secret").mkdir(parents=True)
    write_symlinks(data_path)
    write_secrets(csi_dir, data)
    return csi_dir


def simulate_secret_update(
    csi_dir: Path, updated_data: dict[str, SecretType] | None = None
) -> None:
    """Simulates either TTL expiry / a secret update."""
    old_data_path = csi_dir.joinpath("..data").resolve()
    # Clone the data directory
    new_data_path = Path(csi_dir, f'..{datetime.datetime.today().strftime("%Y_%m_%d_%H_%M_%S.%f")}')
    # TODO support new data
    shutil.copytree(old_data_path, new_data_path)
    # Update the secret
    if updated_data:
        write_secrets(csi_dir, updated_data)
    write_symlinks(new_data_path)
    shutil.rmtree(old_data_path)


def get_secrets_store(csi_dir: str) -> SecretsStore:
    return secrets_store_from_config(
        {
            "secrets.path": csi_dir,
            "secrets.provider": "vault_csi"
        }
    )


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.csi_dir = new_fake_csi(
            {
                "example-service/example-secret": {
                    "request_id": "8487d906-2154-0151-d07e-57f41447326a",
                    "lease_id": "",
                    "lease_duration": 2764800,
                    "renewable": False,
                    "data": {"password": "password", "type": "credential", "username": "reddit"},
                    "warnings": None,
                },
                "example-service/nested/example-nested-secret": {
                    "request_id": "8487d906-2154-0151-d07e-57f41447326a",
                    "lease_id": "",
                    "lease_duration": 2764800,
                    "renewable": False,
                    "data": {"password": "password", "type": "credential", "username": "reddit"},
                    "warnings": None,
                },
                "bare-secret": {
                    "request_id": "8487d906-2154-0151-d07e-57f41447326a",
                    "lease_id": "",
                    "lease_duration": 2764800,
                    "renewable": False,
                    "data": {"password": "password", "type": "credential", "username": "reddit"},
                    "warnings": None,
                },
            }
        )

    def tearDown(self):
        shutil.rmtree(self.csi_dir)

    def test_can_load_secrets(self):
        secrets_store = get_secrets_store(str(self.csi_dir.joinpath("..data")))
        secrets_store.get_raw("example-service/example-secret")

    def test_symlink_updated(self):
        secrets_store = secrets_store_from_config(
            {"secrets": {"path": self.csi_dir, "provider": "vault_csi"}}, self.csi_dir
        )
        simulate_secret_update(self.csi_dir)
        raise NotImplementedError
