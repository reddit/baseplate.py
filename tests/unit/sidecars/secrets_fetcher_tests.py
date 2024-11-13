import configparser
import dataclasses
import datetime
import getpass
import grp
import io
import json
import os
import pathlib
import sys
import typing
import unittest.mock

from pyfakefs.fake_filesystem_unittest import TestCase

from baseplate.lib import config
from baseplate.sidecars import secrets_fetcher

UTC: datetime.timezone
if sys.version_info > (3, 11):
    UTC = datetime.UTC
else:
    from datetime import timezone

    UTC = timezone.utc

whoami = getpass.getuser()
group = grp.getgrgid(os.getgid()).gr_name

configini = f"""
[secret-fetcher]
vault.url = https://vault.example.com:8200/
vault.role = my-server-role
vault.auth_type = aws
vault.mount_point = aws-ec2

output.path = /var/local/secrets.json
output.owner = {whoami}
output.group = {group}
output.mode = 0400

secrets =
    secret/one,
    secret/two,
    secret/three,

callback = scripts/my-transformer  # optional
""".strip()


@dataclasses.dataclass
class FakeVaultClient(secrets_fetcher.VaultClient):
    token_expiration: datetime.datetime
    token: str = "token"

    # @typing.override  # TODO: Added in version 3.12.
    def get_secret(self, secret_name: str) -> tuple[typing.Any, datetime.datetime]:
        return secret_name.upper(), self.token_expiration + datetime.timedelta(seconds=30)


@dataclasses.dataclass
class FakeBadVaultClient(secrets_fetcher.VaultClient):
    token_expiration: datetime.datetime
    token: str = "token-bad"

    # @typing.override  # TODO: Added in version 3.12.
    def get_secret(self, secret_name: str) -> tuple[typing.Any, datetime.datetime]:
        """Return a secret value that is not JSON serializable."""
        s = self.token_expiration
        return s, self.token_expiration + datetime.timedelta(seconds=30)


class Tests(TestCase):
    @classmethod
    def setUpClass(cls):
        spec = {
            "vault": {
                "url": config.DefaultFromEnv(config.String, "BASEPLATE_DEFAULT_VAULT_URL"),
                "role": config.String,
                "auth_type": config.Optional(
                    config.OneOf(**secrets_fetcher.VaultClientFactory.auth_types()),
                    default=secrets_fetcher.VaultClientFactory.auth_types()["aws"],
                ),
                "mount_point": config.DefaultFromEnv(
                    config.String, "BASEPLATE_VAULT_MOUNT_POINT", fallback="aws-ec2"
                ),
            },
            "output": {
                "path": config.Optional(config.String, default="/var/local/secrets.json"),
                "owner": config.Optional(config.UnixUser, default=0),
                "group": config.Optional(config.UnixGroup, default=0),
                "mode": config.Optional(config.Integer(base=8), default=0o400),  # type: ignore
            },
            "secrets": config.Optional(config.TupleOf(config.String), default=[]),
            "callback": config.Optional(config.String),
        }

        parser = configparser.RawConfigParser()
        with io.StringIO(configini) as f:
            parser.read_file(f)
        fetcher_config = dict(parser.items("secret-fetcher"))

        cls.cfg = config.parse_config(fetcher_config, spec)

    def setUp(self):
        self.setUpPyfakefs()
        self.fake_fs().create_file("/var/local/secrets.json", contents="initial contents")

        cfg = self.cfg
        now = datetime.datetime.now(UTC)
        with unittest.mock.patch(
            "baseplate.sidecars.secrets_fetcher.VaultClientFactory",
            autospec=True,
        ) as mock:
            instance = mock.return_value
            instance.get_client.return_value = FakeVaultClient(token_expiration=now)
            f = secrets_fetcher.VaultClientFactory(
                cfg.vault.url, cfg.vault.role, cfg.vault.auth_type, cfg.vault.mount_point
            )
            secrets_fetcher.fetch_secrets(cfg, f)

    def test_is_file(self):
        p = pathlib.Path("/var/local/secrets.json")
        self.assertTrue(p.is_file())

    def test_sets_owner(self):
        p = pathlib.Path("/var/local/secrets.json")
        self.assertEqual(p.owner(), whoami)

    def test_sets_group(self):
        p = pathlib.Path("/var/local/secrets.json")
        self.assertEqual(p.group(), group)

    def test_deletes_temporary_file(self):
        p = pathlib.Path("/var/local/secrets.json" + ".tmp")
        self.assertFalse(p.exists())

    def test_text_contents(self):
        p = pathlib.Path("/var/local/secrets.json")
        text = p.read_text()
        self.assertDictEqual(
            json.loads(text),
            {
                "secrets": {
                    "secret/one": "SECRET/ONE",
                    "secret/two": "SECRET/TWO",
                    "secret/three": "SECRET/THREE",
                },
                "vault": {
                    "token": "token",
                    "url": "https://vault.example.com:8200/",
                },
                "vault_token": "token",
            },
        )


class BadJSONTests(TestCase):
    @classmethod
    def setUpClass(cls):
        spec = {
            "vault": {
                "url": config.DefaultFromEnv(config.String, "BASEPLATE_DEFAULT_VAULT_URL"),
                "role": config.String,
                "auth_type": config.Optional(
                    config.OneOf(**secrets_fetcher.VaultClientFactory.auth_types()),
                    default=secrets_fetcher.VaultClientFactory.auth_types()["aws"],
                ),
                "mount_point": config.DefaultFromEnv(
                    config.String, "BASEPLATE_VAULT_MOUNT_POINT", fallback="aws-ec2"
                ),
            },
            "output": {
                "path": config.Optional(config.String, default="/var/local/secrets.json"),
                "owner": config.Optional(config.UnixUser, default=0),
                "group": config.Optional(config.UnixGroup, default=0),
                "mode": config.Optional(config.Integer(base=8), default=0o400),  # type: ignore
            },
            "secrets": config.Optional(config.TupleOf(config.String), default=[]),
            "callback": config.Optional(config.String),
        }

        parser = configparser.RawConfigParser()
        with io.StringIO(configini) as f:
            parser.read_file(f)
        fetcher_config = dict(parser.items("secret-fetcher"))

        cls.cfg = config.parse_config(fetcher_config, spec)

    def setUp(self):
        self.setUpPyfakefs()
        self.fake_fs().create_file(
            "/var/local/secrets.json",
            contents="initial contents should remain unchanged",
        )

        cfg = self.cfg
        now = datetime.datetime.now(UTC)

        with unittest.mock.patch("baseplate.sidecars.secrets_fetcher.VaultClientFactory") as mock:
            instance = mock.return_value
            instance.get_client.return_value = FakeBadVaultClient(token_expiration=now)
            f = secrets_fetcher.VaultClientFactory(
                cfg.vault.url, cfg.vault.role, cfg.vault.auth_type, cfg.vault.mount_point
            )
            with self.assertRaises(TypeError):
                secrets_fetcher.fetch_secrets(cfg, f)

    def test_temporary_file_is_not_deleted(self):
        p = pathlib.Path("/var/local/secrets.json.tmp")
        self.assertTrue(p.exists())

    def test_temporary_file_is_partially_written(self):
        p = pathlib.Path("/var/local/secrets.json.tmp")
        text = p.read_text()
        self.assertEqual(text, """{\n  "secrets": {\n    "secret/one": """)

    def test_secrets_file_exists(self):
        p = pathlib.Path("/var/local/secrets.json")
        self.assertTrue(p.exists())

    def test_secrets_file_is_unchanged(self):
        p = pathlib.Path("/var/local/secrets.json")
        text = p.read_text()
        self.assertEqual(text, """initial contents should remain unchanged""")
