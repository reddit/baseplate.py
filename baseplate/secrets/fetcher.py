"""Daemon that fetches secrets from Vault and writes them to disk.

This daemon looks for configuration in an INI file passed on the command line.
The file should contain a section looking like:

    [secret-fetcher]
    vault.url = https://vault.example.com:8200/
    vault.role = my-server-role

    output.path = /var/local/secrets.json
    output.owner = www-data
    output.group = www-data
    output.mode = 0400

    secrets =
        secret/one,
        secret/two,
        secret/three,

where each secret is a path to look up in Vault. The daemon authenticates with
Vault as the EC2 server it is running on. Vault can map that context to
appropriate policies accordingly.

The secrets will be read from Vault and written to output.path as a JSON file
with the following structure:

    {
        "secrets": {
            "secret/one": {...},
            "secret/two": {...},
            "secret/three": {...}
        },
        "vault": {
            "token": "9da4241c-3460-11e7-84ac-0e9f9d32522f",
            "url": "https://vault.example.com:8200/"
        }
    }

The vault token and URL can be used for direct communication with Vault using
the server's authority. The file will be updated as secrets expire and need to
be refetched.

The `store` module in this package contains utilities for interacting with this
file from a running service.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import datetime
import grp
import json
import logging
import os
import posixpath
import pwd
import time

import requests

from .._compat import configparser, urljoin
from .. import config


logger = logging.getLogger(__name__)


NONCE_FILENAME = "/var/local/vault.nonce"
VAULT_TOKEN_PREFETCH_TIME = datetime.timedelta(seconds=60)


def fetch_instance_identity():
    """Retrieve the instance identity document from the metadata service.

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html

    """
    logger.debug("Fetching identity.")
    resp = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/pkcs7")
    resp.raise_for_status()
    return resp.text


def load_nonce():
    """Load the nonce from disk."""
    try:
        logger.debug("Loading nonce.")
        with open(NONCE_FILENAME, "r") as f:
            return f.read()
    except IOError as exc:
        logger.debug("Nonce not found: %s.", exc)
        return None


def store_nonce(nonce):
    """Store the nonce to disk securely."""
    logger.debug("Storing nonce.")
    fd = os.open(NONCE_FILENAME, os.O_WRONLY | os.O_CREAT, 0o400)
    with os.fdopen(fd, "w") as f:
        f.write(nonce)


def ttl_to_time(ttl):
    """Return an absolute expiration time given a TTL."""
    return datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)


class VaultClientFactory(object):
    """Factory that makes authenticated clients."""
    def __init__(self, base_url, role):
        self.base_url = base_url
        self.role = role
        self.session = requests.Session()
        self.client = None

    def _make_client(self):
        """Authenticate with Vault and return a client containing that token.

        This authenticates with Vault as a specified role using its AWS EC2
        auth backend. This involves sending to Vault the AWS-signed instance
        identity document from the instance metadata API. Vault should have an
        appropriate role-mapping configured for the server so that appropriate
        policies can be applied to the returned token. For example, to
        authenticate any server with the `my-servers-iam-role` IAM Role:

            vault write /auth/aws-ec2/role/my-server-role \
                bound_iam_arn=arn:aws:iam::12341234:role/my-servers-iam-role \
                policies=my-servers-policies \
                max_ttl=4h

        To combat replay attacks where the identity document is snarfed and
        passed on by an interloper, during first login we store a
        Vault-generated nonce locally in a protected file. This nonce must be
        passed back to Vault on all successive login attempts.

        See https://www.vaultproject.io/docs/auth/aws-ec2.html for more info.

        """
        identity_document = fetch_instance_identity()
        nonce = load_nonce()

        login_data = {
            "role": self.role,
            "pkcs7": identity_document,
        }

        if nonce:
            login_data["nonce"] = nonce

        logger.debug("Logging into Vault.")
        response = self.session.post(
            urljoin(self.base_url, "/v1/auth/aws-ec2/login"),
            json=login_data,
        )
        response.raise_for_status()
        response_payload = response.json()

        nonce = response_payload["auth"]["metadata"]["nonce"]
        store_nonce(nonce)

        return VaultClient(
            self.session,
            self.base_url,
            response_payload["auth"]["client_token"],
            ttl_to_time(response_payload["auth"]["lease_duration"]),
        )

    def get_client(self):
        """Get an authenticated client, reauthenticating if not cached."""
        if not self.client or self.client.is_about_to_expire:
            self.client = self._make_client()
        return self.client


class VaultClient(object):
    """An authenticated vault client.

    Use this as long as is_about_to_expire is False. Once that becomes True,
    get a new client from the factory.

    """
    def __init__(self, session, base_url, token, token_expiration):
        self.session = session
        self.base_url = base_url
        self.token = token
        self.token_expiration = token_expiration

    @property
    def is_about_to_expire(self):
        """Is the token near expiration and in need of regeneration?"""
        expiration = self.token_expiration - VAULT_TOKEN_PREFETCH_TIME
        return expiration < datetime.datetime.utcnow()

    def get_secret(self, secret_name):
        """Get the value and expiration time of a named secret."""
        logger.debug("Fetching secret %r.", secret_name)
        response = self.session.get(
            urljoin(self.base_url, posixpath.join("v1", secret_name)),
            headers={
                "X-Vault-Token": self.token,
            },
        )
        response.raise_for_status()
        payload = response.json()
        return payload["data"], ttl_to_time(payload["lease_duration"])


def octal_integer(text):
    return int(text, base=8)


def user_name_to_uid(text):
    return pwd.getpwnam(text).pw_uid


def group_name_to_gid(text):
    return grp.getgrnam(text).gr_gid


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    arg_parser.add_argument("--debug", default=False, action="store_true",
        help="enable debug logging")
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARNING
    logging.basicConfig(level=level)

    parser = configparser.RawConfigParser()
    parser.readfp(args.config_file)
    fetcher_config = dict(parser.items("secret-fetcher"))

    cfg = config.parse_config(fetcher_config, {
        "vault": {
            "url": config.String,
            "role": config.String,
        },

        "output": {
            "path": config.Optional(config.String, default="/var/local/secrets.json"),
            "owner": config.Optional(user_name_to_uid, default=0),
            "group": config.Optional(group_name_to_gid, default=0),
            "mode": config.Optional(octal_integer, default=0o400),
        },

        "secrets": config.Optional(config.TupleOf(config.String), default=[]),
    })

    # pylint: disable=maybe-no-member
    client_factory = VaultClientFactory(cfg.vault.url, cfg.vault.role)
    while True:
        client = client_factory.get_client()

        secrets = {}
        soonest_expiration = client.token_expiration
        for secret_name in cfg.secrets:
            secrets[secret_name], expiration = client.get_secret(secret_name)
            soonest_expiration = min(soonest_expiration, expiration)

        with open(cfg.output.path + ".tmp", "w") as f:
            os.fchown(f.fileno(), cfg.output.owner, cfg.output.group)
            os.fchmod(f.fileno(), cfg.output.mode)

            json.dump({
                "secrets": secrets,
                "vault": {
                    "token": client.token,
                    "url": cfg.vault.url,
                },

                # this is here to allow an upgrade path. the fetcher should
                # be upgraded first followed by the application workers.
                "vault_token": client.token,
            }, f, indent=2, sort_keys=True)

        # swap out the file contents atomically
        os.rename(cfg.output.path + ".tmp", cfg.output.path)

        time_til_expiration = soonest_expiration - datetime.datetime.utcnow()
        time_to_sleep = time_til_expiration - VAULT_TOKEN_PREFETCH_TIME
        time.sleep(max(int(time_to_sleep.total_seconds()), 1))


if __name__ == "__main__":
    main()
