"""Daemon that fetches secrets from Vault and writes them to disk.

This daemon looks for configuration in an INI file passed on the command line.
The file should contain a section looking like:

    [secret-fetcher]
    vault.url = https://vault.example.com:8200/
    vault.role = my-server-role
    vault.auth_type = {aws,kubernetes}
    vault.mount_point = {aws-ec2,kubernetes}

    output.path = /var/local/secrets.json
    output.owner = www-data
    output.group = www-data
    output.mode = 0400

    secrets =
        secret/one,
        secret/two,
        secret/three,

    callback = scripts/my-transformer  # optional

where each secret is a path to look up in Vault. The daemon authenticates with
Vault as a role using a token obtained from an auth backend designated by `auth_type`.

Currently supported auth types:
    - aws: uses an AWS-signed instance identity document from the instance
    metadata API
    - kubernetes: uses a JWT mounted within a pod associated with a service account

Upon authenticating with this token, the Vault client then gets access based
upon the policies mapped to the role.

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

Some applications require a specific format for their secrets. The `callback` option may
be provided if a different format is needed. This script will be invoked as a subprocess
with the JSON file as its first and only argument. This allows you to read in the secrets,
write to a new file in whatever format needed, and restart other services if necessary.

"""
import argparse
import configparser
import datetime
import json
import logging
import os
import posixpath
import subprocess
import time
import urllib.parse
import uuid

from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

import requests

from baseplate import __version__ as baseplate_version
from baseplate.lib import config
from baseplate.server import EnvironmentInterpolation


logger = logging.getLogger(__name__)


K8S_SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
NONCE_FILENAME = "/var/local/vault.nonce"
VAULT_TOKEN_PREFETCH_TIME = datetime.timedelta(seconds=60)
REAUTHENTICATION_ERROR_MESSAGE = """
Authenication failed! If this instance previously authenticated with a
different nonce, a vault operator may need to remove the instance ID from the
identity whitelist. See
https://www.vaultproject.io/docs/auth/aws.html#client-nonce
""".replace(
    "\n", " "
)


def fetch_instance_identity() -> str:
    """Retrieve the instance identity document from the metadata service.

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html

    """
    logger.debug("Fetching identity.")
    resp = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/pkcs7", timeout=5)
    resp.raise_for_status()
    return resp.text


def generate_nonce() -> str:
    """Return a string value suitable for use as a nonce."""
    logger.debug("Generating a new nonce.")
    return str(uuid.uuid4())


def load_nonce() -> Optional[str]:
    """Load the nonce from disk."""
    try:
        logger.debug("Loading nonce.")
        with open(NONCE_FILENAME, encoding="UTF-8") as f:
            return f.read()
    except OSError as exc:
        logger.debug("Nonce not found: %s.", exc)
        return None


def store_nonce(nonce: str) -> None:
    """Store the nonce to disk securely."""
    logger.debug("Storing nonce.")
    fd = os.open(NONCE_FILENAME, os.O_WRONLY | os.O_CREAT, 0o400)
    with os.fdopen(fd, "w") as f:
        f.write(nonce)


def ttl_to_time(ttl: int) -> datetime.datetime:
    """Return an absolute expiration time given a TTL."""
    return datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)


Authenticator = Callable[["VaultClientFactory"], Tuple[str, datetime.datetime]]


class VaultClientFactory:
    """Factory that makes authenticated clients."""

    def __init__(self, base_url: str, role: str, auth_type: Authenticator, mount_point: str):
        self.base_url = base_url
        self.role = role
        self.auth_type = auth_type
        self.mount_point = mount_point
        self.session = requests.Session()
        self.session.headers[
            "User-Agent"
        ] = f"baseplate.py-{self.__class__.__name__}/{baseplate_version}"
        self.client: Optional["VaultClient"] = None

    def _make_client(self) -> "VaultClient":
        """Obtain a client token from an auth backend and return a Vault client with it."""
        client_token, lease_duration = self.auth_type(self)

        return VaultClient(self.session, self.base_url, client_token, lease_duration)

    def _vault_kubernetes_auth(self) -> Tuple[str, datetime.datetime]:
        r"""Get a client token from Vault through the Kubernetes auth backend.

        This authenticates with Vault as a specified role using its
        Kubernetes auth backend. This involves sending Vault a JSON Web Token
        associated with a Kubernetes service account mounted at a well-known
        location within a running pod. Vault should be configured with a
        mapping binding roles to corresponding Kubernetes service accounts and
        namespaces along with appropriate policies. For example, a pod running
        in the `prod` namespace with the service account name `my-server`
        requires a Vault configuration created like so:

            vault write /auth/kubernetes/cluster-name/role/my-server-role \
                bound_service_account_names=my-server \
                bound_service_account_namespaces=prod \
                policies=my-servers-policies \
                max_ttl=4h

        See https://www.vaultproject.io/docs/auth/kubernetes.html for more info.

        """
        try:
            with open(K8S_SERVICE_ACCOUNT_TOKEN_FILE, encoding="UTF-8") as f:
                token = f.read()
        except OSError:
            logger.error(
                "Could not read Kubernetes token file '%s'", K8S_SERVICE_ACCOUNT_TOKEN_FILE
            )
            raise

        login_data = {"jwt": token, "role": self.role}

        logger.debug("Obtaining Vault token via kubernetes auth.")
        response = self.session.post(
            urllib.parse.urljoin(self.base_url, f"v1/auth/{self.mount_point}/login"),
            json=login_data,
            timeout=5,  # seconds
        )
        response.raise_for_status()
        auth = response.json()["auth"]
        return auth["client_token"], ttl_to_time(auth["lease_duration"])

    def _vault_aws_auth(self) -> Tuple[str, datetime.datetime]:
        r"""Get a client token from Vault through the AWS auth backend.

        This authenticates with Vault as a specified role using its AWS
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

        See https://www.vaultproject.io/docs/auth/aws.html for more info.

        """
        identity_document = fetch_instance_identity()
        nonce = load_nonce()
        if not nonce:
            # By generating our own nonce rather than relying on Vault to
            # generate one for us, we can avoid vault logging the nonce value
            # in plaintext audit logs.
            # https://www.vaultproject.io/docs/auth/aws.html#client-nonce
            nonce = generate_nonce()
            store_nonce(nonce)

        login_data = {"role": self.role, "pkcs7": identity_document, "nonce": nonce}

        logger.debug("Obtaining Vault token via aws auth.")
        response = self.session.post(
            urllib.parse.urljoin(self.base_url, f"v1/auth/{self.mount_point}/login"),
            json=login_data,
            timeout=5,  # seconds
        )
        if response.status_code == 400:
            logger.error(REAUTHENTICATION_ERROR_MESSAGE)
        response.raise_for_status()
        auth = response.json()["auth"]
        return auth["client_token"], ttl_to_time(auth["lease_duration"])

    @staticmethod
    def auth_types() -> Dict[str, Authenticator]:
        """Return a dict of the supported auth types and respective methods."""
        return {
            "aws": VaultClientFactory._vault_aws_auth,
            "kubernetes": VaultClientFactory._vault_kubernetes_auth,
        }

    def get_client(self) -> "VaultClient":
        """Get an authenticated client, reauthenticating if not cached."""
        if not self.client or self.client.is_about_to_expire:
            self.client = self._make_client()
        return self.client


class VaultClient:
    """An authenticated vault client.

    Use this as long as is_about_to_expire is False. Once that becomes True,
    get a new client from the factory.

    """

    def __init__(
        self,
        session: requests.Session,
        base_url: str,
        token: str,
        token_expiration: datetime.datetime,
    ):
        self.session = session
        self.base_url = base_url
        self.token = token
        self.token_expiration = token_expiration

    @property
    def is_about_to_expire(self) -> bool:
        """Return if the token is near expiration and in need of regeneration."""
        expiration = self.token_expiration - VAULT_TOKEN_PREFETCH_TIME
        return expiration < datetime.datetime.utcnow()

    def get_secret(self, secret_name: str) -> Tuple[Any, datetime.datetime]:
        """Get the value and expiration time of a named secret."""
        logger.debug("Fetching secret %r.", secret_name)
        try:
            response = self.session.get(
                urllib.parse.urljoin(self.base_url, posixpath.join("v1", secret_name)),
                headers={"X-Vault-Token": self.token},
                timeout=5,  # seconds
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("Vault client failed to get secret: %s: %s", secret_name, e)
            raise

        payload = response.json()
        return payload["data"], ttl_to_time(payload["lease_duration"])


def fetch_secrets(
    cfg: config.ConfigNamespace, client_factory: VaultClientFactory
) -> datetime.datetime:
    logger.info("Fetching secrets.")
    client = client_factory.get_client()
    secrets = {}
    soonest_expiration = client.token_expiration
    for secret_name in cfg.secrets:
        secrets[secret_name], expiration = client.get_secret(secret_name)
        soonest_expiration = min(soonest_expiration, expiration)

    with open(cfg.output.path + ".tmp", "w", encoding="UTF-8") as f:
        os.fchown(f.fileno(), cfg.output.owner, cfg.output.group)
        os.fchmod(f.fileno(), cfg.output.mode)

        json.dump(
            {
                "secrets": secrets,
                "vault": {"token": client.token, "url": cfg.vault.url},
                # this is here to allow an upgrade path. the fetcher should
                # be upgraded first followed by the application workers.
                "vault_token": client.token,
            },
            f,
            indent=2,
            sort_keys=True,
        )

    # swap out the file contents atomically
    os.rename(cfg.output.path + ".tmp", cfg.output.path)
    logger.info("Secrets fetched.")
    return soonest_expiration


def trigger_callback(
    callback: Optional[str], secrets_file: str, last_proc: Optional[subprocess.Popen] = None
) -> Optional[subprocess.Popen]:
    if callback:
        if last_proc and last_proc.poll() is None:
            logger.info("Previous callback process is still running. Skipping")
        else:
            return subprocess.Popen([callback, secrets_file])  # pylint: disable=R1732
    return last_proc


def main() -> None:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    arg_parser.add_argument(
        "--debug", default=False, action="store_true", help="enable debug logging"
    )
    arg_parser.add_argument(
        "--once",
        default=False,
        action="store_true",
        help="only run the fetcher once rather than as a daemon",
    )
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(message)s", level=level)
    parser = configparser.RawConfigParser(interpolation=EnvironmentInterpolation())
    parser.read_file(args.config_file)
    fetcher_config = dict(parser.items("secret-fetcher"))

    cfg = config.parse_config(
        fetcher_config,
        {
            "vault": {
                "url": config.DefaultFromEnv(config.String, "BASEPLATE_DEFAULT_VAULT_URL"),
                "role": config.String,
                "auth_type": config.Optional(
                    config.OneOf(**VaultClientFactory.auth_types()),
                    default=VaultClientFactory.auth_types()["aws"],
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
        },
    )

    # pylint: disable=maybe-no-member
    client_factory = VaultClientFactory(
        cfg.vault.url, cfg.vault.role, cfg.vault.auth_type, cfg.vault.mount_point
    )

    if args.once:
        logger.info("Running secret fetcher once")
        fetch_secrets(cfg, client_factory)
        trigger_callback(cfg.callback, cfg.output.path)
    else:
        logger.info("Running secret fetcher as a daemon")
        last_proc = None
        while True:
            soonest_expiration = fetch_secrets(cfg, client_factory)
            last_proc = trigger_callback(cfg.callback, cfg.output.path, last_proc)
            time_til_expiration = soonest_expiration - datetime.datetime.utcnow()
            time_to_sleep = time_til_expiration - VAULT_TOKEN_PREFETCH_TIME
            time.sleep(max(int(time_to_sleep.total_seconds()), 1))


if __name__ == "__main__":
    main()
