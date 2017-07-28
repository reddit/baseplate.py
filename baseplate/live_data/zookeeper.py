"""Helpers for interacting with ZooKeeper."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kazoo.client import KazooClient

from .. import config


def zookeeper_client_from_config(secrets, app_config, read_only=None):
    """Configure and return a ZooKeeper client.

    There are several configuration options:

    ``zookeeper.hosts``
        A comma-delimited list of hosts with optional ``chroot`` at the end.
        For example ``zk01:2181,zk02:2181`` or
        ``zk01:2181,zk02:2181/some/root``.
    ``zookeeper.credentials``
        (Optional) A comma-delimited list of paths to secrets in the secrets
        store that contain ZooKeeper authentication credentials. Secrets should
        be of the "simple" type and contain ``username:password``.
    ``zookeeper.timeout``
        (Optional) A time span of how long to wait for each connection attempt.

    The client will attempt forever to reconnect on connection loss.

    :param baseplate.secrets.SecretsStore secrets: A secrets store object
    :param dict raw_config: The application configuration which should have
        settings for the ZooKeeper client.
    :param bool read_only: Whether or not to allow connections to read-only
        ZooKeeper servers.

    :rtype: :py:class:`kazoo.client.KazooClient`

    """
    full_cfg = config.parse_config(app_config, {
        "zookeeper": {
            "hosts": config.String,
            "credentials": config.Optional(
                config.TupleOf(config.String), default=[]),
            "timeout": config.Optional(
                config.Timespan, default=config.Timespan("5 seconds")),
        },
    })

    # pylint: disable=maybe-no-member
    cfg = full_cfg.zookeeper

    auth_data = []
    for path in cfg.credentials:
        credentials = secrets.get_simple(path)
        auth_data.append(("digest", credentials.decode("utf8")))

    return KazooClient(
        cfg.hosts,
        timeout=cfg.timeout.total_seconds(),
        auth_data=auth_data,
        read_only=read_only,
        connection_retry=dict(
            max_tries=-1,  # keep reconnecting forever
            delay=0.1,  # initial delay
            backoff=2,  # exponential backoff
            max_jitter=1,  # maximum amount to jitter sleeptimes
            max_delay=60,  # never wait longer than this
        ),
    )
