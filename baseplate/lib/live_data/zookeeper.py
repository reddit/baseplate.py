"""Helpers for interacting with ZooKeeper."""
from typing import Optional

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.handlers.gevent import SequentialGeventHandler
from prometheus_client import Counter

from baseplate.lib import config
from baseplate.lib.secrets import SecretsStore
from baseplate.server.monkey import gevent_is_patched

SESSION_LOST_TOTAL = Counter(
    "zookeeper_client_session_lost",
    "The number of times a Zookeeper session as been lost for a client.",
)

SESSION_SUSPENDED_TOTAL = Counter(
    "zookeeper_client_session_suspended",
    "The number of times a Zookeeper session as been suspended for a client.",
)

SESSION_SUCCESSFUL_TOTAL = Counter(
    "zookeeper_client_session_connected",
    "The number of times a Zookeeper session has successfully connected for a client.",
)


class SessionStatsListener:
    """A Kazoo listener that monitors changes in connection state.
    Increments an event counter whenever connection state changes in a
    Zookeeper connection.
    """

    def __init__(self):
        pass

    def __call__(self, state):
        if state == KazooState.LOST:
            SESSION_LOST_TOTAL.inc()
        elif state == KazooState.SUSPENDED:
            SESSION_SUSPENDED_TOTAL.inc()
        else:
            SESSION_SUCCESSFUL_TOTAL.inc()


def zookeeper_client_from_config(
    secrets: SecretsStore, app_config: config.RawConfig, read_only: Optional[bool] = None
) -> KazooClient:
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

    :param secrets: A secrets store object
    :param raw_config: The application configuration which should have
        settings for the ZooKeeper client.
    :param read_only: Whether or not to allow connections to read-only
        ZooKeeper servers.

    """
    full_cfg = config.parse_config(
        app_config,
        {
            "zookeeper": {
                "hosts": config.String,
                "credentials": config.Optional(config.TupleOf(config.String), default=[]),
                "timeout": config.Optional(config.Timespan, default=config.Timespan("5 seconds")),
            }
        },
    )

    # pylint: disable=maybe-no-member
    cfg = full_cfg.zookeeper

    auth_data = []
    for path in cfg.credentials:
        credentials = secrets.get_simple(path)
        auth_data.append(("digest", credentials.decode("utf8")))

    # Kazoo needs to use a different handler with gevent.
    if gevent_is_patched():
        handler = SequentialGeventHandler()
    else:
        handler = None

    client = KazooClient(
        cfg.hosts,
        timeout=cfg.timeout.total_seconds(),
        auth_data=auth_data,
        read_only=read_only,
        handler=handler,
        # this retry policy tells Kazoo how often it should attempt connections
        # to ZooKeeper from its worker thread/greenlet. when the connection is
        # lost during normal operation (i.e. after it was first established)
        # Kazoo will do retries quietly in the background while the application
        # continues forward. because of this, we want it to retry forever so
        # that it doesn't just give up at some point. the application can still
        # decide if it wants to exit after being disconnected for an amount of
        # time by polling the KazooClient.connected property.
        #
        # note: KazooClient.start() has a timeout parameter which defaults to
        # 15 seconds and controls the maximum amount of time start() will block
        # waiting for the background thread to confirm it has established a
        # connection. so even though we do infinite retries here, users of this
        # function can configure the amount of time they are willing to wait
        # for initial connection.
        connection_retry=dict(
            max_tries=-1,  # keep reconnecting forever
            delay=0.1,  # initial delay
            backoff=2,  # exponential backoff
            max_jitter=1,  # maximum amount to jitter sleeptimes
            max_delay=60,  # never wait longer than this
        ),
    )

    listener = SessionStatsListener()
    client.add_listener(listener)

    return client
