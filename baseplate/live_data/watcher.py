"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import os
import sys
import time

from .._compat import configparser
from .. import config
from ..secrets import secrets_store_from_config
from .zookeeper import zookeeper_client_from_config


logger = logging.getLogger(__name__)


HEARTBEAT_INTERVAL = 300


class NodeWatcher(object):
    def __init__(self, dest, owner, group, mode):
        self.dest = dest
        self.owner = owner
        self.group = group
        self.mode = mode

    def on_change(self, data, _):
        if data is None:
            # the data node does not exist
            try:
                logger.info("Removing %r; watched node deleted.", self.dest)
                os.unlink(self.dest)
            except OSError as exc:
                logger.debug("%s: couldn't unlink: %s", self.dest, exc)
            return

        # swap out the file atomically so clients watching the file never catch
        # us mid-write.
        logger.info("Updating %r", self.dest)
        with open(self.dest + ".tmp", "wb") as tmpfile:
            if self.owner and self.group:
                os.fchown(tmpfile.fileno(), self.owner, self.group)
            os.fchmod(tmpfile.fileno(), self.mode)

            tmpfile.write(data)
        os.rename(self.dest + ".tmp", self.dest)


def watch_zookeeper_nodes(zookeeper, nodes):
    for node in nodes:
        watcher = NodeWatcher(node.dest, node.owner, node.group, node.mode)
        zookeeper.DataWatch(node.source, watcher.on_change)

    # all the interesting stuff is now happening in the Kazoo worker thread
    # and so we'll just spin and periodically heartbeat to prove we're alive.
    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        # see the comment in baseplate.live_data.zookeeper for explanation of
        # how reconnects work with the background thread.
        if zookeeper.connected:
            for node in nodes:
                try:
                    logger.debug("Heartbeating %s", node.dest)

                    # this will make FileWatchers re-parse the file on the next
                    # read which is unfortunate but we do it anyway. it's
                    # important to monitor that the file is being updated as
                    # accurately as possible rather than using a separate file
                    # or mechanism as a proxy. for example, the actual output
                    # file could have bogus permissions that would go unnoticed
                    # if the heartbeat still worked independently.
                    os.utime(node.dest, None)
                except OSError as exc:
                    logger.warning("%s: could not heartbeat: %s", node.dest, exc)


def main():
    arg_parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__)
    arg_parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    arg_parser.add_argument("--debug", default=False, action="store_true",
        help="enable debug logging")
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(message)s")

    # quiet kazoo's verbose logs a bit
    logging.getLogger("kazoo").setLevel(logging.WARNING)

    parser = configparser.RawConfigParser()
    parser.readfp(args.config_file)
    watcher_config = dict(parser.items("live-data"))

    cfg = config.parse_config(watcher_config, {
        "nodes": config.DictOf({
            "source": config.String,
            "dest": config.String,
            "owner": config.Optional(config.UnixUser),
            "group": config.Optional(config.UnixGroup),
            "mode": config.Optional(config.Integer(base=8), default=0o400),
        }),
    })
    # pylint: disable=maybe-no-member
    nodes = cfg.nodes.values()

    secrets = secrets_store_from_config(watcher_config)
    zookeeper = zookeeper_client_from_config(
        secrets, watcher_config, read_only=True)
    zookeeper.start()
    try:
        watch_zookeeper_nodes(zookeeper, nodes)
    finally:
        zookeeper.stop()


if __name__ == "__main__":
    main()
