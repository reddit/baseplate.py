"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import errno
import logging
import os
import sys
import time

from .._compat import configparser
from .. import config
from ..secrets import secrets_store_from_config
from .zookeeper import zookeeper_client_from_config


logger = logging.getLogger(__name__)


HEARTBEAT_INTERVAL = 60


class NodeWatcher(object):
    def __init__(self, source, dest):
        self.source = source
        self.dest = dest

    def on_change(self, data, _):
        if data is None:
            # the data node does not exist
            try:
                logger.info("Removing %r; watched node deleted.", self.dest)
                os.unlink(self.dest)
            except OSError as exc:
                logger.debug("%s: couldn't unlink: %s", self.dest, exc)
            return

        logger.info("Updating %r", self.dest)
        with open(self.dest + ".tmp", "wb") as tmpfile:
            tmpfile.write(data)
        os.rename(self.dest + ".tmp", self.dest)


def watch_zookeeper_nodes(zookeeper, nodes):
    for node in nodes:
        watcher = NodeWatcher(node.source, node.dest)
        zookeeper.DataWatch(node.source, watcher.on_change)

    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        if zookeeper.connected:
            for node in nodes:
                try:
                    logger.debug("Heartbeating %s", node.dest)
                    os.utime(node.dest, None)
                except OSError as exc:
                    if exc.errno != errno.ENOENT:
                        logger.info("%s: could not heartbeat: %s",
                                    node.dest, exc)


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
    logging.getLogger("kazoo").setLevel(logging.WARNING)

    parser = configparser.RawConfigParser()
    parser.readfp(args.config_file)
    watcher_config = dict(parser.items("live-data"))

    cfg = config.parse_config(watcher_config, {
        "nodes": config.DictOf({
            "source": config.String,
            "dest": config.String,
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
