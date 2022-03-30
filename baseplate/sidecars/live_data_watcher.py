"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
import argparse
import configparser
import json
import logging
import os
import sys
import time

from pathlib import Path
from typing import Any
from typing import NoReturn
from typing import Optional

import requests

from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat

from baseplate.lib import config
from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.server import EnvironmentInterpolation


logger = logging.getLogger(__name__)


HEARTBEAT_INTERVAL = 300


class NodeWatcher:
    def __init__(self, dest: str, owner: int, group: int, mode: int):
        self.dest = dest
        self.owner = owner
        self.group = group
        self.mode = mode

    @staticmethod
    def fetch_data_from_url(url: str) -> Optional[str]:
        data = None
        try:
            # Fetch the data from the url as bytes.
            data = requests.get(url).content
        except requests.exceptions.RequestException as e:
            logger.exception(e)
            return None
        return data

    @staticmethod
    def get_data_to_write(json_data: dict, data: bytes) -> Optional[bytes]:
        # Check if we have a JSON in this special format:
        # data = {
        #    "live_data_watcher_load_type": str
        #    "data": str
        #    "md5_hashed_data": str
        # }
        # If the load type is 'http', this format is an indication that we support
        # downloading the contents of files uploaded to S3, GCS, etc when provided
        # with an accessible URL.
        if json_data.get("live_data_watcher_load_type") == "http":
            # Only write the data if we actually managed to fetch its contents.
            url = json_data.get("data")
            if url is None:
                logger.debug("No url found in zk source JSON node.")
                return None
            url_data = NodeWatcher.fetch_data_from_url(url)
            if url_data is None:
                return None
            return url_data
        else:
            return data

    def on_change(self, data: bytes, _znode_stat: ZnodeStat) -> None:
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
        Path(self.dest).parent.mkdir(parents=True, exist_ok=True)
        with open(self.dest + ".tmp", "wb") as tmpfile:
            if self.owner and self.group:
                os.fchown(tmpfile.fileno(), self.owner, self.group)
            os.fchmod(tmpfile.fileno(), self.mode)
            try:
                json_data = json.loads(data.decode("UTF-8"))
            except json.decoder.JSONDecodeError:
                # If JSON fails to decode, still write the bytes data since
                # we don't necessarily know if the the contents of the znode
                # had to be valid JSON anyways.
                tmpfile.write(data)
            else:
                # If no exceptions, we have valid JSON, and can parse accordingly.
                data_to_write = NodeWatcher.get_data_to_write(json_data, data)
                if data_to_write is not None:
                    tmpfile.write(data_to_write)
                else:
                    logger.warning("No data written to destination node.")
        os.rename(self.dest + ".tmp", self.dest)


def watch_zookeeper_nodes(zookeeper: KazooClient, nodes: Any) -> NoReturn:
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


def main() -> NoReturn:
    arg_parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
    arg_parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    arg_parser.add_argument(
        "--debug", default=False, action="store_true", help="enable debug logging"
    )
    args = arg_parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="%(message)s")

    # quiet kazoo's verbose logs a bit
    logging.getLogger("kazoo").setLevel(logging.WARNING)

    parser = configparser.RawConfigParser(interpolation=EnvironmentInterpolation())
    parser.read_file(args.config_file)
    watcher_config = dict(parser.items("live-data"))

    cfg = config.parse_config(
        watcher_config,
        {
            "nodes": config.DictOf(
                {
                    "source": config.String,
                    "dest": config.String,
                    "owner": config.Optional(config.UnixUser),
                    "group": config.Optional(config.UnixGroup),
                    "mode": config.Optional(config.Integer(base=8), default=0o400),  # type: ignore
                }
            )
        },
    )
    # pylint: disable=maybe-no-member
    nodes = cfg.nodes.values()

    secrets = secrets_store_from_config(watcher_config, timeout=30)
    zookeeper = zookeeper_client_from_config(secrets, watcher_config, read_only=True)
    zookeeper.start()
    try:
        watch_zookeeper_nodes(zookeeper, nodes)
    finally:
        zookeeper.stop()


if __name__ == "__main__":
    main()
