"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
import argparse
import configparser
from enum import Enum
import json
import logging
import os
import sys
import time

from pathlib import Path
from typing import Any
from typing import NoReturn
from typing import Optional

from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat

from baseplate.lib import config
from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.server import EnvironmentInterpolation

import boto3  # type: ignore
from botocore.client import ClientError  # type: ignore


logger = logging.getLogger(__name__)


HEARTBEAT_INTERVAL = 300


class LoaderType(Enum):
    RAW = 'RAW'
    S3 = 'S3'

    @classmethod
    def _missing_(cls, value):
        logger.error("Loader Type %s has not been implemented yet. Defaulting to RAW", value)
        return cls.RAW


class NodeWatcher:
    def __init__(self, dest: str, owner: int, group: int, mode: int):
        self.dest = dest
        self.owner = owner
        self.group = group
        self.mode = mode

    def handle_empty_data(self) -> None:
        # the data node does not exist
        try:
            logger.info("Removing %r; watched node deleted.", self.dest)
            os.unlink(self.dest)
        except OSError as exc:
            logger.debug("%s: couldn't unlink: %s", self.dest, exc)

    def on_change(self, data: Optional[bytes], _znode_stat: ZnodeStat) -> None:
        if data is None:
            self.handle_empty_data()
            return

        loader_type = _parse_loader_type(data)
        if loader_type == LoaderType.S3:
            data = _load_from_s3(data)

        # swap out the file atomically so clients watching the file never catch
        # us mid-write.
        logger.info("Updating %r", self.dest)
        Path(self.dest).parent.mkdir(parents=True, exist_ok=True)
        with open(self.dest + ".tmp", "wb") as tmpfile:
            if self.owner and self.group:
                os.fchown(tmpfile.fileno(), self.owner, self.group)
            os.fchmod(tmpfile.fileno(), self.mode)
            tmpfile.write(data)
        os.rename(self.dest + ".tmp", self.dest)


def _parse_loader_type(data: Optional[bytes]) -> LoaderType:
    try:
        json_data = json.loads(data.decode("UTF-8"))
    except json.decoder.JSONDecodeError:
        logger.debug("Data is not valid JSON. Loading as RAW")
        return LoaderType.RAW

    try:
        loader_type = json_data["live_data_watcher_load_type"]
    except (json.decoder.JSONDecodeError, KeyError, AttributeError, TypeError):
        logger.debug("Data is not a JSON object like {\"live_data_watcher_load_type\":\"load_type\"}. Loading as RAW")
        return LoaderType.RAW

    return LoaderType(loader_type)


def _load_from_s3(data: bytes) -> Optional[bytes]:
    loader_config = json.loads(data.decode("UTF-8"))
    try:
        region_name = loader_config["region_name"]
        s3_kwargs = {
            "Bucket": loader_config["bucket_name"],
            "Key": loader_config["file_key"],
            "SSECustomerKey": loader_config["sse_key"],
            "SSECustomerAlgorithm": "AES256",
        }
    except KeyError as e:
        # We require all of these keys to properly read from S3.
        logger.exception(
                "Failed to update live config: unable to fetch content from s3: source config has invalid or missing keys: %s.",
                e.args[0]
        )
        return None

    s3_client = boto3.client(
        "s3",
        region_name=region_name,
    )

    try:
        s3_object = s3_client.get_object(**s3_kwargs)
        # Returns bytes.
        return s3_object["Body"].read()
    except ClientError as error:
        logger.exception(
            "Failed to update live config: failed to load data from S3. Received error code %s: %s",
            error.response["Error"]["Code"],
            error.response["Error"]["Message"],
        )
    except ValueError as error:
        logger.exception(
            "Failed to update live config: params for loading from S3 are incorrect. Received error: %s",
            error,
        )
    return None


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
