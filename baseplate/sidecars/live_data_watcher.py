"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
import argparse
import configparser
import json
import logging
import os
import sys
import time

from enum import Enum
from pathlib import Path
from typing import Any
from typing import NoReturn
from typing import Optional

import boto3  # type: ignore

from botocore import UNSIGNED  # type: ignore
from botocore.client import ClientError  # type: ignore
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError  # type: ignore
from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat

from baseplate.lib import config
from baseplate.lib.live_data.zookeeper import zookeeper_client_from_config
from baseplate.lib.secrets import secrets_store_from_config
from baseplate.server import EnvironmentInterpolation


logger = logging.getLogger(__name__)


HEARTBEAT_INTERVAL = 300


class LoaderException(Exception):
    pass


class LoaderType(Enum):
    PASSTHROUGH = "PASSTHROUGH"
    S3 = "S3"

    @classmethod
    def _missing_(cls, value: object) -> "LoaderType":
        logger.error(
            "Loader Type %s has not been implemented yet. Defaulting to PASSTHROUGH", value
        )
        return cls.PASSTHROUGH


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
            try:
                data = _load_from_s3(data)
            except LoaderException:
                logger.error("Failed to load data from S3. Not writing to destination file")
                return

        if data is None:
            logger.warning(
                "No data to write to destination file. Something is likely misconfigured."
            )
            return

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


def _parse_loader_type(data: bytes) -> LoaderType:
    try:
        json_data = json.loads(data.decode("UTF-8"))
    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
        logger.debug("Data is not parseable as JSON, loading as PASSTHROUGH")
        return LoaderType.PASSTHROUGH

    try:
        loader_type = json_data["live_data_watcher_load_type"]
    except (KeyError, AttributeError, TypeError):
        logger.debug(
            "Expected dict (JSON object) but got %s. Loading as PASSTHROUGH", str(type(json_data))
        )
        return LoaderType.PASSTHROUGH

    return LoaderType(loader_type)


def _load_from_s3(data: bytes) -> bytes:
    # While many of the baseplate configurations use an ini format,
    # we've opted for json in these internal-to-znode-configs because
    # we want them to be fully controlled by the writer of the znode
    # and json is an easier format for znode authors to work with.
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
            e.args[0],
        )
        raise LoaderException from e

    if loader_config.get("anon") is True:
        # Client needs to be anonymous/unsigned or boto3 will try to read the local credentials
        # on the service pods. And - due to an AWS quirk - any request that comes in signed with
        # credentials will profile for permissions for the resource being requested EVEN if the
        # resource is public. In other words, this means that a given service cannot access
        # a public resource belonging to another cluster/AWS account unless the request credentials
        # are unsigned.
        s3_client = boto3.client(
            "s3",
            config=Config(signature_version=UNSIGNED),
            region_name=region_name,
        )
    else:
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

        raise LoaderException from error
    except EndpointConnectionError as error:
        logger.exception("Unable to retrieve object")
        raise LoaderException from error
    except ValueError as error:
        logger.exception(
            "Failed to update live config: params for loading from S3 are incorrect. Received error: %s",
            error,
        )

        raise LoaderException from error


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
