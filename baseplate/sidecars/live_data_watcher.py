"""Watch nodes in ZooKeeper and sync their contents to disk on change."""
import argparse
import configparser
import json
import logging
import os
import sys
import time
import boto3

from pathlib import Path
from typing import Any
from typing import NoReturn
from typing import Optional


from botocore.client import ClientError

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
    def get_encrypted_json_from_s3(
        self, bucket_name:str, file_key: str, region_name:str, sse_key: str
    ) -> tuple[Optional[dict], dict]:
        s3_client = boto3.client(
            "s3",
            region_name=region_name,
        )
        json_data = None
        # The S3 data must be Server Side Encrypted and we should have the decryption key.
        kwargs = {
            "Bucket": bucket_name,
            "Key": file_key,
            "SSECustomerKey": sse_key,
            "SSECustomerAlgorithm": "AES256",
        }
        try:
            s3_object = s3_client.get_object(**kwargs)
            data = s3_object["Body"].read()
            if data:
                json_data = json.loads(data.decode("utf-8"))
        except ClientError as error:
            logger.exception(
                "Failed to load from S3. Received error code %s: %s",
                error.response["Error"]["Code"],
                error.response["Error"]["Message"],
            )
            return None
        except ValueError as error:
            logger.exception(error)
            return None
        except json.decoder.JSONDecodeError as error:
            logger.exception(error)
            return None
        return json_data

    @staticmethod
    def get_data_to_write(json_data: dict, data: bytes) -> Optional[bytes]:
        # Check if we have a JSON in a special format containing:
        # data = {
        #    "live_data_watcher_load_type": str
        # }
        # If the load type is 'S3', this format is an indication that we support
        # downloading the contents of encrypted files uploaded to S3.
        if json_data.get("live_data_watcher_load_type") == "S3":
            # Only write the data if we actually managed to fetch its contents.
            bucket_name=json_data.get("bucket_name")
            file_key=json_data.get("file_key"),
            sse_key=json_data.get("sse_key")
            region_name = json_data.get("region_name")
            # We require all of these keys to properly read from S3.
            if bucket_name is None or file_key is None or sse_key is None or region_name is None:
                logger.debug("Missing data in live data watch zk node to read from S3.")
                return None
            # If we have all the correct keys, attempt to read the config from S3.
            json_data = NodeWatcher.get_encrypted_json_from_s3(bucket_name=bucket_name, file_key=file_key, region_name=region_name, sse_key=sse_key)
            return json_data
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
                    logger.warning("No data written to destination node. Something is likely misconfigured.")
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
