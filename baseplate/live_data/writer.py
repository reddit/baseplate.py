"""Write a file's contents to a node in ZooKeeper."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import difflib
import logging
import sys

from kazoo.exceptions import NoNodeError, BadVersionError

from .._compat import configparser
from ..secrets import secrets_store_from_config
from .zookeeper import zookeeper_client_from_config


logger = logging.getLogger(__name__)


class WriterError(Exception):
    pass


class NodeDoesNotExistError(WriterError):
    def __init__(self):
        super(NodeDoesNotExistError, self).__init__(
            "Target node does not exist. Please create it with appropriate "
            "ACLs first.")


class UnexpectedChangeError(WriterError):
    def __init__(self):
        super(UnexpectedChangeError, self).__init__(
            "The data in ZooKeeper changed unexpectedly.")


def write_file_to_zookeeper(zookeeper, source_file, dest_path):
    logger.info("Writing to %s in ZooKeeper...", dest_path)

    try:
        current_data, stat = zookeeper.get(dest_path)
        current_version = stat.version
    except NoNodeError:
        raise NodeDoesNotExistError

    new_data = source_file.read()

    if current_data == new_data:
        logger.info("No changes detected. Not writing.")
        return False

    try:
        current_text = current_data.decode("utf8")
        new_text = new_data.decode("utf8")
    except UnicodeDecodeError:
        logger.info("Skipping diff, data appears to be binary.")
    else:
        diff = difflib.unified_diff(
            current_text.splitlines(), new_text.splitlines())

        for line in diff:
            logger.info(line)

    try:
        zookeeper.set(dest_path, new_data, version=current_version)
    except BadVersionError:
        raise UnexpectedChangeError

    logger.info("Wrote data to Zookeeper.")
    return True


def main():
    arg_parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__)
    arg_parser.add_argument("--debug", default=False, action="store_true",
        help="enable debug logging")
    arg_parser.add_argument("config_file", type=argparse.FileType("r"),
        help="path to a configuration file")
    arg_parser.add_argument("source", type=argparse.FileType("rb"),
                            help="file to upload")
    arg_parser.add_argument("dest", help="path in zookeeper")
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

    secrets = secrets_store_from_config(watcher_config)
    zookeeper = zookeeper_client_from_config(secrets, watcher_config)
    zookeeper.start()
    try:
        write_file_to_zookeeper(zookeeper, args.source, args.dest)
    except WriterError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
    finally:
        zookeeper.stop()


if __name__ == "__main__":
    main()
