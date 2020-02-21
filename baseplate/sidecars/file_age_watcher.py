"""Calculate the age of files sync'd to disk by the baseplate live-data watcher.

The watcher periodically updates the mtime even if data hasn't changed, so we
can alert on excessive age as an indication that data sync isn't working.
"""
import argparse
import configparser
import os
import sys
import time

from typing import NoReturn

from baseplate import config
from baseplate.lib.metrics import metrics_client_from_config


HEARTBEAT_INTERVAL = 10


def main() -> NoReturn:
    """
    Sidecar that tracks the age of the file monitored by live-data watcher and secrets fetcher.

    Use this with live_data_watcher and secrets_fetcher to monitor their files and ensure that
    these sidecars are not failing silently.
    """
    arg_parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
    arg_parser.add_argument(
        "config_file", type=argparse.FileType("r"), help="path to a configuration file"
    )
    args = arg_parser.parse_args()

    parser = configparser.RawConfigParser()
    parser.read(args.config_file.name)
    watcher_config = dict(parser.items("file-age-watcher"))
    cfg = config.parse_config(watcher_config, {"file": config.DictOf({"dest": config.String})})

    metrics_client = metrics_client_from_config(watcher_config)
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        now = time.time()
        for name, file in cfg.file.items():
            try:
                mtime = os.path.getmtime(file.dest)
            except OSError:
                mtime = 0

            age = now - mtime
            metrics_client.histogram(f"file-age-watcher.{name}.age").add_sample(age)


if __name__ == "__main__":
    main()
