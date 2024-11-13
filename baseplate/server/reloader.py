"""Automatic reloader for development environments.

This will watch all active source files and the application configuration and
restart the app if anything changes. This should not be used in production
settings.

"""
import logging
import os
import re
import sys
import threading
import time

from typing import Dict
from typing import Iterator
from typing import NoReturn
from typing import Sequence


logger = logging.getLogger(__name__)


def _get_loaded_modules() -> Iterator[str]:
    """Yield filenames for all loaded Python modules."""
    for module in sys.modules.values():
        filename = getattr(module, "__file__", None)
        if filename:
            uncompiled = re.sub("py[co]$", "py", filename)
            yield uncompiled


def _get_watched_files(extra_files: Sequence[str]) -> Iterator[str]:
    """Yield filenames for all files to be watched for modification."""
    yield from _get_loaded_modules()
    yield from extra_files


def _reload_when_files_change(extra_files: Sequence[str]) -> NoReturn:
    """Scan all watched files periodically and re-exec if anything changed."""
    initial_mtimes: Dict[str, float] = {}
    while True:
        for filename in _get_watched_files(extra_files):
            try:
                current_mtime = os.path.getmtime(filename)
            except os.error:
                continue

            initial_mtimes.setdefault(filename, current_mtime)
            if initial_mtimes[filename] < current_mtime:
                logger.debug("Reloading, %s changed", filename)
                os.execl(sys.executable, sys.executable, *sys.argv)

        time.sleep(0.25)


def start_reload_watcher(extra_files: Sequence[str]) -> None:
    """Start a task that will restart the server if any source files change."""
    thread = threading.Thread(target=_reload_when_files_change, args=(extra_files,))
    thread.name = "baseplate reloader"
    thread.daemon = True
    thread.start()
