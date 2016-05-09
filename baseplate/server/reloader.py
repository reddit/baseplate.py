"""Automatic reloader for development environments.

This will watch all active source files and the application configuration and
restart the app if anything changes. This should not be used in production
settings.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import re
import sys
import threading
import time


logger = logging.getLogger(__name__)


def _get_loaded_modules():
    """Yield filenames for all loaded Python modules."""
    for module in sys.modules.values():
        filename = getattr(module, "__file__", None)
        if filename:
            uncompiled = re.sub("py[co]$", "py", filename)
            yield uncompiled


def _get_watched_files(extra_files):
    """Yield filenames for all files to be watched for modification."""
    for filename in _get_loaded_modules():
        yield filename
    for filename in extra_files:
        yield filename


def _reload_when_files_change(extra_files):
    """Scan all watched files periodically and re-exec if anything changed."""
    initial_mtimes = {}
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

        time.sleep(.25)


def start_reload_watcher(extra_files):
    """Start a task that will restart the server if any source files change."""
    thread = threading.Thread(
        target=_reload_when_files_change, args=(extra_files,))
    thread.name = "baseplate reloader"
    thread.daemon = True
    thread.start()
