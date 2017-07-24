from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import tempfile
import unittest

from baseplate import file_watcher

from .. import mock


class FileWatcherTests(unittest.TestCase):
    def test_file_not_found_throws_error(self):
        mock_parser = mock.Mock()
        watcher = file_watcher.FileWatcher("/does_not_exist", mock_parser)
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()
        self.assertEqual(mock_parser.call_count, 0)

    def test_file_loads_and_parses(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"hello!")
            watched_file.flush()
            mock_parser = mock.Mock()
            watcher = file_watcher.FileWatcher(watched_file.name, mock_parser)

            result = watcher.get_data()
            self.assertEqual(result, mock_parser.return_value)
            self.assertEqual(mock_parser.call_count, 1)

            result = watcher.get_data()
            self.assertEqual(mock_parser.call_count, 1)

        # ensure the loaded data stays around even when the file was deleted
        result = watcher.get_data()
        self.assertEqual(result, mock_parser.return_value)
        self.assertEqual(mock_parser.call_count, 1)

    def test_file_reloads_when_changed(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"hello!")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))
            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.read())

            result = watcher.get_data()
            self.assertEqual(result, "hello!")

            watched_file.seek(0)
            watched_file.write(b"breaking news: hello again!")
            watched_file.flush()
            os.utime(watched_file.name, (2, 2))

            result = watcher.get_data()
            self.assertEqual(result, "breaking news: hello again!")

    def test_file_failing_to_parse_on_first_load_raises(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"!")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))
            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=json.load)

            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                watcher.get_data()

    def test_file_failing_to_parse_after_first_load_uses_cached_data(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b'{"a": 1}')
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))
            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=json.load)

            result = watcher.get_data()
            self.assertEqual(result, {"a": 1})

            watched_file.seek(0)
            watched_file.write(b"!")
            watched_file.flush()
            os.utime(watched_file.name, (2, 2))

            result = watcher.get_data()
            self.assertEqual(result, {"a": 1})

            watched_file.seek(0)
            watched_file.write(b'{"b": 3}')
            watched_file.flush()
            os.utime(watched_file.name, (3, 3))

            result = watcher.get_data()
            self.assertEqual(result, {"b": 3})
