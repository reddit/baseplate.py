# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import sys
import tempfile
import unittest

from baseplate import file_watcher
from baseplate.retry import RetryPolicy

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

    @mock.patch("baseplate.retry.RetryPolicy.new")
    def test_timeout(self, mock_retry_factory):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"!")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            mock_retry_policy = mock.MagicMock(spec=RetryPolicy)
            mock_retry_policy.__iter__ = mock.Mock(return_value=iter([3, 2, 1]))
            mock_retry_factory.return_value = mock_retry_policy

            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                file_watcher.FileWatcher(
                    watched_file.name,
                    parser=json.load,
                    timeout=3,
                )


@unittest.skipIf(sys.version_info.major >= 3, "Skipping Python 2 only tests")
class Py2FileWatcherTests(unittest.TestCase):

    def test_open_options_not_supported(self):
        mock_parser = mock.Mock()
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, buffering=1)
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, file="/foo/does_not_exist")
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, mode="w")
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, closefd=False)


@unittest.skipIf(sys.version_info.major < 3, "Skipping Python 3 only tests.")
class Py3FileWatcherTests(unittest.TestCase):

    def test_known_unsupported_open_options(self):
        mock_parser = mock.Mock()
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, file="/foo/does_not_exist")
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, mode="w")
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, closefd=False)

    def test_unknown_unsupported_open_options(self):
        # If an open_options key is not supported but is not one of the "open"
        # arguments that we do not explicitly not support, then the error will
        # not be raised until we first try to open the file.
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"hello!")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))
            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.read(), foo="bar")

            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                result = watcher.get_data()

    def test_open_options_buffering(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"A\nB\nC\nD")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            # buffering = 0 is only supported in binary mode which FileWatcher
            # does not use.
            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.read(), buffering=0)
            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                result = watcher.get_data()

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.line_buffering)
            self.assertFalse(watcher.get_data())

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.line_buffering, buffering=1)
            self.assertTrue(watcher.get_data())

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda x: x.line_buffering, buffering=2)
            self.assertFalse(watcher.get_data())

    def test_open_options_encoding(self):
        file_path = os.path.abspath('data/file_watcher_tests.json')

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ANSI_X3.4-1968')
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='utf-8')
        result = watcher.get_data()
        self.assertEqual(result, {"a": "☃️"})

    def test_open_options_errors(self):
        file_path = os.path.abspath('data/file_watcher_tests.json')

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='ignore')
        self.assertEqual(watcher.get_data(), {"a": ""})

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='replace')
        self.assertEqual(watcher.get_data(), {"a": "������"})

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='surrogateescape')
        self.assertEqual(watcher.get_data(), {"a": "\udce2\udc98\udc83\udcef\udcb8\udc8f"})

        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='backslashreplace')
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()

        # Only supported when writing
        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='xmlcharrefreplace')
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()

        # Only supported when writing
        watcher = file_watcher.FileWatcher(
            file_path, parser=json.load, encoding='ascii', errors='namereplace')
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()

    def test_open_options_newline(self):
        parser = lambda f: f.readlines()
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"A\nB\rC\r\nD")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser)
            self.assertEqual(watcher.get_data(), ["A\n", "B\n", "C\n", "D"])

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, newline='')
            self.assertEqual(watcher.get_data(), ["A\n", "B\r", "C\r\n", "D"])

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, newline='\n')
            self.assertEqual(watcher.get_data(), ["A\n", "B\rC\r\n", "D"])

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, newline='\r')
            self.assertEqual(watcher.get_data(), ["A\nB\r", "C\r", "\nD"])

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, newline='\r\n')
            self.assertEqual(watcher.get_data(), ["A\nB\rC\r\n", "D"])

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, newline='foo')
            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                watcher.get_data()

    @unittest.skipIf((sys.version_info.major, sys.version_info.minor) < (3, 3),
                     "'opener' was not added as an option to 'open' until "
                     "Python version 3.3")
    def test_open_options_opener(self):
        opener = mock.MagicMock()
        opener.return_value = 1
        parser = lambda f: 'foo'

        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"A\nB\rC\r\nD")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=parser, opener=opener)
            self.assertEqual(watcher.get_data(), "foo")

            self.assertEqual(opener.call_count, 1)
            self.assertEqual(opener.call_args[0][0], watched_file.name)
