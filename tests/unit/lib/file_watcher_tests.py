import builtins
import json
import os
import tempfile
import unittest

from unittest import mock

from baseplate.lib import file_watcher
from baseplate.lib.retry import RetryPolicy


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
            watcher = file_watcher.FileWatcher(watched_file.name, parser=lambda x: x.read())

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
            watcher = file_watcher.FileWatcher(watched_file.name, parser=json.load)

            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                watcher.get_data()

    def test_file_failing_to_parse_after_first_load_uses_cached_data(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b'{"a": 1}')
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))
            watcher = file_watcher.FileWatcher(watched_file.name, parser=json.load)

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

    @mock.patch("baseplate.lib.retry.RetryPolicy.new")
    def test_timeout(self, mock_retry_factory):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"!")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            mock_retry_policy = mock.MagicMock(spec=RetryPolicy)
            mock_retry_policy.__iter__ = mock.Mock(return_value=iter([3, 2, 1]))
            mock_retry_factory.return_value = mock_retry_policy

            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                file_watcher.FileWatcher(watched_file.name, parser=json.load, timeout=3)

    def test_binary_mode(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"foo")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda f: f.read(), binary=True
            )

            # mock.mock_open does not appear to work with binary read_data, you
            # end up getting the following error:
            # TypeError: 'str' does not support the buffer interface
            # So all we are really checking is the arguments passed to `open`.
            with mock.patch.object(
                builtins, "open", mock.mock_open(read_data="foo"), create=True
            ) as open_mock:
                watcher.get_data()
            open_mock.assert_called_once_with(
                watched_file.name, encoding=None, mode="rb", newline=None
            )

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda f: f.read(), binary=True
            )
            self.assertEqual(watcher.get_data(), b"foo")

    def test_text_mode(self):
        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"foo")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda f: f.read(), binary=False
            )

            with mock.patch.object(
                builtins, "open", mock.mock_open(read_data="foo"), create=True
            ) as open_mock:
                watcher.get_data()
            open_mock.assert_called_once_with(
                watched_file.name, encoding="UTF-8", mode="r", newline=None
            )

            watcher = file_watcher.FileWatcher(
                watched_file.name, parser=lambda f: f.read(), binary=False
            )
            self.assertEqual(watcher.get_data(), "foo")


class Py3FileWatcherTests(unittest.TestCase):
    def test_cant_set_encoding_and_binary(self):
        mock_parser = mock.Mock()
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, binary=True, encoding="utf-8")

    def test_cant_set_newline_and_binary(self):
        mock_parser = mock.Mock()
        with self.assertRaises(TypeError):
            file_watcher.FileWatcher("/does_not_exist", mock_parser, binary=True, newline="\n")

    def test_encoding_option(self):
        file_path = os.path.abspath("tests/data/file_watcher_tests.json")

        watcher = file_watcher.FileWatcher(file_path, parser=json.load, encoding="ANSI_X3.4-1968")
        with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
            watcher.get_data()

        watcher = file_watcher.FileWatcher(file_path, parser=json.load, encoding="utf-8")
        result = watcher.get_data()
        self.assertEqual(result, {"a": "☃️"})

    def test_newline_option(self):
        def parser(f):
            return f.readlines()

        with tempfile.NamedTemporaryFile() as watched_file:
            watched_file.write(b"A\nB\rC\r\nD")
            watched_file.flush()
            os.utime(watched_file.name, (1, 1))

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser)
            self.assertEqual(watcher.get_data(), ["A\n", "B\n", "C\n", "D"])

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser, newline="")
            self.assertEqual(watcher.get_data(), ["A\n", "B\r", "C\r\n", "D"])

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser, newline="\n")
            self.assertEqual(watcher.get_data(), ["A\n", "B\rC\r\n", "D"])

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser, newline="\r")
            self.assertEqual(watcher.get_data(), ["A\nB\r", "C\r", "\nD"])

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser, newline="\r\n")
            self.assertEqual(watcher.get_data(), ["A\nB\rC\r\n", "D"])

            watcher = file_watcher.FileWatcher(watched_file.name, parser=parser, newline="foo")
            with self.assertRaises(file_watcher.WatchedFileNotAvailableError):
                watcher.get_data()
