import unittest

from baseplate.lib import log_formatter


class CustomJSONFormatterTests(unittest.TestCase):
    def test_dict_with_correct_key(self):
        formatter = log_formatter.CustomJsonFormatter("")
        data = {"levelname": "foo"}
        assert formatter.process_log_record(data) == {"level": "foo"}

    def test_dict_without_correct_key(self):
        formatter = log_formatter.CustomJsonFormatter("")
        data = {"levelno": 1}
        assert formatter.process_log_record(data) == {"level": None, "levelno": 1}

    def test_wrong_type(self):
        formatter = log_formatter.CustomJsonFormatter("")
        with self.assertRaises(AttributeError):
            formatter.process_log_record("foo")
