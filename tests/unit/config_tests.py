from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import socket
import unittest

from baseplate import config


class StringTests(unittest.TestCase):
    def test_parse_string(self):
        result = config.String("whatever")
        self.assertEqual(result, "whatever")

    def test_empty_string_not_ok(self):
        with self.assertRaises(ValueError):
            config.String("")


class IntegerTests(unittest.TestCase):
    def test_parse_integer_valid(self):
        result = config.Integer("337")
        self.assertEqual(result, 337)

    def test_parse_integer_invalid(self):
        with self.assertRaises(ValueError):
            config.Integer("")
        with self.assertRaises(ValueError):
            config.Integer("illegal")

    def test_parse_integer_actually_float(self):
        with self.assertRaises(ValueError):
            config.Integer("1.2")


class FloatTests(unittest.TestCase):
    def test_parse_float_valid(self):
        result = config.Float("1.2")
        self.assertEqual(result, 1.2)

    def test_parse_float_invalid(self):
        with self.assertRaises(ValueError):
            config.Float("")
        with self.assertRaises(ValueError):
            config.Float("sdklfj")


class BooleanTests(unittest.TestCase):
    def test_boolean_valid(self):
        self.assertEqual(config.Boolean("true"), True)
        self.assertEqual(config.Boolean("True"), True)
        self.assertEqual(config.Boolean("false"), False)
        self.assertEqual(config.Boolean("False"), False)

    def test_boolean_invalid(self):
        with self.assertRaises(ValueError):
            config.Boolean("")
        with self.assertRaises(ValueError):
            config.Boolean("wrong")


class EndpointTests(unittest.TestCase):
    def test_endpoint_empty(self):
        with self.assertRaises(ValueError):
            config.Endpoint("")

    def test_endpoint_inet(self):
        result = config.Endpoint("localhost:1234")
        self.assertEqual(result.family, socket.AF_INET)
        self.assertEqual(result.address, ("localhost", 1234))

    def test_endpoint_inet_invalid(self):
        with self.assertRaises(ValueError):
            config.Endpoint("localhost")

    def test_endpoint_unix(self):
        result = config.Endpoint("/this/is/a/path.sock")
        self.assertEqual(result.family, socket.AF_UNIX)
        self.assertEqual(result.address, "/this/is/a/path.sock")


class Base64Tests(unittest.TestCase):
    def test_invalid(self):
        with self.assertRaises(ValueError):
            config.Base64("")

        with self.assertRaises(ValueError):
            config.Base64("dGvzdAo")  # missing padding

    def test_valid(self):
        result = config.Base64("aHVudGVyMg==")
        self.assertEqual(result, b"hunter2")


class TimespanTests(unittest.TestCase):
    def test_timespan_invalid(self):
        with self.assertRaises(ValueError):
            config.Timespan("")

        with self.assertRaises(ValueError):
            config.Timespan("a b")

        with self.assertRaises(ValueError):
            config.Timespan("10 florgles")

        with self.assertRaises(ValueError):
            config.Timespan("a b c")

        with self.assertRaises(ValueError):
            config.Timespan("3.2 hours")

    def test_timespan(self):
        result = config.Timespan("30 milliseconds")
        self.assertAlmostEqual(result.total_seconds(), 0.03)

        result = config.Timespan("1 second")
        self.assertEqual(result.total_seconds(), 1)

        result = config.Timespan("2 seconds")
        self.assertEqual(result.total_seconds(), 2)

        result = config.Timespan("30 minutes")
        self.assertEqual(result.total_seconds(), 1800)

        result = config.Timespan("2 hours")
        self.assertEqual(result.total_seconds(), 7200)

        result = config.Timespan("1 day")
        self.assertEqual(result.total_seconds(), 86400)


class OneOfTests(unittest.TestCase):
    def test_oneof_valid(self):
        parser = config.OneOf(ONE=1, TWO=2, THREE=3)
        self.assertEqual(parser("ONE"), 1)
        self.assertEqual(parser("TWO"), 2)
        self.assertEqual(parser("THREE"), 3)

    def test_oneof_invalid(self):
        parser = config.OneOf(ONE=1, TWO=2, THREE=3)
        with self.assertRaises(ValueError):
            parser("")
        with self.assertRaises(ValueError):
            parser("FOUR")


class TupleTests(unittest.TestCase):
    def test_tupleof_valid(self):
        parser = config.TupleOf(config.Integer)
        self.assertEqual(parser("1,2,3"), [1, 2, 3])
        self.assertEqual(parser("4, 5, 6"), [4, 5, 6])

    def test_tupleof_invalid(self):
        parser = config.TupleOf(config.Integer)

        with self.assertRaises(ValueError):
            parser("")

        with self.assertRaises(ValueError):
            parser("a, b")


class OptionalTests(unittest.TestCase):
    def test_optional_exists(self):
        parser = config.Optional(config.Integer)
        self.assertEqual(parser("33"), 33)

    def test_optional_default(self):
        parser = config.Optional(config.Integer)
        self.assertEqual(parser(""), None)

    def test_optional_invalid(self):
        parser = config.Optional(config.Integer)
        with self.assertRaises(ValueError):
            parser("asdf")


class TestParseConfig(unittest.TestCase):
    def setUp(self):
        self.config = {
            "simple": "oink",
            "foo.bar": "33",
            "foo.baz": "a cool guy",
            "noo.bar": "",
            "deep.so.deep": "very",
        }

    def test_simple_config(self):
        result = config.parse_config(self.config, {
            "simple": config.String,

            "foo": {
                "bar": config.Integer,
            },

            "noo": {
                "bar": config.Optional(config.String, default=""),
            },

            "deep": {
                "so": {
                    "deep": config.String,
                },
            },
        })

        self.assertEqual(result.simple, "oink")
        self.assertEqual(result.foo.bar, 33)
        self.assertEqual(result.noo.bar, "")
        self.assertEqual(result.deep.so.deep, "very")

    def test_missing_key(self):
        with self.assertRaises(config.ConfigurationError):
            config.parse_config(self.config, {
                "foo": {
                    "not_here": config.Integer,
                },
            })

    def test_bad_value(self):
        with self.assertRaises(config.ConfigurationError):
            config.parse_config(self.config, {
                "foo": {
                    "baz": config.Integer,
                },
            })

    def test_dot_in_key(self):
        with self.assertRaises(AssertionError):
            config.parse_config(self.config, {
                "foo.bar": {},
            })

    def test_spec_contains_invalid_object(self):
        with self.assertRaises(AssertionError):
            config.parse_config(self.config, {
                "tree_people": 37,
            })
