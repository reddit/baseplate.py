import socket
import tempfile
import unittest

from unittest.mock import patch

from baseplate.lib import config


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

    def test_non_decimal(self):
        result = config.Integer(base=8)("0600")
        self.assertEqual(result, 384)


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


class FileTests(unittest.TestCase):
    def setUp(self):
        self.tempfile = tempfile.NamedTemporaryFile()
        self.tempfile.write(b"test")
        self.tempfile.flush()

    def tearDown(self):
        self.tempfile.close()

    def test_no_file(self):
        file_opener = config.File()
        with self.assertRaises(ValueError):
            file_opener("/tmp/does_not_exist")

    def test_read_file(self):
        file_opener = config.File()
        the_file = file_opener(self.tempfile.name)
        self.assertEqual(the_file.read(), "test")

    def test_write_file(self):
        file_opener = config.File(mode="w")
        the_file = file_opener(self.tempfile.name)
        the_file.write("cool")
        the_file.close()

        with open(self.tempfile.name) as f:
            self.assertEqual(f.read(), "cool")


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


class TimespanWithLegacyFallbackTests(unittest.TestCase):
    def test_fallback(self):
        result = config.TimespanWithLegacyFallback("30 minutes")
        self.assertEqual(result.total_seconds(), 1800)

        result = config.TimespanWithLegacyFallback("92")
        self.assertEqual(result.total_seconds(), 92)


class PercentTests(unittest.TestCase):
    def test_percentage(self):
        self.assertAlmostEqual(config.Percent("37.2%"), 0.372)
        self.assertAlmostEqual(config.Percent("100%"), 1.0)

    def test_invalid_percentage(self):
        with self.assertRaises(ValueError):
            config.Percent("9")

        with self.assertRaises(ValueError):
            config.Percent("-10%")

        with self.assertRaises(ValueError):
            config.Percent("120%")

        with self.assertRaises(ValueError):
            config.Percent("30%%%%")


class UnixUserTests(unittest.TestCase):
    def test_valid_user(self):
        result = config.UnixUser("root")
        self.assertEqual(result, 0)

    def test_invalid_user(self):
        with self.assertRaises(ValueError):
            config.UnixUser("fhqwhgads")

    def test_uid_fallback(self):
        result = config.UnixUser("1000")
        self.assertEqual(result, 1000)


class UnixGroupTests(unittest.TestCase):
    def test_valid_group(self):
        result = config.UnixGroup("root")
        self.assertEqual(result, 0)

    def test_invalid_group(self):
        with self.assertRaises(ValueError):
            config.UnixGroup("fhqwhgads")

    def test_gid_fallback(self):
        result = config.UnixGroup("1000")
        self.assertEqual(result, 1000)


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


@patch.dict("os.environ", {"BASEPLATE_DEFAULT_VALUE": "default", "NOT_PROVIDED": ""})
class DefaultFromEnvTests(unittest.TestCase):
    def test_use_default_from_env(self):
        parser = config.DefaultFromEnv(config.String, "BASEPLATE_DEFAULT_VALUE")
        self.assertEqual(parser(""), "default")

    def test_empty_default(self):
        parser = config.DefaultFromEnv(config.String, "NOT_PROVIDED")
        self.assertEqual(parser("foo"), "foo")

    def test_use_provided(self):
        parser = config.DefaultFromEnv(config.String, "BASEPALTE_DEFAULT_VALUE")
        self.assertEqual(parser("foo"), "foo")

    def test_provide_none(self):
        parser = config.DefaultFromEnv(config.String, "NOT_PROVIDED")
        self.assertRaises(ValueError, parser, "")

    def test_fallback(self):
        fallback_value = 5
        parser = config.DefaultFromEnv(config.Integer, "NOT_PROVIDED", fallback_value)
        self.assertEqual(parser(""), fallback_value)


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


class FallbackTests(unittest.TestCase):
    def test_primary_option_works(self):
        parser = config.Fallback(config.Percent, config.Float)
        self.assertAlmostEqual(parser("33%"), 0.33)

    def test_fallback_option_works(self):
        parser = config.Fallback(config.Percent, config.Float)
        self.assertAlmostEqual(parser(".44"), 0.44)


class DictOfTests(unittest.TestCase):
    def test_empty(self):
        parser = config.DictOf(config.Integer)
        result = parser.parse("my_key", {"not_related": "a"})
        self.assertEqual(result, {})

    def test_scalar_children(self):
        parser = config.DictOf(config.Integer)
        result = parser.parse("my_key", {"my_key.a": "1", "my_key.b": "2"})
        self.assertEqual(result, {"a": 1, "b": 2})

    def test_vector_children(self):
        parser = config.DictOf({"a": config.Integer, "b": config.String})
        result = parser.parse(
            "my_key",
            {
                "my_key.first.a": "1",
                "my_key.first.b": "test",
                "my_key.first.c": "ignored",
                "my_key.second.a": "2",
                "my_key.second.b": "test",
            },
        )
        self.assertEqual(result, {"first": {"a": 1, "b": "test"}, "second": {"a": 2, "b": "test"}})

    def test_root_level(self):
        parser = config.DictOf({"a": config.Integer, "b": config.String})
        result = parser.parse(
            "",
            {
                "first.a": "1",
                "first.b": "test",
                "first.c": "ignored",
                "second.a": "2",
                "second.b": "test",
            },
        )
        self.assertEqual(result, {"first": {"a": 1, "b": "test"}, "second": {"a": 2, "b": "test"}})


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
        result = config.parse_config(
            self.config,
            {
                "simple": config.String,
                "foo": {"bar": config.Integer},
                "noo": {"bar": config.Optional(config.String, default="")},
                "deep": {"so": {"deep": config.String}},
            },
        )

        self.assertEqual(result.simple, "oink")
        self.assertEqual(result.foo.bar, 33)
        self.assertEqual(result.noo.bar, "")
        self.assertEqual(result.deep.so.deep, "very")

    def test_missing_key(self):
        with self.assertRaises(config.ConfigurationError):
            config.parse_config(self.config, {"foo": {"not_here": config.Integer}})

    def test_bad_value(self):
        with self.assertRaises(config.ConfigurationError):
            config.parse_config(self.config, {"foo": {"baz": config.Integer}})

    def test_dot_in_key(self):
        with self.assertRaises(AssertionError):
            config.parse_config(self.config, {"foo.bar": {}})

    def test_spec_contains_invalid_object(self):
        with self.assertRaises(AssertionError):
            config.parse_config(self.config, {"tree_people": 37})

    def test_subparsers(self):
        result = config.parse_config(self.config, {"foo": config.DictOf(config.String)})
        self.assertEqual(result, {"foo": {"bar": "33", "baz": "a cool guy"}})
