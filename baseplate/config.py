# pylint: disable=invalid-name
"""Configuration parsing and validation.

This module provides ``parse_config`` which turns a dictionary of stringy keys
and values into a structured and typed configuration object.

For example, an INI file like the following:

.. highlight:: ini

.. include:: ../config_example.ini
   :literal:

Might be parsed like the following. Note: when running under the baseplate
server, The ``config_parser.items(...)`` step is taken care of for you and
``raw_config`` is passed as the only argument to your factory function.

.. highlight:: py

.. testsetup:: overview

    from baseplate import config
    from baseplate._compat import configparser
    from tempfile import NamedTemporaryFile
    config_parser = configparser.RawConfigParser()
    config_parser.readfp(open("docs/config_example.ini"))

    tempfile = NamedTemporaryFile()
    tempfile.write("cool")
    tempfile.flush()
    config_parser.set("app:main", "some_file", tempfile.name)

.. doctest:: overview

    >>> raw_config = dict(config_parser.items("app:main"))

    >>> CARDS = config.OneOf(clubs=1, spades=2, diamonds=3, hearts=4)
    >>> cfg = config.parse_config(raw_config, {
    ...     "simple": config.Boolean,
    ...     "cards": config.TupleOf(CARDS),
    ...     "nested": {
    ...         "once": config.Integer,
    ...
    ...         "really": {
    ...             "deep": config.Timespan,
    ...         },
    ...     },
    ...     "some_file": config.File(mode="r"),
    ...     "optional": config.Optional(config.Integer, default=9001),
    ...     "sample_rate": config.Percent,
    ...     "interval": config.Fallback(config.Timespan, config.Integer),
    ... })

    >>> print(cfg.simple)
    True

    >>> print(cfg.cards)
    [1, 2, 3]

    >>> print(cfg.nested.really.deep)
    0:00:03

    >>> cfg.some_file.read()
    'cool'
    >>> cfg.some_file.close()

    >>> cfg.sample_rate
    0.371

    >>> print(cfg.interval)
    0:00:30

.. testcleanup:: overview

    tempfile.close()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import collections
import datetime
import functools
import grp
import pwd
import re
import socket


class ConfigurationError(Exception):
    """Raised when the configuration violates the spec."""
    def __init__(self, key, error):
        super(ConfigurationError, self).__init__(
            "{}: {}".format(key, error))
        self.key = key
        self.error = error


def String(text):
    """A raw string."""
    if not text:
        raise ValueError("no value specified")
    return text


def Float(text):
    """A floating-point number."""
    return float(text)


def Integer(text=None, base=10):
    """An integer.

    To prevent mistakes, this will raise an error if the user attempts
    to configure a non-whole number.

    :param int base: (Optional) If specified, the base of the integer to parse.

    """
    if text is not None:
        # this allows the original config.Integer format
        return int(text, base=base)
    else:
        # and this allows the base to be specified as config.Integer(base=N)
        return functools.partial(int, base=base)


def Boolean(text):
    """True or False, case insensitive."""
    parser = OneOf(true=True, false=False)
    return parser(text.lower())


InternetAddress = collections.namedtuple("InternetAddress", ("host", "port"))


EndpointConfiguration_ = collections.namedtuple(
    "EndpointConfiguration", ("family", "address"))


class EndpointConfiguration(EndpointConfiguration_):
    """A description of a remote endpoint.

    This is a 2-tuple of (``family`` and ``address``).

    ``family``
        One of :py:data:`socket.AF_INET` or :py:data:`socket.AF_UNIX`.

    ``address``
        An address appropriate for the ``family``.

    .. seealso:: :py:func:`baseplate.config.Endpoint`

    """


def Endpoint(text):
    """A remote endpoint to connect to.

    Returns an :py:class:`EndpointConfiguration`.

    If the endpoint is a hostname:port pair, the ``family`` will be
    :py:data:`socket.AF_INET` and ``address`` will be a two-tuple of host and
    port, as expected by :py:mod:`socket`.

    If the endpoint contains a slash (``/``), it will be interpreted as a path
    to a UNIX domain socket. The ``family`` will be :py:data:`socket.AF_UNIX`
    and ``address`` will be the path as a string.

    """
    if not text:
        raise ValueError("no endpoint specified")

    if "/" in text:
        return EndpointConfiguration(socket.AF_UNIX, text)
    else:
        host, sep, port = text.partition(":")
        if sep != ":":
            raise ValueError("no port specified")
        address = InternetAddress(host, int(port))
        return EndpointConfiguration(socket.AF_INET, address)


def Base64(text):
    """A base64 encoded block of data.

    This is useful for arbitrary binary blobs.

    """
    if not text:
        raise ValueError("expected base64 encoded data")

    try:
        return base64.b64decode(text)
    except TypeError as exc:
        raise ValueError(*exc.args)


def File(mode="r"):
    """A path to a file.

    This takes a path to a file and returns an open file object, like
    returned by :py:func:`open`.

    :param str mode: an optional string that specifies the mode in
        which the file is opened.

    """
    def open_file(text):
        try:
            return open(text, mode=mode)
        except IOError:
            raise ValueError("could not open file: %s" % text)
    return open_file


def Timespan(text):
    """A span of time.

    This takes a string of the form "1 second" or "3 days" and returns a
    :py:class:`datetime.timedelta` representing that span of time.

    Units supported are: milliseconds, seconds, minutes, hours, days.

    """
    scale_by_unit = {
        "millisecond": 0.001,
        "second": 1,
        "minute": 60,
        "hour": 60 * 60,
        "day": 24 * 60 * 60,
    }

    parts = text.split()
    if len(parts) != 2:
        raise ValueError("invalid specification")
    count, unit = parts

    count = int(count)
    unit = unit.rstrip("s")  # depluralize

    try:
        scale = scale_by_unit[unit]
    except KeyError:
        raise ValueError("unknown unit")

    return datetime.timedelta(seconds=count * scale)


def Percent(text):
    """A percentage.

    This takes a string of the form "37.2%" or "44%" and
    returns a float in the range [0.0, 1.0].

    """

    if not text.endswith("%"):
        raise ValueError("the value is not a percentage")

    percentage = float(text[:-1]) / 100.

    if not 0 <= percentage <= 1:
        raise ValueError("percentage is out of valid range")

    return percentage


def UnixUser(text):
    """A Unix user name.

    The parsed value will be the integer user ID.

    """
    try:
        return pwd.getpwnam(text).pw_uid
    except KeyError as exc:
        raise ValueError(exc)


def UnixGroup(text):
    """A Unix group name.

    The parsed value will be the integer group ID.

    """
    try:
        return grp.getgrnam(text).gr_gid
    except KeyError as exc:
        raise ValueError(exc)


def OneOf(**options):
    """One of several choices.

    For each ``option``, the name is what should be in the configuration file
    and the value is what it is mapped to.

    For example::

        OneOf(hearts="H", spades="S")

    would parse::

        "hearts"

    into::

        "H"

    """
    def one_of(text):
        try:
            return options[text]
        except KeyError:
            raise ValueError("expected one of {!r}".format(options.keys()))
    return one_of


def TupleOf(T):
    """A comma-delimited list of type T.

    At least one value must be provided. If you want an empty list
    to be a valid choice, wrap with :py:func:`Optional`.

    """
    def tuple_of(text):
        if not text:
            raise ValueError("no values provided")
        split = text.split(",")
        stripped = [item.strip() for item in split]
        return [T(item) for item in stripped if item]
    return tuple_of


def Optional(T, default=None):
    """An option of type T, or ``default`` if not configured."""
    def optional(text):
        if text:
            return T(text)
        else:
            return default
    return optional


def Fallback(T1, T2):
    """An option of type T1, or if that fails to parse, of type T2.

    This is useful for backwards-compatible configuration changes.

    """
    def fallback(text):
        try:
            return T1(text)
        except ValueError:
            return T2(text)
    return fallback


class ConfigNamespace(dict):
    def __init__(self):
        super(ConfigNamespace, self).__init__()
        self.__dict__ = self


class Parser(object):
    """Base for config parsers."""

    @staticmethod
    def from_spec(spec):
        """Return a parser for the given spec object."""
        if isinstance(spec, Parser):
            return spec
        elif isinstance(spec, dict):
            return SpecParser(spec)
        elif callable(spec):
            return CallableParser(spec)
        else:
            raise AssertionError("invalid specification: %r" % spec)

    def parse(self, key_path, raw_config):
        """Parse and return the relevant info for a given key.

        :param str key_path: The key this parser is looking for.
        :param dict raw_config: The full raw configuration dictionary.

        """
        raise NotImplementedError


class SpecParser(Parser):
    """A parser that validates a static specification."""

    def __init__(self, spec):
        self.spec = spec

    def parse(self, root, raw_config):
        parsed = ConfigNamespace()
        for key, spec in self.spec.items():
            assert "." not in key, "dots are not allowed in keys"

            if root:
                key_path = "%s.%s" % (root, key)
            else:
                key_path = key

            parser = Parser.from_spec(spec)
            parsed[key] = parser.parse(key_path, raw_config)
        return parsed


class CallableParser(Parser):
    """A parser that wraps a simple callable."""

    def __init__(self, callable):
        self.callable = callable

    def parse(self, key_path, raw_config):
        raw_value = raw_config.get(key_path, "")

        try:
            return self.callable(raw_value)
        except Exception as exc:
            raise ConfigurationError(key_path, exc)


class DictOf(Parser):
    """A group of options of a given type.

    This is useful for providing data to the application without the
    application having to know ahead of time all of the possible keys.

    .. highlight:: ini

    .. include:: ../config_dictof_example.ini
       :literal:

    .. highlight:: py

    .. testsetup:: dictof_simple

        from baseplate import config
        from baseplate._compat import configparser
        config_parser = configparser.RawConfigParser()
        config_parser.readfp(open("docs/config_dictof_example.ini"))
        raw_config = dict(config_parser.items("app:main"))

    .. doctest:: dictof_simple

        >>> cfg = config.parse_config(raw_config, {
        ...     "population": config.DictOf(config.Integer),
        ... })

        >>> len(cfg.population)
        5

        >>> cfg.population["br"]
        207645000

    It can also be combined with other configuration specs or parsers to parse
    more complicated structures:

    .. highlight:: ini

    .. include:: ../config_dictof_spec_example.ini
       :literal:

    .. highlight:: py

    .. testsetup:: dictof_spec

        from baseplate import config
        from baseplate._compat import configparser
        config_parser = configparser.RawConfigParser()
        config_parser.readfp(open("docs/config_dictof_spec_example.ini"))
        raw_config = dict(config_parser.items("app:main"))

    .. doctest:: dictof_spec

        >>> cfg = config.parse_config(raw_config, {
        ...     "countries": config.DictOf({
        ...         "population": config.Integer,
        ...         "capital": config.String,
        ...     }),
        ... })

        >>> len(cfg.countries)
        5

        >>> cfg.countries["cn"].capital
        'Beijing'

        >>> cfg.countries["id"].population
        263447000

    """

    def __init__(self, spec):
        self.subparser = Parser.from_spec(spec)

    def parse(self, key_path, raw_config):
        # match keys that start out with the prefix we expect (key_path) and
        # extract the subkey from the.key.prefix.{subkey}.the.rest
        if key_path:
            root = key_path + "."
        else:
            root = ""
        matcher = re.compile("^" + root.replace(".", r"\.") + r"([^.]+)")

        values = ConfigNamespace()
        seen_subkeys = set()
        for key in raw_config:
            m = matcher.search(key)
            if not m:
                continue

            subkey = m.group(1)
            if subkey in seen_subkeys:
                continue

            full_path = root + subkey
            values[subkey] = self.subparser.parse(full_path, raw_config)
            seen_subkeys.add(subkey)
        return values


def parse_config(config, spec):
    """Parse options against a spec and return a structured representation.

    :param dict config: The raw stringy configuration dictionary.
    :param dict spec: A specification of what the config should look like.
    :raises: :py:exc:`ConfigurationError` The configuration violated the spec.
    :return: A structured configuration object.

    """
    parser = Parser.from_spec(spec)
    return parser.parse("", config)
