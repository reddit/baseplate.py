"""Memcache serialization/deserialization (and compression) helper methods.

Memcached can only store strings, so to store arbitrary objects we need to
serialize them to strings and be able to deserialize them back to their
original form.

New services should use dump_and_compress() and decompress_and_load().

Services that need to read and write to the same memcache instances as r2
should use pickle_and_compress() and decompress_and_unpickle().

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import json
import zlib

from ..._compat import (
    long,
    pickle,
    string_types,
)


class Flags(object):
    """Memcached client flags

    Flags are an arbitrary 16-bit unsigned integer that the memcache server
    stores along with the data and sends back when the item is retrieved.
    Clients may use this as a bit field to store data-specific information;
    this field is opaque to the server.

    """

    JSON = 1 << 0
    INTEGER = 1 << 1
    LONG = 1 << 2
    ZLIB = 1 << 3


def decompress_and_load(key, serialized, flags):
    """Deserialize data.

    This should be paired with
    :py:func:`~baseplate.context.memcache.lib.make_dump_and_compress_fn`.

    :param str key: the memcached key.
    :param str serialized: the serialized object returned from memcached.
    :param int flags: value stored and returned from memcached for the client
        to use to indicate how the value was serialized.
    :returns: The deserialized value.

    """

    if flags & Flags.ZLIB:
        serialized = zlib.decompress(serialized)
        flags ^= Flags.ZLIB

    if flags == 0:
        return serialized
    elif flags == Flags.INTEGER:
        # python3 doesn't have a long integer type, so all integers are written
        # with Flags.INTEGER. This means that a value written by a python3
        # client can exceed the maximum integer value (sys.maxint). This
        # appears to be ok--python2 will automatically convert to long if the
        # value is too large.
        return int(serialized)
    elif flags == Flags.LONG:
        return long(serialized)
    elif flags == Flags.JSON:
        try:
            return json.loads(serialized)
        except ValueError:
            logging.info('json error', exc_info=True)
            return None
    else:
        logging.info('unrecognized flags')
        return serialized


def make_dump_and_compress_fn(min_compress_length=0, compress_level=1):
    """Make a serializer.

    This should be paired with
    :py:func:`~baseplate.context.memcache.lib.decompress_and_load`.

    The resulting method is a chain of :py:func:`json.loads` and ``zlib``
    compression. Values that are not JSON serializable will result in a
    :py:exc:`TypeError`.

    :param int min_compress_length: the minimum serialized string length to
        enable zlib compression. 0 disables compression.
    :param int compress_level: zlib compression level. 0 disables compression
        and 9 is the maximum value.
    :returns: The serializer.

    """

    assert min_compress_length >= 0
    assert 0 <= compress_level <= 9

    def dump_and_compress(key, value):
        """Serialization method compatible with decompress_and_load().

        :param str key: the memcached key.
        :param value: python object to be serialized and set to memcached.
        :returns: value serialized as str, flags int.
        :raises ValueError: if `value` is not JSON serializable

        """

        if isinstance(value, string_types):
            serialized = value
            flags = 0
        elif isinstance(value, int):
            serialized = "%d" % value
            flags = Flags.INTEGER
        elif isinstance(value, long):
            serialized = "%d" % value
            flags = Flags.LONG
        else:
            # NOTE: json.dumps raises ValueError if `value` is not serializable
            serialized = json.dumps(value)
            flags = Flags.JSON

        if (compress_level and
                min_compress_length and
                len(serialized) > min_compress_length):
            compressed = zlib.compress(serialized, compress_level)
            flags |= Flags.ZLIB
            return compressed, flags
        else:
            return serialized, flags

    return dump_and_compress


class PickleFlags(object):
    """Memcached client flags

    Flags are an arbitrary 16-bit unsigned integer that the memcache server
    stores along with the data and sends back when the item is retrieved.
    Clients may use this as a bit field to store data-specific information;
    this field is opaque to the server.

    """

    PICKLE = 1 << 0
    INTEGER = 1 << 1
    LONG = 1 << 2
    ZLIB = 1 << 3


def decompress_and_unpickle(key, serialized, flags):
    """Deserialize data stored by ``pylibmc``.

    .. warning:: This should only be used when sharing caches with applications
        using ``pylibmc`` (like r2).  New applications should use the safer and
        future proofed
        :py:func:`~baseplate.context.memcache.lib.decompress_and_load`.

    :param str key: the memcached key.
    :param str serialized: the serialized object returned from memcached.
    :param int flags: value stored and returned from memcached for the client
        to use to indicate how the value was serialized.
    :returns str value: the deserialized value.

    """

    if flags & PickleFlags.ZLIB:
        serialized = zlib.decompress(serialized)
        flags ^= PickleFlags.ZLIB

    if flags == 0:
        return serialized
    elif flags == PickleFlags.INTEGER:
        # python3 doesn't have a long integer type, so all integers are written
        # with PickleFlags.INTEGER. This means that a value written by a python3
        # client can exceed the maximum integer value (sys.maxint). This
        # appears to be ok--python2 will automatically convert to long if the
        # value is too large.
        return int(serialized)
    elif flags == PickleFlags.LONG:
        return long(serialized)
    elif flags == PickleFlags.PICKLE:
        try:
            return pickle.loads(serialized)
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None
    else:
        logging.info('unrecognized flags')
        return serialized


def make_pickle_and_compress_fn(min_compress_length=0, compress_level=1):
    """Make a serializer compatible with ``pylibmc`` readers.

    The resulting method is a chain of :py:func:`pickle.dumps` and ``zlib``
    compression. This should be paired with
    :py:func:`~baseplate.context.memcache.lib.decompress_and_unpickle`.

    .. warning:: This should only be used when sharing caches with applications
        using ``pylibmc`` (like r2).  New applications should use the safer and
        future proofed
        :py:func:`~baseplate.context.memcache.lib.make_dump_and_compress_fn`.

    :param int min_compress_length: the minimum serialized string length to
        enable zlib compression. 0 disables compression.
    :param int compress_level: zlib compression level. 0 disables compression
        and 9 is the maximum value.
    :returns func memcache_serializer: the serializer method.

    """

    assert min_compress_length >= 0
    assert 0 <= compress_level <= 9

    def pickle_and_compress(key, value):
        """Serialization method compatible with decompress_and_unpickle().

        :param str key: the memcached key.
        :param value: python object to be serialized and set to memcached.
        :returns: value serialized as str, flags int.

        """

        if isinstance(value, string_types):
            serialized = value
            flags = 0
        elif isinstance(value, int):
            serialized = "%d" % value
            flags = PickleFlags.INTEGER
        elif isinstance(value, long):
            serialized = "%d" % value
            flags = PickleFlags.LONG
        else:
            # use protocol 2 which is the highest value supported by python2
            serialized = pickle.dumps(value, protocol=2)
            flags = PickleFlags.PICKLE

        if (compress_level and
                min_compress_length and
                len(serialized) > min_compress_length):
            compressed = zlib.compress(serialized, compress_level)
            flags |= PickleFlags.ZLIB
            return compressed, flags
        else:
            return serialized, flags

    return pickle_and_compress
