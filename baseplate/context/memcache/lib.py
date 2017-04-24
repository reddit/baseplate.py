import logging
import zlib

from ..._compat import (
    long,
    pickle,
    string_types,
)


"""Memcache serialization/deserialization (and compression) helper methods.

Memcached can only store strings, so to store arbitrary objects we need to
serialize them to strings and be able to deserialize them back to their
original form. General purpose serialization and deserialization can be
achieved with pickle_and_compress() and decompress_and_unpickle().

"""


FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2
FLAG_ZLIB = 1 << 3


def decompress_and_unpickle(key, serialized, flags):
    """Deserialization method compatible with pickle_and_compress().

    :param str key: the memcached key.
    :param str serialized: the serialized object returned from memcached.
    :param int flags: value stored and returned from memcached for the client
        to use to indicate how the value was serialized.
    :returns str value: the deserialized value.

    """

    if flags & FLAG_ZLIB:
        serialized = zlib.decompress(serialized)
        flags ^= FLAG_ZLIB

    if flags == 0:
        return serialized
    elif flags == FLAG_INTEGER:
        # python3 doesn't have a long integer type, so all integers are written
        # with FLAG_INTEGER. This means that a value written by a python3
        # client can exceed the maximum integer value (sys.maxint). This
        # appears to be ok--python2 will automatically convert to long if the
        # value is too large.
        return int(serialized)
    elif flags == FLAG_LONG:
        return long(serialized)
    else:
        try:
            return pickle.loads(serialized)
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None


def make_pickle_and_compress_fn(min_compress_length=0, compress_level=1):
    """Create a serialization method compatible with decompress_and_unpickle().

    The resulting method is a chain of pickling and zlib compression.

    This serializer is compatible with pylibmc.

    :param int min_compress_length: the minimum serialized string length to
        enable zlib compression. 0 disables compression.
    :param int compress_level: zlib compression level. 0 disables compression
        and 9 is the maximum value.
    :returns func memcache_serializer: the serializer method.

    """

    assert min_compress_length >= 0
    assert 0 <= compress_level <= 9

    def pickle_and_compress(key, value):
        """Serialization method compatible with memcache_deserializer.

        :param str key: the memcached key.
        :param value: python object to be serialized and set to memcached.
        :returns: value serialized as str, flags int.

        """

        if isinstance(value, string_types):
            serialized = value
            flags = 0
        elif isinstance(value, int):
            serialized = "%d" % value
            flags = FLAG_INTEGER
        elif isinstance(value, long):
            serialized = "%d" % value
            flags = FLAG_LONG
        else:
            # use protocol 2 which is the highest value supported by python2
            serialized = pickle.dumps(value, protocol=2)
            flags = FLAG_PICKLE

        if (compress_level and
                min_compress_length and
                len(serialized) > min_compress_length):
            compressed = zlib.compress(serialized, compress_level)
            flags |= FLAG_ZLIB
            return compressed, flags
        else:
            return serialized, flags

    return pickle_and_compress
