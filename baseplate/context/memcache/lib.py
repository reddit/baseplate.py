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

    if flags == 0:
        return serialized

    if flags & FLAG_INTEGER:
        return int(serialized)

    if flags & FLAG_LONG:
        return long(serialized)

    if flags & FLAG_PICKLE:
        try:
            return pickle.loads(serialized)
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None

    return serialized


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

        flags = 0

        if isinstance(value, string_types):
            serialized = value
        elif isinstance(value, int):
            flags |= FLAG_INTEGER
            serialized = "%d" % value
        elif isinstance(value, long):
            flags |= FLAG_LONG
            serialized = "%d" % value
        else:
            flags |= FLAG_PICKLE

            # use protocol 2 which is the highest value supported by python2
            serialized = pickle.dumps(value, protocol=2)

        if (compress_level and
                min_compress_length and
                len(serialized) > min_compress_length):
            compressed = zlib.compress(serialized, compress_level)
            flags |= FLAG_ZLIB
            return compressed, flags
        else:
            return serialized, flags

    return pickle_and_compress
