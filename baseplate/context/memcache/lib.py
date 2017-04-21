import logging
import zlib

from ..._compat import (
    BytesIO,
    long,
    pickle,
    string_types,
)


"""Memcache serde helper methods"""


FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2


def python_memcache_serializer(key, value):
    flags = 0

    if isinstance(value, string_types):
        pass
    elif isinstance(value, int):
        flags |= FLAG_INTEGER
        value = "%d" % value
    elif isinstance(value, long):
        flags |= FLAG_LONG
        value = "%d" % value
    else:
        flags |= FLAG_PICKLE
        output = BytesIO()

        # use protocol 2 which is the highest value supported by python2
        pickler = pickle.Pickler(output, protocol=2)
        pickler.dump(value)
        value = output.getvalue()

    return value, flags


def python_memcache_deserializer(key, value, flags):
    if flags == 0:
        return value

    if flags & FLAG_INTEGER:
        return int(value)

    if flags & FLAG_LONG:
        return long(value)

    if flags & FLAG_PICKLE:
        try:
            buf = BytesIO(value)
            unpickler = pickle.Unpickler(buf)
            return unpickler.load()
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None

    return value


FLAG_ZLIB = 1 << 3


def memcache_deserializer(key, value, flags):
    """Deserialization method compatible with make_memcache_serializer.

    :param str key: the memcached key.
    :param str value: the serialized object returned from memcached.
    :param int flags: value stored and returned from memcached for the client
        to use to indicate how the value was serialized.
    :returns str value: the deserialized value. 

    """

    if flags & FLAG_ZLIB:
        value = zlib.decompress(value)
    return python_memcache_deserializer(key, value, flags)


def make_memcache_serializer(min_compress_length=0, compress_level=1):
    """Create a serialization method compatible with memcache_deserializer().

    The resulting method is a chain of python_memcache_serializer (to convert
    arbitrary python objects to str) and zlib compression.

    This serializer is compatible with pylibmc.

    :param int min_compress_length: the minimum serialized string length to
        enable zlib compression. 0 disables compression.
    :param int compress_level: zlib compression level. 0 disables compression
        and 9 is the maximum value.
    :returns func memcache_serializer: the serializer method.

    """

    assert min_compress_length >= 0
    assert 0 <= compress_level <= 9

    def memcache_serializer(key, value):
        """Serialization method compatible with memcache_deserializer.

        :param str key: the memcached key.
        :param value: python object to be serialized and set to memcached.
        :returns: value serialized as str, flags int.

        """

        serialized, flags = python_memcache_serializer(key, value)

        if (compress_level and
                min_compress_length and
                len(serialized) > min_compress_length):
            compressed = zlib.compress(serialized, compress_level)
            flags |= FLAG_ZLIB
            return compressed, flags
        else:
            return serialized, flags
    return memcache_serializer
