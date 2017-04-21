from six import string_types
import logging
import zlib


try:
    import cPickle as pickle
except ImportError:
    # python3
    import pickle

try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        # python3
        from io import StringIO


FLAG_PICKLE = 1 << 0
FLAG_INTEGER = 1 << 1
FLAG_LONG = 1 << 2


# python3 doesn't have a long integer class
try:
    long
except NameError:
    # python3
    long = int
    # don't override FLAG_LONG because we want to be able to deserialize values
    # set by python2 processes. If a python3 process writes a large value as
    # an integer a python3 process will still be able to deserialize it.


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
        output = StringIO()

        # override pickler protocol
        pickler = pickle.Pickler(output, protocol=-1)
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
            buf = StringIO(value)
            unpickler = pickle.Unpickler(buf)
            return unpickler.load()
        except Exception:
            logging.info('Pickle error', exc_info=True)
            return None

    return value


# These serde methods are compatible with pylibmc's implementation.

# extra bit flag from pylibmc, just grabbing the one we need which pymemcache's
# serde.py doesn't have (currently on 1.3.2, but later versions do have this)
FLAG_ZLIB = 1 << 3


def memcache_deserializer(key, value, flags):
    if flags & FLAG_ZLIB:
        value = zlib.decompress(value)
    return python_memcache_deserializer(key, value, flags)


def make_memcache_serializer(min_compress_length=0, compress_level=1):
    def memcache_serializer(key, value):
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
