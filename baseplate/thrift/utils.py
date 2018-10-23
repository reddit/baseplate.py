"""Helpers for working with Thrift."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from thrift.transport.TTransport import TMemoryBuffer


def serialize_thrift(protocol_factory, thrift_object):
    """Serialize a given Thrift object to bytes.

    :param protocol_factory: A factory which will make a Thrift protocol that
        will be used to serialize the payload.
    :param thrift_object: A Thrift object to serialize.
    :returns bytes: Raw bytes representing the Thrift object.

    """
    transport = TMemoryBuffer()
    protocol = protocol_factory.getProtocol(transport)
    thrift_object.write(protocol)
    return transport.getvalue()


def deserialize_thrift(protocol_factory, data, thrift_object):
    """Deserialize bytes into a given Thrift object.

    :param protocol_factory: A factory which will make a Thrift protocol that
        will be used to deserialize the payload.
    :param bytes data: Raw bytes to deserialize.
    :param thrift_object: A Thrift object to deserialize the payload into. All
        its fields will be overwritten.

    """
    transport = TMemoryBuffer(data)
    protocol = protocol_factory.getProtocol(transport)
    thrift_object.read(protocol)
    return thrift_object
