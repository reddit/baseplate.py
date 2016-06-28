"""Application metrics via statsd.

A client for the application metrics aggregator statsd_. Metrics sent to
statsd are aggregated and written to graphite. Statsd is generally used for
whole-system health monitoring and insight into usage patterns.

.. testsetup::

    app_config = {"metrics.endpoint": "", "metrics.namespace": "example"}
    do_something = lambda: ()
    do_something_else = lambda: ()
    do_another_thing = lambda: ()

Basic example usage:

.. testcode::

    from baseplate import make_metrics_client

    client = make_metrics_client(app_config)
    client.counter("events.connect").increment()
    client.gauge("connections").increment()
    client.gauge("workers").replace(4)

    with client.timer("something.todo"):
        do_something()
        do_something_else()

If you have multiple metrics to send, you can batch them up for efficiency:

.. testcode::

    with client.batch() as batch:
        batch.counter("froozles").increment()
        batch.counter("blargs").decrement(delta=3)

        with batch.timer("something"):
            do_another_thing()

and the batch will be sent in as few packets as possible when the `with` block
ends.

.. _statsd: https://github.com/etsy/statsd

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import socket
import time


logger = logging.getLogger(__name__)


def _metric_join(*nodes):
    return b".".join(node.strip(b".") for node in nodes)


class Transport(object):
    def send(self, serialized_metric):
        raise NotImplementedError


class NullTransport(Transport):
    """A transport which doesn't send messages at all."""
    def send(self, serialized_metric):
        for metric_line in serialized_metric.splitlines():
            logger.debug("Would send metric %r", metric_line)


class RawTransport(Transport):
    """A transport which sends messages on a socket."""
    def __init__(self, endpoint):
        self.socket = socket.socket(endpoint.family, socket.SOCK_DGRAM)
        self.socket.connect(endpoint.address)

    def send(self, serialized_metric):
        self.socket.sendall(serialized_metric)


class BufferedTransport(object):
    """A transport which wraps another transport and buffers before sending."""
    def __init__(self, transport):
        self.transport = transport
        self.buffer = []

    def send(self, serialized_metric):
        self.buffer.append(serialized_metric)

    def flush(self):
        # TODO: warn when buffer length reaches MTU
        # TODO: run-length compression
        metrics, self.buffer = self.buffer, []
        message = b"\n".join(metrics)
        self.transport.send(message)


class BaseClient(object):
    def __init__(self, transport, namespace):
        self.transport = transport
        self.namespace = namespace.encode("ascii")

    def timer(self, name):
        """Return a Timer with the given name.

        :param str name: The name the timer should have.

        :rtype: :py:class:`Timer`

        """
        timer_name = _metric_join(self.namespace, name.encode("ascii"))
        return Timer(self.transport, timer_name)

    def counter(self, name):
        """Return a Counter with the given name.

        The sample rate is currently up to your application to enforce.

        :param str name: The name the counter should have.

        :rtype: :py:class:`Counter`

        """
        counter_name = _metric_join(self.namespace, name.encode("ascii"))
        return Counter(self.transport, counter_name)

    def gauge(self, name):
        """Return a Gauge with the given name.

        :param str name: The name the gauge should have.

        :rtype: :py:class:`Gauge`

        """
        gauge_name = _metric_join(self.namespace, name.encode("ascii"))
        return Gauge(self.transport, gauge_name)


class Client(BaseClient):
    """A client for statsd."""

    def batch(self):
        """Return a client-like object which batches up metrics.

        Batching metrics can reduce the number of packets that are sent to
        the stats aggregator.

        :rtype: :py:class:`Batch`

        """
        return Batch(self.transport, self.namespace)


class Batch(BaseClient):
    """A batch of metrics to send to statsd.

    The batch also supports the `context manager protocol`_, for use with
    Python's ``with`` statement. When the context is exited, the batch will
    automatically :py:meth:`flush`.

    .. _context manager protocol:
        https://docs.python.org/3/reference/datamodel.html#context-managers

    """

    # pylint: disable=super-init-not-called
    def __init__(self, transport, namespace):
        self.transport = BufferedTransport(transport)
        self.namespace = namespace

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        self.flush()

    def flush(self):
        """Immediately send the batched metrics."""
        self.transport.flush()


class Timer(object):
    """A timer for recording elapsed times.

    The timer also supports the `context manager protocol`_, for use with
    Python's ``with`` statement. When the context is entered the timer will
    :py:meth:`start` and when exited, the timer will automatically
    :py:meth:`stop`.

    .. _context manager protocol:
        https://docs.python.org/3/reference/datamodel.html#context-managers

    """

    def __init__(self, transport, name):
        self.transport = transport
        self.name = name

        self.start_time = None
        self.stopped = False

    def start(self):
        """Record the current time as the start of the timer."""
        assert not self.start_time, "timer already started"
        assert not self.stopped, "time already stopped"

        self.start_time = time.time()

    def stop(self):
        """Stop the timer and record the total elapsed time."""
        assert self.start_time, "timer not started"
        assert not self.stopped, "time already stopped"

        now = time.time()
        elapsed = (now - self.start_time) * 1000.
        serialized = self.name + (":{:g}|ms".format(elapsed).encode())
        self.transport.send(serialized)

        self.stopped = True

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, value, traceback):
        self.stop()


class Counter(object):
    """A counter for counting events over time."""

    def __init__(self, transport, name):
        self.transport = transport
        self.name = name

    def increment(self, delta=1, sample_rate=1.0):
        """Increment the counter.

        :param float delta: The amount to increase the counter by.
        :param float sample_rate: What rate this counter is sampled at. [0-1].

        """
        parts = [
            self.name + (":{:g}".format(delta).encode()),
            b"c",
        ]

        if sample_rate and sample_rate != 1.0:
            parts.append("@{:g}".format(sample_rate).encode())

        serialized = b"|".join(parts)
        self.transport.send(serialized)

    def decrement(self, delta=1, sample_rate=1.0):
        """Decrement the counter.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(delta=-delta, sample_rate=sample_rate)


class Gauge(object):
    """A gauge representing an arbitrary value.

    Gauges maintain their value over time if not updated. They can be changed
    by relative amounts or have their values wholesale replaced.

    """
    def __init__(self, transport, name):
        self.transport = transport
        self.name = name

    def increment(self, delta=1):
        """Increment the value of the gauge.

        This will change the value of the gauge relative to what it was before.

        :param float delta: The amount to change the gauge by.

        """
        serialized = self.name + (":{:+g}|g".format(delta).encode())
        self.transport.send(serialized)

    def decrement(self, delta=1):
        """Decrement the value of the gauge.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(-delta)

    def replace(self, new_value):
        """Replace the value held by the gauge.

        This will replace the value held by the gauge with no concern for its
        previous value.

        .. note:: Due to the way the protocol works, it is not possible to
            replace gauge values with negative numbers.

        :param float new_value: The new value to store in the gauge.

        """
        assert new_value >= 0, "gauges cannot be replaced with negative numbers"
        serialized = self.name + (":{:g}|g".format(new_value).encode())
        self.transport.send(serialized)


def make_client(namespace, endpoint):
    """Return a configured client.

    :param str namespace: The root key to namespace all metrics under.
    :param baseplate.config.EndpointConfiguration endpoint: The endpoint to
        send metrics to or :py:data:`None`.  If :py:data:`None`, the returned
        client will discard all metrics.
    :return: A configured client.
    :rtype: :py:class:`baseplate.metrics.Client`

    .. seealso:: :py:func:`baseplate.make_metrics_client`.

    """

    if endpoint:
        transport = RawTransport(endpoint)
    else:
        transport = NullTransport()
    return Client(transport, namespace)
