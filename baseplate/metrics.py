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

    from baseplate import metrics_client_from_config

    client = metrics_client_from_config(app_config)
    client.counter("events.connect").increment()
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

from collections import defaultdict


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
        metrics, self.buffer = self.buffer, []
        message = b"\n".join(metrics)
        try:
            self.transport.send(message)
        except socket.error as e:
            logger.error("baseplate metrics batch too large: flush more often \
                or reduce amount done in one request, length %d, %s",
                len(message), e)


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

    def histogram(self, name):
        """Return a Histogram with the given name.

        :param str name: The name the histogram should have.

        :rtype: :py:class:`Histogram`

        """
        histogram_name = _metric_join(self.namespace, name.encode("ascii"))
        return Histogram(self.transport, histogram_name)


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
        self.counters = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        self.flush()

    def flush(self):
        """Immediately send the batched metrics."""
        for name, counter in self.counters.items():
            counter.send()
        self.transport.flush()

    def counter(self, name):
        """Return a BatchCounter with the given name.

        The sample rate is currently up to your application to enforce.

        :param str name: The name the counter should have.

        :rtype: :py:class:`Counter`

        """
        counter_name = _metric_join(self.namespace, name.encode("ascii"))
        batch_counter = self.counters.get(counter_name)
        if batch_counter is None:
            batch_counter = BatchCounter(self.transport, counter_name)
            self.counters[counter_name] = batch_counter

        return batch_counter


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
        self.send(delta, sample_rate)

    def decrement(self, delta=1, sample_rate=1.0):
        """Decrement the counter.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(delta=-delta, sample_rate=sample_rate)

    def send(self, delta, sample_rate):
        """Send the counter to the backend.

        :param float delta: The amount to increase the counter by.
        :param float sample_rate: What rate this counter is sampled at. [0-1].

        """
        parts = [
            self.name + (":{:g}".format(delta).encode()),
            b"c",
        ]

        if sample_rate != 1.0:
            parts.append("@{:g}".format(sample_rate).encode())

        serialized = b"|".join(parts)
        self.transport.send(serialized)


class BatchCounter(Counter):
    """Counter implementation that batches multiple increment calls.

    A new entry in the :term:`packets` entry is created for each sample rate.
    For example, if a counter is incremented multiple times with a sample
    rate of 1.0, there will be one entry in :term:`packets`. If that counter
    is implemented again with a sample rate of 0.5, there will be two entries
    in :term:`packets`. Each packet has an associated delta value.

    Example usage::
        counter = BatchCounter(transport, "counter_name")
        counter.increment()
        do_something_else()
        counter.increment()
        counter.send()

    The above example results in one packet indicating an increase of two
    should be applied to "counter_name".
    """
    def __init__(self, transport, name):
        self.transport = transport
        self.name = name
        self.packets = defaultdict(int)

    def increment(self, delta=1, sample_rate=1.0):
        """Increment the counter.

        :param float delta: The amount to increase the counter by.
        :param float sample_rate: What rate this counter is sampled at. [0-1].

        """
        self.packets[sample_rate] += delta

    def decrement(self, delta=1, sample_rate=1.0):
        """Decrement the counter.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(delta=-delta, sample_rate=sample_rate)

    def send(self):
        for sample_rate, delta in self.packets.items():
            super(BatchCounter, self).send(delta, sample_rate)


class Histogram(object):
    """A bucketed distribution of integer values across a specific range.

    Records data value counts across a configurable integer value range
    with configurable buckets of value precision within that range.

    Configuration of each histogram is managed by the backend service,
    not by this interface. This implementation also depends on histograms
    being supported by the StatsD backend. Specifically, the StatsD
    backend must support the :code:`h` key, e.g. :code:`metric_name:320|h`.
    """
    def __init__(self, transport, name):
        self.transport = transport
        self.name = name

    def add_sample(self, value):
        """Add a new value to the histogram.

        This records a new value to the histogram; the bucket it goes in
        is determined by the backend service configurations.
        """
        serialized = self.name + (":{:g}|h".format(value).encode())
        self.transport.send(serialized)


class Gauge(object):
    """A gauge representing an arbitrary value.

    .. note:: The statsd protocol supports incrementing/decrementing gauges
        from their current value. We do not support that here because this
        feature is unpredictable in face of the statsd server restarting and
        the "current value" being lost.

    """
    def __init__(self, transport, name):
        self.transport = transport
        self.name = name

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

    :param str namespace: The root key to prefix all metrics with.
    :param baseplate.config.EndpointConfiguration endpoint: The endpoint to
        send metrics to or :py:data:`None`.  If :py:data:`None`, the returned
        client will discard all metrics.
    :return: A configured client.
    :rtype: :py:class:`baseplate.metrics.Client`

    .. seealso:: :py:func:`baseplate.metrics_client_from_config`.

    """

    if endpoint:
        transport = RawTransport(endpoint)
    else:
        transport = NullTransport()
    return Client(transport, namespace)
