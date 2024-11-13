"""Application metrics via StatsD.

A client for the application metrics aggregator StatsD_. Metrics sent to
StatsD are aggregated and written to graphite. StatsD is generally used for
whole-system health monitoring and insight into usage patterns.

.. testsetup::

    app_config = {"metrics.endpoint": "", "metrics.namespace": "example"}
    do_something = lambda: ()
    do_something_else = lambda: ()
    do_another_thing = lambda: ()

Basic example usage:

.. testcode::

    from baseplate.lib.metrics import metrics_client_from_config

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

.. _StatsD: https://github.com/statsd/statsd

"""
import collections
import errno
import logging
import socket
import time

from types import TracebackType
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

from baseplate.lib import config


logger = logging.getLogger(__name__)


def _metric_join(*nodes: bytes) -> bytes:
    return b".".join(node.strip(b".") for node in nodes if node)


def _format_tags(tags: Optional[Dict[str, Any]]) -> Optional[bytes]:
    if not tags:
        return None

    parts = []
    for key, value in tags.items():
        parts.append(f"{key}={str(value)}")
    return b"," + ",".join(parts).encode()


class TransportError(Exception):
    pass


class MessageTooBigTransportError(TransportError):
    def __init__(self, message_size: int):
        super().__init__(f"could not send: message of {message_size} bytes too large")
        self.message_size = message_size


class Transport:
    def send(self, serialized_metric: bytes) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError


class NullTransport(Transport):
    """A transport which doesn't send messages at all."""

    def __init__(self, log_if_unconfigured: bool):
        self.log_if_unconfigured = log_if_unconfigured

    def send(self, serialized_metric: bytes) -> None:
        if self.log_if_unconfigured:
            for metric_line in serialized_metric.splitlines():
                logger.debug("Would send metric %r", metric_line)

    def flush(self) -> None:
        pass


class RawTransport(Transport):
    """A transport which sends messages on a socket."""

    def __init__(
        self,
        endpoint: config.EndpointConfiguration,
        swallow_network_errors: bool = False,
    ):
        self.swallow_network_errors = swallow_network_errors
        self.socket = socket.socket(endpoint.family, socket.SOCK_DGRAM)
        self.socket.connect(endpoint.address)

    def send(self, serialized_metric: bytes) -> None:
        try:
            self.socket.sendall(serialized_metric)
        except OSError as exc:
            if self.swallow_network_errors:
                logger.exception("Failed to send to metrics collector")
                return
            if exc.errno == errno.EMSGSIZE:
                raise MessageTooBigTransportError(len(serialized_metric))
            raise TransportError(exc)

    def flush(self) -> None:
        pass


class BufferedTransport(Transport):
    """A transport which wraps another transport and buffers before sending."""

    def __init__(self, transport: Transport):
        self.transport = transport
        self.buffer: List[bytes] = []

    def send(self, serialized_metric: bytes) -> None:
        self.buffer.append(serialized_metric)

    def flush(self) -> None:
        if self.buffer:
            metrics, self.buffer = self.buffer, []
            message = b"\n".join(metrics)
            self.transport.send(message)


class BaseClient:
    def __init__(self, transport: Transport, namespace: str):
        self.transport = transport
        self.base_tags: Dict[str, Any] = {}
        self.namespace = namespace.encode("ascii")

    def timer(self, name: str, tags: Optional[Dict[str, Any]] = None) -> "Timer":
        """Return a Timer with the given name.

        :param name: The name the timer should have.

        """
        timer_name = _metric_join(self.namespace, name.encode("ascii"))
        return Timer(self.transport, timer_name, {**self.base_tags, **(tags or {})})

    def counter(self, name: str, tags: Optional[Dict[str, Any]] = None) -> "Counter":
        """Return a Counter with the given name.

        The sample rate is currently up to your application to enforce.

        :param name: The name the counter should have.

        """
        counter_name = _metric_join(self.namespace, name.encode("ascii"))
        return Counter(self.transport, counter_name, {**self.base_tags, **(tags or {})})

    def gauge(self, name: str, tags: Optional[Dict[str, Any]] = None) -> "Gauge":
        """Return a Gauge with the given name.

        :param name: The name the gauge should have.

        """
        gauge_name = _metric_join(self.namespace, name.encode("ascii"))
        return Gauge(self.transport, gauge_name, {**self.base_tags, **(tags or {})})

    def histogram(self, name: str, tags: Optional[Dict[str, Any]] = None) -> "Histogram":
        """Return a Histogram with the given name.

        :param name: The name the histogram should have.

        """
        histogram_name = _metric_join(self.namespace, name.encode("ascii"))
        return Histogram(self.transport, histogram_name, {**self.base_tags, **(tags or {})})


class Client(BaseClient):
    """A client for StatsD."""

    def batch(self) -> "Batch":
        """Return a client-like object which batches up metrics.

        Batching metrics can reduce the number of packets that are sent to
        the stats aggregator.

        """
        return Batch(self.transport, self.namespace)


class Batch(BaseClient):
    """A batch of metrics to send to StatsD.

    The batch also supports the `context manager protocol`_, for use with
    Python's ``with`` statement. When the context is exited, the batch will
    automatically :py:meth:`flush`.

    .. _context manager protocol:
        https://docs.python.org/3/reference/datamodel.html#context-managers

    """

    # pylint: disable=super-init-not-called
    def __init__(self, transport: Transport, namespace: bytes):
        self.transport = BufferedTransport(transport)
        self.namespace = namespace
        self.base_tags = {}
        self.counters: Dict[bytes, BatchCounter] = {}

    def __enter__(self) -> "Batch":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.flush()
        return None  # don't swallow exception

    def flush(self) -> None:
        """Immediately send the batched metrics."""
        counters, self.counters = self.counters, {}
        for counter in counters.values():
            counter.flush()

        try:
            self.transport.flush()
        except MessageTooBigTransportError as exc:
            counters_by_total = list(
                sorted((c for c in counters.values()), key=lambda c: c.total, reverse=True)
            )
            logger.warning(
                "Metrics batch of %d bytes is too large to send, flush more often or reduce "
                "amount done in this request. See https://baseplate.readthedocs.io/en/latest/guide/faq.html#what-do-i-do-about-metrics-batch-of-n-bytes-is-too-large-to-send. Top counters: %s",
                exc.message_size,
                ", ".join(f"{c.name.decode()}={c.total:.0f}" for c in counters_by_total[:10]),
            )
        except TransportError as exc:
            logger.warning("Failed to send metrics batch: %s", exc)

    def counter(self, name: str, tags: Optional[Dict[str, Any]] = None) -> "Counter":
        """Return a BatchCounter with the given name.

        The sample rate is currently up to your application to enforce.
        :param name: The name the counter should have.
        """
        counter_name = _metric_join(self.namespace, name.encode("ascii"))
        batch_counter = self.counters.get(counter_name)
        if batch_counter is None:
            batch_counter = BatchCounter(self.transport, counter_name, tags)
            self.counters[counter_name] = batch_counter

        return batch_counter


class Timer:
    """A timer for recording elapsed times.

    The timer also supports the `context manager protocol`_, for use with
    Python's ``with`` statement. When the context is entered the timer will
    :py:meth:`start` and when exited, the timer will automatically
    :py:meth:`stop`.

    .. _context manager protocol:
        https://docs.python.org/3/reference/datamodel.html#context-managers

    """

    def __init__(
        self,
        transport: Transport,
        name: bytes,
        tags: Optional[Dict[str, Any]] = None,
    ):
        self.transport = transport
        self.name = name
        if tags:
            self.tags = tags
        else:
            self.tags = {}

        self.start_time: Optional[float] = None
        self.stopped: bool = False
        self.sample_rate = 1.0

    def start(self, sample_rate: float = 1.0) -> None:
        """Record the current time as the start of the timer."""
        assert not self.start_time, "timer already started"
        assert not self.stopped, "timer already stopped"

        self.sample_rate = sample_rate
        self.start_time = time.time()

    def stop(self) -> None:
        """Stop the timer and record the total elapsed time."""
        assert self.start_time, "timer not started"
        assert not self.stopped, "timer already stopped"

        now = time.time()
        elapsed = now - self.start_time
        self.send(elapsed, self.sample_rate)
        self.stopped = True

    def send(self, elapsed: float, sample_rate: float = 1.0) -> None:
        """Directly send a timer value without having to stop/start.

        This can be useful when the timing was managed elsewhere and we just
        want to report the result.
        :param elapsed: The elapsed time in seconds to report.
        """
        serialized = self.name
        formatted_tags = _format_tags(self.tags)
        if formatted_tags:
            serialized += formatted_tags
        serialized += f":{(elapsed * 1000.0):g}|ms".encode()
        if sample_rate < 1.0:
            sampling_info = f"@{sample_rate:g}".encode()
            serialized = b"|".join([serialized, sampling_info])
        self.transport.send(serialized)

    def update_tags(self, tags: Dict) -> None:
        assert not self.stopped
        self.tags.update(tags)

    def __enter__(self) -> None:
        self.start()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.stop()
        return None  # don't swallow exception


class Counter:
    """A counter for counting events over time."""

    def __init__(self, transport: Transport, name: bytes, tags: Optional[Dict[str, Any]] = None):
        self.transport = transport
        self.name = name
        self.tags = tags

    def increment(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        """Increment the counter.

        :param delta: The amount to increase the counter by.
        :param sample_rate: What rate this counter is sampled at. [0-1].

        """
        self.send(delta, sample_rate)

    def decrement(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        """Decrement the counter.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(delta=-delta, sample_rate=sample_rate)

    def send(self, delta: float, sample_rate: float) -> None:
        """Send the counter to the backend.

        :param delta: The amount to increase the counter by.
        :param sample_rate: What rate this counter is sampled at. [0-1].

        """
        serialized = self.name
        formatted_tags = _format_tags(self.tags)
        if formatted_tags:
            serialized += formatted_tags
        serialized += f":{delta:g}".encode() + b"|c"
        if sample_rate < 1.0:
            sampling_info = f"@{sample_rate:g}".encode()
            serialized = b"|".join([serialized, sampling_info])
        self.transport.send(serialized)


class BatchCounter(Counter):
    """Counter implementation that batches multiple increment calls.

    A new entry in the ``packets`` entry is created for each sample rate.  For
    example, if a counter is incremented multiple times with a sample rate of
    1.0, there will be one entry in ``packets``. If that counter is implemented
    again with a sample rate of 0.5, there will be two entries in ``packets``.
    Each packet has an associated delta value.

    Example usage::
        counter = BatchCounter(transport, "counter_name")
        counter.increment()
        do_something_else()
        counter.increment()
        counter.flush()

    The above example results in one packet indicating an increase of two
    should be applied to "counter_name".
    """

    def __init__(self, transport: Transport, name: bytes, tags: Optional[Dict[str, Any]] = None):
        super().__init__(transport, name)
        self.packets: DefaultDict[float, float] = collections.defaultdict(float)
        self.tags = tags

    def increment(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        """Increment the counter.

        :param delta: The amount to increase the counter by.
        :param sample_rate: What rate this counter is sampled at. [0-1].

        """
        self.packets[sample_rate] += delta

    def decrement(self, delta: float = 1.0, sample_rate: float = 1.0) -> None:
        """Decrement the counter.

        This is equivalent to :py:meth:`increment` with delta negated.

        """
        self.increment(delta=-delta, sample_rate=sample_rate)

    @property
    def total(self) -> int:
        return sum(self.packets.values())  # type: ignore

    def flush(self) -> None:
        for sample_rate, delta in self.packets.items():
            super().send(delta, sample_rate)


class Histogram:
    """A bucketed distribution of integer values across a specific range.

    Records data value counts across a configurable integer value range
    with configurable buckets of value precision within that range.

    Configuration of each histogram is managed by the backend service,
    not by this interface. This implementation also depends on histograms
    being supported by the StatsD backend. Specifically, the StatsD
    backend must support the :code:`h` key, e.g. :code:`metric_name:320|h`.
    """

    def __init__(
        self,
        transport: Transport,
        name: bytes,
        tags: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.transport = transport
        self.name = name
        self.tags = tags

    def add_sample(self, value: float) -> None:
        """Add a new value to the histogram.

        This records a new value to the histogram; the bucket it goes in
        is determined by the backend service configurations.
        """
        formatted_tags = _format_tags(self.tags)
        if formatted_tags:
            serialized = self.name + formatted_tags + (f":{value:g}|h".encode())
        else:
            serialized = self.name + (f":{value:g}|h".encode())

        self.transport.send(serialized)


class Gauge:
    """A gauge representing an arbitrary value.

    .. note:: The StatsD protocol supports incrementing/decrementing gauges
        from their current value. We do not support that here because this
        feature is unpredictable in face of the StatsD server restarting and
        the "current value" being lost.

    """

    def __init__(
        self,
        transport: Transport,
        name: bytes,
        tags: Optional[Dict[str, Any]] = None,
    ):
        self.transport = transport
        self.name = name
        self.tags = tags

    def replace(self, new_value: float) -> None:
        """Replace the value held by the gauge.

        This will replace the value held by the gauge with no concern for its
        previous value.

        .. note:: Due to the way the protocol works, it is not possible to
            replace gauge values with negative numbers.

        :param new_value: The new value to store in the gauge.

        """
        assert new_value >= 0, "gauges cannot be replaced with negative numbers"
        formatted_tags = _format_tags(self.tags)
        if formatted_tags:
            serialized = self.name + formatted_tags + (f":{new_value:g}|g".encode())
        else:
            serialized = self.name + (f":{new_value:g}|g".encode())

        self.transport.send(serialized)


def make_client(
    namespace: str,
    endpoint: config.EndpointConfiguration,
    log_if_unconfigured: bool,
    swallow_network_errors: bool = False,
) -> Client:
    """Return a configured client.

    :param namespace: The root key to prefix all metrics with.
    :param endpoint: The endpoint to send metrics to or :py:data:`None`.  If
        :py:data:`None`, the returned client will discard all metrics.
    :param swallow_network_errors: Swallow (log) network errors during sending
        to metrics collector.
    :return: A configured client.

    .. seealso:: :py:func:`baseplate.lib.metrics.metrics_client_from_config`.

    """
    transport: Transport

    if endpoint:
        transport = RawTransport(endpoint, swallow_network_errors=swallow_network_errors)
    else:
        transport = NullTransport(log_if_unconfigured)
    return Client(transport, namespace)


def metrics_client_from_config(raw_config: config.RawConfig) -> Client:
    """Configure and return a metrics client.

    This expects two configuration options:

    ``metrics.namespace``
        The root key to prefix all metrics in this application with.
    ``metrics.endpoint``
        A ``host:port`` pair, e.g. ``localhost:2014``. If an empty string, a
        client that discards all metrics will be returned.
    `metrics.log_if_unconfigured``
        Whether to log metrics when there is no unconfigured endpoint.
        Defaults to false.
    `metrics.swallow_network_errors``
        When false, network errors during sending to metrics collector will
        cause an exception to be thrown. When true, those exceptions are logged
        and swallowed instead.
        Defaults to false.

    :param raw_config: The application configuration which should have
        settings for the metrics client.
    :return: A configured client.

    """
    cfg = config.parse_config(
        raw_config,
        {
            "metrics": {
                "namespace": config.Optional(config.String, default=""),
                "endpoint": config.Optional(config.Endpoint),
                "log_if_unconfigured": config.Optional(config.Boolean, default=False),
                "swallow_network_errors": config.Optional(config.Boolean, default=False),
            }
        },
    )

    # pylint: disable=maybe-no-member
    return make_client(
        namespace=cfg.metrics.namespace,
        endpoint=cfg.metrics.endpoint,
        log_if_unconfigured=cfg.metrics.log_if_unconfigured,
        swallow_network_errors=cfg.metrics.swallow_network_errors,
    )
