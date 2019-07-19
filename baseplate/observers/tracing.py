"""Components for processing Baseplate spans for service request tracing."""

import collections
import json
import logging
import queue
import random
import socket
import threading
import time
from datetime import datetime

import requests
from requests.exceptions import RequestException

from baseplate.lib.message_queue import MessageQueue, TimedOutError
from baseplate import BaseplateObserver, LocalSpan, SpanObserver
from baseplate.lib import warn_deprecated, config


logger = logging.getLogger(__name__)

# Suppress noisy INFO logging of underlying connection management module
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

# Span annotation types
ANNOTATIONS = {
    "CLIENT_SEND": "cs",
    "CLIENT_RECEIVE": "cr",
    "SERVER_SEND": "ss",
    "SERVER_RECEIVE": "sr",
    "LOCAL_COMPONENT": "lc",
    "COMPONENT": "component",
}

# Feature flags
FLAGS = {
    # Ensures the trace passes ALL samplers
    "DEBUG": 1
}


# Max size for a string representation of a span when recorded to a POSIX queue
MAX_SPAN_SIZE = 102400
# Max number of spans iallowed in POSIX queue at one time
MAX_QUEUE_SIZE = 10000


def current_epoch_microseconds():
    """Return current UTC time since epoch in microseconds."""
    epoch_ts = datetime.utcfromtimestamp(0)
    return int((datetime.utcnow() - epoch_ts).total_seconds() * 1000 * 1000)


TracingClient = collections.namedtuple("TracingClient", "service_name sample_rate recorder")


def make_client(
    service_name,
    tracing_endpoint=None,
    tracing_queue_name=None,
    max_span_queue_size=50000,
    num_span_workers=5,
    span_batch_interval=0.5,
    num_conns=100,
    sample_rate=0.1,
    log_if_unconfigured=True,
):
    """Create and return a tracing client based on configuration options.

    This client can be used by the :py:class:`TraceBaseplateObserver`.

    :param str service_name: The name for the service this observer
        is registered to.
    :param baseplate.lib.config.EndpointConfiguration tracing_endpoint: destination
        to record span data.
    :param str tracing_queue_name: POSIX queue name for reporting spans.
    :param int num_conns: pool size for remote recorder connection pool.
    :param int max_span_queue_size: span processing queue limit.
    :param int num_span_workers: number of worker threads for span processing.
    :param float span_batch_interval: wait time for span processing in seconds.
    :param float sample_rate: percentage of unsampled requests to record traces
        for.
    """
    if tracing_queue_name:
        recorder = SidecarRecorder(tracing_queue_name)
    elif tracing_endpoint:
        warn_deprecated("In-app trace publishing is deprecated in favor of the sidecar model.")
        remote_addr = "%s:%s" % tracing_endpoint.address
        recorder = RemoteRecorder(
            remote_addr,
            num_conns=num_conns,
            max_queue_size=max_span_queue_size,
            num_workers=num_span_workers,
            batch_wait_interval=span_batch_interval,
        )
    elif log_if_unconfigured:
        recorder = LoggingRecorder(
            max_queue_size=max_span_queue_size,
            num_workers=num_span_workers,
            batch_wait_interval=span_batch_interval,
        )
    else:
        recorder = NullRecorder(
            max_queue_size=max_span_queue_size,
            num_workers=num_span_workers,
            batch_wait_interval=span_batch_interval,
        )

    return TracingClient(service_name, sample_rate, recorder)


class TraceBaseplateObserver(BaseplateObserver):
    """Distributed tracing observer.

    This observer handles Zipkin-compatible distributed tracing
    instrumentation for both inbound and outbound requests.
    Baseplate span-specific tracing observers (TraceSpanObserver
    and TraceServerSpanObserver) are registered for tracking,
    serializing, and recording span data.

    :param baseplate.observers.tracing.TracingClient client: The client
        where metrics will be sent.

    """

    def __init__(self, tracing_client):
        self.service_name = tracing_client.service_name
        self.sample_rate = tracing_client.sample_rate
        self.recorder = tracing_client.recorder
        try:
            self.hostname = socket.gethostbyname(socket.gethostname())
        except socket.gaierror as e:
            logger.warning("Hostname could not be resolved, error=%s", e)
            self.hostname = "undefined"

    @classmethod
    def force_sampling(cls, span):
        return span.flags and (span.flags & FLAGS["DEBUG"])

    def should_sample(self, span):
        should_sample = False
        if span.sampled is None:
            should_sample = random.random() < self.sample_rate
        else:
            should_sample = span.sampled
        return should_sample or self.force_sampling(span)

    def on_server_span_created(self, context, server_span):
        if self.should_sample(server_span):
            server_span.sampled = True
            observer = TraceServerSpanObserver(
                self.service_name, self.hostname, server_span, self.recorder
            )
            server_span.register(observer)
        else:
            server_span.sampled = False


class TraceSpanObserver(SpanObserver):
    """Span recording observer for outgoing request child spans.

    This observer implements the client-side span portion of a
    Zipkin request trace.
    """

    def __init__(self, service_name, hostname, span, recorder):
        self.service_name = service_name
        self.hostname = hostname
        self.recorder = recorder
        self.span = span
        self.start = None
        self.end = None
        self.elapsed = None
        self.binary_annotations = []
        self.on_set_tag(ANNOTATIONS["COMPONENT"], "baseplate")
        super(TraceSpanObserver, self).__init__()

    def on_start(self):
        self.start = current_epoch_microseconds()
        self.client_send = self.start

    def on_finish(self, exc_info):
        if exc_info:
            self.on_set_tag("error", True)
        self.end = current_epoch_microseconds()
        self.elapsed = self.end - self.start
        self.record()

    def on_set_tag(self, key, value):
        """Translate set tags to tracing binary annotations.

        Number-type values are coerced to strings.
        """
        self.binary_annotations.append(self._create_binary_annotation(key, value))

    def _endpoint_info(self):
        return {"serviceName": self.service_name, "ipv4": self.hostname}

    def _create_time_annotation(self, annotation_type, timestamp):
        """Create Zipkin-compatible Annotation for a span.

        This should be used for generating span annotations with a time component,
        e.g. the core "cs", "cr", "ss", and "sr" Zipkin Annotations
        """
        return {"endpoint": self._endpoint_info(), "timestamp": timestamp, "value": annotation_type}

    def _create_binary_annotation(self, annotation_type, annotation_value):
        """Create Zipkin-compatible BinaryAnnotation for a span.

        This should be used for generating span annotations that
        do not have a time component, e.g. URI, arbitrary request tags
        """
        endpoint_info = self._endpoint_info()

        # Annotation values must be bool or str type.
        if not isinstance(annotation_value, (bool, str)):
            annotation_value = str(annotation_value)

        return {"key": annotation_type, "value": annotation_value, "endpoint": endpoint_info}

    def _to_span_obj(self, annotations, binary_annotations):
        span = {
            "traceId": self.span.trace_id,
            "name": self.span.name,
            "id": self.span.id,
            "timestamp": self.start,
            "duration": self.elapsed,
            "annotations": annotations,
            "binaryAnnotations": binary_annotations,
        }

        span["parentId"] = self.span.parent_id or 0
        return span

    def _serialize(self):
        """Serialize span information into Zipkin-accepted format."""
        annotations = []

        annotations.append(self._create_time_annotation(ANNOTATIONS["CLIENT_SEND"], self.start))

        annotations.append(self._create_time_annotation(ANNOTATIONS["CLIENT_RECEIVE"], self.end))

        return self._to_span_obj(annotations, self.binary_annotations)

    def record(self):
        """Record serialized span."""
        self.recorder.send(self)


class TraceLocalSpanObserver(TraceSpanObserver):
    """Span recording observer for local spans.

    :param str service_name: The name for the service this observer
        is registered to.
    :param str component_name: The name for the local component the span
        for this observer is recording in.
    :param str hostname: Name identifying the host of the service.
    :param baseplate.Span span: Local span for this observer.
    :param baseplate.observers.tracing.Recorder: Recorder for span trace.
    """

    def __init__(self, service_name, component_name, hostname, span, recorder):
        self.component_name = component_name
        super(TraceLocalSpanObserver, self).__init__(service_name, hostname, span, recorder)
        self.binary_annotations.append(
            self._create_binary_annotation(ANNOTATIONS["LOCAL_COMPONENT"], self.component_name)
        )

    def on_start(self):
        self.start = current_epoch_microseconds()

    def on_child_span_created(self, span):
        """Perform tracing-related actions for child spans creation.

        Register new TraceSpanObserver for the child span
        being created so span start and finish get properly recorded.
        """
        if isinstance(span, LocalSpan):
            trace_observer = TraceLocalSpanObserver(
                self.service_name, span.component_name, self.hostname, span, self.recorder
            )

        else:
            trace_observer = TraceSpanObserver(
                self.service_name, self.hostname, span, self.recorder
            )
        span.register(trace_observer)

    def _serialize(self):
        annotations = []
        return self._to_span_obj(annotations, self.binary_annotations)


class TraceServerSpanObserver(TraceSpanObserver):
    """Span recording observer for incoming request spans.

    This observer implements the server-side span portion of a
    Zipkin request trace
    """

    def __init__(self, service_name, hostname, span, recorder):
        self.service_name = service_name
        self.span = span
        self.recorder = recorder
        super(TraceServerSpanObserver, self).__init__(service_name, hostname, span, recorder)

    def on_start(self):
        self.start = current_epoch_microseconds()

    def on_child_span_created(self, span):
        """Perform tracing-related actions for child spans creation.

        Register new TraceSpanObserver for the child span
        being created so span start and finish get properly recorded.
        """
        if isinstance(span, LocalSpan):
            trace_observer = TraceLocalSpanObserver(
                self.service_name, span.component_name, self.hostname, span, self.recorder
            )

        else:
            trace_observer = TraceSpanObserver(
                self.service_name, self.hostname, span, self.recorder
            )
        span.register(trace_observer)

    def _serialize(self):
        """Serialize span information into Zipkin-accepted format."""
        annotations = []

        annotations.append(self._create_time_annotation(ANNOTATIONS["SERVER_RECEIVE"], self.start))
        annotations.append(self._create_time_annotation(ANNOTATIONS["SERVER_SEND"], self.end))

        return self._to_span_obj(annotations, self.binary_annotations)


class BaseBatchRecorder:
    def __init__(self, max_queue_size, num_workers, max_span_batch, batch_wait_interval):
        self.span_queue = queue.Queue(maxsize=max_queue_size)
        self.batch_wait_interval = batch_wait_interval
        self.max_span_batch = max_span_batch
        self.logger = logging.getLogger(self.__class__.__name__)
        for _ in range(num_workers):
            self.flush_worker = threading.Thread(target=self._flush_spans)
            self.flush_worker.name = "span recorder"
            self.flush_worker.daemon = True
            self.flush_worker.start()

    def flush_func(self, spans):
        raise NotImplementedError

    def _flush_spans(self):
        # This reads a batch of at most max_span_batch spans off the recorder queue
        # and sends them to a remote recording endpoint. If the queue
        # empties while being processed before reaching 10 spans, we flush
        # immediately.
        while True:
            spans = []
            try:
                while len(spans) < self.max_span_batch:
                    spans.append(self.span_queue.get_nowait()._serialize())
            except queue.Empty:
                pass
            finally:
                if spans:
                    self.flush_func(spans)
                else:
                    time.sleep(self.batch_wait_interval)

    def send(self, span):
        try:
            self.span_queue.put_nowait(span)
        except Exception as e:
            self.logger.warning("Failed adding span to recording queue: %s", e)


class LoggingRecorder(BaseBatchRecorder):
    """Interface for recording spans to the debug log."""

    def __init__(
        self, max_queue_size=50000, num_workers=5, max_span_batch=100, batch_wait_interval=0.5
    ):
        super(LoggingRecorder, self).__init__(
            max_queue_size, num_workers, max_span_batch, batch_wait_interval
        )

    def flush_func(self, spans):
        """Write a set of spans to debug log."""
        for span in spans:
            self.logger.debug("Span recording: %s", span)


class NullRecorder(BaseBatchRecorder):
    """Noop recorder."""

    def __init__(
        self, max_queue_size=50000, num_workers=5, max_span_batch=100, batch_wait_interval=0.5
    ):
        super(NullRecorder, self).__init__(
            max_queue_size, num_workers, max_span_batch, batch_wait_interval
        )

    def flush_func(self, spans):
        return


class RemoteRecorder(BaseBatchRecorder):
    """Interface for recording spans to a remote collector.

    The RemoteRecorder adds spans to an in-memory Queue for a background
    thread worker to process. It currently does not shut down gracefully -
    in the event of parent process exit, any remaining spans will be discarded.
    """

    def __init__(
        self,
        endpoint,
        num_conns=5,
        num_workers=5,
        max_queue_size=50000,
        max_span_batch=100,
        batch_wait_interval=0.5,
    ):

        super(RemoteRecorder, self).__init__(
            max_queue_size, num_workers, max_span_batch, batch_wait_interval
        )
        adapter = requests.adapters.HTTPAdapter(pool_connections=num_conns, pool_maxsize=num_conns)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.endpoint = "http://%s/api/v1/spans" % endpoint

    def flush_func(self, spans):
        """Send a set of spans to remote collector."""
        try:
            self.session.post(
                self.endpoint,
                data=json.dumps(spans),
                headers={"Content-Type": "application/json"},
                timeout=1,
            )
        except RequestException as e:
            self.logger.warning("Error flushing spans: %s", e)


class TraceTooLargeError(Exception):
    pass


class TraceQueueFullError(Exception):
    pass


MAX_SIDECAR_QUEUE_SIZE = 102400
MAX_SIDECAR_MESSAGE_SIZE = 10000


class SidecarRecorder:
    """Interface for recording spans to a POSIX message queue.

    The SidecarRecorder serializes spans to a string representation before
    adding them to the queue.
    """

    def __init__(self, queue_name):
        self.queue = MessageQueue(
            "/traces-" + queue_name,
            max_messages=MAX_SIDECAR_QUEUE_SIZE,
            max_message_size=MAX_SIDECAR_MESSAGE_SIZE,
        )

    def send(self, span):
        # Don't raise exceptions from here. This is called in the
        # request/response path and should finish cleanly.
        serialized_str = json.dumps(span._serialize())
        if len(serialized_str) > MAX_SIDECAR_MESSAGE_SIZE:
            logger.warning(
                "Trace too big. Traces published to %s are not allowed to be larger "
                "than %d bytes. Received trace is %d bytes. This can be caused by "
                "an excess amount of tags or a large amount of child spans.",
                self.queue.queue.name,
                MAX_SIDECAR_MESSAGE_SIZE,
                len(serialized_str),
            )
        try:
            self.queue.put(serialized_str, timeout=0)
        except TimedOutError:
            logger.warning(
                "Trace queue %s is full. Is trace sidecar healthy?", self.queue.queue.name
            )


def tracing_client_from_config(
    raw_config: config.RawConfig, log_if_unconfigured: bool = True
) -> TracingClient:
    """Configure and return a tracing client.

    This expects one configuration option and can take many optional ones:

    ``tracing.service_name``
        The name for the service this observer is registered to.
    ``tracing.endpoint`` (optional)
        (Deprecated in favor of the sidecar model.) Destination to record span data.
    ``tracing.queue_name`` (optional)
        Name of POSIX queue where spans are recorded
    ``tracing.max_span_queue_size`` (optional)
        Span processing queue limit.
    ``tracing.num_span_workers`` (optional)
        Number of worker threads for span processing.
    ``tracing.span_batch_interval`` (optional)
        Wait time for span processing in seconds.
    ``tracing.num_conns`` (optional)
        Pool size for remote recorder connection pool.
    ``tracing.sample_rate`` (optional)
        Percentage of unsampled requests to record traces for (e.g. "37%")

    :param dict raw_config: The application configuration which should have
        settings for the tracing client.
    :param bool log_if_unconfigured: When the client is not configured, should
        trace spans be logged or discarded silently?
    :return: A configured client.
    :rtype: :py:class:`baseplate.observers.tracing.TracingClient`

    """
    cfg = config.parse_config(
        raw_config,
        {
            "tracing": {
                "service_name": config.String,
                "endpoint": config.Optional(config.Endpoint),
                "queue_name": config.Optional(config.String),
                "max_span_queue_size": config.Optional(config.Integer, default=50000),
                "num_span_workers": config.Optional(config.Integer, default=5),
                "span_batch_interval": config.Optional(
                    config.Timespan, default=config.Timespan("500 milliseconds")
                ),
                "num_conns": config.Optional(config.Integer, default=100),
                "sample_rate": config.Optional(
                    config.Fallback(config.Percent, config.Float), default=0.1
                ),
            }
        },
    )

    # pylint: disable=maybe-no-member
    return make_client(
        service_name=cfg.tracing.service_name,
        tracing_endpoint=cfg.tracing.endpoint,
        tracing_queue_name=cfg.tracing.queue_name,
        max_span_queue_size=cfg.tracing.max_span_queue_size,
        num_span_workers=cfg.tracing.num_span_workers,
        span_batch_interval=cfg.tracing.span_batch_interval.total_seconds(),
        num_conns=cfg.tracing.num_conns,
        sample_rate=cfg.tracing.sample_rate,
        log_if_unconfigured=log_if_unconfigured,
    )
