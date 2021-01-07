"""Components for processing Baseplate spans for service request tracing."""
import collections
import json
import logging
import queue
import random
import socket
import threading
import time
import typing

from datetime import datetime
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional

import requests

from requests.exceptions import RequestException

from baseplate import _ExcInfo
from baseplate import BaseplateObserver
from baseplate import LocalSpan
from baseplate import RequestContext
from baseplate import Span
from baseplate import SpanObserver
from baseplate.lib import config
from baseplate.lib import warn_deprecated
from baseplate.lib.message_queue import MessageQueue
from baseplate.lib.message_queue import TimedOutError
from baseplate.observers.timeout import ServerTimeout


if typing.TYPE_CHECKING:
    SpanQueue = queue.Queue["TraceSpanObserver"]  # pylint: disable=unsubscriptable-object
else:
    SpanQueue = queue.Queue


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
    "DEBUG": "debug",
    "ERROR": "error",
}

# Feature flags
FLAGS = {
    # Ensures the trace passes ALL samplers
    "DEBUG": 1
}


# Max size for a string representation of a span when recorded to a POSIX queue
MAX_SPAN_SIZE = 102400
# Max number of spans allowed in POSIX queue at one time
MAX_QUEUE_SIZE = 10000


def current_epoch_microseconds() -> int:
    """Return current UTC time since epoch in microseconds."""
    epoch_ts = datetime.utcfromtimestamp(0)
    return int((datetime.utcnow() - epoch_ts).total_seconds() * 1000 * 1000)


class TracingClient(NamedTuple):
    service_name: str
    sample_rate: float
    recorder: "Recorder"


def make_client(
    service_name: str,
    tracing_endpoint: Optional[config.EndpointConfiguration] = None,
    tracing_queue_name: Optional[str] = None,
    max_span_queue_size: int = 50000,
    num_span_workers: int = 5,
    span_batch_interval: float = 0.5,
    num_conns: int = 100,
    sample_rate: float = 0.1,
    log_if_unconfigured: bool = True,
) -> TracingClient:
    """Create and return a tracing client based on configuration options.

    This client can be used by the :py:class:`TraceBaseplateObserver`.

    :param service_name: The name for the service this observer
        is registered to.
    :param tracing_endpoint: Destination to record span data.
    :param tracing_queue_name: POSIX queue name for reporting spans.
    :param num_conns: pool size for remote recorder connection pool.
    :param max_span_queue_size: span processing queue limit.
    :param num_span_workers: number of worker threads for span processing.
    :param span_batch_interval: wait time for span processing in seconds.
    :param sample_rate: percentage of unsampled requests to record traces for.
    """
    recorder: Recorder
    if tracing_queue_name:
        recorder = SidecarRecorder(tracing_queue_name)
    elif tracing_endpoint:
        warn_deprecated("In-app trace publishing is deprecated in favor of the sidecar model.")
        remote_addr = str(tracing_endpoint.address)
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

    def __init__(self, tracing_client: TracingClient):
        self.service_name = tracing_client.service_name
        self.sample_rate = tracing_client.sample_rate
        self.recorder = tracing_client.recorder
        try:
            self.hostname = socket.gethostbyname(socket.gethostname())
        except socket.gaierror as e:
            logger.warning("Hostname could not be resolved, error=%s", e)
            self.hostname = "undefined"

    @classmethod
    def force_sampling(cls, span: Span) -> bool:
        if span.flags:
            return span.flags & FLAGS["DEBUG"] == FLAGS["DEBUG"]
        return False

    def should_sample(self, span: Span) -> bool:
        should_sample = False
        if span.sampled is None:
            should_sample = random.random() < self.sample_rate
        else:
            should_sample = span.sampled
        return should_sample or self.force_sampling(span)

    def on_server_span_created(self, context: RequestContext, server_span: Span) -> None:
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

    def __init__(self, service_name: str, hostname: str, span: Span, recorder: "Recorder"):
        self.service_name = service_name
        self.hostname = hostname
        self.recorder = recorder
        self.span = span
        self.start: Optional[int] = None
        self.end: Optional[int] = None
        self.elapsed: Optional[int] = None
        self.binary_annotations: List[Dict[str, Any]] = []
        self.counters: DefaultDict[str, float] = collections.defaultdict(float)
        self.on_set_tag(ANNOTATIONS["COMPONENT"], "baseplate")
        super().__init__()

    def on_start(self) -> None:
        self.start = current_epoch_microseconds()
        self.client_send = self.start

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if exc_info:
            self.on_set_tag(ANNOTATIONS["ERROR"], True)

        # Set a debug annotation for downstream processing to
        # utilize in span filtering.
        if self.span.flags and (self.span.flags & FLAGS["DEBUG"]):
            self.on_set_tag(ANNOTATIONS["DEBUG"], True)

        self.end = current_epoch_microseconds()
        self.elapsed = self.end - typing.cast(int, self.start)

        for key, value in self.counters.items():
            self.binary_annotations.append(self._create_binary_annotation(f"counter.{key}", value))

        self.recorder.send(self)

    def on_set_tag(self, key: str, value: Any) -> None:
        """Translate set tags to tracing binary annotations.

        Number-type values are coerced to strings.
        """
        self.binary_annotations.append(self._create_binary_annotation(key, value))

    def on_incr_tag(self, key: str, delta: float) -> None:
        self.counters[key] += delta

    def _endpoint_info(self) -> Dict[str, str]:
        return {"serviceName": self.service_name, "ipv4": self.hostname}

    def _create_time_annotation(self, annotation_type: str, timestamp: int) -> Dict[str, Any]:
        """Create Zipkin-compatible Annotation for a span.

        This should be used for generating span annotations with a time component,
        e.g. the core "cs", "cr", "ss", and "sr" Zipkin Annotations
        """
        return {"endpoint": self._endpoint_info(), "timestamp": timestamp, "value": annotation_type}

    def _create_binary_annotation(
        self, annotation_type: str, annotation_value: Any
    ) -> Dict[str, Any]:
        """Create Zipkin-compatible BinaryAnnotation for a span.

        This should be used for generating span annotations that
        do not have a time component, e.g. URI, arbitrary request tags
        """
        endpoint_info = self._endpoint_info()

        # Annotation values must be str type.
        if isinstance(annotation_value, bool):
            # only "lower" bool values so we aren't affecting any other types
            annotation_value = str(annotation_value).lower()
        elif not isinstance(annotation_value, str):
            annotation_value = str(annotation_value)

        return {"key": annotation_type, "value": annotation_value, "endpoint": endpoint_info}

    def _to_span_obj(
        self, annotations: List[Dict[str, Any]], binary_annotations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
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

    def _serialize(self) -> Dict[str, Any]:
        """Serialize span information into Zipkin-accepted format."""
        annotations = []

        annotations.append(
            self._create_time_annotation(ANNOTATIONS["CLIENT_SEND"], typing.cast(int, self.start))
        )

        annotations.append(
            self._create_time_annotation(ANNOTATIONS["CLIENT_RECEIVE"], typing.cast(int, self.end))
        )

        return self._to_span_obj(annotations, self.binary_annotations)


class TraceLocalSpanObserver(TraceSpanObserver):
    """Span recording observer for local spans.

    :param service_name: The name for the service this observer is registered
        to.
    :param component_name: The name for the local component the span for this
        observer is recording in.
    :param hostname: Name identifying the host of the service.
    :param span: Local span for this observer.
    :param recorder: Recorder for span trace.
    """

    def __init__(
        self,
        service_name: str,
        component_name: str,
        hostname: str,
        span: Span,
        recorder: "Recorder",
    ):
        self.component_name = component_name
        super().__init__(service_name, hostname, span, recorder)
        self.binary_annotations.append(
            self._create_binary_annotation(ANNOTATIONS["LOCAL_COMPONENT"], self.component_name)
        )

    def on_start(self) -> None:
        self.start = current_epoch_microseconds()

    def on_child_span_created(self, span: Span) -> None:
        """Perform tracing-related actions for child spans creation.

        Register new TraceSpanObserver for the child span
        being created so span start and finish get properly recorded.
        """
        trace_observer: TraceSpanObserver
        if isinstance(span, LocalSpan):
            trace_observer = TraceLocalSpanObserver(
                self.service_name,
                typing.cast(str, span.component_name),
                self.hostname,
                span,
                self.recorder,
            )

        else:
            trace_observer = TraceSpanObserver(
                self.service_name, self.hostname, span, self.recorder
            )
        span.register(trace_observer)

    def _serialize(self) -> Dict[str, Any]:
        return self._to_span_obj([], self.binary_annotations)


class TraceServerSpanObserver(TraceSpanObserver):
    """Span recording observer for incoming request spans.

    This observer implements the server-side span portion of a
    Zipkin request trace
    """

    def __init__(self, service_name: str, hostname: str, span: Span, recorder: "Recorder"):
        self.service_name = service_name
        self.span = span
        self.recorder = recorder
        super().__init__(service_name, hostname, span, recorder)

    def on_start(self) -> None:
        self.start = current_epoch_microseconds()

    def on_finish(self, exc_info: Optional[_ExcInfo]) -> None:
        if exc_info and exc_info[0] is not None and issubclass(ServerTimeout, exc_info[0]):
            self.on_set_tag("timed_out", True)

        super().on_finish(exc_info)

    def on_child_span_created(self, span: Span) -> None:
        """Perform tracing-related actions for child spans creation.

        Register new TraceSpanObserver for the child span
        being created so span start and finish get properly recorded.
        """
        trace_observer: TraceSpanObserver
        if isinstance(span, LocalSpan):
            trace_observer = TraceLocalSpanObserver(
                self.service_name,
                typing.cast(str, span.component_name),
                self.hostname,
                span,
                self.recorder,
            )

        else:
            trace_observer = TraceSpanObserver(
                self.service_name, self.hostname, span, self.recorder
            )
        span.register(trace_observer)

    def _serialize(self) -> Dict[str, Any]:
        """Serialize span information into Zipkin-accepted format."""
        annotations: List[Dict[str, Any]] = []

        annotations.append(
            self._create_time_annotation(
                ANNOTATIONS["SERVER_RECEIVE"], typing.cast(int, self.start)
            )
        )
        annotations.append(
            self._create_time_annotation(ANNOTATIONS["SERVER_SEND"], typing.cast(int, self.end))
        )

        return self._to_span_obj(annotations, self.binary_annotations)


class Recorder:
    def send(self, span: TraceSpanObserver) -> None:
        raise NotImplementedError


class BaseBatchRecorder(Recorder):
    def __init__(
        self, max_queue_size: int, num_workers: int, max_span_batch: int, batch_wait_interval: float
    ):
        self.span_queue: SpanQueue = queue.Queue(maxsize=max_queue_size)
        self.batch_wait_interval = batch_wait_interval
        self.max_span_batch = max_span_batch
        self.logger = logging.getLogger(self.__class__.__name__)
        for _ in range(num_workers):
            self.flush_worker = threading.Thread(target=self._flush_spans)
            self.flush_worker.name = "span recorder"
            self.flush_worker.daemon = True
            self.flush_worker.start()

    def flush_func(self, spans: List[Dict[str, Any]]) -> None:
        raise NotImplementedError

    def _flush_spans(self) -> None:
        # This reads a batch of at most max_span_batch spans off the recorder queue
        # and sends them to a remote recording endpoint. If the queue
        # empties while being processed before reaching 10 spans, we flush
        # immediately.
        while True:
            spans: List[Dict[str, Any]] = []
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

    def send(self, span: TraceSpanObserver) -> None:
        try:
            self.span_queue.put_nowait(span)
        except Exception as e:
            self.logger.warning("Failed adding span to recording queue: %s", e)


class LoggingRecorder(BaseBatchRecorder):
    """Interface for recording spans to the debug log."""

    def __init__(
        self,
        max_queue_size: int = 50000,
        num_workers: int = 5,
        max_span_batch: int = 100,
        batch_wait_interval: float = 0.5,
    ):
        super().__init__(max_queue_size, num_workers, max_span_batch, batch_wait_interval)

    def flush_func(self, spans: List[Dict[str, Any]]) -> None:
        """Write a set of spans to debug log."""
        for span in spans:
            self.logger.debug("Span recording: %s", span)


class NullRecorder(BaseBatchRecorder):
    """Noop recorder."""

    def __init__(
        self,
        max_queue_size: int = 50000,
        num_workers: int = 5,
        max_span_batch: int = 100,
        batch_wait_interval: float = 0.5,
    ):
        super().__init__(max_queue_size, num_workers, max_span_batch, batch_wait_interval)

    def flush_func(self, spans: List[Dict[str, Any]]) -> None:
        return


class RemoteRecorder(BaseBatchRecorder):
    """Interface for recording spans to a remote collector.

    The RemoteRecorder adds spans to an in-memory Queue for a background
    thread worker to process. It currently does not shut down gracefully -
    in the event of parent process exit, any remaining spans will be discarded.
    """

    def __init__(
        self,
        endpoint: str,
        num_conns: int = 5,
        num_workers: int = 5,
        max_queue_size: int = 50000,
        max_span_batch: int = 100,
        batch_wait_interval: float = 0.5,
    ):

        super().__init__(max_queue_size, num_workers, max_span_batch, batch_wait_interval)
        adapter = requests.adapters.HTTPAdapter(pool_connections=num_conns, pool_maxsize=num_conns)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.endpoint = f"http://{endpoint}/api/v1/spans"

    def flush_func(self, spans: List[Dict[str, Any]]) -> None:
        """Send a set of spans to remote collector."""
        try:
            self.session.post(
                self.endpoint,
                data=json.dumps(spans).encode("utf8"),
                headers={"Content-Type": "application/json"},
                timeout=1,
            )
        except RequestException as e:
            self.logger.warning("Error flushing spans: %s", e)


class TraceTooLargeError(Exception):
    pass


class TraceQueueFullError(Exception):
    pass


class SidecarRecorder(Recorder):
    """Interface for recording spans to a POSIX message queue.

    The SidecarRecorder serializes spans to a string representation before
    adding them to the queue.
    """

    def __init__(self, queue_name: str):
        self.queue = MessageQueue(
            "/traces-" + queue_name, max_messages=MAX_QUEUE_SIZE, max_message_size=MAX_SPAN_SIZE,
        )

    def send(self, span: TraceSpanObserver) -> None:
        # Don't raise exceptions from here. This is called in the
        # request/response path and should finish cleanly.
        serialized_str = json.dumps(span._serialize()).encode("utf8")
        if len(serialized_str) > MAX_SPAN_SIZE:
            logger.warning(
                "Trace too big. Traces published to %s are not allowed to be larger "
                "than %d bytes. Received trace is %d bytes. This can be caused by "
                "an excess amount of tags or a large amount of child spans.",
                self.queue.queue.name,
                MAX_SPAN_SIZE,
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

    :param raw_config: The application configuration which should have settings
        for the tracing client.
    :param log_if_unconfigured: When the client is not configured, should
        trace spans be logged or discarded silently?
    :return: A configured client.

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
