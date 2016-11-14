"""Components for processing Baseplate spans for service request tracing."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import calendar

import json
import logging
import socket
import threading
import time
from datetime import datetime

import requests

from .._compat import queue
from ..core import BaseplateObserver, SpanObserver, TraceInfo


logger = logging.getLogger(__name__)


# Span annotation types
ANNOTATIONS = {
    'CLIENT_SEND': 'cs',
    'CLIENT_RECEIVE': 'cr',
    'SERVER_SEND': 'ss',
    'SERVER_RECEIVE': 'sr',
}


def current_epoch_microseconds():
    """Return current UTC time since epoch in microseconds."""
    epoch_ts = datetime.utcfromtimestamp(0)
    return int((datetime.utcnow() - epoch_ts).
               total_seconds() * 1000 * 1000)


class TraceBaseplateObserver(BaseplateObserver):
    """Distributed tracing observer.

    This observer handles Zipkin-compatible distributed tracing
    instrumentation for both inbound and outbound requests.
    Baseplate span-specific tracing observers (TraceSpanObserver
    and TraceServerSpanObserver) are registered for tracking,
    serializing, and recording span data.

    :param str service_name: The name for the service this observer
        is registered to.
    :param str tracing_endpoint: A 'hostname:port' destination to record
        span data.
    :param int max_conns: pool size for remote recorder connections.
    """
    def __init__(self, service_name, tracing_endpoint=None,
                 max_conns=100, max_span_queue=10000):
        self.service_name = service_name

        if tracing_endpoint:
            self.recorder = RemoteRecorder(tracing_endpoint,
                                           max_conns=max_conns,
                                           max_span_queue=max_span_queue)
        else:
            self.recorder = NullRecorder()

    def on_server_span_created(self, context, server_span):
        observer = TraceServerSpanObserver(self.service_name,
                                           server_span,
                                           self.recorder)
        server_span.register(observer)


class TraceSpanObserver(SpanObserver):
    """Span recording observer for outgoing request child spans.

    This observer implements the client-side span portion of a
    Zipkin request trace.
    """
    def __init__(self, service_name, span, recorder):
        self.service_name = service_name
        self.recorder = recorder
        self.span = span
        self.start = None
        self.end = None
        self.elapsed = None
        self.binary_annotations = []
        super(TraceSpanObserver, self).__init__()

    def on_start(self):
        self.start = current_epoch_microseconds()
        self.client_send = self.start

    def on_finish(self, exc_info):
        self.end = current_epoch_microseconds()
        self.elapsed = self.end - self.start
        self.record()

    def _create_time_annotation(self, annotation_type, timestamp):
        """Create Zipkin-compatible Annotation for a span.

        This should be used for generating span annotations with a time component,
        e.g. the core "cs", "sr", "ss", and "sr" Zipkin Annotations
        """
        endpoint_info = {
            'serviceName': self.service_name,
            'ipv4': socket.gethostbyname(socket.gethostname()),
        }
        return {
            'endpoint': endpoint_info,
            'timestamp': timestamp,
            'value': annotation_type,
        }

    def _create_binary_annotation(self, ):
        """Create Zipkin-compatible BinaryAnnotation for a span.

        This should be used for generating span annotations that
        do not have a time component, e.g. URI, arbitrary request tags
        """
        # TODO: Implement.
        raise NotImplementedError

    def _to_span_obj(self, annotations, binary_annotations):
        span = {
            "traceId": self.span.trace_id,
            "name": self.span.name,
            "id": self.span.id,
            "timestamp": self.start,
            "duration": self.elapsed,
            "annotations": annotations,
            "binary_annotations": binary_annotations,
        }

        span['parentId'] = self.span.parent_id or 0
        return span

    def _serialize(self):
        """Serialize span information into Zipkin-accepted format."""
        annotations = []

        annotations.append(
            self._create_time_annotation(
                ANNOTATIONS['CLIENT_RECEIVE'],
                self.start,
            )
        )

        annotations.append(
            self._create_time_annotation(
                ANNOTATIONS['CLIENT_SEND'],
                self.end,
            )
        )

        return self._to_span_obj(annotations, self.binary_annotations)

    def record(self):
        """Record serialized span."""
        serialized = self._serialize()
        self.recorder.send(serialized)


class TraceServerSpanObserver(TraceSpanObserver):
    """Span recording observer for incoming request spans.

    This observer implements the server-side span portion of a
    Zipkin request trace
    """

    def __init__(self, service_name, span, recorder):
        self.service_name = service_name
        self.span = span
        self.recorder = recorder
        super(TraceServerSpanObserver, self).__init__(service_name,
                                                      span,
                                                      recorder)

    def on_start(self):
        self.start = current_epoch_microseconds()

    def on_child_span_created(self, child_span):
        child_span_observer = TraceSpanObserver(self.recorder)
        child_span.register(TraceSpanObserver(self.recorder))

    def on_finish(self, exc_info):
        self.end = current_epoch_microseconds()
        self.elapsed = self.end - self.start
        self.record()

    def _serialize(self):
        """Serialize span information into Zipkin-accepted format."""
        annotations = []

        annotations.append(
            self._create_time_annotation(
                ANNOTATIONS['SERVER_RECEIVE'],
                self.start,
            )
        )
        annotations.append(
            self._create_time_annotation(
                ANNOTATIONS['SERVER_SEND'],
                self.end,
            )
        )

        return self._to_span_obj(annotations, [])


class NullRecorder(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def send(self, span):
        """Write a set of spans to debug log."""
        self.logger.debug("Span recording for trace_id=%s: %s",
                          span['traceId'],
                          span)


class RemoteRecorder(object):
    """Interface for recording spans to a remote collector.

    The RemoteRecorder adds spans to an in-memory Queue for a background
    thread worker to process. It currently does not shut down gracefully -
    in the event of parent process exit, any remaining spans will be discarded.
    """
    def __init__(self, endpoint, max_conns, max_queue_size=10000):
        self.logger = logging.getLogger(self.__class__.__name__)

        adapter = requests.adapters.HTTPAdapter(pool_connections=max_conns,
                                                pool_maxsize=max_conns)
        self.session = requests.Session()
        self.session.mount('http://', adapter)
        self.endpoint = "http://%s/api/v1/spans" % endpoint

        self.span_queue = queue.Queue(maxsize=max_queue_size)
        self.flush_worker = None
        self.flush_worker = threading.Thread(target=self._flush_spans)
        self.flush_worker.name = "span remote recorder"
        self.flush_worker.daemon = True
        self.flush_worker.start()

    def _flush_spans(self):
        """Send a set of spans to remote collector.

        This reads a batch of at most 10 spans off the recorder queue
        and sends them to a remote recording endpoint. If the queue
        empties while being processed before reaching 10 spans, we flush
        immediately.
        """
        while True:
            spans = []
            try:
                while len(spans) < 10:
                    spans.append(self.span_queue.get_nowait())
            except queue.Empty:
                pass
            finally:
                if spans:
                    recording_req = requests.Request(
                        'POST', self.endpoint,
                        json=spans,
                        headers={
                            'Content-Type': 'application/json',
                        }).prepare()
                    self.session.send(recording_req, timeout=2)
                else:
                    time.sleep(0.1)

    def send(self, span):
        try:
            self.span_queue.put_nowait(span)
        except Exception as e:
            self.logger.error("Failed adding span to recording queue: %s", e)
