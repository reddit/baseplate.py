import abc
import datetime
import logging
import os
import queue
import signal
import socket
import uuid

from threading import Thread
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

from gevent.pywsgi import LoggingLogAdapter
from gevent.pywsgi import WSGIServer
from gevent.server import StreamServer

import baseplate.lib.config

from baseplate.lib.retry import RetryPolicy
from baseplate.observers.timeout import ServerTimeout
from baseplate.server import runtime_monitor


logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from wsgiref.types import StartResponse  # pylint: disable=import-error,no-name-in-module

WSGIEnvironment = Dict[str, Any]
HealthcheckCallback = Callable[[WSGIEnvironment], bool]


class HealthcheckApp:
    def __init__(self, callback: Optional[HealthcheckCallback] = None) -> None:
        self.callback = callback

    def __call__(self, environ: WSGIEnvironment, start_response: "StartResponse") -> List[bytes]:
        ok = True
        if self.callback:
            ok = self.callback(environ)

        if ok:
            start_response("200 OK", [("Content-Type", "application/json")])
            return [b'{"fun message": "all good, boss"}']
        start_response("503 UNAVAILABLE", [("Content-Type", "text/plain")])
        return [b"he's dead jim"]


def make_simple_healthchecker(
    listener: socket.socket, callback: Optional[HealthcheckCallback] = None
) -> WSGIServer:
    return WSGIServer(
        listener=listener,
        application=HealthcheckApp(callback),
        log=LoggingLogAdapter(logger, level=logging.DEBUG),
    )


class PumpWorker(abc.ABC):
    """Reads messages off of a message queue and puts them into a queue.Queue for handling by a MessageHandler.

    The QueueConsumerServer will run a single PumpWorker in its own thread.
    """

    @abc.abstractmethod
    def run(self) -> None:
        """Run the worker.

        This is the method called by the the PumpWorker Thread within the
        QueueConsumerServer and is ultimately responsible for getting messages
        off of the message queue and putting them on the work queue.
        """

    @abc.abstractmethod
    def stop(self) -> None:
        """Signal the PumpWorker that it should stop receiving new messages from its message queue."""


class MessageHandler(abc.ABC):
    """Processes the messages supplied by the PumpWorker.

    A MessageHandler is expected to have a Baseplate object that it can use to
    generate a RequestContext and a ServerSpan before processing each message.

    The QueueConsumerServer will run multiple MessageHandlers, each in their own
    Thread based on the max_concurrency value it is given.  Each MessageHandler
    will be wrapped in a QueueConsumer object that will handle reading new
    messages off of the work queue and passing them to the MessageHandler as well
    as providing the methods that start and stop the work done by the Threads.
    """

    @abc.abstractmethod
    def handle(self, message: Any) -> None:
        """Handle a message supplied by a PumpWorker.

        This method is what the server calls when it reads a new message off of
        the internal work queue that is fed by the PumpWorker.  This is both the
        method where you should handle processing the message and setting up
        the RequestContext and ServerSpan for Baseplate. We recommend that that
        you only implement boilerplate, such as Baseplate setup and standard
        ack/requeue behavior for the message, in this method.  The actual business
        logic around handling a message should be given to the MessageHandler
        when it is constructed rather than defined in here.
        """


class QueueConsumerFactory(abc.ABC):
    """Factory for building all of the objects needed to run a QueueConsumerServer.

    This is the type of object that you build in your `service.make_queue_consumer`
    function to be passed to `make_server`.
    """

    @abc.abstractmethod
    def build_pump_worker(self, work_queue: queue.Queue) -> PumpWorker:
        """Build an object implementing the PumpWorker interface.

        `work_queue` is the Queue that will be shared between the PumpWorker and
        and the QueueConsumer to pass messages to the MessageHandler, be sure
        that your PumpWorker uses `work_queue`.
        """

    @abc.abstractmethod
    def build_message_handler(self) -> MessageHandler:
        """Build an object implementing the MessageHandler interface."""

    @abc.abstractmethod
    def build_health_checker(self, listener: socket.socket) -> StreamServer:
        """Build an HTTP server to service health checks."""


class QueueConsumer:
    """Wrapper around a MessageHandler object that interfaces with the work_queue and starts/stops the handle loop.

    This object is used by the QueueConsumerServer to wrap a MessageHandler object
    before creating a worker Thread.  This allows the MessageHandler to focus soley
    on handling a single message while the QueueConsumer pulls messages from the
    work_queue for the MessageHandler and handles starting/stopping based on the
    commands from the server.
    """

    def __init__(self, work_queue: queue.Queue, message_handler: MessageHandler):
        self.id = uuid.uuid4()
        self.work_queue = work_queue
        self.message_handler = message_handler
        self.started = False
        self.stopped = False
        self._queue_timeout = 5

    def stop(self) -> None:
        """Signal the QueueConsumer to stop processing."""
        assert self.started
        assert not self.stopped
        self.stopped = True

    def run(self) -> None:
        """Run the queue consumer until stopped or hit an unhandled Exception."""
        assert not self.started
        assert not self.stopped
        logger.debug("Consumer <%s> starting.", self.id)
        self.started = True
        while not self.stopped:
            try:
                # We set a timeout so we can periodically check if we should
                # stop, this way we will actually return if we have recieved a
                # `stop()` call rather than hanging in a `get` waiting for a
                # new message.  If we did not do this, we would not be able to
                # wait for all of our workers to finish before stopping the
                # server.
                message = self.work_queue.get(timeout=self._queue_timeout)
            except queue.Empty:
                pass
            else:
                # Ensure that if self.message_handler.handle throws a `queue.Empty`
                # error, that bubbles up and is not treated as though `self.work_queue`
                # is empty
                self.message_handler.handle(message)
        logger.debug("Consumer <%s> stopping.", self.id)


class QueueConsumerServer:
    """Server for running long-lived queue consumers."""

    def __init__(
        self,
        pump: PumpWorker,
        handlers: Sequence[QueueConsumer],
        healthcheck_server: StreamServer,
        stop_timeout: datetime.timedelta,
    ):
        self.pump = pump
        self.handlers = handlers
        self.healthcheck_server = healthcheck_server
        self.stop_timeout = stop_timeout

        def watcher(fn: Callable) -> Callable:
            """Terminates the server (gracefully) if `fn` raises an Exception.

            Used to monitor the pump and handler threads for Exceptions so we can
            shut down the server if one of them exits unexpectedly.
            """

            def _run_and_terminate(*a: Any, **kw: Any) -> Any:
                try:
                    return fn(*a, **kw)
                except (Exception, ServerTimeout):
                    logger.exception("Unhandled error in pump or handler thread, terminating.")
                    self._terminate()
                return None

            return _run_and_terminate

        self.pump_thread = Thread(target=watcher(self.pump.run), daemon=True)
        self.threads = [Thread(target=watcher(handler.run)) for handler in self.handlers]
        self.started = False
        self.stopped = False

    @classmethod
    def new(
        cls,
        max_concurrency: int,
        consumer_factory: QueueConsumerFactory,
        listener: socket.socket,
        stop_timeout: datetime.timedelta,
    ) -> "QueueConsumerServer":
        """Build a new QueueConsumerServer."""
        # We want to give some headroom on the queue so our handlers can grab
        # a new message right after they finish so we keep an extra
        # max_concurrency / 2 messages in the queue.
        maxsize = max_concurrency + max_concurrency // 2
        work_queue: queue.Queue = queue.Queue(maxsize=maxsize)
        handlers = [
            QueueConsumer(
                work_queue=work_queue, message_handler=consumer_factory.build_message_handler()
            )
            for _ in range(max_concurrency)
        ]
        return cls(
            pump=consumer_factory.build_pump_worker(work_queue),
            handlers=handlers,
            healthcheck_server=consumer_factory.build_health_checker(listener),
            stop_timeout=stop_timeout,
        )

    def _terminate(self) -> None:
        """Send a SIGTERM signal to ourselves so baseplate can call `stop`."""
        assert self.started
        if not self.stopped:
            os.kill(os.getpid(), signal.SIGTERM)

    def start(self) -> None:
        """Start the server.

        Starts all of the worker threads and the healthcheck server (if it exists).

        Should only be called once and should not be called after the server is
        stopped, will raise an AssertionError in either of those cases.
        """
        assert not self.started
        assert not self.stopped
        logger.debug("Starting server.")
        self.started = True
        logger.debug("Starting pump thread.")
        self.pump_thread.start()
        logger.debug("Starting message handler threads.")
        for thread in self.threads:
            thread.start()
        logger.debug("Starting healthcheck server.")
        self.healthcheck_server.start()
        logger.debug("Server started.")

    def stop(self) -> None:
        """Start the server.

        Stop all of the worker threads and the healthcheck server (if it exists).
        Waits for the handler threads to drain before returning but does not wait
        for the pump or watcher threads to finish.

        Should only be called once and should not be before the server is
        started, will raise an AssertionError in either of those cases.
        """
        assert self.started
        assert not self.stopped
        logger.debug("Stopping server.")
        self.stopped = True
        # Stop the pump first so we stop consuming messages from the message
        # queue
        logger.debug("Stopping pump thread.")
        self.pump.stop()
        # It's important to call `handler.stop()` before calling `join` on the
        # handler threads, otherwise we'll be waiting for threads that have not
        # been instructed to stop.
        logger.debug("Stopping message handler threads.")
        for handler in self.handlers:
            handler.stop()
        retry_policy = RetryPolicy.new(budget=self.stop_timeout.total_seconds())
        logger.debug("Waiting for message handler threads to drain.")
        for time_remaining, thread in zip(retry_policy, self.threads):
            thread.join(timeout=time_remaining)
        # Stop the healthcheck server last
        logger.debug("Stopping healthcheck server.")
        self.healthcheck_server.stop()
        logger.debug("Server stopped.")


def make_server(
    server_config: Dict[str, str], listener: socket.socket, app: QueueConsumerFactory
) -> QueueConsumerServer:
    """Make a queue consumer server for long running queue consumer apps.

    If you require that you process messages in-order, your handler is heavily CPU
    bound, or you don't do any IO when handling a message you should restrict
    max_concurrency to 1.
    """
    cfg = baseplate.lib.config.parse_config(
        server_config,
        {
            "max_concurrency": baseplate.lib.config.Integer,
            "stop_timeout": baseplate.config.Optional(
                baseplate.config.Timespan, default=datetime.timedelta(seconds=30)
            ),
        },
    )

    runtime_monitor.start(server_config, app, pool=None)

    return QueueConsumerServer.new(
        consumer_factory=app,
        max_concurrency=cfg.max_concurrency,
        listener=listener,
        stop_timeout=cfg.stop_timeout,
    )
