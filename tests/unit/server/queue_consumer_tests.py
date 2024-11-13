import datetime
import itertools
import os
import socket
import time

from queue import Empty as QueueEmpty
from queue import Queue
from threading import Thread
from unittest import mock

import pytest
import webtest

from gevent.server import StreamServer

from baseplate.observers.timeout import ServerTimeout
from baseplate.server.queue_consumer import HealthcheckApp
from baseplate.server.queue_consumer import MessageHandler
from baseplate.server.queue_consumer import PumpWorker
from baseplate.server.queue_consumer import QueueConsumer
from baseplate.server.queue_consumer import QueueConsumerFactory
from baseplate.server.queue_consumer import QueueConsumerServer


pytestmark = pytest.mark.skipif(
    "CI" not in os.environ, reason="tests takes too long to run for normal local iteration"
)


class FakePumpWorker(PumpWorker):
    def __init__(self, work_queue, raises=None):
        self.work_queue = work_queue
        self.started = False
        self.stopped = False
        self.raises = raises

    def run(self):
        self.started = True
        if self.raises is not None:
            raise self.raises
        for i in itertools.count():
            self.work_queue.put(i)
            if self.stopped:
                break
            else:
                time.sleep(0.1)

    def stop(self):
        self.stopped = True


class FakeMessageHandler(MessageHandler):
    def __init__(self, raises=None):
        self.raises = raises
        self.messages = []

    def handle(self, message):
        if self.raises is not None:
            raise self.raises
        self.messages.append(message)
        if len(self.messages) > 10:
            self.messages = self.messages[1:10]


class FakeQueueConsumerFactory(QueueConsumerFactory):
    def __init__(self, pump_raises=None, handler_raises=None):
        self.pump_raises = pump_raises
        self.handler_raises = handler_raises

    def build_pump_worker(self, work_queue):
        return FakePumpWorker(work_queue, raises=self.pump_raises)

    def build_message_handler(self):
        return FakeMessageHandler(raises=self.handler_raises)

    def build_health_checker(self, listener):
        return mock.Mock(spec=StreamServer)


class TestQueueConsumer:
    @pytest.fixture
    def consumer(self):
        c = QueueConsumer(Queue(maxsize=5), FakeMessageHandler())
        c._queue_timeout = 0.1
        yield c
        try:
            c.stop()
        except AssertionError:
            pass

    def test_stop_before_run(self, consumer):
        with pytest.raises(AssertionError):
            consumer.stop()

    def test_run(self, consumer):
        for i in range(5):
            consumer.work_queue.put(i)
        t = Thread(target=consumer.run, daemon=True)
        t.start()
        time.sleep(0.1)
        assert consumer.message_handler.messages == [i for i in range(5)]
        assert t.is_alive()
        with pytest.raises(AssertionError):
            consumer.run()

    def test_stop(self, consumer):
        t = Thread(target=consumer.run)
        t.start()
        time.sleep(0.1)
        assert t.is_alive()
        consumer.stop()
        time.sleep(0.5)
        t.join()
        assert consumer.started
        assert consumer.stopped
        assert not t.is_alive()
        with pytest.raises(AssertionError):
            consumer.stop()

    def test_handler_error_stops(self, consumer):
        consumer.work_queue.put(0)
        consumer.message_handler.raises = Exception()
        with pytest.raises(Exception):
            consumer.run()

    def test_queue_empty_in_handler_error(self, consumer):
        # QueueConsumer.run sets a timeout when checking it's work queue so it
        # can periodically check if it has been stopped.  We want to be sure we
        # only catch `queue.Empty` in that case, but let `queue.Empty` errors
        # thrown by the MessageHandler bubble up.
        consumer.work_queue.put(0)
        consumer.message_handler.raises = QueueEmpty()
        with pytest.raises(QueueEmpty):
            consumer.run()


class TestQueueConsumerServer:
    @pytest.fixture
    def build_server(self):
        server = [None]

        def _build_server(max_concurrency=5, pump_raises=None, handler_raises=None):
            server[0] = QueueConsumerServer.new(
                consumer_factory=FakeQueueConsumerFactory(
                    pump_raises=pump_raises, handler_raises=handler_raises
                ),
                max_concurrency=max_concurrency,
                listener=mock.Mock(spec=socket.socket),
                stop_timeout=datetime.timedelta(seconds=30),
            )
            return server[0]

        yield _build_server

        try:
            if server[0] is not None:
                server[0].stop()
        except AssertionError:
            pass

    @pytest.fixture
    def server(self, build_server):
        return build_server()

    def test_new(self):
        max_concurrency = 5
        server = QueueConsumerServer.new(
            consumer_factory=FakeQueueConsumerFactory(),
            max_concurrency=max_concurrency,
            listener=mock.Mock(spec=socket.socket),
            stop_timeout=datetime.timedelta(seconds=30),
        )
        assert not server.started
        assert not server.stopped
        assert not server.pump.started
        assert not server.pump.stopped
        assert server.pump.work_queue.maxsize == 7
        server.healthcheck_server.start.assert_not_called()
        server.healthcheck_server.stop.assert_not_called()
        assert len(server.handlers) == max_concurrency
        assert len(server.handlers) == len(server.threads)
        for handler in server.handlers:
            assert not handler.started
            assert not handler.stopped

    def test_start(self, server):
        server.start()
        assert server.started
        assert not server.stopped
        assert server.pump.started
        assert not server.pump.stopped
        server.healthcheck_server.start.assert_called_once()
        server.healthcheck_server.stop.assert_not_called()
        for handler in server.handlers:
            assert handler.started
            assert not handler.stopped
        with pytest.raises(AssertionError):
            server.start()

    def test_stop(self, server):
        server.start()
        server.stop()
        assert server.started
        assert server.stopped
        assert server.pump.started
        assert server.pump.stopped
        server.healthcheck_server.start.assert_called_once()
        server.healthcheck_server.stop.assert_called_once()
        for handler in server.handlers:
            assert handler.started
            assert handler.stopped
        with pytest.raises(AssertionError):
            server.stop()

    def test_stop_before_start(self, server):
        with pytest.raises(AssertionError):
            server.stop()

    def test_pump_exception_terminates(self, build_server):
        server = build_server(max_concurrency=1, pump_raises=Exception())
        server._terminate = mock.Mock()
        server.start()
        time.sleep(0.5)
        server._terminate.assert_called_once()

    def test_handler_exception_terminates(self, build_server):
        server = build_server(max_concurrency=1, handler_raises=Exception())
        server._terminate = mock.Mock()
        server.start()
        time.sleep(0.5)
        server._terminate.assert_called_once()

    def test_handler_timeout_terminates(self, build_server):
        server = build_server(max_concurrency=1, handler_raises=ServerTimeout("", 10, False))
        server._terminate = mock.Mock()
        server.start()
        time.sleep(0.5)
        server._terminate.assert_called_once()


def test_healthcheck():
    healthcheck_app = HealthcheckApp()
    test_app = webtest.TestApp(healthcheck_app)

    response = test_app.get("/health")
    assert response.content_type == "application/json"
    assert isinstance(response.json, dict)
