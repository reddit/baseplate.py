import socket

from datetime import timedelta
from queue import Queue
from unittest import mock

import kombu
import pytest

from gevent.server import StreamServer

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate.frameworks.queue_consumer.kombu import FatalMessageHandlerError
from baseplate.frameworks.queue_consumer.kombu import KombuConsumerWorker
from baseplate.frameworks.queue_consumer.kombu import KombuMessageHandler
from baseplate.frameworks.queue_consumer.kombu import KombuQueueConsumerFactory
from baseplate.frameworks.queue_consumer.kombu import KombuBatchConsumerWorker
from baseplate.frameworks.queue_consumer.kombu import KombuBatchMessageHandler
from baseplate.frameworks.queue_consumer.kombu import KombuBatchQueueConsumerFactory
from baseplate.lib.batched_queue import BatchedQueue

from .... import does_not_raise


@pytest.fixture
def context():
    return mock.Mock(spec=RequestContext)


@pytest.fixture
def span():
    return mock.Mock(spec=ServerSpan)


@pytest.fixture
def baseplate(context, span):
    bp = mock.MagicMock(spec=Baseplate)
    bp._metrics_client = None
    bp.make_server_span().__enter__.return_value = span
    bp.make_context_object.return_value = context
    # Reset the mock calls since setting up the span actually triggers a "call"
    # to bp.make_server_span
    bp.reset_mock()
    return bp


@pytest.fixture
def name():
    return "test-handler"


class TestKombuMessageHandler:
    @pytest.fixture
    def message(self):
        msg = mock.Mock(spec=kombu.Message)
        msg.delivery_info = {
            "routing_key": "routing-key",
            "consumer_tag": "consumer-tag",
            "delivery_tag": "delivery-tag",
            "exchange": "exchange",
        }
        msg.decode.return_value = {"foo": "bar"}
        return msg

    def test_handle(self, context, span, baseplate, name, message):
        handler_fn = mock.Mock()
        handler = KombuMessageHandler(baseplate, name, handler_fn)
        handler.handle(message)
        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, name)
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "consumer"),
                mock.call("amqp.routing_key", "routing-key"),
                mock.call("amqp.consumer_tag", "consumer-tag"),
                mock.call("amqp.delivery_tag", "delivery-tag"),
                mock.call("amqp.exchange", "exchange"),
            ],
            any_order=True,
        )
        handler_fn.assert_called_once_with(context, message.decode(), message)
        message.ack.assert_called_once()

    @pytest.mark.parametrize(
        "err,expectation",
        [
            (ValueError(), does_not_raise()),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError)),
        ],
    )
    def test_errors(self, err, expectation, baseplate, name, message):
        def handler_fn(ctx, body, msg):
            raise err

        handler = KombuMessageHandler(baseplate, name, handler_fn)
        with expectation:
            handler.handle(message)
        message.requeue.assert_called_once()

    @pytest.mark.parametrize(
        "err,expectation",
        [
            (ValueError(), does_not_raise()),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError)),
        ],
    )
    def test_errors_with_error_handler_fn(
        self, err, expectation, context, baseplate, name, message
    ):
        def handler_fn(ctx, body, msg):
            raise err

        error_handler_fn = mock.Mock()

        handler = KombuMessageHandler(baseplate, name, handler_fn, error_handler_fn)
        with expectation:
            handler.handle(message)
        error_handler_fn.assert_called_once_with(context, message.decode(), message, err)
        message.ack.assert_not_called()
        message.requeue.assert_not_called()


class TestKombuBatchMessageHandler:
    @pytest.fixture
    def messages(self):
        msg1 = mock.Mock(spec=kombu.Message)
        msg1.acknowledged = False
        msg1.delivery_info = {
            "routing_key": "routing-key-1",
            "consumer_tag": "consumer-tag-1",
            "delivery_tag": "delivery-tag-1",
            "exchange": "exchange-1",
        }
        msg1.decode.return_value = {"foo": "bar"}

        msg2 = mock.Mock(spec=kombu.Message)
        msg2.acknowledged = False
        msg2.delivery_info = {
            "routing_key": "routing-key-2",
            "consumer_tag": "consumer-tag-2",
            "delivery_tag": "delivery-tag-2",
            "exchange": "exchange-2",
        }
        msg2.decode.return_value = {"foo": "bar"}

        return [msg1, msg2]

    def test_handle(self, context, span, baseplate, name, messages):
        handler_fn = mock.Mock()
        handler = KombuBatchMessageHandler(baseplate, name, handler_fn)
        handler.handle(messages)
        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, name)
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "batch_consumer"),
                mock.call("size", 2),
                mock.call("amqp.routing_key_0", "routing-key-1"),
                mock.call("amqp.consumer_tag_0", "consumer-tag-1"),
                mock.call("amqp.delivery_tag_0", "delivery-tag-1"),
                mock.call("amqp.exchange_0", "exchange-1"),
                mock.call("amqp.routing_key_1", "routing-key-2"),
                mock.call("amqp.consumer_tag_1", "consumer-tag-2"),
                mock.call("amqp.delivery_tag_1", "delivery-tag-2"),
                mock.call("amqp.exchange_1", "exchange-2"),
            ],
            any_order=True,
        )
        handler_fn.assert_called_once_with(context, messages)
        for message in messages:
            message.ack.assert_called_once()

    @pytest.mark.parametrize(
        "err,expectation",
        [
            (ValueError(), does_not_raise()),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError)),
        ],
    )
    def test_errors(self, err, expectation, baseplate, name, messages):
        def handler_fn(ctx, messages):
            raise err

        handler = KombuBatchMessageHandler(baseplate, name, handler_fn)
        with expectation:
            handler.handle(messages)
        for message in messages:
            message.requeue.assert_called_once()

    @pytest.mark.parametrize(
        "err,expectation",
        [
            (ValueError(), does_not_raise()),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError)),
        ],
    )
    def test_errors_with_error_handler_fn(
        self, err, expectation, context, baseplate, name, messages
    ):
        def handler_fn(ctx, messages):
            raise err

        error_handler_fn = mock.Mock()

        handler = KombuBatchMessageHandler(baseplate, name, handler_fn, error_handler_fn)
        with expectation:
            handler.handle(messages)
        error_handler_fn.assert_called_once_with(context, messages, err)
        for message in messages:
            message.ack.assert_not_called()
            message.requeue.assert_not_called()


@pytest.fixture
def connection():
    return mock.Mock(spec=kombu.Connection)


@pytest.fixture
def exchange():
    return mock.Mock(spec=kombu.Exchange)


@pytest.fixture
def routing_keys():
    return ["rk-1", "rk-2"]


class TestQueueConsumerFactory:
    @pytest.fixture
    def make_queue_consumer_factory(self, baseplate, exchange, connection, name, routing_keys):
        def _make_queue_consumer_factory(health_check_fn=None):
            return KombuQueueConsumerFactory.new(
                baseplate=baseplate,
                exchange=exchange,
                connection=connection,
                queue_name=name,
                routing_keys=routing_keys,
                handler_fn=lambda ctx, body, msg: True,
                error_handler_fn=lambda ctx, body, msg: True,
                health_check_fn=health_check_fn,
            )

        return _make_queue_consumer_factory

    def test_new(self, baseplate, exchange, connection, name, routing_keys):
        handler_fn = mock.Mock()
        error_handler_fn = mock.Mock()
        health_check_fn = mock.Mock()
        factory = KombuQueueConsumerFactory.new(
            baseplate=baseplate,
            exchange=exchange,
            connection=connection,
            queue_name=name,
            routing_keys=routing_keys,
            handler_fn=handler_fn,
            error_handler_fn=error_handler_fn,
            health_check_fn=health_check_fn,
        )
        assert factory.baseplate == baseplate
        assert factory.connection == connection
        assert factory.name == name
        assert factory.handler_fn == handler_fn
        assert factory.error_handler_fn == error_handler_fn
        assert factory.health_check_fn == health_check_fn
        for routing_key, queue in zip(routing_keys, factory.queues):
            assert queue.routing_key == routing_key
            assert queue.name == name
            assert queue.exchange == exchange

    def test_build_pump_worker(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        work_queue = Queue(maxsize=10)
        pump = factory.build_pump_worker(work_queue)
        assert isinstance(pump, KombuConsumerWorker)
        assert pump.connection == factory.connection
        assert pump.queues == factory.queues
        assert pump.work_queue == work_queue

    def test_build_message_handler(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        handler = factory.build_message_handler()
        assert isinstance(handler, KombuMessageHandler)
        assert handler.baseplate == factory.baseplate
        assert handler.name == factory.name
        assert handler.handler_fn == factory.handler_fn
        assert handler.error_handler_fn == factory.error_handler_fn

    @pytest.mark.parametrize("health_check_fn", [None, lambda req: True])
    def test_build_health_checker(self, health_check_fn, make_queue_consumer_factory):
        factory = make_queue_consumer_factory(health_check_fn=health_check_fn)
        listener = mock.Mock(spec=socket.socket)
        health_checker = factory.build_health_checker(listener)
        assert isinstance(health_checker, StreamServer)


class TestKombuBatchQueueConsumerFactory:
    @pytest.fixture
    def make_queue_consumer_factory(self, baseplate, exchange, connection, name, routing_keys):
        def _make_queue_consumer_factory(health_check_fn=None):
            return KombuBatchQueueConsumerFactory.new(
                baseplate=baseplate,
                exchange=exchange,
                connection=connection,
                queue_name=name,
                routing_keys=routing_keys,
                handler_fn=lambda ctx, body, msg: True,
                error_handler_fn=lambda ctx, body, msg: True,
                health_check_fn=health_check_fn,
                batch_size=1,
                batch_timeout=timedelta(seconds=1)
            )

        return _make_queue_consumer_factory

    def test_new(self, baseplate, exchange, connection, name, routing_keys):
        handler_fn = mock.Mock()
        error_handler_fn = mock.Mock()
        health_check_fn = mock.Mock()
        factory = KombuBatchQueueConsumerFactory.new(
            baseplate=baseplate,
            exchange=exchange,
            connection=connection,
            queue_name=name,
            routing_keys=routing_keys,
            handler_fn=handler_fn,
            error_handler_fn=error_handler_fn,
            health_check_fn=health_check_fn,
            batch_size=1,
            batch_timeout=timedelta(seconds=1)
        )
        assert factory.baseplate == baseplate
        assert factory.connection == connection
        assert factory.name == name
        assert factory.handler_fn == handler_fn
        assert factory.error_handler_fn == error_handler_fn
        assert factory.health_check_fn == health_check_fn
        for routing_key, queue in zip(routing_keys, factory.queues):
            assert queue.routing_key == routing_key
            assert queue.name == name
            assert queue.exchange == exchange

    def test_build_pump_worker(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        work_queue = Queue(maxsize=10)
        pump = factory.build_pump_worker(work_queue)
        assert isinstance(pump, KombuBatchConsumerWorker)
        assert pump.connection == factory.connection
        assert pump.queues == factory.queues
        assert isinstance(pump.work_queue, BatchedQueue)
        assert pump.work_queue._work_queue == work_queue

    def test_build_message_handler(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        handler = factory.build_message_handler()
        assert isinstance(handler, KombuBatchMessageHandler)
        assert handler.baseplate == factory.baseplate
        assert handler.name == factory.name
        assert handler.handler_fn == factory.handler_fn
        assert handler.error_handler_fn == factory.error_handler_fn

    @pytest.mark.parametrize("health_check_fn", [None, lambda req: True])
    def test_build_health_checker(self, health_check_fn, make_queue_consumer_factory):
        factory = make_queue_consumer_factory(health_check_fn=health_check_fn)
        listener = mock.Mock(spec=socket.socket)
        health_checker = factory.build_health_checker(listener)
        assert isinstance(health_checker, StreamServer)


@pytest.fixture
def queues(name, exchange, routing_keys):
    return [kombu.Queue(name=name, exchange=exchange, routing_key=key) for key in routing_keys]


class TestKombuConsumerWorker:
    @pytest.fixture
    def consumer_worker(self, connection, queues):
        return KombuConsumerWorker(connection, queues, Queue(maxsize=10))

    def test_initial_state(self, consumer_worker):
        assert getattr(consumer_worker, "should_stop", None) is not True

    @mock.patch("baseplate.frameworks.queue_consumer.kombu.ConsumerMixin.run")
    def test_run(self, run, consumer_worker):
        consumer_worker.run()
        run.assert_called_once()

    def test_stop(self, consumer_worker):
        consumer_worker.stop()
        assert consumer_worker.should_stop is True


class TestKombuBatchConsumerWorker:
    @pytest.fixture
    def consumer_worker(self, connection, queues):
        return KombuBatchConsumerWorker(connection, queues, BatchedQueue(Queue(maxsize=10), 1, timedelta(seconds=1)))

    def test_initial_state(self, consumer_worker):
        assert getattr(consumer_worker, "should_stop", None) is not True

    @ mock.patch("baseplate.frameworks.queue_consumer.kombu.ConsumerMixin.run")
    def test_run(self, run, consumer_worker):
        consumer_worker.run()
        run.assert_called_once()

    def test_stop(self, consumer_worker):
        consumer_worker.stop()
        assert consumer_worker.should_stop is True
