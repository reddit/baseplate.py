import socket
import time

from queue import Queue
from unittest import mock

import kombu
import pytest

from gevent.server import StreamServer
from prometheus_client import REGISTRY

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate.frameworks.queue_consumer.kombu import AMQP_ACTIVE_MESSAGES
from baseplate.frameworks.queue_consumer.kombu import AMQP_PROCESSED_TOTAL
from baseplate.frameworks.queue_consumer.kombu import AMQP_PROCESSING_TIME
from baseplate.frameworks.queue_consumer.kombu import AMQP_REJECTED_REASON_RETRIES
from baseplate.frameworks.queue_consumer.kombu import AMQP_REJECTED_REASON_TTL
from baseplate.frameworks.queue_consumer.kombu import AMQP_REJECTED_TOTAL
from baseplate.frameworks.queue_consumer.kombu import AMQP_REPUBLISHED_TOTAL
from baseplate.frameworks.queue_consumer.kombu import AmqpConsumerPrometheusLabels
from baseplate.frameworks.queue_consumer.kombu import FatalMessageHandlerError
from baseplate.frameworks.queue_consumer.kombu import KombuConsumerWorker
from baseplate.frameworks.queue_consumer.kombu import KombuMessageHandler
from baseplate.frameworks.queue_consumer.kombu import KombuQueueConsumerFactory
from baseplate.frameworks.queue_consumer.kombu import RetryMode
from baseplate.lib.errors import RecoverableException
from baseplate.lib.errors import UnrecoverableException

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
    def setup(self):
        AMQP_PROCESSING_TIME.clear()
        AMQP_PROCESSED_TOTAL.clear()
        AMQP_REPUBLISHED_TOTAL.clear()
        AMQP_REJECTED_TOTAL.clear()
        AMQP_ACTIVE_MESSAGES.clear()

    @pytest.fixture
    def message(self, connection):
        msg = mock.Mock(spec=kombu.Message)
        msg.delivery_info = {
            "routing_key": "routing-key",
            "consumer_tag": "consumer-tag",
            "delivery_tag": "delivery-tag",
            "exchange": "exchange",
        }
        msg.channel.connection.client = connection
        msg.headers = {}
        msg.decode.return_value = {"foo": "bar"}
        return msg

    @pytest.mark.parametrize(
        "ttl_delta,handled",
        [
            (None, True),
            (-60, False),
            (60, True),
        ],
    )
    def test_handle(self, ttl_delta, handled, context, span, baseplate, name, message):
        handler_fn = mock.Mock()
        handler = KombuMessageHandler(baseplate, name, handler_fn)
        prom_labels = AmqpConsumerPrometheusLabels(
            amqp_address="hostname:port",
            amqp_virtual_host="/",
            amqp_exchange_name="exchange",
            amqp_routing_key="routing-key",
        )
        if ttl_delta:
            message.headers["x-ttl"] = int(time.time()) + ttl_delta
        handler.handle(message)

        if handled:
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
            message.reject.assert_not_called()
        else:
            baseplate.make_context_object.assert_not_called()
            baseplate.make_server_span.assert_not_called()
            message.ack.assert_not_called()
            message.reject.assert_called_once()

        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"amqp_success": "true", "le": "+Inf"}},
            )
            == (1 if handled else None)
        )
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"amqp_success": "true"}},
            )
            == (1 if handled else None)
        )
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_REPUBLISHED_TOTAL._name}_total",
                {**prom_labels._asdict()},
            )
            is None
        )
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_REJECTED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_RETRIES}},
            )
            is None
        )
        assert (
            REGISTRY.get_sample_value(
                f"{AMQP_REJECTED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_TTL}},
            )
            == (None if handled else 1)
        )
        assert REGISTRY.get_sample_value(
            f"{AMQP_ACTIVE_MESSAGES._name}", prom_labels._asdict()
        ) == (0 if handled else None)

    @pytest.mark.parametrize(
        "err,expectation,requeued",
        [
            (RecoverableException(), does_not_raise(), True),
            (UnrecoverableException(), does_not_raise(), False),
            (ValueError(), does_not_raise(), True),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError), True),
        ],
    )
    def test_errors(self, err, expectation, requeued, baseplate, name, message):
        def handler_fn(ctx, body, msg):
            raise err

        prom_labels = AmqpConsumerPrometheusLabels(
            amqp_address="hostname:port",
            amqp_virtual_host="/",
            amqp_exchange_name="exchange",
            amqp_routing_key="routing-key",
        )
        mock_manager = mock.Mock()
        with mock.patch.object(
            AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
            "inc",
            wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
                "dec",
                wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")
                handler = KombuMessageHandler(baseplate, name, handler_fn)
                with expectation:
                    handler.handle(message)
                if requeued:
                    message.requeue.assert_called_once()
                else:
                    message.requeue.assert_not_called()

                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSING_TIME._name}_bucket",
                        {**prom_labels._asdict(), **{"amqp_success": "false", "le": "+Inf"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"amqp_success": "false"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_ACTIVE_MESSAGES._name}", prom_labels._asdict()
                    )
                    == 0
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REPUBLISHED_TOTAL._name}_total",
                        {**prom_labels._asdict()},
                    )
                    is None
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_RETRIES}},
                    )
                    is None
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_TTL}},
                    )
                    is None
                )

                # we need to assert that not only the end result is 0, but that we increased and then decreased to that value
                assert mock_manager.mock_calls == [mock.call.inc(), mock.call.dec()]

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

        prom_labels = AmqpConsumerPrometheusLabels(
            amqp_address="hostname:port",
            amqp_virtual_host="/",
            amqp_exchange_name="exchange",
            amqp_routing_key="routing-key",
        )
        mock_manager = mock.Mock()
        with mock.patch.object(
            AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
            "inc",
            wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
                "dec",
                wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")

                handler = KombuMessageHandler(baseplate, name, handler_fn, error_handler_fn)
                with expectation:
                    handler.handle(message)
                error_handler_fn.assert_called_once_with(context, message.decode(), message, err)
                message.ack.assert_not_called()
                message.requeue.assert_not_called()

                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSING_TIME._name}_bucket",
                        {**prom_labels._asdict(), **{"amqp_success": "false", "le": "+Inf"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"amqp_success": "false"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_ACTIVE_MESSAGES._name}", prom_labels._asdict()
                    )
                    == 0
                )
                # we need to assert that not only the end result is 0, but that we increased and then decreased to that value
                assert mock_manager.mock_calls == [mock.call.inc(), mock.call.dec()]

                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REPUBLISHED_TOTAL._name}_total",
                        {**prom_labels._asdict()},
                    )
                    is None
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_RETRIES}},
                    )
                    is None
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_RETRIES}},
                    )
                    is None
                )

    @pytest.mark.parametrize(
        "err,expectation,attempt,limit,republished",
        [
            (RecoverableException(), does_not_raise(), None, None, True),
            (RecoverableException(), does_not_raise(), 1, None, True),
            (RecoverableException(), does_not_raise(), 5, None, False),
            (RecoverableException(), does_not_raise(), 3, 3, False),
            (UnrecoverableException(), does_not_raise(), None, None, False),
            (ValueError(), does_not_raise(), None, None, True),
            (FatalMessageHandlerError(), pytest.raises(FatalMessageHandlerError), None, None, True),
        ],
    )
    def test_errors_with_republish(
        self, err, expectation, attempt, limit, republished, baseplate, name, message
    ):
        """Handler is configured with retries limit of 5. Message can contain its own limit too."""

        def handler_fn(ctx, body, msg):
            raise err

        prom_labels = AmqpConsumerPrometheusLabels(
            amqp_address="hostname:port",
            amqp_virtual_host="/",
            amqp_exchange_name="exchange",
            amqp_routing_key="routing-key",
        )
        mock_manager = mock.Mock()
        with mock.patch.object(
            AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
            "inc",
            wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).inc,
        ) as active_inc_spy_method:
            mock_manager.attach_mock(active_inc_spy_method, "inc")
            with mock.patch.object(
                AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()),
                "dec",
                wraps=AMQP_ACTIVE_MESSAGES.labels(**prom_labels._asdict()).dec,
            ) as active_dec_spy_method:
                mock_manager.attach_mock(active_dec_spy_method, "dec")
                handler = KombuMessageHandler(
                    baseplate, name, handler_fn, retry_mode=RetryMode.REPUBLISH, retry_limit=5
                )
                if attempt:
                    message.headers["x-retry-count"] = attempt
                if limit:
                    message.headers["x-retry-limit"] = limit
                with expectation:
                    handler.handle(message)

                message.requeue.assert_not_called()
                if republished:
                    message.channel.prepare_message.assert_called_once()
                    message.channel.basic_publish.assert_called_once()
                    message.ack.assert_called_once()
                    message.reject.assert_not_called()
                else:
                    message.channel.prepare_message.assert_not_called()
                    message.channel.basic_publish.assert_not_called()
                    message.ack.assert_not_called()
                    message.reject.assert_called_once()

                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSING_TIME._name}_bucket",
                        {**prom_labels._asdict(), **{"amqp_success": "false", "le": "+Inf"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_PROCESSED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"amqp_success": "false"}},
                    )
                    == 1
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_ACTIVE_MESSAGES._name}", prom_labels._asdict()
                    )
                    == 0
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REPUBLISHED_TOTAL._name}_total",
                        {**prom_labels._asdict()},
                    )
                    == (1 if republished else None)
                )
                retry_reached_expectation = None
                if attempt:
                    if attempt >= 5 or (limit and attempt >= limit):
                        retry_reached_expectation = 1
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_RETRIES}},
                    )
                    == retry_reached_expectation
                )
                assert (
                    REGISTRY.get_sample_value(
                        f"{AMQP_REJECTED_TOTAL._name}_total",
                        {**prom_labels._asdict(), **{"reason_code": AMQP_REJECTED_REASON_TTL}},
                    )
                    is None
                )
                # we need to assert that not only the end result is 0, but that we increased and then decreased to that value
                assert mock_manager.mock_calls == [mock.call.inc(), mock.call.dec()]


@pytest.fixture
def connection():
    return mock.Mock(spec=kombu.Connection, virtual_host="/", host="hostname:port")


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
            worker_kwargs = {"prefetch_limit": 1}
            return KombuQueueConsumerFactory.new(
                baseplate=baseplate,
                exchange=exchange,
                connection=connection,
                queue_name=name,
                routing_keys=routing_keys,
                handler_fn=lambda ctx, body, msg: True,
                error_handler_fn=lambda ctx, body, msg: True,
                health_check_fn=health_check_fn,
                worker_kwargs=worker_kwargs,
            )

        return _make_queue_consumer_factory

    def test_new(self, baseplate, exchange, connection, name, routing_keys):
        handler_fn = mock.Mock()
        error_handler_fn = mock.Mock()
        health_check_fn = mock.Mock()
        worker_kwargs = {"prefetch_limit": 1}
        factory = KombuQueueConsumerFactory.new(
            baseplate=baseplate,
            exchange=exchange,
            connection=connection,
            queue_name=name,
            routing_keys=routing_keys,
            handler_fn=handler_fn,
            error_handler_fn=error_handler_fn,
            health_check_fn=health_check_fn,
            worker_kwargs=worker_kwargs,
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
        assert worker_kwargs == worker_kwargs

    def test_build_pump_worker(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        work_queue = Queue(maxsize=10)
        pump = factory.build_pump_worker(work_queue)
        assert isinstance(pump, KombuConsumerWorker)
        assert pump.connection == factory.connection
        assert pump.queues == factory.queues
        assert pump.work_queue == work_queue
        assert pump.kwargs == {"prefetch_limit": 1}

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
