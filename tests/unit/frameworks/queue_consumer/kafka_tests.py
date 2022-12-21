import socket

from queue import Queue
from unittest import mock

import confluent_kafka
import pytest

from gevent.server import StreamServer
from prometheus_client import REGISTRY

from baseplate import Baseplate
from baseplate import RequestContext
from baseplate import ServerSpan
from baseplate.frameworks.queue_consumer.kafka import FastConsumerFactory
from baseplate.frameworks.queue_consumer.kafka import InOrderConsumerFactory
from baseplate.frameworks.queue_consumer.kafka import KAFKA_ACTIVE_MESSAGES
from baseplate.frameworks.queue_consumer.kafka import KAFKA_PROCESSED_TOTAL
from baseplate.frameworks.queue_consumer.kafka import KAFKA_PROCESSING_TIME
from baseplate.frameworks.queue_consumer.kafka import KafkaConsumerPrometheusLabels
from baseplate.frameworks.queue_consumer.kafka import KafkaConsumerWorker
from baseplate.frameworks.queue_consumer.kafka import KafkaMessageHandler
from baseplate.lib import metrics


@pytest.fixture
def context():
    context = mock.Mock(spec=RequestContext)
    context.metrics = mock.Mock(spec=metrics.Client)
    return context


@pytest.fixture
def span():
    sp = mock.MagicMock(spec=ServerSpan)
    sp.make_child().__enter__.return_value = mock.MagicMock()
    return sp


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
    return "kafka_consumer.test_group"


class TestKafkaMessageHandler:
    def setup(self):
        KAFKA_PROCESSING_TIME.clear()
        KAFKA_PROCESSED_TOTAL.clear()
        KAFKA_ACTIVE_MESSAGES.clear()

    @pytest.fixture
    def message(self):
        msg = mock.Mock(spec=confluent_kafka.Message)
        msg.topic.return_value = "topic_1"
        msg.key.return_value = "key_1"
        msg.partition.return_value = 3
        msg.offset.return_value = 33
        msg.timestamp.return_value = 123456
        msg.value.return_value = b"message-payload"
        msg.error.return_value = None
        return msg

    @mock.patch("baseplate.frameworks.queue_consumer.kafka.time")
    @pytest.mark.parametrize("prometheus_client_name", [None, "my_kafka_client_name"])
    def test_handle(self, time, context, span, baseplate, name, message, prometheus_client_name):
        time.time.return_value = 2.0
        time.perf_counter.side_effect = [1, 2]

        prom_labels = KafkaConsumerPrometheusLabels(
            kafka_client_name=prometheus_client_name if prometheus_client_name is not None else "",
            kafka_topic="topic_1",
        )

        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock(
            return_value={"endpoint_timestamp": 1000.0, "body": "some text"}
        )
        on_success_fn = mock.Mock()

        mock_timer = mock.Mock()
        context.metrics.timer.return_value = mock_timer

        mock_gauge = mock.Mock()
        context.metrics.gauge.return_value = mock_gauge

        if prometheus_client_name is None:
            handler = KafkaMessageHandler(
                baseplate, name, handler_fn, message_unpack_fn, on_success_fn
            )
        else:
            handler = KafkaMessageHandler(
                baseplate,
                name,
                handler_fn,
                message_unpack_fn,
                on_success_fn,
                prometheus_client_name=prometheus_client_name,
            )
        handler.handle(message)

        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, f"{name}.handler")
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "consumer"),
                mock.call("kafka.topic", "topic_1"),
                mock.call("kafka.key", "key_1"),
                mock.call("kafka.partition", 3),
                mock.call("kafka.offset", 33),
                mock.call("kafka.timestamp", 123456),
            ],
            any_order=True,
        )
        message_unpack_fn.assert_called_once_with(b"message-payload")
        handler_fn.assert_called_once_with(
            context, {"endpoint_timestamp": 1000.0, "body": "some text"}, message
        )
        on_success_fn.assert_called_once_with(
            context, {"endpoint_timestamp": 1000.0, "body": "some text"}, message
        )

        context.metrics.timer.assert_called_once_with(f"{name}.topic_1.latency")
        mock_timer.send.assert_called_once_with(1.0)

        context.metrics.gauge.assert_called_once_with(f"{name}.topic_1.offset.3")
        mock_gauge.replace.assert_called_once_with(33)

        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"kafka_success": "true", "le": "+Inf"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"kafka_success": "true"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{KAFKA_ACTIVE_MESSAGES._name}", prom_labels._asdict()) == 0
        )

    def test_handle_no_endpoint_timestamp(self, context, span, baseplate, name, message):
        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock(return_value={"body": "some text"})
        on_success_fn = mock.Mock()

        mock_gauge = mock.Mock()
        context.metrics.gauge.return_value = mock_gauge

        prom_labels = KafkaConsumerPrometheusLabels(
            kafka_client_name="",
            kafka_topic="topic_1",
        )

        handler = KafkaMessageHandler(baseplate, name, handler_fn, message_unpack_fn, on_success_fn)
        handler.handle(message)

        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, f"{name}.handler")
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "consumer"),
                mock.call("kafka.topic", "topic_1"),
                mock.call("kafka.key", "key_1"),
                mock.call("kafka.partition", 3),
                mock.call("kafka.offset", 33),
                mock.call("kafka.timestamp", 123456),
            ],
            any_order=True,
        )
        message_unpack_fn.assert_called_once_with(b"message-payload")
        handler_fn.assert_called_once_with(context, {"body": "some text"}, message)
        on_success_fn.assert_called_once_with(context, {"body": "some text"}, message)

        context.metrics.timer.assert_not_called()

        context.metrics.gauge.assert_called_once_with(f"{name}.topic_1.offset.3")
        mock_gauge.replace.assert_called_once_with(33)

        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"kafka_success": "true", "le": "+Inf"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"kafka_success": "true"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{KAFKA_ACTIVE_MESSAGES._name}", prom_labels._asdict()) == 0
        )

    def test_handle_kafka_error(self, context, span, baseplate, name, message):
        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock()
        on_success_fn = mock.Mock()

        # we can't actually create an instance of KafkaError, so use a mock
        error_mock = mock.Mock()
        error_mock.str.return_value = "kafka error"
        message.error.return_value = error_mock

        handler = KafkaMessageHandler(baseplate, name, handler_fn, message_unpack_fn, on_success_fn)

        prom_labels = KafkaConsumerPrometheusLabels(
            kafka_client_name="",
            kafka_topic="topic_1",
        )

        with pytest.raises(ValueError):
            handler.handle(message)

        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, f"{name}.handler")
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_not_called()
        message_unpack_fn.assert_not_called()
        handler_fn.assert_not_called()
        on_success_fn.assert_not_called()
        context.metrics.timer.assert_not_called()
        context.metrics.gauge.assert_not_called()

        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"kafka_success": "false", "le": "+Inf"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"kafka_success": "false"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{KAFKA_ACTIVE_MESSAGES._name}", prom_labels._asdict()) == 0
        )

    def test_handle_unpack_error(self, context, span, baseplate, name, message):
        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock(side_effect=ValueError("something bad happened"))
        on_success_fn = mock.Mock()

        context.span = span

        handler = KafkaMessageHandler(baseplate, name, handler_fn, message_unpack_fn, on_success_fn)

        prom_labels = KafkaConsumerPrometheusLabels(
            kafka_client_name="",
            kafka_topic="topic_1",
        )

        handler.handle(message)

        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, f"{name}.handler")
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "consumer"),
                mock.call("kafka.topic", "topic_1"),
                mock.call("kafka.key", "key_1"),
                mock.call("kafka.partition", 3),
                mock.call("kafka.offset", 33),
                mock.call("kafka.timestamp", 123456),
            ],
            any_order=True,
        )
        span.incr_tag.assert_called_once_with(f"{name}.topic_1.invalid_message")
        message_unpack_fn.assert_called_once_with(b"message-payload")
        handler_fn.assert_not_called()
        on_success_fn.assert_not_called()
        context.metrics.timer.assert_not_called()
        context.metrics.gauge.assert_not_called()

        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"kafka_success": "false", "le": "+Inf"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"kafka_success": "false"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{KAFKA_ACTIVE_MESSAGES._name}", prom_labels._asdict()) == 0
        )

    def test_handle_handler_error(self, context, span, baseplate, name, message):
        handler_fn = mock.Mock(side_effect=ValueError("something went wrong"))
        message_unpack_fn = mock.Mock(
            return_value={"endpoint_timestamp": 1000.0, "body": "some text"}
        )
        on_success_fn = mock.Mock()

        handler = KafkaMessageHandler(baseplate, name, handler_fn, message_unpack_fn, on_success_fn)

        prom_labels = KafkaConsumerPrometheusLabels(
            kafka_client_name="",
            kafka_topic="topic_1",
        )

        with pytest.raises(ValueError):
            handler.handle(message)

        baseplate.make_context_object.assert_called_once()
        baseplate.make_server_span.assert_called_once_with(context, f"{name}.handler")
        baseplate.make_server_span().__enter__.assert_called_once()
        span.set_tag.assert_has_calls(
            [
                mock.call("kind", "consumer"),
                mock.call("kafka.topic", "topic_1"),
                mock.call("kafka.key", "key_1"),
                mock.call("kafka.partition", 3),
                mock.call("kafka.offset", 33),
                mock.call("kafka.timestamp", 123456),
            ],
            any_order=True,
        )
        message_unpack_fn.assert_called_once_with(b"message-payload")
        handler_fn.assert_called_once_with(
            context, {"endpoint_timestamp": 1000.0, "body": "some text"}, message
        )
        on_success_fn.assert_not_called()
        context.metrics.timer.assert_not_called()
        context.metrics.gauge.assert_not_called()

        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSING_TIME._name}_bucket",
                {**prom_labels._asdict(), **{"kafka_success": "false", "le": "+Inf"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(
                f"{KAFKA_PROCESSED_TOTAL._name}_total",
                {**prom_labels._asdict(), **{"kafka_success": "false"}},
            )
            == 1
        )
        assert (
            REGISTRY.get_sample_value(f"{KAFKA_ACTIVE_MESSAGES._name}", prom_labels._asdict()) == 0
        )


@pytest.fixture
def bootstrap_servers():
    return "127.0.0.1:9092"


@pytest.fixture
def group_id():
    return "test_service.test_group"


@pytest.fixture
def topics():
    return ["topic_1"]


class TestInOrderConsumerFactory:
    @mock.patch("confluent_kafka.Consumer")
    def test_make_kafka_consumer(
        self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics
    ):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer
        extra_config = {"queued.max.messages.kbytes": 10000}

        _consumer = InOrderConsumerFactory.make_kafka_consumer(
            bootstrap_servers, group_id, topics, extra_config
        )

        assert _consumer == mock_consumer

        kafka_consumer.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "heartbeat.interval.ms": 3000,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 300000,
                "enable.auto.commit": "false",
                "queued.max.messages.kbytes": 10000,
            }
        )
        mock_consumer.subscribe.assert_called_once()
        assert mock_consumer.subscribe.call_args_list[0][0][0] == topics

    @mock.patch("confluent_kafka.Consumer")
    def test_make_kafka_consumer_unknown_topic(
        self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics
    ):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer
        extra_config = {"queued.max.messages.kbytes": 100}

        with pytest.raises(AssertionError):
            InOrderConsumerFactory.make_kafka_consumer(
                bootstrap_servers, group_id, ["topic_4"], extra_config
            )

        kafka_consumer.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "heartbeat.interval.ms": 3000,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 300000,
                "enable.auto.commit": "false",
                "queued.max.messages.kbytes": 100,
            }
        )
        mock_consumer.subscribe.assert_not_called()

    @mock.patch("confluent_kafka.Consumer")
    def test_init(self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer

        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock()
        health_check_fn = mock.Mock()
        factory = InOrderConsumerFactory.new(
            name=name,
            baseplate=baseplate,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topics=topics,
            handler_fn=handler_fn,
            message_unpack_fn=message_unpack_fn,
            health_check_fn=health_check_fn,
        )
        assert factory.name == name
        assert factory.baseplate == baseplate
        assert factory.handler_fn == handler_fn
        assert factory.message_unpack_fn == message_unpack_fn
        assert factory.health_check_fn == health_check_fn
        assert factory.consumer == mock_consumer

    @pytest.fixture
    def make_queue_consumer_factory(self, name, baseplate, bootstrap_servers, group_id, topics):
        @mock.patch("confluent_kafka.Consumer")
        def _make_queue_consumer_factory(kafka_consumer, health_check_fn=None):
            mock_consumer = mock.Mock()
            mock_consumer.list_topics.return_value = mock.Mock(
                topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
            )
            kafka_consumer.return_value = mock_consumer

            return InOrderConsumerFactory.new(
                name=name,
                baseplate=baseplate,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                topics=topics,
                handler_fn=lambda ctx, data, msg: True,
                message_unpack_fn=lambda b: {},
                health_check_fn=health_check_fn,
            )

        return _make_queue_consumer_factory

    def test_build_pump_worker(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        work_queue = Queue(maxsize=10)
        pump = factory.build_pump_worker(work_queue)
        assert isinstance(pump, KafkaConsumerWorker)
        assert pump.consumer == factory.consumer
        assert pump.work_queue == work_queue

    def test_build_message_handler(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        handler = factory.build_message_handler()
        assert isinstance(handler, KafkaMessageHandler)
        assert handler.baseplate == factory.baseplate
        assert handler.name == factory.name
        assert handler.handler_fn == factory.handler_fn
        assert handler.message_unpack_fn == factory.message_unpack_fn
        assert handler.on_success_fn.__name__ == "commit_offset"

    def test_build_multiple_message_handlers(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()

        factory.build_message_handler()

        with pytest.raises(AssertionError):
            factory.build_message_handler()

    @pytest.mark.parametrize("health_check_fn", [None, lambda req: True])
    def test_build_health_checker(self, health_check_fn, make_queue_consumer_factory):
        factory = make_queue_consumer_factory(health_check_fn=health_check_fn)
        listener = mock.Mock(spec=socket.socket)
        health_checker = factory.build_health_checker(listener)
        assert isinstance(health_checker, StreamServer)


class TestFastConsumerFactory:
    @mock.patch("confluent_kafka.Consumer")
    def test_make_kafka_consumer(
        self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics
    ):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer

        _consumer = FastConsumerFactory.make_kafka_consumer(bootstrap_servers, group_id, topics)

        assert _consumer == mock_consumer

        kafka_consumer.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "heartbeat.interval.ms": 3000,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 300000,
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": 5000,
                "enable.auto.offset.store": "true",
                "on_commit": FastConsumerFactory._commit_callback,
            }
        )
        mock_consumer.subscribe.assert_called_once()
        assert mock_consumer.subscribe.call_args_list[0][0][0] == topics

    @mock.patch("confluent_kafka.Consumer")
    def test_make_kafka_consumer_unknown_topic(
        self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics
    ):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer

        with pytest.raises(AssertionError):
            FastConsumerFactory.make_kafka_consumer(bootstrap_servers, group_id, ["topic_4"])

        kafka_consumer.assert_called_once_with(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "heartbeat.interval.ms": 3000,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 300000,
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": 5000,
                "enable.auto.offset.store": "true",
                "on_commit": FastConsumerFactory._commit_callback,
            }
        )
        mock_consumer.subscribe.assert_not_called()

    @mock.patch("confluent_kafka.Consumer")
    def test_init(self, kafka_consumer, name, baseplate, bootstrap_servers, group_id, topics):
        mock_consumer = mock.Mock()
        mock_consumer.list_topics.return_value = mock.Mock(
            topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
        )
        kafka_consumer.return_value = mock_consumer

        handler_fn = mock.Mock()
        message_unpack_fn = mock.Mock()
        health_check_fn = mock.Mock()
        factory = FastConsumerFactory.new(
            name=name,
            baseplate=baseplate,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topics=topics,
            handler_fn=handler_fn,
            message_unpack_fn=message_unpack_fn,
            health_check_fn=health_check_fn,
        )
        assert factory.name == name
        assert factory.baseplate == baseplate
        assert factory.handler_fn == handler_fn
        assert factory.message_unpack_fn == message_unpack_fn
        assert factory.health_check_fn == health_check_fn
        assert factory.consumer == mock_consumer

    @pytest.fixture
    def make_queue_consumer_factory(self, name, baseplate, bootstrap_servers, group_id, topics):
        @mock.patch("confluent_kafka.Consumer")
        def _make_queue_consumer_factory(kafka_consumer, health_check_fn=None):
            mock_consumer = mock.Mock()
            mock_consumer.list_topics.return_value = mock.Mock(
                topics={"topic_1": mock.Mock(), "topic_2": mock.Mock(), "topic_3": mock.Mock()}
            )
            kafka_consumer.return_value = mock_consumer

            return FastConsumerFactory.new(
                name=name,
                baseplate=baseplate,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                topics=topics,
                handler_fn=lambda ctx, data, msg: True,
                message_unpack_fn=lambda b: {},
                health_check_fn=health_check_fn,
            )

        return _make_queue_consumer_factory

    def test_build_pump_worker(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        work_queue = Queue(maxsize=10)
        pump = factory.build_pump_worker(work_queue)
        assert isinstance(pump, KafkaConsumerWorker)
        assert pump.consumer == factory.consumer
        assert pump.work_queue == work_queue

    def test_build_message_handler(self, make_queue_consumer_factory):
        factory = make_queue_consumer_factory()
        handler = factory.build_message_handler()
        assert isinstance(handler, KafkaMessageHandler)
        assert handler.baseplate == factory.baseplate
        assert handler.name == factory.name
        assert handler.handler_fn == factory.handler_fn
        assert handler.message_unpack_fn == factory.message_unpack_fn
        assert handler.on_success_fn is None

    @pytest.mark.parametrize("health_check_fn", [None, lambda req: True])
    def test_build_health_checker(self, health_check_fn, make_queue_consumer_factory):
        factory = make_queue_consumer_factory(health_check_fn=health_check_fn)
        listener = mock.Mock(spec=socket.socket)
        health_checker = factory.build_health_checker(listener)
        assert isinstance(health_checker, StreamServer)


class TestKafkaConsumerWorker:
    @pytest.fixture
    def consumer_worker(self, baseplate, name):
        mock_consumer = mock.Mock()
        mock_queue = mock.Mock(spec=Queue)
        return KafkaConsumerWorker(baseplate, name, mock_consumer, mock_queue)

    def test_initial_state(self, consumer_worker):
        assert consumer_worker.started is False
        assert consumer_worker.stopped is False

    @mock.patch("baseplate.frameworks.queue_consumer.kafka.time")
    def test_run(self, time, span, baseplate, name, consumer_worker):
        msg1 = mock.Mock()
        msg2 = mock.Mock()
        msg3 = mock.Mock()
        consumer_worker.consumer.consume.side_effect = [(), (msg1,), (msg2,), (msg3,)]

        with mock.patch.object(
            KafkaConsumerWorker, "stopped", create=True, new_callable=mock.PropertyMock
        ) as stopped_value:
            stopped_value.side_effect = [False, False, False, True]
            consumer_worker.run()

        baseplate.make_context_object.mock_calls == [mock.call(), mock.call(), mock.call()]
        baseplate.make_server_span.mock_calls == [
            mock.call(context, f"{name}.pump"),
            mock.call(context, f"{name}.pump"),
            mock.call(context, f"{name}.pump"),
        ]
        span.make_child.mock_calls == [
            mock.call("kafka.consume"),
            mock.call("kafka.work_queue_put"),
            mock.call("kafka.consume"),
            mock.call("kafka.work_queue_put"),
            mock.call("kafka.consume"),
            mock.call("kafka.work_queue_put"),
        ]

        assert consumer_worker.started is True
        assert consumer_worker.consumer.consume.mock_calls == [
            mock.call(num_messages=1, timeout=0),
            mock.call(num_messages=1, timeout=0),
            mock.call(num_messages=1, timeout=0),
        ]

        time.sleep.assert_called_once_with(1)
        assert consumer_worker.work_queue.put.mock_calls == [mock.call(msg1), mock.call(msg2)]

    def test_stop(self, consumer_worker):
        consumer_worker.stop()
        assert consumer_worker.stopped is True
