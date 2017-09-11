from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time
import unittest

from datetime import timedelta

from baseplate.core import ServerSpan
from baseplate.events import EventQueue
from baseplate.experiments import (
    Experiments,
    ExperimentsContextFactory,
    experiments_client_from_config,
)
from baseplate.file_watcher import FileWatcher, WatchedFileNotAvailableError

from ... import mock

THIRTY_DAYS = timedelta(days=30).total_seconds()


class TestExperiments(unittest.TestCase):

    def setUp(self):
        super(TestExperiments, self).setUp()
        self.event_queue = mock.Mock(spec=EventQueue)
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.trace_id = "123456"
        self.user_id = "t2_1"
        self.user_name = "gary"

    def test_that_we_only_send_bucketing_event_once(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
        ):
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 1)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 1)

    def test_that_override_true_has_no_effect(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=True)
            self.assertEqual(self.event_queue.put.call_count, 1)
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=True)
            self.assertEqual(self.event_queue.put.call_count, 1)

    def test_that_bucketing_events_are_not_sent_with_override_false(self):
        """Don't send events when override is False"""
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=False)
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=False)
            self.assertEqual(self.event_queue.put.call_count, 0)
            p.return_value = None
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=False)
            self.assertEqual(self.event_queue.put.call_count, 0)

    def test_that_bucketing_events_not_sent_if_no_variant(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value=None,
        ):
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 0)

    def test_that_bucketing_events_not_sent_if_experiment_disables(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
        ), mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.should_log_bucketing",
            return_value=False,
        ):
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id)
            self.assertEqual(self.event_queue.put.call_count, 0)
            experiments.variant("test", user_id=self.user_id,
                                bucketing_event_override=True)
            self.assertEqual(self.event_queue.put.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_load_config(self):
        self.mock_filewatcher.get_data.side_effect = WatchedFileNotAvailableError("path", None)  # noqa
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_parse_config(self):
        self.mock_filewatcher.get_data.side_effect = TypeError()
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_find_experiment(self):
        self.mock_filewatcher.get_data.return_value = {
            "other_test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            event_queue=self.event_queue,
            server_span=self.mock_span,
            context_name="test",
        )
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id)
        self.assertEqual(self.event_queue.put.call_count, 0)


class ExperimentsClientFromConfigTests(unittest.TestCase):
    def test_make_clients(self):
        event_queue = mock.Mock(spec=EventQueue)
        experiments = experiments_client_from_config({
            "experiments.path": "/tmp/test",
        }, event_queue)
        self.assertIsInstance(experiments, ExperimentsContextFactory)
