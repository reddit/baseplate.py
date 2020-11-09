import datetime
import json
import time
import unittest

from datetime import timedelta
from unittest import mock

from baseplate import ServerSpan
from baseplate.lib.edge_context import AuthenticationToken
from baseplate.lib.edge_context import User
from baseplate.lib.events import DebugLogger
from baseplate.lib.experiments import EventType
from baseplate.lib.experiments import Experiments
from baseplate.lib.experiments import experiments_client_from_config
from baseplate.lib.experiments import ExperimentsContextFactory
from baseplate.lib.file_watcher import FileWatcher


THIRTY_DAYS = timedelta(days=30).total_seconds()


class TestExperiments(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self.mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.mock_span.context = None
        self.mock_span.trace_id = "123456"
        self.user_name = "gary"
        self.mock_authentication_token = mock.Mock(spec=AuthenticationToken)
        self.mock_authentication_token.subject = "t2_1"
        self.mock_authentication_token.user_roles = set()
        self.user = User(
            authentication_token=self.mock_authentication_token,
            loid="t2_1",
            cookie_created_ms=10000,
        )

    def test_bucketing_event_fields(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 10, "control_1": 10, "control_2": 10},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value="active"
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, app_name="r2")
            self.assertEqual(self.event_logger.log.call_count, 1)
        event_fields = self.event_logger.log.call_args[1]

        self.assertEqual(event_fields["variant"], "active")
        self.assertEqual(event_fields["user_id"], "t2_1")
        self.assertEqual(event_fields["logged_in"], True)
        self.assertEqual(event_fields["app_name"], "r2")
        self.assertEqual(event_fields["cookie_created_timestamp"], 10000)
        self.assertEqual(event_fields["event_type"], EventType.BUCKET)
        self.assertNotEqual(event_fields["span"], None)

        self.assertEqual(getattr(event_fields["experiment"], "id"), 1)
        self.assertEqual(getattr(event_fields["experiment"], "name"), "test")
        self.assertEqual(getattr(event_fields["experiment"], "owner"), "test_owner")
        self.assertEqual(getattr(event_fields["experiment"], "version"), "1")

    def test_bucketing_event_fields_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 10, "control_1": 10, "control_2": 10},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value="active"
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, app_name="r2")
            self.assertEqual(self.event_logger.log.call_count, 1)
        event_fields = self.event_logger.log.call_args[1]

        self.assertEqual(event_fields["variant"], "active")
        self.assertEqual(event_fields["user_id"], "t2_1")
        self.assertEqual(event_fields["logged_in"], True)
        self.assertEqual(event_fields["app_name"], "r2")
        self.assertEqual(event_fields["cookie_created_timestamp"], 10000)
        self.assertEqual(event_fields["event_type"], EventType.BUCKET)
        self.assertNotEqual(event_fields["span"], None)

        self.assertEqual(getattr(event_fields["experiment"], "id"), 1)
        self.assertEqual(getattr(event_fields["experiment"], "name"), "test")
        self.assertEqual(getattr(event_fields["experiment"], "owner"), "test_owner")
        self.assertEqual(getattr(event_fields["experiment"], "version"), "1")

    def test_bucketing_event_fields_without_baseplate_user_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 10, "control_1": 10, "control_2": 10},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value="active"
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user_id="t2_2", app_name="r2", logged_in=True)
            self.assertEqual(self.event_logger.log.call_count, 1)
        event_fields = self.event_logger.log.call_args[1]

        self.assertEqual(event_fields["variant"], "active")
        self.assertEqual(event_fields["user_id"], "t2_2")
        self.assertEqual(event_fields["logged_in"], True)
        self.assertEqual(event_fields["app_name"], "r2")
        self.assertEqual(event_fields["event_type"], EventType.BUCKET)
        self.assertNotEqual(event_fields["span"], None)

        self.assertEqual(getattr(event_fields["experiment"], "id"), 1)
        self.assertEqual(getattr(event_fields["experiment"], "name"), "test")
        self.assertEqual(getattr(event_fields["experiment"], "owner"), "test_owner")
        self.assertEqual(getattr(event_fields["experiment"], "version"), "1")

    def test_that_we_only_send_bucketing_event_once_with_cfg_data(self):
        cfg_data = {
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
                    "variants": {"active": 10, "control_1": 10, "control_2": 10},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value="active"
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 1)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 1)

    def test_exposure_event_fields_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 10, "control_1": 10, "control_2": 10},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.expose("test", variant_name="control_1", user=self.user, app_name="r2")
        self.assertEqual(self.event_logger.log.call_count, 1)

        event_fields = self.event_logger.log.call_args[1]

        self.assertEqual(event_fields["variant"], "control_1")
        self.assertEqual(event_fields["user_id"], "t2_1")
        self.assertEqual(event_fields["logged_in"], True)
        self.assertEqual(event_fields["app_name"], "r2")
        self.assertEqual(event_fields["cookie_created_timestamp"], 10000)
        self.assertEqual(event_fields["event_type"], EventType.EXPOSE)
        self.assertNotEqual(event_fields["span"], None)

        self.assertEqual(getattr(event_fields["experiment"], "id"), 1)
        self.assertEqual(getattr(event_fields["experiment"], "name"), "test")
        self.assertEqual(getattr(event_fields["experiment"], "owner"), "test_owner")
        self.assertEqual(getattr(event_fields["experiment"], "version"), "1")

    def test_that_override_true_has_no_effect_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, bucketing_event_override=True)
            self.assertEqual(self.event_logger.log.call_count, 1)
            experiments.variant("test", user=self.user, bucketing_event_override=True)
            self.assertEqual(self.event_logger.log.call_count, 1)

    def test_is_valid_experiment(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            is_valid = experiments.is_valid_experiment("test")
            self.assertEqual(is_valid, True)

            is_valid = experiments.is_valid_experiment("test2")
            self.assertEqual(is_valid, False)

    def test_is_valid_experiment_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            is_valid = experiments.is_valid_experiment("test")
            self.assertEqual(is_valid, True)

            is_valid = experiments.is_valid_experiment("test2")
            self.assertEqual(is_valid, False)

    def test_get_all_experiment_names(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            },
            "test2": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            },
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            experiment_names = experiments.get_all_experiment_names()
            self.assertEqual(len(experiment_names), 2)
            self.assertEqual("test" in experiment_names, True)
            self.assertEqual("test2" in experiment_names, True)

    def test_get_all_experiment_names_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            },
            "test2": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            },
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            experiment_names = experiments.get_all_experiment_names()
            self.assertEqual(len(experiment_names), 2)
            self.assertEqual("test" in experiment_names, True)
            self.assertEqual("test2" in experiment_names, True)

    def test_that_bucketing_events_are_not_sent_with_override_false_with_cfg_data(self):
        """Don't send events when override is False."""
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)
            p.return_value = None
            experiments.variant("test", user=self.user, bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_no_variant_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value=None
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_experiment_disables_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.variant", return_value="active"
        ), mock.patch(
            "baseplate.lib.experiments.providers.r2.R2Experiment.should_log_bucketing",
            return_value=False,
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user, bucketing_event_override=True)
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_config_is_empty_with_cfg_data(self):
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data={},
            global_cache={},
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_find_experiment_with_cfg_data(self):
        cfg_data = {
            "other_test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_none_returned_on_variant_call_with_bad_id_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": "1",
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 50, "control_1": 25, "control_2": 25},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_times_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {"active": 50, "control_1": 25, "control_2": 25},
                },
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_experiment_with_cfg_data(self):
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)


@mock.patch("baseplate.lib.experiments.FileWatcher")
class ExperimentsClientFromConfigTests(unittest.TestCase):
    def test_make_clients(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config(
            {"experiments.path": "/tmp/test"}, event_logger
        )
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with(
            "/tmp/test", json.load, timeout=None, backoff=None
        )

    def test_timeout(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config(
            {"experiments.path": "/tmp/test", "experiments.timeout": "60 seconds"}, event_logger
        )
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with(
            "/tmp/test", json.load, timeout=60.0, backoff=None
        )

    def test_prefix(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config(
            {"r2_experiments.path": "/tmp/test", "r2_experiments.timeout": "60 seconds"},
            event_logger,
            prefix="r2_experiments.",
        )
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with(
            "/tmp/test", json.load, timeout=60.0, backoff=None
        )


class ExperimentsGlobalCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self._mock_filewatcher = mock.Mock(spec=FileWatcher)
        self.mock_span = mock.MagicMock(spec=ServerSpan)
        self.experiments_factory = ExperimentsContextFactory("test", self.event_logger)
        self.experiments_factory._filewatcher = self._mock_filewatcher
        self.one_hour_ago = time.time() - datetime.timedelta(hours=1).total_seconds()
        self.two_hour_ago = time.time() - datetime.timedelta(hours=2).total_seconds()

    def test_config_can_not_load(self):
        self._mock_filewatcher.get_data_and_mtime.side_effect = TypeError()
        experiments = self.experiments_factory.make_object_for_context("test", self.mock_span)
        self.assertEqual(experiments._cfg_data, {})

        exp_res = experiments._get_experiment("test")
        self.assertIsNone(exp_res)

    def test_global_cache_updated(self):
        self._mock_filewatcher.get_data_and_mtime.return_value = (
            {
                "test": {
                    "id": 1,
                    "name": "test",
                    "owner": "test",
                    "type": "r2",
                    "version": "1",
                    "start_ts": time.time() - THIRTY_DAYS,
                    "stop_ts": time.time() + THIRTY_DAYS,
                    "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
                }
            },
            self.one_hour_ago,
        )
        experiments = self.experiments_factory.make_object_for_context("test", self.mock_span)

        exp_res = experiments._get_experiment("test")
        self.assertTrue("test" in self.experiments_factory._global_cache)
        self.assertEqual(exp_res.name, "test")

    @mock.patch("baseplate.lib.experiments.parse_experiment")
    def test_experiments_with_same_name_same_cache(self, m_parse_experiment):
        self.experiments_factory.cfg_mtime = 0.0
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test",
                "type": "r2",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
            }
        }
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.one_hour_ago

        # experiment_one add test to global cache
        experiment_one = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_one._get_experiment("test")

        self.assertTrue("test" in self.experiments_factory._global_cache)
        m_parse_experiment.assert_called_once()

        # experiment_two just use the cache
        experiment_two = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_two._get_experiment("test")

        m_parse_experiment.assert_called_once()
        self.assertTrue("test" in self.experiments_factory._global_cache)

    @mock.patch("baseplate.lib.experiments.parse_experiment")
    def test_experiments_with_different_name_same_cache(self, m_parse_experiment):
        self.experiments_factory.cfg_mtime = 0.0
        value1 = {
            "id": 1,
            "name": "test1",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }
        value2 = {
            "id": 2,
            "name": "test2",
            "owner": "test",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }
        cfg_data = {"test1": value1, "test2": value2}
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.one_hour_ago

        # experiment_one add test1 to global cache
        experiment_one = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_one._get_experiment("test1")

        self.assertTrue("test1" in self.experiments_factory._global_cache)
        self.assertFalse("test2" in self.experiments_factory._global_cache)
        m_parse_experiment.assert_called_once_with(value1)

        # experiment_two add test2 to global cache
        experiment_two = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_two._get_experiment("test2")

        m_parse_experiment.assert_called_with(value2)
        self.assertTrue("test1" in self.experiments_factory._global_cache)
        self.assertTrue("test2" in self.experiments_factory._global_cache)

    def test_experiments_with_same_name_updated_cache(self):
        self.experiments_factory.cfg_mtime = 0.0
        owner1 = "test1"
        owner2 = "test2"
        value1 = {
            "id": 1,
            "name": "test",
            "owner": owner1,
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }
        value2 = {
            "id": 1,
            "name": "test",
            "owner": owner2,
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }

        cfg_data = {"test": value1}

        # experiment_one add value 1 in global cache
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.two_hour_ago
        experiment_one = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_one._get_experiment("test")

        self.assertTrue("test" in self.experiments_factory._global_cache)
        self.assertEqual(experiment_one._global_cache["test"].owner, owner1)
        self.assertEqual(self.experiments_factory._global_cache["test"].owner, owner1)

        # updated test config file to value2
        cfg_data = {"test": value2}
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.one_hour_ago
        experiment_two = self.experiments_factory.make_object_for_context("test", self.mock_span)
        self.assertEqual(self.experiments_factory._global_cache, {})

        experiment_two._get_experiment("test")
        self.assertTrue("test" in self.experiments_factory._global_cache)
        self.assertEqual(experiment_two._global_cache["test"].owner, owner2)

        # experiment_one global cache still use old one
        self.assertEqual(experiment_one._global_cache["test"].owner, owner1)

        # global cache was updated by experiment_two
        self.assertEqual(self.experiments_factory._global_cache["test"].owner, owner2)

    def test_experiments_with_different_name_updated_cache(self):
        self.experiments_factory.cfg_mtime = 0.0
        value1 = {
            "id": 1,
            "name": "test1",
            "owner": "test1",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }
        value2 = {
            "id": 2,
            "name": "test2",
            "owner": "test2",
            "type": "r2",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"variants": {"active": 10, "control_1": 10, "control_2": 10}},
        }

        cfg_data = {"test1": value1, "test2": value2}

        # experiment_one add test1 into global cache
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.two_hour_ago
        experiment_one = self.experiments_factory.make_object_for_context("test", self.mock_span)
        experiment_one._get_experiment("test1")

        self.assertTrue("test1" in self.experiments_factory._global_cache)
        self.assertFalse("test2" in self.experiments_factory._global_cache)
        self.assertEqual(self.experiments_factory._global_cache["test1"].owner, "test1")

        # updated test config file
        # experiment_two add test2 into global cache
        self._mock_filewatcher.get_data_and_mtime.return_value = cfg_data, self.one_hour_ago
        experiment_two = self.experiments_factory.make_object_for_context("test", self.mock_span)
        self.assertEqual(self.experiments_factory._global_cache, {})

        experiment_two._get_experiment("test2")
        self.assertTrue("test2" in experiment_two._global_cache)
        self.assertFalse("test1" in experiment_two._global_cache)
        self.assertEqual(experiment_two._global_cache["test2"].owner, "test2")

        # experiment_one global cache still use old one
        self.assertEqual(experiment_one._global_cache["test1"].owner, "test1")

        # global cache only contains test2 experiment
        self.assertTrue("test2" in self.experiments_factory._global_cache)
        self.assertFalse("test1" in self.experiments_factory._global_cache)
        self.assertEqual(self.experiments_factory._global_cache["test2"].owner, "test2")
