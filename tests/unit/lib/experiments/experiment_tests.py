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
from baseplate.lib.experiments import ExperimentsGlobalCache
from baseplate.lib.file_watcher import FileWatcherWithUpdatedFlag
from baseplate.lib.file_watcher import WatchedFileNotAvailableError


THIRTY_DAYS = timedelta(days=30).total_seconds()


class TestExperiments(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.event_logger = mock.Mock(spec=DebugLogger)
        self._mock_filewatcher = mock.Mock(spec=FileWatcherWithUpdatedFlag)
        self.mock_experiments_global_cache = ExperimentsGlobalCache(self._mock_filewatcher)
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
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_bucketing_event_fields_without_baseplate_user(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_that_we_only_send_bucketing_event_once(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_exposure_event_fields(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_that_override_true_has_no_effect(self):
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            is_valid = experiments.is_valid_experiment("test")
            self.assertEqual(is_valid, True)

            is_valid = experiments.is_valid_experiment("test2")
            self.assertEqual(is_valid, False)

    def test_get_all_experiment_names(self):
        self._mock_filewatcher.get_data.return_value = (
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        with mock.patch("baseplate.lib.experiments.providers.r2.R2Experiment.variant") as p:
            p.return_value = "active"
            experiment_names = experiments.get_all_experiment_names()
            self.assertEqual(len(experiment_names), 2)
            self.assertEqual("test" in experiment_names, True)
            self.assertEqual("test2" in experiment_names, True)

    def test_that_bucketing_events_are_not_sent_with_override_false(self):
        """Don't send events when override is False."""
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_that_bucketing_events_not_sent_if_no_variant(self):
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_that_bucketing_events_not_sent_if_experiment_disables(self):
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
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

    def test_that_bucketing_events_not_sent_if_cant_load_config(self):
        self._mock_filewatcher.get_data.side_effect = WatchedFileNotAvailableError(
            "path", None
        )  # noqa
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_parse_config(self):
        self._mock_filewatcher.get_data.side_effect = TypeError()
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_find_experiment(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_none_returned_on_variant_call_with_bad_id(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_times(self):
        self._mock_filewatcher.get_data.return_value = (
            {
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
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_experiment(self):
        self._mock_filewatcher.get_data.return_value = (
            {
                "test": {
                    "id": 1,
                    "name": "test",
                    "owner": "test_owner",
                    "type": "r2",
                    "version": "1",
                    "start_ts": time.time() - THIRTY_DAYS,
                    "stop_ts": time.time() + THIRTY_DAYS,
                }
            },
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)


@mock.patch("baseplate.lib.experiments.FileWatcherWithUpdatedFlag")
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
        self._mock_filewatcher = mock.Mock(spec=FileWatcherWithUpdatedFlag)
        self.mock_experiments_global_cache = ExperimentsGlobalCache(self._mock_filewatcher)
        self.mock_span = mock.MagicMock(spec=ServerSpan)

    def test_global_cache_updated(self):
        self._mock_filewatcher.get_data.return_value = (
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
            True,
        )
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiments = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        exp_res = experiments._get_experiment("test")
        self.assertTrue("test" in global_cache)
        self.assertEqual(exp_res.name, "test")

    @mock.patch("baseplate.lib.experiments.parse_experiment")
    def test_experiments_with_same_name_stale_cache(self, m_parse_experiment):
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

        # experiment_one add test to global cache
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_one = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_one._get_experiment("test")
        self.assertTrue("test" in global_cache)
        m_parse_experiment.assert_called_once()

        # experiment_two just use the cache
        self._mock_filewatcher.get_data.return_value = cfg_data, False
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_two = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_two._get_experiment("test")
        m_parse_experiment.assert_called_once()
        self.assertTrue("test" in global_cache)

    @mock.patch("baseplate.lib.experiments.parse_experiment")
    def test_experiments_with_different_name_stale_cache(self, m_parse_experiment):
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

        # experiment_one add test1 to global cache
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_one = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_one._get_experiment("test1")
        self.assertTrue("test1" in global_cache)
        self.assertFalse("test2" in global_cache)
        m_parse_experiment.assert_called_once_with(value1)

        # experiment_two add test2 to global cache
        self._mock_filewatcher.get_data.return_value = cfg_data, False
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_two = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_two._get_experiment("test2")
        m_parse_experiment.assert_called_with(value2)
        self.assertTrue("test1" in global_cache)
        self.assertTrue("test2" in global_cache)

    def test_experiments_with_same_name_updated_cache(self):
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
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_one = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_one._get_experiment("test")
        self.assertTrue("test" in global_cache)
        self.assertEqual(global_cache["test"].owner, owner1)

        # updated test config file to value2
        cfg_data = {"test": value2}
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        self.assertEqual(global_cache, {})
        experiment_two = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_two._get_experiment("test")
        self.assertTrue("test" in global_cache)
        self.assertEqual(global_cache["test"].owner, owner2)

        # experiment_one global cache still use old one
        self.assertEqual(experiment_one._global_cache["test"].owner, owner1)

        # global cache was updated by experiment_two
        self._mock_filewatcher.get_data.return_value = cfg_data, False
        global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()[1]
        self.assertEqual(global_cache["test"].owner, owner2)

    def test_experiments_with_different_name_updated_cache(self):
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
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        experiment_one = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_one._get_experiment("test1")
        self.assertTrue("test1" in global_cache)
        self.assertFalse("test2" in global_cache)
        self.assertEqual(global_cache["test1"].owner, "test1")

        # updated test config file
        # experiment_two add test2 into global cache
        self._mock_filewatcher.get_data.return_value = cfg_data, True
        cfg_data, global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()
        self.assertEqual(global_cache, {})
        experiment_two = Experiments(
            server_span=self.mock_span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache=global_cache,
            event_logger=self.event_logger,
        )
        experiment_two._get_experiment("test2")
        self.assertTrue("test2" in global_cache)
        self.assertFalse("test1" in global_cache)
        self.assertEqual(global_cache["test2"].owner, "test2")

        # experiment_one global cache still use old one
        self.assertEqual(experiment_one._global_cache["test1"].owner, "test1")

        # global cache only contains test2 experiment
        self._mock_filewatcher.get_data.return_value = cfg_data, False
        global_cache = self.mock_experiments_global_cache.get_cfg_and_global_cache()[1]
        self.assertTrue("test2" in global_cache)
        self.assertFalse("test1" in global_cache)
        self.assertEqual(global_cache["test2"].owner, "test2")
