
import json
import time
import unittest

from datetime import timedelta

from baseplate.core import ServerSpan, User, AuthenticationToken
from baseplate.events import DebugLogger
from baseplate.experiments import (
    EventType,
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 1)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 1)

    def test_exposure_event_fields(self):
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.expose(
            "test",
            variant_name="control_1",
            user=self.user,
            app_name="r2"
        )
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user,
                                bucketing_event_override=True)
            self.assertEqual(self.event_logger.log.call_count, 1)
            experiments.variant("test", user=self.user,
                                bucketing_event_override=True)
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
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
                "experiment": {
                    "variants": {
                        "active": 10,
                        "control_1": 10,
                        "control_2": 10,
                    }
                }
            },
            "test2": {
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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
            experiment_names = experiments.get_all_experiment_names()
            self.assertEqual(len(experiment_names), 2)
            self.assertEqual("test" in experiment_names, True)
            self.assertEqual("test2" in experiment_names, True)

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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
        ) as p:
            p.return_value="active"
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user,
                                bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user,
                                bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)
            p.return_value = None
            experiments.variant("test", user=self.user,
                                bucketing_event_override=False)
            self.assertEqual(self.event_logger.log.call_count, 0)

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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value=None,
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)

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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )

        with mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.variant",
            return_value="active",
        ), mock.patch(
            "baseplate.experiments.providers.r2.R2Experiment.should_log_bucketing",
            return_value=False,
        ):
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user)
            self.assertEqual(self.event_logger.log.call_count, 0)
            experiments.variant("test", user=self.user,
                                bucketing_event_override=True)
            self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_load_config(self):
        self.mock_filewatcher.get_data.side_effect = WatchedFileNotAvailableError("path", None)  # noqa
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_that_bucketing_events_not_sent_if_cant_parse_config(self):
        self.mock_filewatcher.get_data.side_effect = TypeError()
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

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
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)
        experiments.variant("test", user=self.user)
        self.assertEqual(self.event_logger.log.call_count, 0)

    def test_none_returned_on_variant_call_with_bad_id(self):
        self.mock_filewatcher.get_data.return_value = {
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
                    "variants": {
                        "active": 50,
                        "control_1": 25,
                        "control_2": 25,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_times(self):
        self.mock_filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "owner": "test_owner",
                "type": "r2",
                "version": "1",
                "experiment": {
                    "id": 1,
                    "name": "test",
                    "variants": {
                        "active": 50,
                        "control_1": 25,
                        "control_2": 25,
                    }
                }
            }
        }
        experiments = Experiments(
            config_watcher=self.mock_filewatcher,
            server_span=self.mock_span,
            context_name="test",
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)

    def test_none_returned_on_variant_call_with_no_experiment(self):
        self.mock_filewatcher.get_data.return_value = {
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
            event_logger=self.event_logger,
        )
        self.assertEqual(self.event_logger.log.call_count, 0)
        variant = experiments.variant("test", user=self.user)
        self.assertEqual(variant, None)


@mock.patch("baseplate.experiments.FileWatcher")
class ExperimentsClientFromConfigTests(unittest.TestCase):

    def test_make_clients(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config({
            "experiments.path": "/tmp/test",
        }, event_logger)
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with("/tmp/test", json.load, timeout=None)

    def test_timeout(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config({
            "experiments.path": "/tmp/test",
            "experiments.timeout": "60 seconds",
        }, event_logger)
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with("/tmp/test", json.load, timeout=60.0)

    def test_prefix(self, file_watcher_mock):
        event_logger = mock.Mock(spec=DebugLogger)
        experiments = experiments_client_from_config({
            "r2_experiments.path": "/tmp/test",
            "r2_experiments.timeout": "60 seconds",
        }, event_logger, prefix="r2_experiments.")
        self.assertIsInstance(experiments, ExperimentsContextFactory)
        file_watcher_mock.assert_called_once_with("/tmp/test", json.load, timeout=60.0)
