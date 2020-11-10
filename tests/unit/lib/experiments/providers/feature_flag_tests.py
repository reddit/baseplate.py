import collections
import logging
import time
import unittest

from datetime import timedelta
from unittest import mock

from baseplate import ServerSpan
from baseplate.lib.events import EventQueue
from baseplate.lib.experiments import Experiments
from baseplate.lib.experiments.providers import parse_experiment
from baseplate.lib.file_watcher import FileWatcher

logger = logging.getLogger(__name__)


THIRTY_DAYS = timedelta(days=30).total_seconds()


class TestFeatureFlag(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.user_id = "t2_beef"
        self.user_name = "gary"
        self.user_logged_in = True

    def _assert_fuzzy_percent_true(self, results, percent):
        stats = collections.Counter(results)
        total = sum(stats.values())
        # _roughly_ `percent` should have been `True`
        diff = abs((float(stats[True]) / total) - (percent / 100.0))
        self.assertTrue(diff < 0.1)

    def test_does_not_log_bucketing_event(self):
        event_queue = mock.Mock(spec=EventQueue)
        filewatcher = mock.Mock(spec=FileWatcher)
        span = mock.MagicMock(spec=ServerSpan)
        filewatcher.get_data.return_value = {
            "test": {
                "id": 1,
                "name": "test",
                "type": "feature_flag",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "targeting": {"logged_in": [True, False]},
                    "variants": {"active": 100},
                },
            }
        }
        experiments = Experiments(config_watcher=filewatcher, server_span=span, context_name="test")
        self.assertEqual(event_queue.put.call_count, 0)
        variant = experiments.variant("test", user_id=self.user_id, logged_in=True)
        self.assertEqual(variant, "active")
        self.assertEqual(event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id, logged_in=True)
        self.assertEqual(event_queue.put.call_count, 0)

    def test_does_not_log_bucketing_event_with_cfg_data(self):
        event_queue = mock.Mock(spec=EventQueue)
        span = mock.MagicMock(spec=ServerSpan)
        cfg_data = {
            "test": {
                "id": 1,
                "name": "test",
                "type": "feature_flag",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "targeting": {"logged_in": [True, False]},
                    "variants": {"active": 100},
                },
            }
        }
        experiments = Experiments(
            config_watcher=mock.Mock(spec=FileWatcher),
            server_span=span,
            context_name="test",
            cfg_data=cfg_data,
            global_cache={},
        )
        self.assertEqual(event_queue.put.call_count, 0)
        variant = experiments.variant("test", user_id=self.user_id, logged_in=True)
        self.assertEqual(variant, "active")
        self.assertEqual(event_queue.put.call_count, 0)
        experiments.variant("test", user_id=self.user_id, logged_in=True)
        self.assertEqual(event_queue.put.call_count, 0)

    def test_admin_enabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"admin": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )

    def test_admin_disabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"admin": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=[]
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["beta"]
            ),
            "active",
        )

    def test_employee_enabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"employee": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["employee"]
            ),
            "active",
        )

    def test_employee_disabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"employee": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=[]
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["beta"]
            ),
            "active",
        )

    def test_beta_enabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"beta": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["beta"]
            ),
            "active",
        )

    def test_beta_disabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"beta": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=[]
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )

    def test_gold_enabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"gold": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["gold"]
            ),
            "active",
        )

    def test_gold_disabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_roles": {"gold": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=[]
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )

    def test_percent_loggedin(self):
        num_users = 2000

        def simulate_percent_loggedin(wanted_percent):
            cfg = {
                "id": 1,
                "name": "test_feature",
                "type": "feature_flag",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "targeting": {"logged_in": [True]},
                    "variants": {"active": wanted_percent},
                },
            }
            feature_flag = parse_experiment(cfg)
            return (
                feature_flag.variant(user_id="t2_%s" % str(i), logged_in=True) == "active"
                for i in range(num_users)
            )

        self.assertFalse(any(simulate_percent_loggedin(0)))
        self.assertTrue(all(simulate_percent_loggedin(100)))
        self._assert_fuzzy_percent_true(simulate_percent_loggedin(25), 25)
        self._assert_fuzzy_percent_true(simulate_percent_loggedin(10), 10)
        self._assert_fuzzy_percent_true(simulate_percent_loggedin(50), 50)
        self._assert_fuzzy_percent_true(simulate_percent_loggedin(99), 99)

    def test_percent_loggedout(self):
        num_users = 2000

        def simulate_percent_loggedout(wanted_percent):
            cfg = {
                "id": 1,
                "name": "test_feature",
                "type": "feature_flag",
                "version": "1",
                "start_ts": time.time() - THIRTY_DAYS,
                "stop_ts": time.time() + THIRTY_DAYS,
                "experiment": {
                    "targeting": {"logged_in": [False]},
                    "variants": {"active": wanted_percent},
                },
            }
            feature_flag = parse_experiment(cfg)
            return (
                feature_flag.variant(user_id="t2_%s" % str(i), logged_in=False) == "active"
                for i in range(num_users)
            )

        self.assertFalse(any(simulate_percent_loggedout(0)))
        self.assertTrue(all(simulate_percent_loggedout(100)))
        self._assert_fuzzy_percent_true(simulate_percent_loggedout(25), 25)
        self._assert_fuzzy_percent_true(simulate_percent_loggedout(10), 10)
        self._assert_fuzzy_percent_true(simulate_percent_loggedout(50), 50)
        self._assert_fuzzy_percent_true(simulate_percent_loggedout(99), 99)

    def test_url_enabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"url_features": {"test_state": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, url_features=["test_state"]
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id,
                logged_in=self.user_logged_in,
                url_features=["x", "test_state"],
            ),
            "active",
        )

    def test_url_disabled(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"url_features": {"test_state": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, url_features=["x"]
            ),
            "active",
        )

    def test_user_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {
                    "user_name": {"Gary": "active", "dave": "active", "ALL_UPPERCASE": "active"}
                },
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_name="Gary"
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_name=self.user_name
            ),
            "active",
        )
        all_uppercase_id = "t2_f00d"
        all_uppercase_name = "ALL_UPPERCASE"
        self.assertEqual(
            feature_flag.variant(
                user_id=all_uppercase_id, logged_in=True, user_name=all_uppercase_name
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=all_uppercase_id, logged_in=True, user_name=all_uppercase_name.lower()
            ),
            "active",
        )

    def test_user_not_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"overrides": {"user_name": {}}, "variants": {"active": 0}},
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_name=self.user_name
            ),
            "active",
        )
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"user_name": {"dave": "active", "joe": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_name=self.user_name
            ),
            "active",
        )

    def test_subreddit_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"subreddit": {"WTF": "active", "aww": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subreddit="WTF"
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subreddit="wtf"
            ),
            "active",
        )

    def test_subreddit_not_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"overrides": {"subreddit": {}}, "variants": {"active": 0}},
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subreddit="wtf"
            ),
            "active",
        )
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"subreddit": {"wtfoobar": "active", "aww": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subreddit="wtf"
            ),
            "active",
        )

    def test_subdomain_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"subdomain": {"beta": "active", "www": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subdomain="beta"
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subdomain="BETA"
            ),
            "active",
        )

    def test_subdomain_not_in(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {"overrides": {"subdomain": {}}, "variants": {"active": 0}},
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subdomain="beta"
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in, subdomain=""),
            "active",
        )
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {"subdomain": {"www": "active", "betanauts": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, subdomain="beta"
            ),
            "active",
        )

    def test_multiple(self):
        # is_admin, globally off should still be False
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "global_override": None,
            "experiment": {
                "overrides": {"user_roles": {"admin": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )

        # globally on but not admin should still be True
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "global_override": "active",
            "experiment": {
                "overrides": {"user_roles": {"admin": "active"}},
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )

        # no URL but admin should still be True
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "overrides": {
                    "user_roles": {"admin": "active"},
                    "url_features": {"test_featurestate": "active"},
                },
                "variants": {"active": 0},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_roles=["admin"]
            ),
            "active",
        )
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id,
                logged_in=self.user_logged_in,
                url_features=["test_featurestate"],
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )

    def test_is_newer_than(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "newer_than": int(time.time()) - THIRTY_DAYS,
                "variants": {"active": 100},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_created=int(time.time())
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )

    def test_is_not_newer_than(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True]},
                "newer_than": int(time.time()) + THIRTY_DAYS,
                "variants": {"active": 100},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertNotEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_created=int(time.time())
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )

    def test_newer_than_only_on_logged_in_check(self):
        cfg = {
            "id": 1,
            "name": "test_feature",
            "type": "feature_flag",
            "version": "1",
            "start_ts": time.time() - THIRTY_DAYS,
            "stop_ts": time.time() + THIRTY_DAYS,
            "experiment": {
                "targeting": {"logged_in": [True], "user_name": ["gary"]},
                "newer_than": int(time.time()) + THIRTY_DAYS,
                "variants": {"active": 100},
            },
        }
        feature_flag = parse_experiment(cfg)
        self.assertEqual(
            feature_flag.variant(
                user_id=self.user_id, logged_in=self.user_logged_in, user_name="gary"
            ),
            "active",
        )
        self.assertNotEqual(
            feature_flag.variant(user_id=self.user_id, logged_in=self.user_logged_in), "active"
        )
