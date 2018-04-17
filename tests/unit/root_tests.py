from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate import tracing_client_from_config
from baseplate.config import ConfigurationError
from baseplate.diagnostics.tracing import LoggingRecorder, NullRecorder


class TracingClientTests(unittest.TestCase):
    def test_not_configured_at_all(self):
        with self.assertRaises(ConfigurationError):
            tracing_client_from_config({})

    def test_most_simple_config(self):
        client = tracing_client_from_config({
            "tracing.service_name": "example_name",
        })

        self.assertEqual(client.service_name, "example_name")
        self.assertIsInstance(client.recorder, LoggingRecorder)

    def test_most_simple_config_without_logging(self):
        client = tracing_client_from_config({
            "tracing.service_name": "example_name",
        }, log_if_unconfigured=False)

        self.assertEqual(client.service_name, "example_name")
        self.assertIsInstance(client.recorder, NullRecorder)

    def test_sample_rate_fallback(self):
        client = tracing_client_from_config({
            "tracing.service_name": "example_name",
            "tracing.sample_rate": "30%",
        })
        self.assertAlmostEqual(client.sample_rate, .3)

        client = tracing_client_from_config({
            "tracing.service_name": "example_name",
            "tracing.sample_rate": ".4",
        })
        self.assertAlmostEqual(client.sample_rate, .4)
