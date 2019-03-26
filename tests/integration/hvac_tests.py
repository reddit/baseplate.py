from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

try:
    from hvac.exceptions import InvalidRequest
except ImportError:
    raise unittest.SkipTest("hvac is not installed")

from baseplate.context.hvac import hvac_factory_from_config
from baseplate.secrets import SecretsStore
from baseplate.core import Baseplate
from baseplate.integration.thrift import RequestContext

from . import TestBaseplateObserver, skip_if_server_unavailable
from .. import mock


skip_if_server_unavailable("vault", 8200)


class HvacTests(unittest.TestCase):
    def setUp(self):
        app_config = {}
        secrets_store = mock.Mock(spec=SecretsStore)
        secrets_store.get_vault_url.return_value = "http://localhost:8200/"
        secrets_store.get_vault_token.return_value = "b4c6f298-3f80-11e7-8b88-5254001e7ad3"

        factory = hvac_factory_from_config(app_config, secrets_store)

        self.baseplate_observer = TestBaseplateObserver()

        baseplate = Baseplate()
        baseplate.register(self.baseplate_observer)
        baseplate.add_to_context("vault", factory)

        self.context = RequestContext()
        self.server_span = baseplate.make_server_span(self.context, "test")

    def test_simple(self):
        with self.server_span:
            is_initialized = self.context.vault.is_initialized()
        self.assertTrue(is_initialized)

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNone(span_observer.on_finish_exc_info)
        span_observer.span.name == "vault.request"
        span_observer.assert_tag("http.method", "GET")
        span_observer.assert_tag("http.url", "/v1/sys/init")
        span_observer.assert_tag("http.status_code", 200)

    def test_error(self):
        with self.server_span:
            with self.assertRaises(InvalidRequest):
                self.context.vault.initialize()

        server_span_observer = self.baseplate_observer.get_only_child()
        span_observer = server_span_observer.get_only_child()
        self.assertTrue(span_observer.on_start_called)
        self.assertTrue(span_observer.on_finish_called)
        self.assertIsNotNone(span_observer.on_finish_exc_info)
        span_observer.span.name == "vault.request"
        span_observer.assert_tag("http.method", "PUT")
        span_observer.assert_tag("http.url", "/v1/sys/init")
