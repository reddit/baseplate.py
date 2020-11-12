import unittest

from unittest import mock

import raven

from baseplate import config
from baseplate.observers import sentry


class StubRavenClient(raven.Client):
    def __init__(self, **kwargs):
        self.stub_events_sent = 0
        return super().__init__(**kwargs)

    def send(self, **kwargs):
        self.stub_events_sent += 1
        return None

    def is_enabled(self):
        return True


class TestException(Exception):
    pass


@mock.patch("raven.Client", new=StubRavenClient)
class SentryIgnoreExceptionsTest(unittest.TestCase):
    def observe_and_raise(self, cli, exc):
        @cli.capture_exceptions
        def _raise_exception():
            raise exc

        return _raise_exception

    def observe_all(self, cli, excs):
        for exc in excs:
            fn = self.observe_and_raise(cli, exc)
            try:
                fn()
            except exc:
                exc

    def test_default_ignore_exceptions(self):
        cli = sentry.error_reporter_from_config(dict(), __name__)
        self.observe_all(cli, sentry.ALWAYS_IGNORE_EXCEPTIONS)
        self.assertEqual(cli.stub_events_sent, 0)

    def test_report_exception(self):
        cli = sentry.error_reporter_from_config(dict(), __name__)
        self.assertRaises(TestException, self.observe_and_raise(cli, TestException))
        self.assertEqual(cli.stub_events_sent, 1)

    def test_ignere_exception(self):
        cli = sentry.error_reporter_from_config(
            {"sentry.ignore_exceptions": TestException.__name__}, __name__,
        )
        self.assertRaises(TestException, self.observe_and_raise(cli, TestException))
        self.assertEqual(cli.stub_events_sent, 0)

        # If 'ignore_exceptions' is defined default filters aren't included.
        self.observe_all(cli, sentry.ALWAYS_IGNORE_EXCEPTIONS)
        self.assertEqual(cli.stub_events_sent, len(sentry.ALWAYS_IGNORE_EXCEPTIONS))

    def test_additional_ignored_exceptions(self):
        cli = sentry.error_reporter_from_config(
            {"sentry.additional_ignore_exceptions": TestException.__name__}, __name__,
        )
        self.assertRaises(TestException, self.observe_and_raise(cli, TestException))
        self.assertEqual(cli.stub_events_sent, 0)

        self.observe_all(cli, sentry.ALWAYS_IGNORE_EXCEPTIONS)
        self.assertEqual(cli.stub_events_sent, 0)

    def test_both_ignored_exceptions(self):
        conf = {
            "sentry.ignore_exceptions": TestException.__name__,
            "sentry.additional_ignore_exceptions": TestException.__name__,
        }
        self.assertRaises(
            config.ConfigurationError, sentry.error_reporter_from_config, conf, __name__
        )
