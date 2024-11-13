import base64
import unittest

from unittest import mock

from baseplate.lib.crypto import validate_signature
from baseplate.testing.lib.secrets import FakeSecretsStore


has_csrf_policy = True
try:
    from baseplate.frameworks.pyramid.csrf import _make_csrf_token_payload, TokenCSRFStoragePolicy
except ImportError:
    has_csrf_policy = False


@unittest.skipIf(not has_csrf_policy, "Does not have the required pyramid version")
@mock.patch("baseplate.lib.crypto.time")
class TokenCSRFStoragePolicyTests(unittest.TestCase):
    def setUp(self):
        secrets = FakeSecretsStore(
            {
                "secrets": {
                    "secret/csrf/signing-key": {
                        "type": "versioned",
                        "current": base64.b64encode(b"test"),
                        "encoding": "base64",
                    }
                }
            }
        )
        self.policy = TokenCSRFStoragePolicy(secrets=secrets, secret_path="secret/csrf/signing-key")

    def test_make_csrf_token_payload(self, _):
        prefix, payload = _make_csrf_token_payload(version=1, account_id="t2_1")
        self.assertEqual(prefix, "1")
        self.assertEqual(payload, "1.t2_1")

    def test_new_csrf_token(self, time_mock):
        time_mock.time.return_value = 1000.0
        request = mock.Mock()
        request.authenticated_userid = "t2_1"
        token = self.policy.new_csrf_token(request)
        self.assertTrue(token.startswith("1."))
        self.assertEqual(token, "1.AQAA-BEAAF-br-ovnk0q8Wd0kA98-jsak9elbMqo0WbjT0GuyRTD")
        signature = token.split(".")[-1]
        validate_signature(self.policy._get_secret(), "1.t2_1", signature)

    def test_get_csrf_token(self, _):
        request = mock.Mock()
        request.params = {"csrf_token": "token"}
        self.assertEqual(self.policy.get_csrf_token(request), "token")

    def test_get_csrf_token_missing(self, _):
        request = mock.Mock()
        request.params = {}
        self.assertIs(self.policy.get_csrf_token(request), None)

    def test_check_csrf_token_pass(self, time_mock):
        time_mock.time.return_value = 1000.0
        token = "1.AQAA-BEAAF-br-ovnk0q8Wd0kA98-jsak9elbMqo0WbjT0GuyRTD"
        request = mock.Mock()
        request.authenticated_userid = "t2_1"
        self.assertTrue(self.policy.check_csrf_token(request, token))

    def test_check_csrf_token_tampered(self, time_mock):
        time_mock.time.return_value = 1000.0
        token = "2.AQAA-BEAAF-br-ovnk0q8Wd0kA98-jsak9elbMqo0WbjT0GuyRTD"
        request = mock.Mock()
        request.authenticated_userid = "t2_1"
        self.assertFalse(self.policy.check_csrf_token(request, token))

    def test_check_csrf_token_user_mismatch(self, time_mock):
        time_mock.time.return_value = 1000.0
        token = "1.AQAA-BEAAF-br-ovnk0q8Wd0kA98-jsak9elbMqo0WbjT0GuyRTD"
        request = mock.Mock()
        request.authenticated_userid = "t2_2"
        self.assertFalse(self.policy.check_csrf_token(request, token))

    def test_check_csrf_token_expired(self, time_mock):
        # This signature was generated with a timestamp of 1.0 and an
        # expiration of 1 second, so it expired after
        # datetime.datetime(1970, 1, 1, 0, 0, 2)
        time_mock.time.return_value = 1000.0
        token = "1.AQAAAgAAAEW5P28E0nhnhNrFyQshn9OTlRhOgg3EkjXcNtUXHD9P"
        request = mock.Mock()
        request.authenticated_userid = "t2_2"
        self.assertFalse(self.policy.check_csrf_token(request, token))

    def test_check_csrf_token_null(self, _):
        request = mock.Mock()
        request.authenticated_userid = "t2_1"
        self.assertFalse(self.policy.check_csrf_token(request, None))

    def test_check_csrf_token_invalid(self, time_mock):
        time_mock.time.return_value = 1000.0
        token = "1.foo.AQAA-BEAAF-br-ovnk0q8Wd0kA98-jsak9elbMqo0WbjT0GuyRTD"
        request = mock.Mock()
        request.authenticated_userid = "t2_1"
        self.assertFalse(self.policy.check_csrf_token(request, token))
