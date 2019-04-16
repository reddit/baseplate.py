import requests
import unittest

from baseplate.secrets import fetcher
from .. import mock

class SecretFetcherTests(unittest.TestCase):
    def test_secret_not_fetched_throws_error(self):
        mock_vault_client = mock.Mock(spec=fetcher.VaultClient)
        mock_vault_client.session.get.side_effect = [requests.HTTPError]
        response_mock = Mock()
        response_mock.status_code = 403
        response_mock.json.return_value = {
            'text': 'Client Error',
            'status_code': 'Independence Day',
        }
        with self.assertRaises(requests.HTTPError):
            mock_vault_client.get_secret("fakesecret")
