import unittest

from baseplate.lib.edge_context import InvalidAuthenticationToken
from baseplate.lib.edge_context import NoAuthenticationError
from baseplate.lib.edge_context import ValidatedAuthenticationToken


class AuthenticationTokenTests(unittest.TestCase):
    def test_validated_authentication_token(self):
        payload = {
            "sub": "t2_user",
            "exp": 1574458470,
            "client_id": "client_id",
            "roles": ["role_a"],
            "client_type": "type_a",
            "scopes": ["scope_a"],
            "loid": {"id": "t2_user", "created_ms": 1574458470},
        }
        token = ValidatedAuthenticationToken(payload)
        self.assertEqual(token.subject, "t2_user")
        self.assertEqual(token.user_roles, {"role_a"})
        self.assertEqual(token.oauth_client_id, "client_id")
        self.assertEqual(token.oauth_client_type, "type_a")
        self.assertEqual(token.scopes, {"scope_a"})
        self.assertEqual(token.loid, "t2_user")
        self.assertEqual(token.loid_created_ms, 1574458470)

    def test_validated_authentication_token_none(self):
        payload = {
            "sub": "t2_user",
            "exp": 1574458470,
            "client_id": None,
            "client_type": None,
            "scopes": None,
            "loid": None,
        }
        token = ValidatedAuthenticationToken(payload)
        self.assertEqual(token.subject, "t2_user")
        self.assertEqual(token.user_roles, set())
        self.assertEqual(token.oauth_client_id, None)
        self.assertEqual(token.oauth_client_type, None)
        self.assertEqual(token.scopes, set())
        self.assertEqual(token.loid, None)
        self.assertEqual(token.loid_created_ms, None)

    def test_invalidated_authentication_token(self):
        token = InvalidAuthenticationToken()
        for attr in dir(token):
            if attr.startswith("__"):
                continue
            with self.assertRaises(NoAuthenticationError):
                getattr(token, attr)
