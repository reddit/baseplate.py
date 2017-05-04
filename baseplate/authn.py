import jwt


class TokenError(Exception):
    """Base class for all token-related errors."""


class SubNotPresentError(TokenError):
    """Raised when "sub" was not found in a JWT payload and was expected."""
    pass


def is_authorized_for_sub(jwt_token, sub, key):
    """Validate the ``sub`` of a JWT token using ``key``."""
    decoded = jwt.decode(jwt_token, key=key)
    if 'sub' not in decoded:
        raise SubNotPresentError
    return sub == decoded['sub']
