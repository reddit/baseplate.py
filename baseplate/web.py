"""Utilities useful for frontend web services"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re

from ._compat import urlparse, string_types

# Characters that might cause parsing differences in different implementations
# Spaces only seem to cause parsing differences when occurring directly before
# the scheme
URL_PROBLEMATIC_RE = re.compile(
    r'(\A\x20|[\x00-\x19\u1680\u180E\u2000-\u2029\u205f\u3000\\])',
    re.UNICODE
)
WEB_SAFE_SCHEMES = {"http", "https"}


def is_web_safe_url(url):
    """Determine if this URL could cause issues with different parsers"""

    # There's no valid reason for this, and just serves to confuse UAs
    # and urllib2.
    if url.startswith("///"):
        return False

    # Reject any URLs that contain characters known to cause parsing
    # differences between parser implementations
    if re.search(URL_PROBLEMATIC_RE, url):
            return False

    parsed = urlparse(url)

    # Double-checking the above
    if not parsed.hostname and parsed.path.startswith("//"):
        return False

    # A host-relative link with a scheme like `https:/baz` or `https:?quux`
    if parsed.scheme and not parsed.hostname:
        return False

    # Credentials in the netloc?
    if "@" in parsed.netloc:
        return False

    # `javascript://www.example.com/%0D%Aalert(1)` is not safe, obviously
    if parsed.scheme and parsed.scheme.lower() not in WEB_SAFE_SCHEMES:
        return False

    return True


def is_same_domain(host, patterns):
    """
    Return ``True`` if the host is either an exact match or a match
    to the wildcard pattern.
    Any pattern beginning with a period matches a domain and all of its
    subdomains. (e.g. ``.example.com`` matches ``example.com`` and
    ``foo.example.com``). Anything else is an exact string match.
    An empty pattern is considered equivalent to "no domain".
    """
    if not patterns:
        return False
    # Normalize `None` to `""`
    if not host:
        host = ""

    if isinstance(patterns, string_types):
        patterns = (patterns.lower(),)

    for pattern in patterns:
        pattern = pattern.lower()
        matches = (
            pattern[:1] == '.' and (host.endswith(pattern) or host == pattern[1:]) or
            pattern == host
        )
        if matches:
            return True
    return False


def is_safe_redirect_url(url, allowed_bases):
    if not is_web_safe_url(url):
        return False
    return is_same_domain(urlparse(url).hostname, patterns=allowed_bases)
