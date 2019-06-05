import unittest

from baseplate.url import get_domain
from baseplate.url import is_subdomain
from baseplate.url import strip_www

URL = "https://www.reddit.com/r/blop/comments/bx12la/husker_sleep_blop/"
REDDIT_DOMAIN = "reddit.com"


class UrlTests(unittest.TestCase):
    def test_get_domain(self):
        self.assertEqual(get_domain(URL), REDDIT_DOMAIN)

    def test_is_subdomain(self):
        self.assertTrue(is_subdomain(get_domain(URL), REDDIT_DOMAIN))

    def test_strip_www(self):
        self.assertEqual(strip_www("www." + REDDIT_DOMAIN), REDDIT_DOMAIN)
        self.assertEqual(strip_www("slither.io"), "slither.io")
