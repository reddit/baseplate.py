from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest

from baseplate import web


class IsSameDomainTests(unittest.TestCase):
    def test_string_pattern(self):
        self.assertTrue(web.is_same_domain("foo.com", "foo.com"))
        self.assertFalse(web.is_same_domain("baz.com.com", "baz.com"))

    def test_iterable_patterns(self):
        patterns = {"foo.com", "bar.com"}
        self.assertTrue(web.is_same_domain("foo.com", patterns))
        self.assertTrue(web.is_same_domain("bar.com", patterns))
        self.assertFalse(web.is_same_domain("baz.com", patterns))

    def test_subdomain_matching(self):
        self.assertTrue(web.is_same_domain("foo.com", {".foo.com"}))
        self.assertTrue(web.is_same_domain("bar.foo.com", {".foo.com"}))
        self.assertFalse(web.is_same_domain("bar.foo.com", {"foo.com"}))
        self.assertTrue(web.is_same_domain("baz.bar.foo.com", {".foo.com"}))
        self.assertFalse(web.is_same_domain("baz.bar.foo.com", {"bar.foo.com"}))
        self.assertTrue(web.is_same_domain("baz.bar.foo.com", {".bar.foo.com"}))
        self.assertFalse(web.is_same_domain("foo.com", {".bar.foo.com"}))


class IsWebSafeURLTests(unittest.TestCase):
    def assertIsWebSafeUrl(self, url):
        self.assertTrue(web.is_web_safe_url(url))

    def assertIsNotWebSafeUrl(self, url):
        self.assertFalse(web.is_web_safe_url(url))

    def test_normal_urls(self):
        self.assertIsWebSafeUrl("https://example.com/")
        self.assertIsWebSafeUrl("https://en.example.com/")
        self.assertIsWebSafeUrl("https://foobar.baz.example.com/quux/?a")
        self.assertIsWebSafeUrl("#anchorage")
        self.assertIsWebSafeUrl("?path_relative_queries")
        self.assertIsWebSafeUrl("/")
        self.assertIsWebSafeUrl("/cats")
        self.assertIsWebSafeUrl("/cats/")
        self.assertIsWebSafeUrl("/cats/#maru")
        self.assertIsWebSafeUrl("//foobaz.example.com/aa/baz#quux")
        # XXX: This is technically a legal relative URL, are there any UAs
        # stupid enough to treat this as absolute?
        self.assertIsWebSafeUrl("path_relative_subpath.com")

    def test_weird_protocols(self):
        self.assertIsNotWebSafeUrl(
            "javascript://example.com/%0d%0aalert(1)"
        )
        self.assertIsNotWebSafeUrl("hackery:whatever")

    def test_http_auth(self):
        # There's no legitimate reason to include HTTP auth details in the URL,
        # they only serve to confuse everyone involved.
        # For example, this used to be the behaviour of `UrlParser`, oops!
        # > UrlParser("http://everyoneforgets:aboutthese@/baz.com/").unparse()
        # 'http:///baz.com/'
        self.assertIsNotWebSafeUrl("http://foo:bar@/example.com/")

    def test_browser_quirks(self):
        # Some browsers try to be helpful and ignore characters in URLs that
        # they think might have been accidental (I guess due to things like:
        # `<a href=" http://badathtml.com/ ">`. We need to ignore those when
        # determining if a URL is local.
        self.assertIsNotWebSafeUrl("/\x00/example.com")
        self.assertIsNotWebSafeUrl("\x09//example.com")
        self.assertIsNotWebSafeUrl(" http://example.com/")

        # This is makes sure we're not vulnerable to a bug in
        # urlparse / urlunparse.
        # urlunparse(urlparse("////foo.com")) == "//foo.com"! screwy!
        self.assertIsNotWebSafeUrl("////example.com/")
        self.assertIsNotWebSafeUrl("//////example.com/")
        # Similar, but with a scheme
        self.assertIsNotWebSafeUrl("http:///example.com/")
        # Webkit and co like to treat backslashes as equivalent to slashes in
        # different places, maybe to make OCD Windows users happy.
        self.assertIsNotWebSafeUrl(r"/\example.com/")
        # On chrome this goes to example.com, not a subdomain of reddit.com!
        self.assertIsNotWebSafeUrl(
            r"http://\\example.com\a.example.com/foo"
        )

        # Combo attacks!
        self.assertIsNotWebSafeUrl(r"///\example.com/")
        self.assertIsNotWebSafeUrl(r"\\example.com")
        self.assertIsNotWebSafeUrl("/\x00//\\example.com/")


class IsSafeRedirectUriTests(unittest.TestCase):
    ALLOWED_BASES = {"foo.com", ".bar.com"}

    def _getAllowedBases(self, additional_bases):
        additional_bases = additional_bases or frozenset()
        return self.ALLOWED_BASES.union(additional_bases)

    def assertIsSafeRedirectUrl(self, url, additional_bases=None):
        bases = self._getAllowedBases(additional_bases)
        self.assertTrue(web.is_safe_redirect_url(url, bases))

    def assertIsNotSafeRedirectUrl(self, url, additional_bases=None):
        bases = self._getAllowedBases(additional_bases)
        self.assertFalse(web.is_safe_redirect_url(url, bases))

    def test_valid_redirect_urls_allowed(self):
        self.assertIsSafeRedirectUrl("http://foo.com")
        self.assertIsSafeRedirectUrl("http://bar.com")
        self.assertIsSafeRedirectUrl("http://baz.bar.com")

    def test_invalid_redirect_urls_not_allowed(self):
        self.assertIsNotSafeRedirectUrl("http://bar.foo.com")
        self.assertIsNotSafeRedirectUrl("http://foo.bar.com.baz")
        self.assertIsNotSafeRedirectUrl("javascript://bar.com")
        self.assertIsNotSafeRedirectUrl("http://baz.com")

    def test_host_relative_redirect_urls(self):
        self.assertIsNotSafeRedirectUrl("/foo")
        # Having `""` in the allowed_bases list means host-relative URLs
        # are allowed
        self.assertIsSafeRedirectUrl("/foo", additional_bases={""})

    def unsafe_urls_disallowed(self):
        self.assertIsNotSafeRedirectUrl("http://foo:bar@foo.com/")
        self.assertIsNotSafeRedirectUrl("http://foo.com/\\")
