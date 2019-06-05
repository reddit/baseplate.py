"""URL domain parsing."""


import re

DOMAIN_REGEX = re.compile("(?i)(?:.+?://)?([^/:#?]*)")
DOMAIN_PREFIX_REGEX = re.compile(r"^www\d*\.")


def is_subdomain(subdomain: str, base: str) -> bool:
    """Check if a domain is equal to or a subdomain of a base domain."""
    return subdomain == base or (subdomain is not None and subdomain.endswith("." + base))


def get_domain(url: str) -> str:
    """Take a URL and return the domain part (minus www.) if present."""
    match = DOMAIN_REGEX.search(url)
    if match:
        domain = strip_www(match.group(1))
    else:
        domain = url
    return domain.lower()


def strip_www(domain: str) -> str:
    """Remove preceding www from the domain."""
    stripped = domain
    if domain.count(".") > 1:
        prefix = DOMAIN_PREFIX_REGEX.findall(domain)
        if domain.startswith("www") and prefix:
            stripped = ".".join(domain.split(".")[1:])
    return stripped
