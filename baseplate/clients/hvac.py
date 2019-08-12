import datetime

from typing import Any

import hvac
import requests

from baseplate import Span
from baseplate.clients import ContextFactory
from baseplate.lib import config
from baseplate.lib.secrets import SecretsStore


def hvac_factory_from_config(
    app_config: config.RawConfig, secrets_store: SecretsStore, prefix: str = "vault."
) -> "HvacContextFactory":
    """Make an HVAC client factory from a configuration dictionary.

    The keys useful to :py:func:`hvac_factory_from_config` should be prefixed,
    e.g.  ``vault.timeout``. The ``prefix`` argument specifies the prefix used
    to filter keys.

    Supported keys:

    * ``timeout``: How long to wait for calls to Vault.
        (:py:func:`~baseplate.lib.config.Timespan`)

    :param app_config: The raw application configuration.
    :param secrets_store: A configured secrets store from which we can get a
        Vault authentication token.
    :param prefix: The prefix for configuration keys.

    """
    assert prefix.endswith(".")
    parser = config.SpecParser(
        {"timeout": config.Optional(config.Timespan, default=datetime.timedelta(seconds=1))}
    )
    options = parser.parse(prefix[:-1], app_config)

    return HvacContextFactory(secrets_store, options.timeout)


class HvacClient(config.Parser):
    """Configure an HVAC client.

    This is meant to be used with
    :py:meth:`baseplate.Baseplate.configure_context`.

    See :py:func:`hvac_factory_from_config` for available configuration settings.

    :param secrets: The configured secrets store for this application.

    """

    def __init__(self, secrets: SecretsStore):
        self.secrets = secrets

    def parse(self, key_path: str, raw_config: config.RawConfig) -> "HvacContextFactory":
        return hvac_factory_from_config(
            raw_config, secrets_store=self.secrets, prefix=f"{key_path}."
        )


class HvacContextFactory(ContextFactory):
    """HVAC client context factory.

    This factory will attach a proxy object which acts like an
    :py:class:`hvac.Client` to an attribute on the
    :py:class:`~baseplate.RequestContext`. All methods that talk to Vault will
    be automatically instrumented for tracing and diagnostic metrics.

    :param baseplate.lib.secrets.SecretsStore secrets_store: Configured secrets
        store from which we can get a Vault authentication token.
    :param datetime.timedelta timeout: How long to wait for calls to Vault.

    """

    def __init__(self, secrets_store: SecretsStore, timeout: datetime.timedelta):
        self.secrets = secrets_store
        self.timeout = timeout
        self.session = requests.Session()

    def make_object_for_context(self, name: str, span: Span) -> "InstrumentedHvacClient":
        vault_url = self.secrets.get_vault_url()
        vault_token = self.secrets.get_vault_token()

        return InstrumentedHvacClient(
            url=vault_url,
            token=vault_token,
            timeout=self.timeout.total_seconds(),
            session=self.session,
            context_name=name,
            server_span=span,
        )


class InstrumentedHvacClient(hvac.Client):
    def __init__(
        self,
        url: str,
        token: str,
        timeout: float,
        session: requests.Session,
        context_name: str,
        server_span: Span,
    ):
        self.context_name = context_name
        self.server_span = server_span

        super().__init__(url=url, token=token, timeout=timeout, session=session)

    # this ugliness is us undoing the name mangling that __request turns into
    # inside python. this feels very dirty.
    def _Client__request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        span_name = f"{self.context_name}.request"
        with self.server_span.make_child(span_name) as span:
            span.set_tag("http.method", method.upper())
            span.set_tag("http.url", url)

            # pylint: disable=no-member
            response = super()._Client__request(method=method, url=url, **kwargs)

            # this means we can't get the status code from error responses.
            # that's unfortunate, but hvac doesn't make it easy.
            span.set_tag("http.status_code", response.status_code)
        return response
