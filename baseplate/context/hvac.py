"""Integration with HVAC, a Vault Python client, for advanced Vault features.

See `HVAC's README`_ for documentation on the methods available from its client.

.. note:: The :py:class:`~baseplate.secrets.SecretsStore` handles the most
    common use case of Vault in a Baseplate application: secure retrieval of
    secret tokens. This client is only necessary when taking advantage of more
    advanced features of Vault such as the `Transit backend`_ or `Cubbyholes`_.
    If these don't sound familiar, check out the secrets store before digging
    in here.

.. _Transit backend: https://www.vaultproject.io/docs/secrets/transit/
.. _Cubbyholes: https://www.vaultproject.io/docs/secrets/cubbyhole/index.html
.. _HVAC's README: https://github.com/ianunruh/hvac/blob/master/README.md

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime

import hvac
import requests

from . import ContextFactory
from .. import config


def hvac_factory_from_config(app_config, secrets_store, prefix="vault."):
    """Make an HVAC client factory from a configuration dictionary.

    The keys useful to :py:func:`hvac_factory_from_config` should be prefixed,
    e.g.  ``vault.timeout``. The ``prefix`` argument specifies the prefix used
    to filter keys.

    Supported keys:

    * ``timeout``: How long to wait for calls to Vault.

    :param dict app_config: The raw application configuration.
    :param baseplate.secrets.SecretsStore secrets_store: A configured secrets
        store from which we can get a Vault authentication token.
    :param str prefix: The prefix for configuration keys.

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "timeout": config.Optional(config.Timespan,
                                       default=datetime.timedelta(seconds=1)),
        },
    })
    options = getattr(cfg, config_prefix)

    return HvacContextFactory(
        secrets_store,
        options.timeout,
    )


class HvacContextFactory(ContextFactory):
    """HVAC client context factory.

    This factory will attach a proxy object which acts like an
    :py:class:`hvac.Client` to an attribute on the :term:`context object`. All
    methods that talk to Vault will be automatically instrumented for tracing
    and diagnostic metrics.

    :param baseplate.secrets.SecretsStore secrets_store: Configured secrets
        store from which we can get a Vault authentication token.
    :param datetime.timedelta timeout: How long to wait for calls to Vault.

    """
    def __init__(self, secrets_store, timeout):
        self.secrets = secrets_store
        self.timeout = timeout
        self.session = requests.Session()

    def make_object_for_context(self, name, server_span):
        vault_url = self.secrets.get_vault_url()
        vault_token = self.secrets.get_vault_token()

        return InstrumentedHvacClient(
            url=vault_url,
            token=vault_token,
            timeout=self.timeout.total_seconds(),
            session=self.session,
            context_name=name,
            server_span=server_span,
        )


class InstrumentedHvacClient(hvac.Client):
    def __init__(self, url, token, timeout, session, context_name, server_span):
        self.context_name = context_name
        self.server_span = server_span

        super(InstrumentedHvacClient, self).__init__(
            url=url, token=token, timeout=timeout, session=session)

    # this ugliness is us undoing the name mangling that __request turns into
    # inside python. this feels very dirty.
    def _Client__request(self, method, url, **kwargs):
        span_name = "{}.request".format(self.context_name)
        with self.server_span.make_child(span_name) as span:
            span.set_tag("http.method", method.upper())
            span.set_tag("http.url", url)

            response = super(InstrumentedHvacClient, self)._Client__request(
                method=method, url=url, **kwargs)

            # this means we can't get the status code from error responses.
            # that's unfortunate, but hvac doesn't make it easy.
            span.set_tag("http.status_code", response.status_code)
        return response
