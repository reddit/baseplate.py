``baseplate.secrets``
=====================

.. automodule:: baseplate.secrets

Fetcher Daemon
--------------

The secret fetcher is a sidecar that is run as a single daemon on each server.
It authenticates to Vault as the server itself and gets appropriate policies
for access to secrets accordingly. Once authenticated, it fetches a given list
of secrets from Vault and stores all of the data in a local file. It will
automatically re-fetch secrets as their leases expire, ensuring that key
rotation happens on schedule.

Because this is a sidecar, individual application processes don't need to talk
directly to Vault for simple secret tokens (but can do so if needed for more
complex operations like using the Transit backend). This reduces the load on
Vault and adds a safety net if Vault becomes unavailable.


Secret Store
------------

The secret store is the in-application integration with the file output of the
fetcher daemon.

.. autofunction:: secrets_store_from_config

.. autoclass:: SecretsStore
   :members:

.. autoclass:: VersionedSecret
   :members:

Exceptions
~~~~~~~~~~

.. autoexception:: CorruptSecretError

.. autoexception:: SecretNotFoundError

.. autoexception:: SecretsNotAvailableError
