``baseplate.lib.secrets``
=========================

.. automodule:: baseplate.lib.secrets

Fetcher Daemon
--------------

The secret fetcher is a sidecar that is run as a single daemon on each server.
It can authenticate to Vault either as the server itself (through an AWS-signed
instance identity document) or through a mounted JWT when running within a
Kubernetes pod. It then gets access to secrets based upon the policies mapped
to the role it authenticated as. Once authenticated, it fetches a given
list of secrets from Vault and stores all of the data in a local file.
It will automatically re-fetch secrets as their leases expire, ensuring
that key rotation happens on schedule.

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

.. autoclass:: CredentialSecret
   :members:

Exceptions
~~~~~~~~~~

.. autoexception:: CorruptSecretError

.. autoexception:: SecretNotFoundError

.. autoexception:: SecretsNotAvailableError

Testing
-------

.. autoclass:: baseplate.testing.lib.secrets.FakeSecretsStore
