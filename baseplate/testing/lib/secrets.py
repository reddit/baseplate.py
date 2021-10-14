from typing import Dict

from baseplate import Span
from baseplate.lib.secrets import SecretsStore
from baseplate.lib.secrets import VaultFileParser
from baseplate.testing.lib.file_watcher import FakeFileWatcher


class FakeSecretsStore(SecretsStore):
    """Fake secrets store for testing purposes.

    Use this in place of a :py:class:`~baseplate.lib.secrets.SecretsStore` in
    tests to avoid having to load an actual file:

    .. testsetup::

        from baseplate.testing.lib.secrets import FakeSecretsStore

    .. doctest::

        >>> secrets = FakeSecretsStore({
        ...    "secrets": {
        ...        "secret/foo/bar": {
        ...            "type": "versioned",
        ...            "current": "hunter2",
        ...        },
        ...    },
        ... })
        >>> secrets.get_versioned("secret/foo/bar")
        VersionedSecret(previous=None, current=b'hunter2', next=None)

    .. versionadded:: 1.5

    """

    # pylint: disable=super-init-not-called
    def __init__(self, fake_secrets: Dict) -> None:
        self.path_type = "file"
        self.path = ""
        self._filewatchers = {}
        self._filewatchers[self.path] = FakeFileWatcher(fake_secrets)
        self.parser = VaultFileParser

    def make_object_for_context(self, name: str, span: Span) -> SecretsStore:
        return self
