from abc import abstractmethod


class KnownException(Exception):
    """A base class for exceptions expected by queue consumer,
    that signals to the consumer whether it should be retried or not.
    """

    @abstractmethod
    def is_recoverable(self) -> bool:
        pass


class RecoverableException(KnownException):
    def is_recoverable(self) -> bool:
        return True


class UnrecoverableException(KnownException):
    def is_recoverable(self) -> bool:
        return False
