from abc import abstractmethod


class KnownException(Exception):
    @abstractmethod
    def is_recoverable(self) -> bool:
        pass


class RecoverableException(KnownException):
    def is_recoverable(self) -> bool:
        return True


class UnrecoverableException(KnownException):
    def is_recoverable(self) -> bool:
        return False
