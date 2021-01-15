from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Optional


class EdgeContextFactory(ABC):
    """Abstract base for a factory that parses edge context data."""

    @abstractmethod
    def from_upstream(self, header_value: Optional[bytes]) -> Any:
        """Parse a serialized edge context header from an inbound request.

        :param header_value: The raw bytes of the header payload.

        """
        ...
