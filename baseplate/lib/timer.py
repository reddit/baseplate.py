import time

from datetime import timedelta
from threading import Thread
from typing import Callable
from typing import Optional


class Timer:
    """A restartable timer that execute an action after a timeout or at an interval."""

    def __init__(
        self, action: Callable[[], None], interval: timedelta, is_repeating: bool = False,
    ) -> None:
        self.action = action
        self.interval = interval
        self.is_repeating = is_repeating
        self._thread: Optional[Thread] = None

    def start(self) -> None:
        if self.is_running():
            return
        self._thread = Thread(target=self._run)

    def stop(self) -> None:
        self._thread = None

    def _run(self) -> None:
        thread = self._thread
        while self.is_running() and self._thread == thread:
            time.sleep(self.interval.total_seconds())
            self.action()
            if not self.is_repeating:
                self.stop()

    def is_running(self) -> bool:
        return self._thread is not None

    def __del__(self) -> None:
        self.stop()
