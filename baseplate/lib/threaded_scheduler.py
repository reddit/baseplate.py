import time

from datetime import timedelta
from threading import Event
from threading import Thread
from typing import Optional

from schedule import Scheduler


class ThreadedScheduler(Scheduler):
    """A scheduler that runs continuously in a separate thread."""

    def __init__(self) -> None:
        super().__init__()
        self._stop_event: Optional[Event] = None

    def is_running(self) -> bool:
        return self._stop_event is not None

    def run(self, interval: timedelta) -> None:
        """Spawns a new thread and runs pending jobs at a regular interval."""
        if self.is_running():
            self.stop()

        stop_event = Event()

        class ScheduleThread(Thread):
            @classmethod
            def run(cls) -> None:
                while not stop_event.is_set():
                    self.run_pending()
                    time.sleep(interval.total_seconds())

        thread = ScheduleThread()
        thread.start()

        self._stop_event = stop_event

    def stop(self) -> None:
        if self._stop_event:
            self._stop_event.set()
            self._stop_event = None
