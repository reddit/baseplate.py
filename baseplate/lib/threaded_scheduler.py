import threading
import time

from schedule import CancelJob
from schedule import Scheduler


class ThreadedScheduler(Scheduler):
    """
    A scheduler that runs continuously in a separate thread
    """

    def __init__(self):
        super().__init__()
        self._stop_event = None

    def is_running(self):
        return self._stop_event is not None

    def run(self, interval=1):
        """
        Spawns a new thread and runs pending jobs at a regular interval
        """
        if self.is_running():
            self.stop()

        stop_event = threading.Event()

        class ScheduleThread(threading.Thread):
            @classmethod
            def run(cls):
                while not stop_event.is_set():
                    self.run_pending()
                    time.sleep(interval)

        thread = ScheduleThread()
        thread.start()

        self._stop_event = stop_event

    def stop(self):
        if self._stop_event:
            self._stop_event.set()
            self._stop_event = None
