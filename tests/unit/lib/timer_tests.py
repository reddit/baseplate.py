from datetime import timedelta
import pytest
from time import sleep

from baseplate.lib.timer import Timer


class Counter:
    def __init__(self) -> None:
        self._value = 0

    def inc(self) -> None:
        self._value = self._value + 1

    def value(self) -> int:
        return self._value

    def set_value(self, value: int) -> None:
        self._value = value


@pytest.fixture
def counter() -> Counter:
    return Counter()


class TestTimer:
    def test_no_repeat_will_execute_once(self, counter: Counter) -> None:
        timer = Timer(action=counter.inc, interval=timedelta(seconds=0.1), is_repeating=False)
        timer.start()
        sleep(0.5)
        assert counter.value() == 1

    def test_repeat_will_execute_more_than_once(self, counter: Counter) -> None:
        timer = Timer(action=counter.inc, interval=timedelta(seconds=0.1), is_repeating=True)
        timer.start()
        sleep(0.51)
        assert counter.value() == 5

    def test_no_repeat_will_not_execute_after_stop(self, counter: Counter) -> None:
        timer = Timer(action=counter.inc, interval=timedelta(seconds=0.2), is_repeating=False)
        timer.start()
        sleep(0.1)
        timer.stop()
        value_after_stop = counter.value()
        assert value_after_stop == 0
        sleep(0.3)
        assert counter.value() == value_after_stop

    def test_repeat_will_not_execute_after_stop(self, counter: Counter) -> None:
        timer = Timer(action=counter.inc, interval=timedelta(seconds=0.1), is_repeating=True)
        timer.start()
        sleep(0.21)
        timer.stop()
        value_after_stop = counter.value()
        assert value_after_stop == 2
        sleep(0.31)
        assert counter.value() == value_after_stop

    def test_timer_is_running(self, counter: Counter) -> None:
        timer = Timer(action=counter.inc, interval=timedelta(seconds=0.1), is_repeating=True)
        timer.start()
        assert timer.is_running()

    def test_timer_will_stop_when_going_out_of_scope(self, counter: Counter) -> None:
        def action() -> None:
            timer = Timer(action=counter.inc, interval=timedelta(seconds=0.1), is_repeating=True)
            timer.start()
        action()
        sleep(1)
        assert counter.value() == 0

    def test_timer_will_not_execute_old_action_after_restart(self, counter: Counter) -> None:
        def action_1():
            counter.set_value(1000)
        
        def action_2():
            counter.inc()
        
        timer = Timer(action=action_1, interval=timedelta(seconds=0.2), is_repeating=False)
        timer.start()
        sleep(0.1)
        timer.stop()
        timer.action = action_2
        timer.start()
        sleep(0.5)
        assert counter.value() < 1000
