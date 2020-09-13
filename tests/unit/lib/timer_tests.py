from datetime import timedelta
from time import sleep
from unittest.mock import call
from unittest.mock import Mock

import pytest

from baseplate.lib.timer import Timer


@pytest.fixture
def mock() -> Mock:
    return Mock()


class TestTimer:
    def test_no_repeat_will_execute_once(self, mock: Mock) -> None:
        timer = Timer(action=mock, interval=timedelta(milliseconds=100), is_repeating=False)
        try:
            timer.start()
            sleep(0.5)
            mock.assert_called_once()
        finally:
            timer.stop()

    def test_repeat_will_execute_more_than_once(self, mock: Mock) -> None:
        timer = Timer(action=mock, interval=timedelta(milliseconds=100), is_repeating=True)
        try:
            timer.start()
            sleep(0.51)
            mock.assert_has_calls([call(), call(), call(), call(), call()])
        finally:
            timer.stop()

    def test_no_repeat_will_not_execute_after_stop(self, mock: Mock) -> None:
        timer = Timer(action=mock, interval=timedelta(seconds=0.2), is_repeating=False)
        try:
            timer.start()
            sleep(0.1)
            timer.stop()
            mock.assert_not_called()
            sleep(0.3)
            mock.assert_not_called()
        finally:
            timer.stop()

    def test_repeat_will_not_execute_after_stop(self, mock: Mock) -> None:
        timer = Timer(action=mock, interval=timedelta(seconds=0.1), is_repeating=True)
        try:
            timer.start()
            sleep(0.21)
            timer.stop()
            mock.assert_has_calls([call(), call()])
            sleep(0.31)
            mock.assert_has_calls([call(), call()])
        finally:
            timer.stop()

    def test_timer_is_running(self, mock: Mock) -> None:
        timer = Timer(action=mock, interval=timedelta(seconds=0.1), is_repeating=True)
        try:
            timer.start()
            assert timer.is_running()
        finally:
            timer.stop()

    def test_timer_will_not_execute_old_action_after_restart(self) -> None:
        action_1 = Mock()
        action_2 = Mock()

        timer = Timer(action=action_1, interval=timedelta(seconds=0.2), is_repeating=False)
        try:
            timer.start()
            sleep(0.1)
            timer.stop()
            timer.action = action_2
            timer.start()
            sleep(0.5)
            action_1.assert_not_called()
            action_2.assert_called()
        finally:
            timer.stop()
