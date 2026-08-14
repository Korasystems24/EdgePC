import pytest

from src.application.ports.worker import FatalError
from src.application.watchdog.policy import decide
from src.domain.enum import WatchdogAction

_CONFIG = FatalError(reason="misconfigured", restartable=False)
_HUNG = FatalError(reason="source hung", restartable=True)


@pytest.mark.parametrize(
    ("is_alive", "task_done", "fatal_error", "has_started", "expected"),
    [
        (True, False, None, True, WatchdogAction.KEEP),
        (False, False, None, False, WatchdogAction.KEEP),
        (False, False, None, True, WatchdogAction.RESTART),
        (True, True, None, True, WatchdogAction.RESTART),
        (False, True, None, True, WatchdogAction.RESTART),
        (False, True, None, False, WatchdogAction.RESTART),
        (True, True, _CONFIG, True, WatchdogAction.LEAVE_STOPPED),
        (False, True, _CONFIG, True, WatchdogAction.LEAVE_STOPPED),
        (True, False, _CONFIG, True, WatchdogAction.LEAVE_STOPPED),
        (True, True, _HUNG, True, WatchdogAction.EXIT),
        (False, True, _HUNG, True, WatchdogAction.EXIT),
        (True, False, _HUNG, True, WatchdogAction.EXIT),
    ],
)
def test_decide(
    is_alive: bool,
    task_done: bool,
    fatal_error: FatalError | None,
    has_started: bool,
    expected: WatchdogAction,
) -> None:
    assert (
        decide(
            is_alive=is_alive,
            task_done=task_done,
            fatal_error=fatal_error,
            has_started=has_started,
        )
        == expected
    )
