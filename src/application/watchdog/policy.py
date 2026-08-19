from enum import StrEnum

from src.domain.value_objects.fatal_error import FatalError


class WatchdogAction(StrEnum):
    KEEP = "keep"
    RESTART = "restart"
    LEAVE_STOPPED = "leave_stopped"
    EXIT = "exit"


def decide(
    *,
    is_alive: bool,
    task_done: bool,
    fatal_error: FatalError | None,
    has_started: bool,
) -> WatchdogAction:
    if fatal_error is not None:
        if fatal_error.restartable:
            return WatchdogAction.EXIT
        return WatchdogAction.LEAVE_STOPPED
    if task_done:
        return WatchdogAction.RESTART
    if not is_alive:
        if not has_started:
            return WatchdogAction.KEEP
        return WatchdogAction.RESTART
    return WatchdogAction.KEEP
