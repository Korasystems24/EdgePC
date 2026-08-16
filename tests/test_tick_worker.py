from src.domain.value_objects.fatal_error import FatalError
from src.application.tick_worker import TickWorker


class _IdleWorker(TickWorker):
    async def _safe_tick(self) -> None:
        return None


def test_tick_failure_stores_last_error_even_when_log_is_rate_limited() -> None:
    worker = _IdleWorker(name="ingestion", interval=1.0)
    worker._consecutive_failures = 1
    worker._log_tick_failure("warning", "Источник недоступен: {}", "timeout")
    first_at = worker.last_error_at
    assert worker.last_error == "Источник недоступен: timeout"
    assert first_at is not None
    assert first_at.utcoffset() is not None
    assert first_at.utcoffset().total_seconds() == 0

    worker._consecutive_failures = 2
    worker._log_tick_failure("warning", "Источник недоступен: {}", "again")
    assert worker.last_error == "Источник недоступен: again"
    assert worker.last_error_at is not None
    assert worker.last_error_at >= first_at


def test_tick_failure_message_survives_braces_in_argument() -> None:
    worker = _IdleWorker(name="ingestion", interval=1.0)
    worker._log_tick_failure("error", "Источник недоступен: {}", "{timeout}")
    assert worker.last_error == "Источник недоступен: {timeout}"


def test_reset_failures_clears_last_error() -> None:
    worker = _IdleWorker(name="ingestion", interval=1.0)
    worker._log_tick_failure("error", "boom")
    worker._reset_failures()
    assert worker.last_error is None
    assert worker.last_error_at is None


def test_log_throttle_keys_are_independent() -> None:
    worker = _IdleWorker(name="ingestion", interval=1.0)
    assert worker._should_log("failure") is True
    assert worker._should_log("failure") is False
    assert worker._should_log("network") is True
    worker._clear_log_throttle("failure")
    assert worker._should_log("failure") is True
    assert worker._should_log("network") is False


def test_set_fatal_error_records_reason_and_stops() -> None:
    worker = _IdleWorker(name="dispatch", interval=1.0)
    worker._running = True
    worker._set_fatal_error(
        FatalError(reason="dispatch misconfigured", restartable=False)
    )
    assert worker.fatal_error == FatalError(
        reason="dispatch misconfigured", restartable=False
    )
    assert worker.last_error == "dispatch misconfigured"
    assert worker.last_error_at is not None
    assert worker._running is False
