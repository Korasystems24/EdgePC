from datetime import UTC, datetime

from src.domain.value_objects.fatal_error import FatalError


class FailureTracker:
    """Состояние ошибок воркера: счётчик сбоев подряд, последняя ошибка и
    фатальный отказ.

    Это же состояние читают Watchdog (решение о рестарте) и health-снапшот,
    поэтому оно собрано в одном объекте, а не разбросано по полям воркера.
    """

    def __init__(self) -> None:
        self._consecutive = 0
        self._last_error: str | None = None
        self._last_error_at: datetime | None = None
        self._fatal_error: FatalError | None = None

    @property
    def consecutive(self) -> int:
        return self._consecutive

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def last_error_at(self) -> datetime | None:
        return self._last_error_at

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    def note_error(self, message: str) -> None:
        """Запомнить ошибку, не считая её новым сбоем: счётчик уже увеличен
        выше по стеку."""
        self._last_error = message
        self._last_error_at = datetime.now(tz=UTC)

    def record_failure(self, message: str) -> None:
        self._consecutive += 1
        self.note_error(message)

    def set_fatal(self, error: FatalError) -> None:
        self.note_error(error.reason)
        self._fatal_error = error

    def reset(self) -> None:
        """Успешный тик: серия сбоев прервана. Фатальный отказ не сбрасывается —
        из него выходят только пересозданием воркера."""
        self._consecutive = 0
        self._last_error = None
        self._last_error_at = None

    def clear_errors(self) -> None:
        """Старт воркера: прошлые ошибки к новому циклу не относятся. Счётчик
        сбоев переживает старт — по нему видно, что воркер уже перезапускался."""
        self._last_error = None
        self._last_error_at = None
        self._fatal_error = None
