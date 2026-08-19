import time

LOG_INTERVAL_S = 60.0


class LogThrottle:
    """Пропускает не больше одного сообщения на ключ за интервал.

    Нужен, чтобы часовой оффлайн не превратился в тысячу одинаковых строк в
    логе: ключ («failure», «network», «storage») отвечает за свой вид сообщений
    независимо от остальных.
    """

    def __init__(self, interval: float = LOG_INTERVAL_S) -> None:
        self._interval = interval
        self._last_at: dict[str, float] = {}

    def allows(self, key: str) -> bool:
        now = time.monotonic()
        last = self._last_at.get(key)
        if last is not None and now - last < self._interval:
            return False
        self._last_at[key] = now
        return True

    def clear(self, key: str) -> None:
        """Забыть ключ, чтобы следующее сообщение прошло сразу: состояние
        сменилось, и молчать больше не нужно."""
        self._last_at.pop(key, None)
