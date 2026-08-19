from dataclasses import dataclass
from enum import StrEnum, auto

from src.domain.ports.storage_monitor import StorageMonitor


class StorageDecision(StrEnum):
    ALLOW = auto()
    """Места достаточно, пакет можно читать и сохранять."""

    PAUSED = auto()
    """Места мало: новые пакеты не пишем, накопленное не удаляем."""

    RESUMED = auto()
    """Место вернулось с запасом, пауза снята."""


@dataclass(frozen=True)
class StorageCheck:
    decision: StorageDecision
    free_bytes: int


class StorageGate:
    """Разрешает или запрещает запись по свободному месту, с гистерезисом.

    Возобновление требует большего запаса, чем остановка: иначе сбор дёргался бы
    туда-обратно на одном и том же значении. Решение о логах принимает воркер —
    здесь только состояние паузы.
    """

    def __init__(
        self,
        monitor: StorageMonitor,
        *,
        min_free_bytes: int,
        resume_free_bytes: int,
    ) -> None:
        self._monitor = monitor
        self._min_free_bytes = min_free_bytes
        self._resume_free_bytes = resume_free_bytes
        self._paused = False

    @property
    def paused(self) -> bool:
        return self._paused

    def check(self) -> StorageCheck:
        """Бросает StorageError, если свободное место прочитать не удалось."""
        free_bytes = self._monitor.free_bytes()

        if self._paused:
            if free_bytes >= self._resume_free_bytes:
                self._paused = False
                return StorageCheck(StorageDecision.RESUMED, free_bytes)
            return StorageCheck(StorageDecision.PAUSED, free_bytes)

        if free_bytes < self._min_free_bytes:
            self._paused = True
            return StorageCheck(StorageDecision.PAUSED, free_bytes)

        return StorageCheck(StorageDecision.ALLOW, free_bytes)
