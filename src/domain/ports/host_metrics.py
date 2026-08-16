from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class HostResourceSample:
    """Срез CPU/RAM/диска для HealthSnapshot. Без I/O-смысла — только цифры."""

    cpu_percent: float
    memory_percent: float
    disk_percent: float


class HostMetrics(ABC):
    """Метрики железа. Реализация (psutil) живёт в infrastructure."""

    @abstractmethod
    def sample(self) -> HostResourceSample:
        """Неблокирующий снимок. cpu_percent(interval=None), без sleep."""
