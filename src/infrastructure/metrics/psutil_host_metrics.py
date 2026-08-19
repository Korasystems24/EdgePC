from pathlib import Path

import psutil

from src.domain.exceptions import StorageError
from src.domain.ports.host_metrics import HostMetrics, HostResourceSample


class PsutilHostMetrics(HostMetrics):
    def __init__(self, disk_path: str | Path) -> None:
        self._disk_path = str(disk_path)
        # Первый вызов с interval=None всегда 0.0 — прогрев, чтобы sample()
        # вернул осмысленное значение.
        psutil.cpu_percent(interval=None)

    def sample(self) -> HostResourceSample:
        try:
            disk_percent = float(psutil.disk_usage(self._disk_path).percent)
        except OSError as e:
            raise StorageError(
                f"Не удалось прочитать использование диска для {self._disk_path}: {e}"
            ) from e
        try:
            cpu_percent = psutil.cpu_percent(interval=None)
            memory_percent = psutil.virtual_memory().percent
        except (OSError, psutil.Error) as e:
            raise StorageError(f"Не удалось прочитать CPU/RAM: {e}") from e
        return HostResourceSample(
            cpu_percent=_clamp_percent(cpu_percent),
            memory_percent=_clamp_percent(memory_percent),
            disk_percent=_clamp_percent(disk_percent),
        )


def _clamp_percent(value: float) -> float:
    return min(100.0, max(0.0, float(value)))
