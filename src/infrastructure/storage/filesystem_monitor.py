import shutil
from pathlib import Path

from src.domain.exceptions import StorageError
from src.domain.ports.storage_monitor import StorageMonitor


class FilesystemStorageMonitor(StorageMonitor):
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def free_bytes(self) -> int:
        try:
            return shutil.disk_usage(self._path).free
        except OSError as e:
            raise StorageError(
                f"Не удалось прочитать свободное место для {self._path}: {e}"
            ) from e
