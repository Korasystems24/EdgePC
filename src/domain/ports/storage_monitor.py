from abc import ABC, abstractmethod


class StorageMonitor(ABC):
    @abstractmethod
    def free_bytes(self) -> int:
        """Свободные байты на файловой системе каталога БД."""
