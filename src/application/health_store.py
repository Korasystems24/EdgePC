from src.domain.entities.health_snapshot import HealthSnapshot


class HealthStore:
    """Один актуальный снапшот в памяти."""

    def __init__(self) -> None:
        self._snapshot: HealthSnapshot | None = None

    def put(self, snapshot: HealthSnapshot) -> None:
        self._snapshot = snapshot

    def current(self) -> HealthSnapshot | None:
        return self._snapshot
