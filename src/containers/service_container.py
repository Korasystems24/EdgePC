from loguru import logger
from src.application.worker_manager import WorkerManager

from src.application.ingestion.factory import RadarAdapterFactory
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.config.settings import Settings, settings
from src.domain.ports.radar_adapter import RadarAdapter


class ServiceContainer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._radar_adapter: RadarAdapter | None = None
        self._ingestion_service: IngestionService | None = None
        self._ingestion_worker: IngestionWorker | None = None
        self._worker_manager: WorkerManager | None = None

    def get_radar_adapter(self) -> RadarAdapter:
        if self._radar_adapter is None:
            logger.debug("Инициализация RadarAdapter")
            self._radar_adapter = RadarAdapterFactory.create_adapter(self._settings)
        return self._radar_adapter

    def get_ingestion_service(self) -> IngestionService:
        if self._ingestion_service is None:
            self._ingestion_service = IngestionService(self.get_radar_adapter())
        return self._ingestion_service

    def get_ingestion_worker(self) -> IngestionWorker:
        if self._ingestion_worker is None:
            self._ingestion_worker = IngestionWorker(
                name="ingestion",
                service=self.get_ingestion_service(),
                interval=self._settings.ingestion_interval,
            )
        return self._ingestion_worker

    def get_worker_manager(self) -> WorkerManager:
        if self._worker_manager is None:
            self._worker_manager = WorkerManager(workers=[self.get_ingestion_worker()])
        return self._worker_manager


container = ServiceContainer(settings=settings)
