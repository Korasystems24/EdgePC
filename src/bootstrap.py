import asyncio

from loguru import logger

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.worker_manager import WorkerManager
from src.config.settings import Settings, settings
from src.domain.ports.network_gateway import NetworkGateway
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.repositories.telemetry_repository import (
    SqlAlchemyTelemetryRepository,
)
from src.infrastructure.database.sqlite_client import SqliteClient
from src.infrastructure.network.factory import create_network_gateway
from src.infrastructure.radar.factory import create_radar_adapter
from src.infrastructure.storage.filesystem_monitor import FilesystemStorageMonitor


def _build_worker_manager(
    app_settings: Settings,
    db_client: DatabaseClient,
    gateway: NetworkGateway,
) -> WorkerManager:
    """Сборка графа зависимостей. Новые сервисы и воркеры добавляются здесь."""
    radar_adapter = create_radar_adapter(app_settings)
    telemetry_repository = SqlAlchemyTelemetryRepository(db_client)

    ingestion_service = IngestionService(
        radar_adapter=radar_adapter,
        repository=telemetry_repository,
    )
    storage_monitor = FilesystemStorageMonitor(path=app_settings.db.dir)
    ingestion_worker = IngestionWorker(
        name="ingestion",
        service=ingestion_service,
        storage_monitor=storage_monitor,
        interval=app_settings.wcf.ingestion_interval,
        min_free_bytes=app_settings.db.min_free_bytes,
        resume_free_bytes=app_settings.db.resume_free_bytes,
    )

    dispatch_service = DispatchService(
        repository=telemetry_repository,
        gateway=gateway,
        batch_size=app_settings.dispatch.batch_size,
        retention_ttl=app_settings.dispatch.retention_ttl,
        mass_rejection_sample=app_settings.dispatch.mass_rejection_sample,
        quarantine_after_attempts=app_settings.dispatch.quarantine_after_attempts,
    )
    dispatch_worker = DispatchWorker(
        name="dispatch",
        service=dispatch_service,
        settings=app_settings.dispatch,
    )

    return WorkerManager(workers=[ingestion_worker, dispatch_worker])


async def _run(manager: WorkerManager) -> None:
    """Жизненный цикл воркеров: запуск и корректная остановка."""
    log = logger.bind(name="bootstrap")
    try:
        await manager.run_all()
    except asyncio.CancelledError:
        log.info("Получен сигнал отмены (Ctrl+C), запускаем shutdown...")
        raise
    except Exception:
        log.exception("Необработанная ошибка в воркерах")
        raise
    finally:
        manager.stop_all()


async def bootstrap_application(app_settings: Settings = settings) -> None:
    log = logger.bind(name="bootstrap")
    log.info("Запуск приложения в боевом режиме")
    db_client = SqliteClient(url=app_settings.db.url)
    gateway = create_network_gateway(app_settings)
    try:
        await db_client.init_database()
        manager = _build_worker_manager(app_settings, db_client, gateway)
        await _run(manager)
    finally:
        try:
            await gateway.close()
        finally:
            await db_client.close()
            log.info("Все компоненты успешно очищены. Программа завершена.")
