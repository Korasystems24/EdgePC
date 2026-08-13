import asyncio

from loguru import logger

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.worker_manager import WorkerManager
from src.config.settings import Settings, settings
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.repositories.telemetry_repository import (
    SqlAlchemyTelemetryRepository,
)
from src.infrastructure.database.sqlite_client import SqliteClient
from src.infrastructure.network.factory import create_network_gateway
from src.infrastructure.radar.factory import create_radar_adapter


def _build_worker_manager(
    app_settings: Settings, db_client: DatabaseClient
) -> WorkerManager:
    """Сборка графа зависимостей. Новые сервисы и воркеры добавляются здесь."""
    radar_adapter = create_radar_adapter(app_settings)
    telemetry_repository = SqlAlchemyTelemetryRepository(db_client)
    gateway = create_network_gateway(app_settings)

    ingestion_service = IngestionService(
        radar_adapter=radar_adapter,
        repository=telemetry_repository,
    )
    ingestion_worker = IngestionWorker(
        name="ingestion",
        service=ingestion_service,
        interval=app_settings.wcf.ingestion_interval,
    )

    dispatch_service = DispatchService(
        repository=telemetry_repository,
        gateway=gateway,
        batch_size=app_settings.dispatch.batch_size,
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
    try:
        await db_client.init_database()
        manager = _build_worker_manager(app_settings, db_client)
        await _run(manager)
    finally:
        await db_client.close()
        log.info("Все компоненты успешно очищены. Программа завершена.")
