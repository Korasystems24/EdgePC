import asyncio
import os
import signal
import time

from loguru import logger

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.config.settings import Settings, settings
from src.domain.exceptions import RadarSourceHung
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.radar_adapter import RadarAdapter
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
    radar_adapter: RadarAdapter,
) -> WorkerManager:
    """Сборка графа зависимостей. Новые сервисы и воркеры добавляются здесь."""
    telemetry_repository = SqlAlchemyTelemetryRepository(db_client)

    ingestion_service = IngestionService(
        radar_adapter=radar_adapter,
        repository=telemetry_repository,
    )
    storage_monitor = FilesystemStorageMonitor(path=app_settings.db.dir)

    def ingestion_factory() -> IngestionWorker:
        return IngestionWorker(
            name="ingestion",
            service=ingestion_service,
            storage_monitor=storage_monitor,
            interval=app_settings.wcf.ingestion_interval,
            tick_deadline=app_settings.wcf.tick_deadline,
            min_free_bytes=app_settings.db.min_free_bytes,
            resume_free_bytes=app_settings.db.resume_free_bytes,
            heartbeat_max_silence=app_settings.heartbeat_max_silence,
        )

    dispatch_service = DispatchService(
        repository=telemetry_repository,
        gateway=gateway,
        batch_size=app_settings.dispatch.batch_size,
        retention_ttl=app_settings.dispatch.retention_ttl,
        mass_rejection_sample=app_settings.dispatch.mass_rejection_sample,
        quarantine_after_attempts=app_settings.dispatch.quarantine_after_attempts,
    )

    def dispatch_factory() -> DispatchWorker:
        return DispatchWorker(
            name="dispatch",
            service=dispatch_service,
            settings=app_settings.dispatch,
            heartbeat_max_silence=app_settings.heartbeat_max_silence,
        )

    return WorkerManager(
        factories={
            "ingestion": ingestion_factory,
            "dispatch": dispatch_factory,
        },
        restart_join_timeout=app_settings.restart_join_timeout,
    )


def _build_watchdog(manager: WorkerManager, app_settings: Settings) -> Watchdog:
    watchdog_settings = app_settings.watchdog
    return Watchdog(
        manager,
        interval=watchdog_settings.interval,
        restart_limit=watchdog_settings.restart_limit,
        restart_window_seconds=watchdog_settings.restart_window_seconds,
    )


def _install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)


def _remove_stop_signals() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.remove_signal_handler(sig)


async def shutdown_runtime(
    watchdog: Watchdog | None,
    watchdog_task: asyncio.Task[None] | None,
    manager: WorkerManager | None,
    radar_adapter: RadarAdapter | None,
    gateway: NetworkGateway | None,
    db_client: DatabaseClient | None,
    timeout: float,
) -> None:
    log = logger.bind(name="bootstrap")
    deadline = time.monotonic() + timeout
    if watchdog_task is not None:
        watchdog_task.remove_done_callback(on_watchdog_task_done)
    try:
        if watchdog is not None:
            log.info("Shutdown: останавливаем Watchdog")
            watchdog.stop()
        if watchdog_task is not None and not watchdog_task.done():
            remaining = max(0.0, deadline - time.monotonic())
            _done, pending = await asyncio.wait(
                {watchdog_task}, timeout=remaining
            )
            if pending:
                watchdog_task.cancel()
                await asyncio.gather(watchdog_task, return_exceptions=True)
        if (
            watchdog_task is not None
            and watchdog_task.done()
            and not watchdog_task.cancelled()
        ):
            exc = watchdog_task.exception()
            if exc is not None:
                log.opt(exception=exc).warning(
                    "Watchdog завершился во время shutdown, продолжаем остановку"
                )
        if manager is not None:
            log.info("Shutdown: запрещаем новые тики и ждём воркеров")
            remaining = max(0.0, deadline - time.monotonic())
            await manager.stop_all(timeout=remaining)
    finally:
        try:
            if radar_adapter is not None:
                log.info("Shutdown: закрываем WCF executor")
                await radar_adapter.close()
        finally:
            try:
                if gateway is not None:
                    log.info("Shutdown: закрываем HTTP client")
                    await gateway.close()
            finally:
                if db_client is not None:
                    log.info("Shutdown: закрываем SQLite")
                    await db_client.close()
                log.info("Все компоненты успешно очищены. Программа завершена.")


def _exit_if_source_hung(radar_adapter: RadarAdapter | None) -> None:
    if radar_adapter is None:
        return
    try:
        radar_adapter.raise_if_hung()
    except RadarSourceHung:
        logger.bind(name="bootstrap").warning(
            "Источник не вернул управление после shutdown, принудительный выход"
        )
        os._exit(0)


def on_watchdog_task_done(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    logger.bind(name="bootstrap").opt(exception=exc).error(
        "Watchdog завершился неожиданно, завершаем процесс"
    )
    os._exit(1)


async def bootstrap_application(app_settings: Settings = settings) -> None:
    log = logger.bind(name="bootstrap")
    log.info("Запуск приложения в боевом режиме")

    stop = asyncio.Event()
    _install_stop_signals(stop)

    db_client: DatabaseClient | None = None
    radar_adapter: RadarAdapter | None = None
    gateway: NetworkGateway | None = None
    manager: WorkerManager | None = None
    watchdog: Watchdog | None = None
    watchdog_task: asyncio.Task[None] | None = None
    try:
        db_client = SqliteClient(url=app_settings.db.url)
        radar_adapter = create_radar_adapter(app_settings)
        gateway = create_network_gateway(app_settings)
        await db_client.init_database()
        if stop.is_set():
            log.info("Получен сигнал остановки до запуска воркеров")
            return
        manager = _build_worker_manager(app_settings, db_client, gateway, radar_adapter)
        manager.start_all()
        await asyncio.sleep(0)
        watchdog = _build_watchdog(manager, app_settings)
        watchdog_task = asyncio.create_task(watchdog.start(), name="watchdog")
        watchdog_task.add_done_callback(on_watchdog_task_done)
        await stop.wait()
        log.info("Получен сигнал остановки, запускаем shutdown...")
    finally:
        try:
            await shutdown_runtime(
                watchdog,
                watchdog_task,
                manager,
                radar_adapter,
                gateway,
                db_client,
                timeout=app_settings.shutdown_timeout,
            )
        finally:
            _remove_stop_signals()
            _exit_if_source_hung(radar_adapter)
