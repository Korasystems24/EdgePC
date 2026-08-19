import asyncio
import os
import signal
import time

from loguru import logger

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.guard.guard import EventLoopGuard
from src.application.health_store import HealthStore
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.config.settings import Settings, settings
from src.domain.exceptions import RadarSourceHung
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.host_metrics import HostMetrics
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import TelemetryRepository
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.repositories.event_repository import (
    SqlAlchemyEventRepository,
)
from src.infrastructure.database.repositories.telemetry_repository import (
    SqlAlchemyTelemetryRepository,
)
from src.infrastructure.database.sqlite_client import SqliteClient
from src.infrastructure.metrics.psutil_host_metrics import PsutilHostMetrics
from src.infrastructure.network.factory import create_network_gateway
from src.infrastructure.radar.factory import create_radar_adapter
from src.infrastructure.storage.filesystem_monitor import FilesystemStorageMonitor


def _build_worker_manager(
    app_settings: Settings,
    gateway: NetworkGateway,
    radar_adapter: RadarAdapter,
    telemetry_repository: TelemetryRepository,
    event_repository: EventRepository,
    storage_monitor: StorageMonitor,
    health_store: HealthStore,
) -> WorkerManager:
    """Сборка графа зависимостей. Новые сервисы и воркеры добавляются здесь."""
    ingestion_service = IngestionService(
        radar_adapter=radar_adapter,
        repository=telemetry_repository,
    )

    def ingestion_factory() -> IngestionWorker:
        return IngestionWorker(
            name="ingestion",
            service=ingestion_service,
            settings=app_settings.ingestion,
            storage_monitor=storage_monitor,
            event_repository=event_repository,
            heartbeat_max_silence=app_settings.heartbeat_max_silence,
        )

    dispatch_service = DispatchService(
        repository=telemetry_repository,
        events=event_repository,
        gateway=gateway,
        health=health_store,
        batch_size=app_settings.dispatch.batch_size,
        event_batch_size=app_settings.dispatch.event_batch_size,
        retention_ttl=app_settings.dispatch.retention_ttl,
        mass_rejection_sample=app_settings.dispatch.mass_rejection_sample,
        quarantine_after_attempts=app_settings.dispatch.quarantine_after_attempts,
        events_timeout=app_settings.dispatch.events_send_timeout,
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


def _build_watchdog(
    manager: WorkerManager,
    app_settings: Settings,
    *,
    events: EventRepository,
    telemetry: TelemetryRepository,
    storage: StorageMonitor,
    host_metrics: HostMetrics,
    health: HealthStore,
) -> Watchdog:
    return Watchdog(
        manager,
        events=events,
        telemetry=telemetry,
        storage=storage,
        host_metrics=host_metrics,
        health=health,
        settings=app_settings.watchdog,
    )


def _install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)


def _remove_stop_signals() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.remove_signal_handler(sig)


def _build_event_loop_guard():
    loop = asyncio.get_running_loop()
    guard = EventLoopGuard(loop, threshold_s=3.0, check_interval_s=0.5)
    return guard


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
            _done, pending = await asyncio.wait({watchdog_task}, timeout=remaining)
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
        os._exit(1)


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
    guard: EventLoopGuard | None = None
    try:
        guard = _build_event_loop_guard()
        guard.start()
        log.info("Запущен event loop guard")

        db_client = SqliteClient(url=app_settings.db.url)
        radar_adapter = create_radar_adapter(
            app_settings.radar_source,
            wcf=app_settings.wcf,
            mock=app_settings.mock_radar,
        )
        gateway = create_network_gateway(
            app_settings.network_gateway,
            dispatch=app_settings.dispatch,
            mock=app_settings.mock_gateway,
            bunker_id=app_settings.bunker_id,
        )
        await db_client.init_database()
        if stop.is_set():
            log.info("Получен сигнал остановки до запуска воркеров")
            return
        telemetry_repository = SqlAlchemyTelemetryRepository(db_client)
        event_repository = SqlAlchemyEventRepository(db_client)
        storage_monitor = FilesystemStorageMonitor(path=app_settings.db.dir)
        health_store = HealthStore()
        host_metrics = PsutilHostMetrics(app_settings.db.dir)

        manager = _build_worker_manager(
            app_settings,
            gateway,
            radar_adapter,
            telemetry_repository,
            event_repository,
            storage_monitor,
            health_store,
        )
        manager.start_all()
        await asyncio.sleep(0)
        watchdog = _build_watchdog(
            manager,
            app_settings,
            events=event_repository,
            telemetry=telemetry_repository,
            storage=storage_monitor,
            host_metrics=host_metrics,
            health=health_store,
        )
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
            if guard is not None:
                guard.stop()
            _remove_stop_signals()
            _exit_if_source_hung(radar_adapter)
