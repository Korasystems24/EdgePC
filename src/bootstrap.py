import asyncio
from contextlib import AsyncExitStack

from loguru import logger

from src.application.health_store import HealthStore
from src.composition import (
    build_event_loop_guard,
    build_watchdog,
    build_worker_manager,
)
from src.config.settings import Settings
from src.domain.ports.radar_adapter import RadarAdapter
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
from src.runtime import (
    closing,
    exit_if_source_hung,
    install_stop_signals,
    on_watchdog_task_done,
    remove_stop_signals,
    stop_workers,
)


async def bootstrap_application(app_settings: Settings) -> None:
    """Собрать приложение, дождаться сигнала остановки и разобрать его обратно.

    Порядок разборки задаётся порядком регистрации в `AsyncExitStack`: он
    раскручивается в обратную сторону, поэтому сначала останавливаются воркеры,
    и только потом закрывается то, чем они пользовались.
    """
    log = logger.bind(name="bootstrap")
    log.info("Запуск приложения в боевом режиме")

    stop = asyncio.Event()
    install_stop_signals(stop)

    radar_adapter: RadarAdapter | None = None
    guard = build_event_loop_guard(app_settings)
    try:
        guard.start()
        log.info(
            "Запущен event loop guard: порог {}с, проверка каждые {}с",
            app_settings.guard.threshold,
            app_settings.guard.check_interval,
        )

        async with AsyncExitStack() as resources:
            resources.callback(
                log.info, "Все компоненты успешно очищены. Программа завершена."
            )

            db_client = SqliteClient(url=app_settings.db.url)
            resources.push_async_callback(closing, "SQLite", db_client.close)

            gateway = create_network_gateway(
                app_settings.network_gateway,
                dispatch=app_settings.dispatch,
                mock=app_settings.mock_gateway,
                bunker_id=app_settings.bunker_id,
            )
            resources.push_async_callback(closing, "HTTP client", gateway.close)

            radar_adapter = create_radar_adapter(
                app_settings.radar_source,
                wcf=app_settings.wcf,
                mock=app_settings.mock_radar,
            )
            resources.push_async_callback(
                closing, "WCF executor", radar_adapter.close
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

            manager = build_worker_manager(
                app_settings,
                gateway=gateway,
                radar_adapter=radar_adapter,
                telemetry_repository=telemetry_repository,
                event_repository=event_repository,
                storage_monitor=storage_monitor,
                health_store=health_store,
            )
            manager.start_all()
            await asyncio.sleep(0)

            watchdog = build_watchdog(
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
            resources.push_async_callback(
                stop_workers,
                watchdog,
                watchdog_task,
                manager,
                app_settings.shutdown_timeout,
            )

            await stop.wait()
            log.info("Получен сигнал остановки, запускаем shutdown...")
    finally:
        guard.stop()
        remove_stop_signals()
        exit_if_source_hung(radar_adapter)
