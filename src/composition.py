"""Граф зависимостей приложения. Новые сервисы и воркеры добавляются здесь."""

import asyncio

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.guard import EventLoopGuard
from src.application.health_store import HealthStore
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.watchdog.watchdog import Watchdog
from src.application.worker_manager import WorkerManager
from src.config.settings import Settings
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.host_metrics import HostMetrics
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import TelemetryRepository
from src.runtime import exit_immediately


def build_worker_manager(
    app_settings: Settings,
    *,
    gateway: NetworkGateway,
    radar_adapter: RadarAdapter,
    telemetry_repository: TelemetryRepository,
    event_repository: EventRepository,
    storage_monitor: StorageMonitor,
    health_store: HealthStore,
) -> WorkerManager:
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
        isolation_budget=app_settings.dispatch.isolation_budget,
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


def build_watchdog(
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


def build_event_loop_guard(app_settings: Settings) -> EventLoopGuard:
    return EventLoopGuard(
        asyncio.get_running_loop(),
        threshold_s=app_settings.guard.threshold,
        check_interval_s=app_settings.guard.check_interval,
        on_dead=lambda message: exit_immediately(1, message),
    )
