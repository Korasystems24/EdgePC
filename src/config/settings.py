from __future__ import annotations

from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
)
from pydantic.functional_validators import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.config import timing
from src.config.enums import NetworkGatewayType, RadarSourceType
from src.domain.constants import BYTES_IN_MIB


class DatabaseSettings(BaseModel):
    """Настройки подключения к БД."""

    name: str = "app.db"
    dir: str = "data"

    @property
    def url(self) -> str:
        return f"sqlite+aiosqlite:///{self.dir}/{self.name}"


class LogSettings(BaseModel):
    """Каталог файловых логов. Текущий файл и zip-архивы ротации живут здесь."""

    dir: str = "logs"
    file: str = "prod.log"

    @property
    def path(self) -> Path:
        return Path(self.dir) / self.file


class WcfSettings(BaseModel):
    """Настройки WCF-клиента радара."""

    wsdl_url: str | None = None
    timeout: int = Field(default=3, gt=0)

    @model_validator(mode="after")
    def wsdl_url_not_blank(self) -> Self:
        if self.wsdl_url is not None and not self.wsdl_url.strip():
            raise ValueError("wcf.wsdl_url не должен быть пустой строкой")
        return self


class IngestionSettings(BaseModel):
    """Настройки цикла сбора: интервал, дедлайн тика, пауза при полном диске."""

    interval: float = Field(default=5.0, gt=0)
    tick_deadline: float = Field(default=20.0, gt=0)
    min_free_mb: int = 200
    resume_free_mb: int = 250

    @property
    def min_free_bytes(self) -> int:
        return self.min_free_mb * BYTES_IN_MIB

    @property
    def resume_free_bytes(self) -> int:
        return self.resume_free_mb * BYTES_IN_MIB

    @model_validator(mode="after")
    def check_hysteresis(self) -> Self:
        timing.check_storage_hysteresis(
            min_free_mb=self.min_free_mb,
            resume_free_mb=self.resume_free_mb,
        )
        return self


class MockIOSettings(BaseModel):
    """Задержка и доля сбоев для одного mock-адаптера."""

    delay: float = Field(default=1.0, ge=0)
    fail_rate: float = Field(default=0.05, ge=0, le=1)


class HttpTransportSettings(BaseModel):
    """Всё, что знает только HTTP-гейтвей: адрес, токен, таймауты, ретраи."""

    server_url: str = ""
    auth_token: SecretStr = SecretStr("")
    connect_timeout: float = 5.0
    read_timeout: float = 5.0
    write_timeout: float = 5.0
    pool_timeout: float = 5.0
    retry_attempts: int = 3
    retry_wait_max: float = 1.0

    @property
    def http_timeout(self) -> float:
        """Худший случай одного запроса: самый долгий из четырёх таймаутов."""
        return max(
            self.connect_timeout,
            self.read_timeout,
            self.write_timeout,
            self.pool_timeout,
        )

    @property
    def retrying_send_budget(self) -> float:
        """Худший случай одной отправки с ретраями."""
        return self.retry_attempts * (self.http_timeout + self.retry_wait_max)


class DispatchSettings(BaseModel):
    """Настройки диспетчера для отправки данных в сервер."""

    interval: float = Field(default=10.0, gt=0)
    tick_deadline: float = Field(default=45.0, gt=0)
    batch_size: int = 200
    event_batch_size: int = 50
    retention_ttl: timedelta = timedelta(days=7)
    retention_interval: timedelta = timedelta(hours=1)
    retention_timeout: float = Field(default=5.0, gt=0)
    quarantine_after_attempts: int = 3
    mass_rejection_sample: int = 10

    http: HttpTransportSettings = Field(default_factory=HttpTransportSettings)

    @property
    def events_send_timeout(self) -> float:
        """Сколько тика можно отдать событиям, чтобы телеметрия успела уйти."""
        return (
            self.tick_deadline - self.http.http_timeout - self.http.retrying_send_budget
        )

    @property
    def heartbeat_budget(self) -> float:
        """Худший разрыв между heartbeat: тик (до дедлайна) либо ожидание
        интервала, что дольше, плюс ретенция после тика."""
        return max(self.tick_deadline, self.interval) + self.retention_timeout

    @model_validator(mode="after")
    def check_budgets_fit_tick(self) -> Self:
        timing.check_dispatch_budgets_fit_tick(
            tick_deadline=self.tick_deadline,
            http_timeout=self.http.http_timeout,
            retrying_send_budget=self.http.retrying_send_budget,
        )
        return self


class WatchdogSettings(BaseModel):
    """Настройки надзора за воркерами."""

    interval: float = Field(default=5.0, gt=0)
    inspect_timeout: float = Field(default=10.0, gt=0)
    restart_limit: int = Field(default=3, gt=0)
    restart_window_seconds: float = Field(default=600.0, gt=0)


class EventLoopGuardSettings(BaseModel):
    """Порог, после которого страж считает event loop мёртвым и убивает процесс.

    Ловить нужно вечную блокировку, а не всплеск GIL-контенции: единственный
    тяжёлый синхронный код в процессе — разбор WSDL внутри zeep, и он держит GIL
    в потоке WCF. Поэтому порог заметно больше секундных пауз.
    """

    threshold: float = Field(default=15.0, gt=0)
    check_interval: float = Field(default=1.0, gt=0)

    @model_validator(mode="after")
    def check_interval_shorter_than_threshold(self) -> Self:
        timing.check_guard_interval_shorter_than_threshold(
            check_interval=self.check_interval,
            threshold=self.threshold,
        )
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    debug: bool = True
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.WCF
    network_gateway: NetworkGatewayType = NetworkGatewayType.MOCK
    shutdown_timeout: float = Field(default=8.0, gt=0)
    restart_join_timeout: float = Field(default=5.0, gt=0)
    heartbeat_max_silence: float = Field(default=60.0, gt=0)

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    log: LogSettings = Field(default_factory=LogSettings)
    wcf: WcfSettings = Field(default_factory=WcfSettings)
    mock_radar: MockIOSettings = Field(default_factory=MockIOSettings)
    mock_gateway: MockIOSettings = Field(default_factory=MockIOSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    dispatch: DispatchSettings = Field(default_factory=DispatchSettings)
    watchdog: WatchdogSettings = Field(default_factory=WatchdogSettings)
    guard: EventLoopGuardSettings = Field(default_factory=EventLoopGuardSettings)

    @model_validator(mode="after")
    def check_timing_hierarchy(self) -> Self:
        """Проверки живут в src/config/timing.py — там же описана вся иерархия."""
        timing.check_wcf_first_tick_fits_deadline(
            ingestion_tick_deadline=self.ingestion.tick_deadline,
            wcf_timeout=self.wcf.timeout,
        )
        timing.check_heartbeat_covers_ticks(
            heartbeat_max_silence=self.heartbeat_max_silence,
            ingestion_tick_deadline=self.ingestion.tick_deadline,
            ingestion_interval=self.ingestion.interval,
            dispatch_heartbeat_budget=self.dispatch.heartbeat_budget,
        )
        timing.check_restart_join_fits_shutdown(
            restart_join_timeout=self.restart_join_timeout,
            shutdown_timeout=self.shutdown_timeout,
        )
        timing.check_inspect_shorter_than_heartbeat(
            inspect_timeout=self.watchdog.inspect_timeout,
            heartbeat_max_silence=self.heartbeat_max_silence,
        )
        timing.check_restart_join_fits_inspect(
            restart_join_timeout=self.restart_join_timeout,
            inspect_timeout=self.watchdog.inspect_timeout,
        )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Единственный экземпляр настроек. Компоненты получают его параметром, а не
    импортом, поэтому подменить настройки можно в точке сборки."""
    return Settings()
