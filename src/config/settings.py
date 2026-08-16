from __future__ import annotations

from datetime import timedelta
from typing import Self

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
)
from pydantic.functional_validators import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.domain.constants import (
    BYTES_IN_MIB,
    SQLITE_SAVE_BUDGET_S,
    WCF_FIRST_TICK_HTTP_CALLS,
)
from src.domain.enum import NetworkGatewayType, RadarSourceType


class DatabaseSettings(BaseModel):
    """Настройки подключения к БД."""

    name: str = "app.db"
    dir: str = "data"

    @property
    def url(self) -> str:
        return f"sqlite+aiosqlite:///{self.dir}/{self.name}"


class WcfSettings(BaseModel):
    """Настройки WCF-клиента радара."""

    wsdl_url: str | None = None
    timeout: int = Field(default=3, gt=0)

    @model_validator(mode="after")
    def wsdl_url_not_blank(self) -> Self:
        if self.wsdl_url is not None and not self.wsdl_url.strip():
            raise ValueError("wcf.wsdl_url must not be blank")
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


class MockIOSettings(BaseModel):
    """Задержка и доля сбоев для одного mock-адаптера."""

    delay: float = Field(default=1.0, ge=0)
    fail_rate: float = Field(default=0.05, ge=0, le=1)


class DispatchSettings(BaseModel):
    """Настройки диспетчера для отправки данных в сервер."""

    interval: float = 10.0
    batch_size: int = 200
    retention_ttl: timedelta = timedelta(days=7)
    waiting_period_ttl: timedelta = timedelta(hours=1)
    tick_deadline: float = Field(default=45.0, gt=0)
    quarantine_after_attempts: int = 3
    mass_rejection_sample: int = 10
    event_batch_size: int = 50
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
        return max(
            self.connect_timeout,
            self.read_timeout,
            self.write_timeout,
            self.pool_timeout,
        )

    @property
    def retrying_send_budget(self) -> float:
        return self.retry_attempts * (self.http_timeout + self.retry_wait_max)

    @property
    def events_send_timeout(self) -> float:
        return self.tick_deadline - self.http_timeout - self.retrying_send_budget

    @model_validator(mode="after")
    def http_timeout_shorter_than_deadline(self) -> Self:
        http_timeout = self.http_timeout
        if http_timeout >= self.tick_deadline:
            raise ValueError(
                f"dispatch HTTP timeouts (max {http_timeout}) must be < "
                f"dispatch.tick_deadline ({self.tick_deadline})"
            )
        retrying = self.retrying_send_budget
        if retrying >= self.tick_deadline:
            raise ValueError(
                f"dispatch retry budget ({retrying}) must be < "
                f"dispatch.tick_deadline ({self.tick_deadline})"
            )
        sequential = http_timeout + 2 * retrying
        if sequential > self.tick_deadline:
            raise ValueError(
                f"dispatch health+events+telemetry budget ({sequential}) must be <= "
                f"dispatch.tick_deadline ({self.tick_deadline})"
            )
        return self


class WatchdogSettings(BaseModel):
    """Настройки надзора за воркерами."""

    interval: float = Field(default=5.0, gt=0)
    inspect_timeout: float = Field(default=4.0, gt=0)
    restart_limit: int = Field(default=3, gt=0)
    restart_window_seconds: float = Field(default=600.0, gt=0)


class Settings(BaseSettings):
    debug: bool = True
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.WCF
    network_gateway: NetworkGatewayType = NetworkGatewayType.MOCK
    log_file_path: str = "prod.log"
    shutdown_timeout: float = Field(default=8.0, gt=0)
    restart_join_timeout: float = Field(default=5.0, gt=0)
    heartbeat_max_silence: float = Field(default=60.0, gt=0)

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    wcf: WcfSettings = Field(default_factory=WcfSettings)
    mock_radar: MockIOSettings = Field(default_factory=MockIOSettings)
    mock_gateway: MockIOSettings = Field(default_factory=MockIOSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    dispatch: DispatchSettings = Field(default_factory=DispatchSettings)
    watchdog: WatchdogSettings = Field(default_factory=WatchdogSettings)

    @model_validator(mode="after")
    def wcf_first_tick_fits_ingestion_deadline(self) -> Self:
        budget = WCF_FIRST_TICK_HTTP_CALLS * self.wcf.timeout + SQLITE_SAVE_BUDGET_S
        if self.ingestion.tick_deadline <= budget:
            raise ValueError(
                f"ingestion.tick_deadline ({self.ingestion.tick_deadline}) must be > "
                f"WCF first-tick budget ({WCF_FIRST_TICK_HTTP_CALLS} "
                f"* wcf.timeout {self.wcf.timeout} + {SQLITE_SAVE_BUDGET_S}s save "
                f"= {budget})"
            )
        return self

    @model_validator(mode="after")
    def heartbeat_longer_than_ticks(self) -> Self:
        longest_tick = max(
            self.ingestion.tick_deadline, self.dispatch.tick_deadline
        )
        if self.heartbeat_max_silence <= longest_tick:
            raise ValueError(
                f"heartbeat_max_silence ({self.heartbeat_max_silence}) must be > "
                f"longest tick_deadline ({longest_tick})"
            )
        return self

    @model_validator(mode="after")
    def restart_join_fits_shutdown(self) -> Self:
        if self.restart_join_timeout >= self.shutdown_timeout:
            raise ValueError(
                f"restart_join_timeout ({self.restart_join_timeout}) must be < "
                f"shutdown_timeout ({self.shutdown_timeout})"
            )
        return self

    @model_validator(mode="after")
    def inspect_timeout_shorter_than_heartbeat(self) -> Self:
        if self.watchdog.inspect_timeout >= self.heartbeat_max_silence:
            raise ValueError(
                f"watchdog.inspect_timeout ({self.watchdog.inspect_timeout}) must be < "
                f"heartbeat_max_silence ({self.heartbeat_max_silence})"
            )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )


settings = Settings()
