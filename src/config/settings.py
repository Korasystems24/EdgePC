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

from src.domain.enum import NetworkGatewayType, RadarSourceType

_BYTES_IN_MIB = 1024 * 1024


class DatabaseSettings(BaseModel):
    """Настройки подключения к БД."""

    name: str = "app.db"
    dir: str = "data"
    min_free_mb: int = 200
    resume_free_mb: int = 250

    @property
    def url(self) -> str:
        return f"sqlite+aiosqlite:///{self.dir}/{self.name}"

    @property
    def min_free_bytes(self) -> int:
        return self.min_free_mb * _BYTES_IN_MIB

    @property
    def resume_free_bytes(self) -> int:
        return self.resume_free_mb * _BYTES_IN_MIB


class WcfSettings(BaseModel):
    """Настройки WCF-источника радара."""

    wsdl_url: str | None = None

    timeout: int = Field(default=3, gt=0)
    tick_deadline: float = Field(default=8.0, gt=0)
    ingestion_interval: float = Field(default=5.0, gt=0)

    @model_validator(mode="after")
    def timeout_shorter_than_deadline(self) -> Self:
        if self.timeout >= self.tick_deadline:
            raise ValueError(
                f"wcf.timeout ({self.timeout}) must be < "
                f"wcf.tick_deadline ({self.tick_deadline})"
            )
        return self


class MockSettings(BaseModel):
    """Настройки mock-источника (для отладки/тестов без реального радара)."""

    delay: int = 1
    fail_rate: float = 0.05


class DispatchSettings(BaseModel):
    """Настройки диспетчера для отправки данных в сервер."""

    interval: float = 10.0
    batch_size: int = 200
    retention_ttl: timedelta = timedelta(days=7)
    waiting_period_ttl: timedelta = timedelta(hours=1)
    tick_deadline: float = Field(default=45.0, gt=0)
    quarantine_after_attempts: int = 3
    mass_rejection_sample: int = 10
    server_url: str = ""
    auth_token: SecretStr = SecretStr("")
    connect_timeout: float = 5.0
    read_timeout: float = 5.0
    write_timeout: float = 5.0
    pool_timeout: float = 5.0
    retry_attempts: int = 3
    retry_wait_max: float = 4.0

    @model_validator(mode="after")
    def http_timeout_shorter_than_deadline(self) -> Self:
        http_timeout = max(
            self.connect_timeout,
            self.read_timeout,
            self.write_timeout,
            self.pool_timeout,
        )
        if http_timeout >= self.tick_deadline:
            raise ValueError(
                f"dispatch HTTP timeouts (max {http_timeout}) must be < "
                f"dispatch.tick_deadline ({self.tick_deadline})"
            )
        return self


class Settings(BaseSettings):
    debug: bool = True
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.MOCK
    network_gateway: NetworkGatewayType = NetworkGatewayType.MOCK
    log_file_path: str = "prod.log"
    shutdown_timeout: float = Field(default=8.0, gt=0)
    max_consecutive_failures: int = Field(default=10, gt=0)
    heartbeat_max_silence: float = Field(default=30.0, gt=0)

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    wcf: WcfSettings = Field(default_factory=WcfSettings)
    mock: MockSettings = Field(default_factory=MockSettings)
    dispatch: DispatchSettings = Field(default_factory=DispatchSettings)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )


settings = Settings()
