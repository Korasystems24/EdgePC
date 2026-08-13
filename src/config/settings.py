from __future__ import annotations

from datetime import timedelta

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.domain.enum import NetworkGatewayType, RadarSourceType


class DatabaseSettings(BaseModel):
    """Настройки подключения к БД."""

    name: str = "app.db"
    dir: str = "data"

    @property
    def url(self) -> str:
        return f"sqlite+aiosqlite:///{self.dir}/{self.name}"


class WcfSettings(BaseModel):
    """Настройки WCF-источника радара."""

    wsdl_url: str | None = None
    ingestion_interval: float = 5


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


class Settings(BaseSettings):
    debug: bool = True
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.MOCK
    network_gateway: NetworkGatewayType = NetworkGatewayType.MOCK
    log_file_path: str = "prod.log"

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
