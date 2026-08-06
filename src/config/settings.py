from __future__ import annotations

from pydantic import (
    BaseModel,
    Field,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.domain.enum import RadarSourceType


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


class Settings(BaseSettings):
    debug: bool = True
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.WCF
    log_file_path: str = "prod.log"

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    wcf: WcfSettings = Field(default_factory=WcfSettings)
    mock: MockSettings = Field(default_factory=MockSettings)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )


settings = Settings()
