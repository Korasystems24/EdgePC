from pydantic_settings import BaseSettings

from src.domain.enum import RadarSourceType


class Settings(BaseSettings):
    # Общие
    debug: bool = False
    bunker_id: str = "bunker-001"
    radar_source: RadarSourceType = RadarSourceType.WCF

    # WCF
    wcf_wsdl_url: str | None = None

    # Mock-параметры
    mock_delay: int = 1
    mock_fail: float = 0.05

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
