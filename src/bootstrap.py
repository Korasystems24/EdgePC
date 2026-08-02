from typing import Any

from loguru import logger

from src.application.ingestion.factory import RadarAdapterFactory
from src.application.ingestion.service import IngestionService
from src.config.settings import settings


async def bootstrap_application() -> Any:
    logger.info("Система запущена и готова к работе!")
    radar_adapter = RadarAdapterFactory.create_adapter(settings)
    ingestion_service = IngestionService(radar_adapter)
    packet = await ingestion_service.handler_service()
    print(packet)
