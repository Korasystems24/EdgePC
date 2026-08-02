import asyncio
from datetime import UTC, datetime

from src.application.ingestion.service import IngestionService


class IngestionWorker:
    def __init__(self, ingestion_service: IngestionService) -> None:
        self.ingestion_service = ingestion_service

    async def start(self):
        while True:
            try:
                await asyncio.sleep(4)
                await self.ingestion_service.handler_service()
            except Exception as e:  # TODO добавить кастомные исключения  # noqa: BLE001
                print(e)

    async def stop(self):
        datetime.now(tz=UTC).timestamp()
