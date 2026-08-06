import asyncio

from loguru import logger

from src.application.ingestion.worker import IngestionWorker


class WorkerManager:
    def __init__(self, workers: list[IngestionWorker]) -> None:
        self._workers = workers

    async def run_all(self) -> None:
        logger.info(
            "Запуск {} воркер(ов): {}",
            len(self._workers),
            [w.name for w in self._workers],
        )
        tasks = [asyncio.create_task(w.start(), name=w.name) for w in self._workers]
        await asyncio.gather(*tasks)

    def stop_all(self) -> None:
        for w in self._workers:
            w.stop()

    def health_report(self) -> dict[str, bool]:
        report = {w.name: w.is_alive() for w in self._workers}
        logger.debug("Health report: {}", report)
        return report
