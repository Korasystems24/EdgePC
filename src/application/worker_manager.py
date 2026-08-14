import asyncio

from loguru import logger

from src.application.ports.worker import Worker


class WorkerManager:
    def __init__(self, workers: list[Worker]) -> None:
        self._workers = workers
        self._tasks: list[asyncio.Task[None]] = []

    def start_all(self) -> None:
        logger.info(
            "Запуск {} воркер(ов): {}",
            len(self._workers),
            [w.name for w in self._workers],
        )
        self._tasks = []
        for worker in self._workers:
            task = asyncio.create_task(worker.start(), name=worker.name)
            task.add_done_callback(self._on_task_done)
            self._tasks.append(task)

    async def stop_all(self, timeout: float) -> None:
        for worker in self._workers:
            worker.stop()
        if not self._tasks:
            return

        logger.info("Ожидание остановки воркеров, timeout={}с", timeout)
        _done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for task in pending:
            logger.warning(
                "Воркер {} не завершился за {}с, отменяем",
                task.get_name(),
                timeout,
            )
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def health_report(self) -> dict[str, bool]:
        report = {w.name: w.is_alive() for w in self._workers}
        logger.info("Health report: {}", report)
        return report

    @staticmethod
    def _on_task_done(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.opt(exception=exc).error(
                "Воркер {} завершился с ошибкой",
                task.get_name(),
            )
