import asyncio
import time

from loguru import logger

from src.application.ports.worker import TaskState, Worker, WorkerFactory
from src.domain.exceptions import WorkerAlreadyRunning, WorkerRestartStuck
from src.domain.value_objects.worker_health import WorkerHealth


class WorkerManager:
    def __init__(
        self,
        factories: dict[str, WorkerFactory],
        *,
        restart_join_timeout: float,
    ) -> None:
        self._factories = factories
        self._workers: dict[str, Worker] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._restart_join_timeout = restart_join_timeout
        self._log = logger.bind(name="worker_manager")

    def start_all(self) -> None:
        self._log.info(
            "Запуск {} воркер(ов): {}",
            len(self._factories),
            list(self._factories),
        )
        for name in self._factories:
            self._spawn(name)

    def task_state(self, name: str) -> TaskState:
        task = self._tasks[name]
        if not task.done():
            return TaskState(done=False, exception=None)
        if task.cancelled():
            return TaskState(done=True, exception=None)
        return TaskState(done=True, exception=task.exception())

    def worker(self, name: str) -> Worker:
        return self._workers[name]

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._factories)

    async def restart(self, name: str) -> None:
        worker = self._workers[name]
        task = self._tasks[name]
        if not task.done():
            worker.stop()
            task.cancel()
            _done, pending = await asyncio.wait(
                {task}, timeout=self._restart_join_timeout
            )
            if pending:
                self._log.error(
                    "Воркер {} не завершился за {}с, вторую задачу не создаём",
                    name,
                    self._restart_join_timeout,
                )
                raise WorkerRestartStuck(
                    f"воркер {name} не завершился за {self._restart_join_timeout}с"
                )
        self._spawn(name)

    def _spawn(self, name: str) -> None:
        current = self._tasks.get(name)
        if current is not None and not current.done():
            raise WorkerAlreadyRunning(
                f"воркер {name} ещё работает, вторую задачу не создаём"
            )
        worker = self._factories[name]()
        self._workers[name] = worker
        task = asyncio.create_task(worker.start(), name=name)
        task.add_done_callback(self._on_task_done)
        self._tasks[name] = task

    async def stop_all(self, timeout: float) -> None:
        for worker in self._workers.values():
            worker.stop()
        if not self._tasks:
            return

        self._log.info("Ожидание остановки воркеров")
        deadline = time.monotonic() + timeout
        _done, pending = await asyncio.wait(self._tasks.values(), timeout=timeout)
        if not pending:
            return
        for task in pending:
            self._log.warning(
                "Воркер {} не завершился вовремя, отменяем",
                task.get_name(),
            )
            task.cancel()
        remaining = max(0.0, deadline - time.monotonic())
        _done, pending = await asyncio.wait(pending, timeout=remaining)
        for task in pending:
            self._log.error(
                "Воркер {} не завершился после отмены",
                task.get_name(),
            )

    def health_report(self) -> dict[str, WorkerHealth]:
        return {
            name: self._worker_health(worker)
            for name, worker in self._workers.items()
        }

    @staticmethod
    def _worker_health(worker: Worker) -> WorkerHealth:
        return WorkerHealth(
            alive=worker.is_alive(),
            last_error=worker.last_error,
            last_error_at=worker.last_error_at,
            fatal=worker.fatal_error,
        )

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.opt(exception=exc).error(
                "Воркер {} завершился с ошибкой",
                task.get_name(),
            )
