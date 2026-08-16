from collections.abc import Callable

from src.application.ports.worker import Worker

type WorkerFactory = Callable[[], Worker]
