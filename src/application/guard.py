import asyncio
import threading
import time
from collections.abc import Callable


class EventLoopGuard:
    """Отдельный поток
    Единственная задача: убедиться, что event loop вообще жив.
    Не читает БД, не лезет в сеть, не рестартует воркеров.

    Что делать с мёртвым loop, решает не страж: способ умереть передаётся
    в `on_dead` при сборке приложения.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        threshold_s: float,
        check_interval_s: float,
        on_dead: Callable[[str], None],
    ):
        self._loop = loop
        self._threshold_s = threshold_s
        self._check_interval_s = check_interval_s
        self._on_dead = on_dead
        self._last_alive_at = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="event-loop-guard",
            daemon=True,
        )

    def start(self) -> None:
        self._last_alive_at = time.monotonic()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _mark_alive(self) -> None:
        self._last_alive_at = time.monotonic()

    def _run(self) -> None:
        while not self._stop.wait(self._check_interval_s):
            try:
                self._loop.call_soon_threadsafe(self._mark_alive)
            except RuntimeError:
                return

            silence = time.monotonic() - self._last_alive_at
            if silence > self._threshold_s:
                self._on_dead(
                    f"event loop не отвечает {silence:.1f}с, убиваем процесс"
                )
                # Штатный обработчик не возвращается (убивает процесс). Если
                # вернулся — приговор уже вынесен, повторять его каждую секунду
                # незачем.
                return
