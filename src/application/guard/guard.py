import asyncio
import faulthandler
import os
import threading
import time


class EventLoopGuard:
    """Отдельный поток
    Единственная задача: убедиться, что event loop вообще жив.
    Не читает БД, не лезет в сеть, не рестартует воркеров."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        threshold_s: float = 5.0,
        check_interval_s: float = 1.0,
    ):
        self._loop = loop
        self._threshold_s = threshold_s
        self._check_interval_s = check_interval_s
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

    def _die(self, message: str) -> None:
        try:
            faulthandler.dump_traceback(all_threads=True)
        finally:
            try:
                os.write(2, f"{message}\n".encode())
            except OSError:
                pass
            os._exit(1)

    def _run(self) -> None:
        while not self._stop.wait(self._check_interval_s):
            try:
                self._loop.call_soon_threadsafe(self._mark_alive)
            except RuntimeError:
                return

            silence = time.monotonic() - self._last_alive_at
            if silence > self._threshold_s:
                self._die(f"event loop не отвечает {silence:.1f}с, убиваем процесс")
