"""Как процесс живёт и как он умирает: сигналы, остановка, политика выхода."""

import asyncio
import contextlib
import faulthandler
import os
import signal
import time
from collections.abc import Awaitable, Callable
from typing import NoReturn

from loguru import logger

from src.application.watchdog.watchdog import Watchdog
from src.application.worker_manager import WorkerManager
from src.domain.exceptions import ProcessMustExit, RadarSourceHung
from src.domain.ports.radar_adapter import RadarAdapter


def install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)


def remove_stop_signals() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.remove_signal_handler(sig)


def exit_after_flush(code: int) -> NoReturn:
    """Выход из event loop, когда логгер ещё работает.

    Логи уходят через `enqueue=True`, поэтому без `complete()` запись о причине
    смерти не успевает добраться ни до stderr, ни до файла.
    """
    logger.complete()
    os._exit(code)


def exit_immediately(code: int, message: str) -> NoReturn:
    """Выход, когда на логгер рассчитывать нельзя.

    Так умирает страж event loop: очередь loguru обслуживается тем самым
    заблокированным потоком, поэтому причина пишется прямо в fd 2, а трейсбеки
    всех потоков — единственный шанс увидеть, кто держал loop.
    """
    try:
        faulthandler.dump_traceback(all_threads=True)
    finally:
        with contextlib.suppress(OSError):
            os.write(2, f"{message}\n".encode())
        os._exit(code)


async def closing(what: str, close: Callable[[], Awaitable[None]]) -> None:
    """Закрыть внешний ресурс, объявив об этом в логе."""
    logger.bind(name="bootstrap").info("Shutdown: закрываем {}", what)
    await close()


async def stop_workers(
    watchdog: Watchdog,
    watchdog_task: asyncio.Task[None],
    manager: WorkerManager,
    timeout: float,
) -> None:
    """Сначала перестают появляться новые тики, потом закрываются ресурсы.

    Общий дедлайн на весь шаг: сколько бы ни ждал Watchdog, воркерам достаётся
    остаток, иначе SIGTERM превратился бы в бесконечное ожидание.
    """
    log = logger.bind(name="bootstrap")
    deadline = time.monotonic() + timeout

    # Штатное завершение Watchdog больше не повод убивать процесс.
    watchdog_task.remove_done_callback(on_watchdog_task_done)

    log.info("Shutdown: останавливаем Watchdog")
    watchdog.stop()

    if not watchdog_task.done():
        remaining = max(0.0, deadline - time.monotonic())
        _done, pending = await asyncio.wait({watchdog_task}, timeout=remaining)
        if pending:
            watchdog_task.cancel()
            await asyncio.gather(watchdog_task, return_exceptions=True)

    if watchdog_task.done() and not watchdog_task.cancelled():
        exc = watchdog_task.exception()
        if exc is not None:
            log.opt(exception=exc).warning(
                "Watchdog завершился во время shutdown, продолжаем остановку"
            )

    log.info("Shutdown: запрещаем новые тики и ждём воркеров")
    await manager.stop_all(timeout=max(0.0, deadline - time.monotonic()))


def exit_if_source_hung(radar_adapter: RadarAdapter | None) -> None:
    """Поток чтения WCF не daemon и не отменяем: если он застрял в блокирующем
    вызове, процесс без принудительного выхода никогда не завершится."""
    if radar_adapter is None:
        return
    try:
        radar_adapter.raise_if_hung()
    except RadarSourceHung:
        logger.bind(name="bootstrap").warning(
            "Источник не вернул управление после shutdown, принудительный выход"
        )
        exit_after_flush(1)


def on_watchdog_task_done(task: asyncio.Task[None]) -> None:
    """Без Watchdog процесс остаётся без надзора, поэтому его смерть в любом
    виде означает выход: снаружи нас поднимет restart-политика."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return

    log = logger.bind(name="bootstrap")
    if isinstance(exc, ProcessMustExit):
        # Задуманная эскалация: воркер не поднимается рестартами, лечит только
        # новый процесс. Это не баг, поэтому без трейсбека.
        log.warning("Watchdog требует перезапуск процесса: {}", exc)
    else:
        log.opt(exception=exc).error(
            "Watchdog завершился неожиданно, завершаем процесс"
        )
    exit_after_flush(1)
