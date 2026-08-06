import asyncio
import time

from loguru import logger

from src.application.ingestion.service import IngestionService
from src.domain.exceptions import (
    RadarDataInvalid,
    RadarSourceMisconfigured,
    RadarSourceUnavailable,
)


class IngestionWorker:
    def __init__(
        self,
        name: str,
        service: IngestionService,
        interval: float = 5.0,
        max_consecutive_failures: int = 10,
    ) -> None:
        self.name = name
        self._service = service
        self._interval = interval
        self._max_consecutive_failures = max_consecutive_failures
        self._running = False
        self._last_heartbeat: float = 0.0
        self._consecutive_failures = 0
        self._fatal_error: Exception | None = None

    async def start(self) -> None:
        self._running = True
        logger.bind(worker=self.name).info("Воркер запущен")

        while self._running:
            tick_started_at = time.monotonic()
            await self._safe_tick()
            self._last_heartbeat = time.monotonic()

            if self._consecutive_failures >= self._max_consecutive_failures:
                logger.bind(worker=self.name).critical(
                    "Превышен лимит подряд идущих ошибок ({}), остановка воркера",
                    self._consecutive_failures,
                )
                self._fatal_error = RuntimeError(
                    f"{self.name}: too many consecutive failures"
                )
                self._running = False
                break

            elapsed = time.monotonic() - tick_started_at
            await asyncio.sleep(max(0.0, self._interval - elapsed))

    async def _safe_tick(self) -> None:
        try:
            await self._service.collect_once()
            self._consecutive_failures = 0  # успех — сбрасываем счётчик
        except RadarSourceUnavailable as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).warning(
                "Источник недоступен ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
        except RadarSourceMisconfigured as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).error(
                "Требуется вмешательство в конфигурацию: {}", e
            )
        except RadarDataInvalid as e:
            # это не критично по своей природе — данные плохие, но источник жив, не считаем как failure
            logger.bind(worker=self.name).warning(
                "Некорректные данные, пропуск тика: {}", e
            )
        except Exception:  # noqa: BLE001
            self._consecutive_failures += 1
            logger.bind(worker=self.name).exception("Непредвиденная ошибка")

    def is_alive(self, max_silence: float = 30.0) -> bool:
        if not self._running:
            return False
        return (time.monotonic() - self._last_heartbeat) < max_silence

    def stop(self) -> None:
        self._running = False
        logger.bind(worker=self.name).info("Воркер остановлен")
