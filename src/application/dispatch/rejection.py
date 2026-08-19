import asyncio

from loguru import logger

from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository


class RejectionIsolator:
    """Ищет виновника, когда сервер отверг пачку целиком (HTTP 422).

    Батч подтверждается целиком, поэтому по ответу неизвестно, какой из 200
    пакетов сервер счёл мусорным. Вместо того чтобы похоронить всю пачку,
    выборка пересылается по одному: прошедшие помечаются отправленными,
    персонально отклонённые копят попытки и после порога уходят в карантин.

    Медленный путь включается только на 422, которого в штатной жизни нет.
    """

    def __init__(
        self,
        repository: TelemetryRepository,
        gateway: NetworkGateway,
        *,
        sample_size: int,
        quarantine_after_attempts: int,
        budget: float,
    ) -> None:
        self._repository = repository
        self._gateway = gateway
        self._sample_size = sample_size
        self._quarantine_after_attempts = quarantine_after_attempts
        self._budget = budget
        self._log = logger.bind(name="dispatch")

    async def isolate(self, batch: list[StoredPacket]) -> int:
        """Возвращает число пакетов, которые сервер всё-таки принял."""
        sample = batch[: self._sample_size]
        self._log.warning(
            "Пачка получила 422, проверяем {} пакетов по одному",
            len(sample),
        )

        sent = 0
        rejected: list[StoredPacket] = []
        try:
            # Свой бюджет, а не дедлайн тика: отмена извне пришла бы прямо в
            # цикл, и сохранять отказы пришлось бы уже в отменённой задаче.
            async with asyncio.timeout(self._budget):
                for item in sample:
                    try:
                        await self._gateway.send_telemetry_once([item.packet])
                        await self._repository.set_sent([item.id])
                        sent += 1
                    except DispatchRejected:
                        rejected.append(item)
        except NetworkUnavailable:
            await self._count_partial(rejected, sent=sent)
            raise
        except TimeoutError as exc:
            await self._count_partial(rejected, sent=sent)
            raise NetworkUnavailable(
                f"422-изоляция не уложилась в {self._budget}с, "
                f"проверено {sent + len(rejected)} из {len(sample)} пакетов"
            ) from exc

        if self._looks_like_schema_drift(sample, rejected):
            # Намеренно ничего не помечаем: если сервер отвергает подряд всё,
            # виноват контракт, а не данные, и карантинить выборку нельзя.
            raise DispatchMisconfigured(
                f"Сервер отверг {len(rejected)} пакетов подряд, похоже сломан контракт"
            )

        await self._count_attempts(rejected)
        self._log.warning(
            "Сервер отверг {} пакетов, копят попытки / карантин. "
            "Отправлено только {} пакетов",
            len(rejected),
            sent,
        )
        return sent

    def _looks_like_schema_drift(
        self, sample: list[StoredPacket], rejected: list[StoredPacket]
    ) -> bool:
        """Правило не применяется к маленькой выборке: иначе единственный
        по-настоящему плохой пакет никогда бы не дошёл до карантина."""
        return len(sample) >= self._sample_size and len(rejected) == len(sample)

    async def _count_partial(self, rejected: list[StoredPacket], *, sent: int) -> None:
        """Проверка оборвалась на середине выборки: сеть пропала или кончился
        бюджет. Отказы надо зачесть, иначе медленная изоляция каждый тик
        начинает сначала и никогда не доводит их до карантина. Но пока ни один
        пакет не прошёл, отказ неотличим от сломанного контракта — правило
        `_looks_like_schema_drift` до конца выборки не дошло, поэтому карантинить
        такую выборку нельзя.
        """
        if not sent:
            return
        await self._count_attempts(rejected)

    async def _count_attempts(self, rejected: list[StoredPacket]) -> None:
        await self._repository.update_rejected(
            [item.id for item in rejected],
            self._quarantine_after_attempts,
        )
