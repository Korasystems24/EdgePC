from collections.abc import Callable, Generator
from contextlib import contextmanager

from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.exceptions import StorageError


@contextmanager
def storage_errors(
    message: str, *, error: type[StorageError] = StorageError
) -> Generator[None, None, None]:
    """Наружу из репозитория выходит только доменная ошибка хранилища:
    вызывающий не должен знать про SQLAlchemy."""
    try:
        yield
    except SQLAlchemyError as e:
        raise error(f"{message}: {e}") from e


async def read_pending[RowT, ItemT](
    session: AsyncSession,
    query: Select[tuple[RowT]],
    *,
    build: Callable[[RowT], ItemT],
    on_corrupt: Callable[[RowT, Exception], None],
) -> list[ItemT]:
    """Прочитать пачку доставляемых строк, битые убрать в карантин.

    Битая строка — та, которую нельзя превратить в доменный объект. Отправить её
    невозможно, а порядок соблюдается по id, поэтому без карантина она навсегда
    заблокировала бы всё, что записано после неё.

    Пачка может вернуться неполной, и это нормально: ровно `limit` строк никому
    не нужно (пустая пачка — не ошибка), а битые уже помечены карантином, так что
    следующий тик пройдёт мимо них и заберёт остальное.
    """
    result = await session.execute(query)
    items: list[ItemT] = []
    quarantined = False

    for row in result.scalars().all():
        try:
            items.append(build(row))
        except (ValidationError, TypeError, ValueError) as e:
            on_corrupt(row, e)
            quarantined = True

    if quarantined:
        await session.commit()
    return items
