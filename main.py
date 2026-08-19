import asyncio
import contextlib

from src.bootstrap import bootstrap_application
from src.config.settings import get_settings
from src.logging_config import configure_logging


async def main() -> None:
    settings = get_settings()
    configure_logging(settings)

    await bootstrap_application(settings)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
