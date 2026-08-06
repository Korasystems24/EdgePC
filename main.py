import asyncio

from src.config.settings import settings
from src.logging_config import configure_logging


async def main() -> None:
    configure_logging()
    if settings.debug:
        from src.entrypoints.wcf_debug import run
    else:
        from src.bootstrap import bootstrap_application as run

    await run()


if __name__ == "__main__":
    asyncio.run(main())
