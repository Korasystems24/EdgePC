import asyncio

from src.config.settings import settings


async def main() -> None:

    import sys

    from loguru import logger

    logger.remove()

    logger.add(sys.stderr, level=("DEBUG" if settings.debug else "INFO"))
    logger.add(
        "prod.log",
        level="INFO",
        rotation="10 MB",
        retention="1 month",
        compression="gz",
    )

    if settings.debug:
        from src.entrypoints.wcf_debug import run
    else:
        from src.bootstrap import bootstrap_application as run

    await run()


if __name__ == "__main__":
    asyncio.run(main())
