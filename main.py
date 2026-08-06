import asyncio

from src.bootstrap import bootstrap_application
from src.logging_config import configure_logging


async def main() -> None:
    configure_logging()

    await bootstrap_application()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
