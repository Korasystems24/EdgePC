import sys

from loguru import logger

from src.config.settings import settings


def configure_logging() -> None:
    logger.remove()

    logger.add(
        sys.stderr,
        level="DEBUG" if settings.debug else "INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        backtrace=True,
        diagnose=settings.debug,
        enqueue=True,
    )

    logger.add(
        settings.log_file_path,
        level="INFO",
        rotation="10 MB",
        retention="14 days",
        compression="zip",
        encoding="utf-8",
        enqueue=True,
    )

    logger.info("Инициализация и запуск сервиса логирования")
