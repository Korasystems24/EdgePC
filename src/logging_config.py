import sys

from loguru import logger

from src.config.settings import settings


def configure_logging() -> None:
    logger.remove()
    logger.configure(extra={"name": "core"})
    console_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "[<magenta>{extra[name]}</magenta>] - <level>{message}</level>"
    )
    logger.add(
        sys.stderr,
        level="DEBUG" if settings.debug else "INFO",
        format=console_format,
        backtrace=True,
        diagnose=settings.debug,
        enqueue=True,
    )
    file_format = (
        "{time:YYYY-MM-DD HH:mm:ss} | "
        "{level: <8} | "
        "{name}:{function}:{line} "
        "[{extra[name]}] - {message}"
    )
    logger.add(
        settings.log_file_path,
        level="INFO",
        rotation="10 MB",
        retention="14 days",
        compression="zip",
        format=file_format,
        encoding="utf-8",
        enqueue=True,
    )

    logger.info("Инициализация и запуск сервиса логирования")
