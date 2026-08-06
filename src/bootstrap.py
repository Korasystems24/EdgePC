from loguru import logger

from src.containers.service_container import container


async def bootstrap_application() -> None:
    logger.info("Запуск приложения в боевом режиме")

    manager = container.get_worker_manager()
    await manager.run_all()
