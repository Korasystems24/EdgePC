import asyncio


async def sleep_until_event(stop: asyncio.Event, seconds: float) -> None:

    if seconds <= 0:
        return
    try:
        async with asyncio.timeout(seconds):
            await stop.wait()
    except TimeoutError:
        return
