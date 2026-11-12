"""Entry point for the standalone rebuild-worker container."""
from __future__ import annotations

import asyncio
import signal

from .worker import RebuildWorker


async def _main() -> None:
    worker = RebuildWorker()
    await worker.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await stop_event.wait()
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(_main())
