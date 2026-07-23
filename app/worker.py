"""Worker entrypoint: runs the processor, retry scheduler and DLQ archiver.

All three share one event loop and one producer. Scale horizontally with
`docker compose up --scale worker=3` — Kafka's group coordinator hands each
replica a slice of the partitions, and because the Kafka key is the order id,
all events for one order still land on one worker in order.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from app import db, redis_client
from app.config import settings
from app.kafka.admin import ensure_topics
from app.kafka.consumers import BaseConsumer, DlqArchiver, OrderConsumer, RetryScheduler
from app.kafka.producer import start_producer, stop_producer
from app.logging_config import setup_logging

setup_logging()
log = logging.getLogger(__name__)


async def main() -> None:
    await db.create_schema()
    await ensure_topics()
    await start_producer()

    consumers: list[BaseConsumer] = [OrderConsumer(), RetryScheduler(), DlqArchiver()]
    tasks = [asyncio.create_task(c.run(), name=c.name) for c in consumers]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("%s worker running (%d consumers)", settings.app_name, len(consumers))

    done_waiter = asyncio.create_task(stop.wait())
    await asyncio.wait([done_waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)

    # Graceful drain: let each loop finish its current record, then commit.
    log.info("shutdown requested — draining consumers")
    for consumer in consumers:
        await consumer.stop()
    done_waiter.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    await stop_producer()
    await redis_client.close_redis()
    await db.dispose()
    log.info("worker stopped")


def run() -> None:  # pragma: no cover - console script
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
