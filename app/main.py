"""FastAPI entrypoint (the write side + read side of the system)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app import db, redis_client
from app.api.routes import router
from app.config import settings
from app.kafka.admin import ensure_topics
from app.kafka.producer import start_producer, stop_producer
from app.logging_config import setup_logging

setup_logging()
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.create_schema()
    await ensure_topics()
    await start_producer()
    log.info("%s API ready", settings.app_name)
    yield
    await stop_producer()
    await redis_client.close_redis()
    await db.dispose()


app = FastAPI(
    title="OrderFlow",
    version="0.1.0",
    summary="Event-driven order processing with FastAPI, Kafka and Redis",
    description=(
        "POST /orders returns 202 and publishes `order.created` to Kafka. A "
        "consumer group processes each event exactly once (Redis idempotency "
        "guard over at-least-once delivery), retries transient failures on a "
        "delay topic with exponential backoff, and dead-letters what it "
        "cannot process. GET /orders/{id} is served cache-aside from Redis."
    ),
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


def run() -> None:  # pragma: no cover - console script
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
