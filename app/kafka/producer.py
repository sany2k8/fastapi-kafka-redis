"""Process-wide Kafka producer."""

from __future__ import annotations

import json
import logging

from aiokafka import AIOKafkaProducer

from app.config import settings
from app.kafka import headers as H
from app.schemas import OrderEvent

log = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


def _serializer(value: object) -> bytes:
    return json.dumps(value, default=str, separators=(",", ":")).encode()


async def start_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.bootstrap_list,
            value_serializer=_serializer,
            key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
            # acks=all + idempotence: a producer-side retry cannot silently
            # duplicate or reorder a record within a partition.
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            linger_ms=5,
            request_timeout_ms=15_000,
        )
        await _producer.start()
        log.info("kafka producer connected to %s", settings.kafka_bootstrap_servers)
    return _producer


def get_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError("producer not started")
    return _producer


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def ping() -> bool:
    try:
        producer = get_producer()
        await producer.client.fetch_all_metadata()
        return True
    except Exception as exc:  # pragma: no cover - health probe
        log.warning("kafka ping failed: %s", exc)
        return False


async def publish_event(
    event: OrderEvent,
    *,
    topic: str | None = None,
    attempt: int = 1,
    not_before: float | None = None,
    error: str | None = None,
    origin_topic: str | None = None,
) -> None:
    """Publish an order event.

    The Kafka key is the order id, so every event for one order lands on the
    same partition and is therefore processed in order.
    """
    producer = get_producer()
    await producer.send_and_wait(
        topic or settings.topic_orders,
        value=event.model_dump(mode="json"),
        key=str(event.order_id),
        headers=H.build(
            attempt=attempt,
            not_before=not_before,
            error=error,
            origin_topic=origin_topic,
        ),
    )


async def publish_raw(
    topic: str,
    *,
    key: str | None,
    value: dict,
    headers: H.Headers,
) -> None:
    """Forward an already-decoded record verbatim (used by retry -> main)."""
    await get_producer().send_and_wait(topic, value=value, key=key, headers=headers)
