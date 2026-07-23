"""The three consumers that make up the worker.

    orders.created ──▶ OrderConsumer ──success──▶ completed
          ▲                  │
          │                  ├──transient (attempt < max)──▶ orders.retry
          │                  └──permanent / attempts exhausted──▶ orders.dlq
          │                                                          │
    RetryScheduler ◀── orders.retry                     DlqArchiver ◀─┘

Every consumer commits offsets *manually, after* the work is durable, which
makes delivery at-least-once. The Redis idempotency guard is what turns
at-least-once delivery into effectively-once processing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from enum import Enum, auto

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from pydantic import ValidationError

from app import cache, redis_client
from app import repository as repo
from app.config import settings
from app.db import session_scope
from app.errors import PermanentError, TransientError
from app.idempotency import Claim, ClaimStatus, IdempotencyGuard
from app.kafka import headers as H
from app.kafka.producer import publish_raw
from app.models import TERMINAL_STATUSES, OrderStatus
from app.processing import backoff_delay, fulfil
from app.redis_client import get_redis
from app.schemas import OrderEvent

log = logging.getLogger(__name__)


class Ack(Enum):
    COMMIT = auto()  # done with this record, advance the offset
    REDELIVER = auto()  # leave the offset alone, see this record again


class BaseConsumer:
    """Manual-commit consume loop with cooperative shutdown."""

    topic: str
    group_id: str
    name: str

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = asyncio.Event()

    def _build(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.bootstrap_list,
            group_id=self.group_id,
            # Offsets are ours to manage: commit only once the side effects
            # are durable, otherwise a crash would silently lose messages.
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode()),
            key_deserializer=lambda b: b.decode() if b else None,
            max_poll_records=20,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=3_000,
        )

    async def run(self) -> None:
        consumer = self._consumer = self._build()
        await consumer.start()
        log.info("[%s] consuming %s as group %s", self.name, self.topic, self.group_id)
        try:
            while not self._stopping.is_set():
                batch = await consumer.getmany(timeout_ms=1000)
                for tp, records in batch.items():
                    for record in records:
                        if self._stopping.is_set():
                            return
                        ack = await self._safe_handle(record)
                        if ack is Ack.REDELIVER:
                            # Rewind to this record and abandon the rest of
                            # the batch for this partition — anything after it
                            # would jump the queue.
                            consumer.seek(tp, record.offset)
                            break
                        await consumer.commit({tp: record.offset + 1})
        except asyncio.CancelledError:
            raise
        finally:
            log.info("[%s] stopping", self.name)
            await consumer.stop()

    async def stop(self) -> None:
        self._stopping.set()

    async def _safe_handle(self, record: ConsumerRecord) -> Ack:
        try:
            return await self.handle(record)
        except Exception:
            # Never let one bad record kill the loop; redeliver and back off.
            log.exception(
                "[%s] unhandled error on %s[%s]@%s",
                self.name,
                record.topic,
                record.partition,
                record.offset,
            )
            await asyncio.sleep(1.0)
            return Ack.REDELIVER

    async def handle(self, record: ConsumerRecord) -> Ack:  # pragma: no cover
        raise NotImplementedError


# --------------------------------------------------------------------------
# 1. Main processor
# --------------------------------------------------------------------------


class OrderConsumer(BaseConsumer):
    name = "processor"

    def __init__(self) -> None:
        super().__init__()
        self.topic = settings.topic_orders
        self.group_id = settings.group_processors
        self._guard = IdempotencyGuard(get_redis())

    async def handle(self, record: ConsumerRecord) -> Ack:
        attempt = H.get_attempt(record.headers)

        try:
            event = OrderEvent.model_validate(record.value)
        except ValidationError as exc:
            # Poison pill: it will never parse, so retrying is pointless.
            log.error("[processor] undecodable record -> DLQ: %s", exc)
            await self._to_dlq(record, attempt=attempt, error=f"schema violation: {exc}")
            return Ack.COMMIT

        await redis_client.incr_stat(redis_client.STAT_CONSUMED)

        # --- idempotency gate -------------------------------------------
        claim = await self._guard.claim(event.event_id)
        if claim.status is ClaimStatus.ALREADY_DONE:
            log.info("[processor] duplicate event %s — skipping", event.event_id)
            await redis_client.incr_stat(redis_client.STAT_DUPLICATES)
            return Ack.COMMIT
        if claim.status is ClaimStatus.IN_PROGRESS:
            # A peer holds the lock. Do NOT commit — if that peer dies, this
            # message must still be processed by someone.
            log.info("[processor] event %s in flight elsewhere — redeliver", event.event_id)
            await asyncio.sleep(0.25)
            return Ack.REDELIVER

        try:
            return await self._process(event, record, attempt, claim)
        except Exception:
            # We could not reach a verdict (Postgres down, Redis down, ...).
            # Hand the claim back so the redelivery can take it, and let
            # _safe_handle redeliver without committing.
            await self._guard.release(claim)
            raise

    async def _process(
        self, event: OrderEvent, record: ConsumerRecord, attempt: int, claim: Claim
    ) -> Ack:
        order_id = event.order_id

        # Second line of defence: even if the guard were bypassed, an order
        # that already reached a terminal state is never reprocessed.
        async with session_scope() as session:
            order = await repo.get_order(session, order_id)
            if order is None:
                await self._guard.complete(claim)
                await self._to_dlq(record, attempt=attempt, error=f"no such order {order_id}")
                return Ack.COMMIT
            if order.status in TERMINAL_STATUSES:
                log.info("[processor] order %s already %s — skipping", order_id, order.status)
                await self._guard.complete(claim)
                await redis_client.incr_stat(redis_client.STAT_DUPLICATES)
                return Ack.COMMIT

            await repo.transition(
                session,
                order,
                OrderStatus.PROCESSING,
                note=f"attempt {attempt}",
                attempts=attempt,
            )
        await cache.put_order(get_redis(), order)

        try:
            await fulfil(event)
        except PermanentError as exc:
            return await self._fail(event, record, attempt, claim, str(exc), retryable=False)
        except TransientError as exc:
            return await self._fail(event, record, attempt, claim, str(exc), retryable=True)

        async with session_scope() as session:
            order = await repo.get_order(session, order_id)
            await repo.transition(
                session,
                order,
                OrderStatus.COMPLETED,
                note="fulfilled",
                attempts=attempt,
            )
        await cache.put_order(get_redis(), order)

        # Only now is the event permanently marked as handled — before the
        # offset commit, so a crash in between costs a duplicate delivery that
        # the guard will absorb, not a lost order.
        await self._guard.complete(claim)
        await redis_client.incr_stat(redis_client.STAT_COMPLETED)
        log.info("[processor] order %s completed on attempt %d", order_id, attempt)
        return Ack.COMMIT

    async def _fail(
        self,
        event: OrderEvent,
        record: ConsumerRecord,
        attempt: int,
        claim: Claim,
        error: str,
        *,
        retryable: bool,
    ) -> Ack:
        order_id = event.order_id
        can_retry = retryable and attempt < settings.max_attempts

        if can_retry:
            delay = backoff_delay(attempt)
            async with session_scope() as session:
                order = await repo.get_order(session, order_id)
                await repo.transition(
                    session,
                    order,
                    OrderStatus.PENDING,
                    note=f"attempt {attempt} failed ({error}); retrying in {delay}s",
                    attempts=attempt,
                    failure_reason=error,
                )
            await cache.put_order(get_redis(), order)

            await publish_raw(
                settings.topic_retry,
                key=str(order_id),
                value=record.value,
                headers=H.build(
                    attempt=attempt + 1,
                    not_before=time.time() + delay,
                    error=error,
                    origin_topic=record.topic,
                ),
            )
            # Release, not complete: the replay must be allowed to claim it.
            await self._guard.release(claim)
            await redis_client.incr_stat(redis_client.STAT_RETRIES)
            log.warning(
                "[processor] order %s attempt %d failed (%s) — retry in %.1fs",
                order_id,
                attempt,
                error,
                delay,
            )
            return Ack.COMMIT

        reason = "permanent failure" if not retryable else "retries exhausted"
        async with session_scope() as session:
            order = await repo.get_order(session, order_id)
            if order is not None:
                await repo.transition(
                    session,
                    order,
                    OrderStatus.FAILED,
                    note=f"{reason}: {error}",
                    attempts=attempt,
                    failure_reason=error,
                )
        if order is not None:
            await cache.put_order(get_redis(), order)

        await self._to_dlq(record, attempt=attempt, error=f"{reason}: {error}")
        await self._guard.complete(claim)
        await redis_client.incr_stat(redis_client.STAT_FAILED)
        log.error(
            "[processor] order %s dead-lettered after %d attempts: %s", order_id, attempt, error
        )
        return Ack.COMMIT

    async def _to_dlq(self, record: ConsumerRecord, *, attempt: int, error: str) -> None:
        await publish_raw(
            settings.topic_dlq,
            key=record.key,
            value=record.value if isinstance(record.value, dict) else {"raw": record.value},
            headers=H.build(attempt=attempt, error=error, origin_topic=record.topic),
        )


# --------------------------------------------------------------------------
# 2. Retry scheduler
# --------------------------------------------------------------------------


class RetryScheduler(BaseConsumer):
    """Delay topic.

    Records land here with an `x-not-before` header. The scheduler waits until
    that moment, then republishes onto the main topic. Because delays are
    capped well below `max.poll.interval.ms`, simply sleeping in the loop is
    safe; a system with hour-long delays would instead use tiered delay topics
    (retry-5s / retry-1m / retry-30m) so each one only ever sleeps a little.
    """

    name = "retry"

    def __init__(self) -> None:
        super().__init__()
        self.topic = settings.topic_retry
        self.group_id = settings.group_retry

    async def handle(self, record: ConsumerRecord) -> Ack:
        attempt = H.get_attempt(record.headers)
        wait = H.get_not_before(record.headers) - time.time()
        if wait > 0:
            log.info("[retry] holding attempt %d for %.1fs", attempt, wait)
            await asyncio.sleep(min(wait, settings.retry_max_delay_seconds))

        await publish_raw(
            settings.topic_orders,
            key=record.key,
            value=record.value,
            headers=H.build(attempt=attempt, error=H.get_error(record.headers)),
        )
        log.info("[retry] replayed attempt %d onto %s", attempt, settings.topic_orders)
        return Ack.COMMIT


# --------------------------------------------------------------------------
# 3. DLQ archiver
# --------------------------------------------------------------------------


class DlqArchiver(BaseConsumer):
    """Persists dead letters so they are inspectable over HTTP.

    A DLQ nobody can read is just a slower way of dropping messages.
    """

    name = "dlq"

    def __init__(self) -> None:
        super().__init__()
        self.topic = settings.topic_dlq
        self.group_id = settings.group_dlq

    async def handle(self, record: ConsumerRecord) -> Ack:
        payload = record.value if isinstance(record.value, dict) else {"raw": record.value}
        order_id: uuid.UUID | None = None
        try:
            order_id = uuid.UUID(str(payload.get("order_id")))
        except (ValueError, TypeError):
            pass

        async with session_scope() as session:
            await repo.record_dead_letter(
                session,
                order_id=order_id,
                event_id=payload.get("event_id"),
                source_topic=H.get_origin_topic(record.headers) or record.topic,
                partition=record.partition,
                offset=record.offset,
                attempts=H.get_attempt(record.headers),
                error=H.get_error(record.headers),
                payload=payload,
            )

        await redis_client.incr_stat(redis_client.STAT_DLQ)
        log.warning("[dlq] archived dead letter for order %s", order_id)
        return Ack.COMMIT
