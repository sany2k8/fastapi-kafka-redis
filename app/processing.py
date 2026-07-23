"""The actual order-processing work performed by the consumer."""

from __future__ import annotations

import asyncio
import logging
import random

from app.config import settings
from app.errors import PermanentError, TransientError
from app.schemas import FailMode, OrderEvent

log = logging.getLogger(__name__)

# SKUs the catalogue will never know about — used to demo permanent failures.
UNKNOWN_SKU_PREFIX = "BAD-"
MAX_ORDER_VALUE_CENTS = 1_000_000_00


def validate(event: OrderEvent) -> None:
    """Business validation. Pure, synchronous, and fully unit-testable.

    Re-validating here rather than trusting the API is deliberate: the topic
    is the contract, and events could be produced by something other than our
    own endpoint.
    """
    if event.quantity <= 0:
        raise PermanentError(f"quantity must be positive, got {event.quantity}")
    if event.amount_cents < 0:
        raise PermanentError(f"amount cannot be negative, got {event.amount_cents}")
    if event.amount_cents > MAX_ORDER_VALUE_CENTS:
        raise PermanentError("order exceeds the maximum permitted value")
    if event.sku.upper().startswith(UNKNOWN_SKU_PREFIX):
        raise PermanentError(f"unknown SKU {event.sku!r}")
    if event.fail_mode is FailMode.PERMANENT:
        raise PermanentError("simulated permanent failure (fail_mode=permanent)")


async def fulfil(event: OrderEvent) -> None:
    """Stand-in for the real work: charge, reserve stock, notify.

    Anything here that talks to the outside world is where a `TransientError`
    would come from in a real system.
    """
    validate(event)

    if settings.processing_delay_seconds:
        await asyncio.sleep(settings.processing_delay_seconds)

    if event.fail_mode is FailMode.TRANSIENT:
        raise TransientError("simulated downstream timeout (fail_mode=transient)")


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter.

    Jitter matters: without it, a batch of messages that failed together comes
    back together and hammers the recovering dependency in lockstep.
    """
    ceiling = min(
        settings.retry_max_delay_seconds,
        settings.retry_base_delay_seconds * (2 ** max(0, attempt - 1)),
    )
    return round(random.uniform(settings.retry_base_delay_seconds / 2, ceiling), 3)
