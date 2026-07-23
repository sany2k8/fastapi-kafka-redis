from __future__ import annotations

import uuid

import pytest

from app.config import settings
from app.errors import PermanentError, TransientError
from app.processing import backoff_delay, fulfil, validate
from app.schemas import FailMode, OrderEvent


def make_event(**overrides) -> OrderEvent:
    base = dict(
        order_id=uuid.uuid4(),
        customer_id="cust-1",
        sku="WIDGET-1",
        quantity=2,
        amount_cents=1999,
    )
    return OrderEvent(**{**base, **overrides})


def test_valid_order_passes():
    validate(make_event())


@pytest.mark.parametrize(
    "overrides",
    [
        {"quantity": 0},
        {"quantity": -3},
        {"amount_cents": -1},
        {"amount_cents": 10_000_000_00},
        {"sku": "BAD-999"},
        {"fail_mode": FailMode.PERMANENT},
    ],
)
def test_unfixable_orders_are_permanent(overrides):
    """Permanent means "retrying will never help" — straight to the DLQ."""
    with pytest.raises(PermanentError):
        validate(make_event(**overrides))


async def test_transient_failure_is_retryable():
    with pytest.raises(TransientError):
        await fulfil(make_event(fail_mode=FailMode.TRANSIENT))


async def test_happy_path_completes():
    await fulfil(make_event())  # no exception == fulfilled


def test_backoff_grows_and_is_capped():
    assert backoff_delay(1) <= settings.retry_base_delay_seconds * 1
    assert backoff_delay(10) <= settings.retry_max_delay_seconds
    # Full jitter: successive delays for the same attempt should differ.
    assert len({backoff_delay(5) for _ in range(20)}) > 1


def test_backoff_is_never_negative():
    assert all(backoff_delay(n) >= 0 for n in range(1, 12))
