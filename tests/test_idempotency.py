"""The idempotency guard is the load-bearing piece — test its state machine."""

from __future__ import annotations

import asyncio

import pytest

from app.idempotency import ClaimStatus, IdempotencyGuard
from app.redis_client import Keys


async def test_first_claim_wins(redis):
    guard = IdempotencyGuard(redis)
    claim = await guard.claim("evt-1")
    assert claim.status is ClaimStatus.ACQUIRED
    assert claim.acquired


async def test_concurrent_claim_sees_in_progress(redis):
    guard = IdempotencyGuard(redis)
    first = await guard.claim("evt-1")
    second = await guard.claim("evt-1")

    assert first.status is ClaimStatus.ACQUIRED
    # A peer is mid-flight: the caller must redeliver, not commit.
    assert second.status is ClaimStatus.IN_PROGRESS
    assert second.token != first.token


async def test_completed_event_is_skipped_forever(redis):
    guard = IdempotencyGuard(redis)
    claim = await guard.claim("evt-1")
    assert await guard.complete(claim)

    replay = await guard.claim("evt-1")
    assert replay.status is ClaimStatus.ALREADY_DONE
    assert await redis.get(Keys.event_guard("evt-1")) == "done"


async def test_release_lets_a_retry_reclaim(redis):
    guard = IdempotencyGuard(redis)
    claim = await guard.claim("evt-1")
    assert await guard.release(claim)

    retry = await guard.claim("evt-1")
    assert retry.status is ClaimStatus.ACQUIRED


async def test_release_is_compare_and_set(redis):
    """A consumer whose lock expired must not free the new owner's lock."""
    guard = IdempotencyGuard(redis, lock_ttl=1)
    stale = await guard.claim("evt-1")
    await redis.delete(Keys.event_guard("evt-1"))  # simulate TTL expiry
    fresh = await guard.claim("evt-1")

    assert await guard.release(stale) is False
    assert await redis.get(Keys.event_guard("evt-1")) == fresh.token


async def test_complete_is_compare_and_set(redis):
    """Likewise, a stale owner must not stamp 'done' over a live claim."""
    guard = IdempotencyGuard(redis)
    stale = await guard.claim("evt-1")
    await redis.delete(Keys.event_guard("evt-1"))
    fresh = await guard.claim("evt-1")

    assert await guard.complete(stale) is False
    assert await redis.get(Keys.event_guard("evt-1")) == fresh.token
    assert await guard.complete(fresh) is True


async def test_lock_carries_a_ttl(redis):
    """A crashed consumer must never wedge an event permanently."""
    guard = IdempotencyGuard(redis, lock_ttl=30)
    await guard.claim("evt-1")
    assert 0 < await redis.ttl(Keys.event_guard("evt-1")) <= 30


async def test_only_one_of_many_racers_acquires(redis):
    guard = IdempotencyGuard(redis)
    claims = await asyncio.gather(*(guard.claim("evt-hot") for _ in range(25)))
    assert sum(c.acquired for c in claims) == 1


@pytest.mark.parametrize("event_id", ["a", "b", "c"])
async def test_distinct_events_do_not_interfere(redis, event_id):
    guard = IdempotencyGuard(redis)
    await guard.claim("other")
    assert (await guard.claim(event_id)).acquired
