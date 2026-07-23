"""Redis-backed idempotency guard for the Kafka consumer.

Kafka gives us *at-least-once* delivery: a rebalance, a crash between
processing and committing, or a retry replay will all hand us the same event
twice. This guard makes the second delivery a no-op.

The guard is a small state machine on one key per `event_id`:

    (absent)  --SET NX-->  <token>  --CAS-->  "done"
                              |
                              +--CAS release (on failure) --> (absent)

* `SET key <token> NX EX ttl` is the atomic claim — exactly one consumer wins.
* A loser that reads `"done"` knows the work already happened and can skip.
* A loser that reads someone else's token knows a peer is mid-flight; it must
  *not* commit the offset, so the message is retried rather than dropped.
* Release and completion both use a compare-and-set Lua script, so a consumer
  whose lock already expired can never clobber the newer owner's state.

The one assumption: `idempotency_lock_ttl_seconds` must exceed the worst-case
processing time. If it does not, the lock expires mid-flight and a concurrent
delivery can genuinely process the event twice — the classic distributed-lock
caveat, and the reason processing itself is written to be replay-safe.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from enum import StrEnum

from redis.asyncio import Redis

from app.config import settings
from app.redis_client import Keys

log = logging.getLogger(__name__)

DONE = "done"

# Delete the key only if we still own it.
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

# Flip the key to the "done" tombstone only if we still own it.
_COMPLETE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], 'done', 'EX', ARGV[2])
    return 1
end
return 0
"""


class ClaimStatus(StrEnum):
    ACQUIRED = "acquired"  # we own it, go process
    ALREADY_DONE = "already_done"  # processed before, skip and commit
    IN_PROGRESS = "in_progress"  # a peer owns it, skip WITHOUT committing


@dataclass(slots=True)
class Claim:
    status: ClaimStatus
    key: str
    token: str

    @property
    def acquired(self) -> bool:
        return self.status is ClaimStatus.ACQUIRED


class IdempotencyGuard:
    def __init__(
        self, redis: Redis, *, lock_ttl: int | None = None, done_ttl: int | None = None
    ) -> None:
        self._redis = redis
        self._lock_ttl = lock_ttl or settings.idempotency_lock_ttl_seconds
        self._done_ttl = done_ttl or settings.idempotency_done_ttl_seconds
        self._release = redis.register_script(_RELEASE_LUA)
        self._complete = redis.register_script(_COMPLETE_LUA)

    async def claim(self, event_id: str) -> Claim:
        key = Keys.event_guard(event_id)
        token = uuid.uuid4().hex

        # SETNX + expiry in one round trip — no window where the key exists
        # without a TTL (which would deadlock the event forever on a crash).
        if await self._redis.set(key, token, nx=True, ex=self._lock_ttl):
            return Claim(ClaimStatus.ACQUIRED, key, token)

        current = await self._redis.get(key)
        if current is None:
            # Expired between SET and GET — race for it once more.
            if await self._redis.set(key, token, nx=True, ex=self._lock_ttl):
                return Claim(ClaimStatus.ACQUIRED, key, token)
            current = await self._redis.get(key)

        if current == DONE:
            return Claim(ClaimStatus.ALREADY_DONE, key, token)
        return Claim(ClaimStatus.IN_PROGRESS, key, token)

    async def complete(self, claim: Claim) -> bool:
        """Mark the event permanently handled. Idempotent, CAS-guarded."""
        ok = await self._complete(keys=[claim.key], args=[claim.token, self._done_ttl])
        if not ok:
            log.warning(
                "idempotency lock for %s was lost before completion (lock TTL too short?)",
                claim.key,
            )
        return bool(ok)

    async def release(self, claim: Claim) -> bool:
        """Give the claim back so a retry can pick the event up again."""
        return bool(await self._release(keys=[claim.key], args=[claim.token]))

    async def forget(self, event_id: str) -> None:
        """Test/ops escape hatch: drop the guard entirely."""
        await self._redis.delete(Keys.event_guard(event_id))
