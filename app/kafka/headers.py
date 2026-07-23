"""Retry/routing metadata carried in Kafka record headers.

Headers keep the *message body* immutable across retries: a record replayed
from `orders.retry` back onto `orders.created` has the exact same bytes it had
on first publish, while the attempt counter, the next-eligible timestamp and
the last error live alongside it.
"""

from __future__ import annotations

from datetime import UTC, datetime

ATTEMPT = "x-attempt"
NOT_BEFORE = "x-not-before"  # epoch seconds, float
ERROR = "x-error"
ORIGIN_TOPIC = "x-origin-topic"
FAILED_AT = "x-failed-at"

Headers = list[tuple[str, bytes]]


def build(
    *,
    attempt: int,
    not_before: float | None = None,
    error: str | None = None,
    origin_topic: str | None = None,
) -> Headers:
    headers: Headers = [(ATTEMPT, str(attempt).encode())]
    if not_before is not None:
        headers.append((NOT_BEFORE, repr(not_before).encode()))
    if error is not None:
        # Keep headers small; the full error is persisted in the DLQ archive.
        headers.append((ERROR, error[:500].encode("utf-8", "replace")))
    if origin_topic is not None:
        headers.append((ORIGIN_TOPIC, origin_topic.encode()))
        headers.append((FAILED_AT, datetime.now(UTC).isoformat().encode()))
    return headers


def to_dict(headers: Headers | tuple | None) -> dict[str, str]:
    if not headers:
        return {}
    return {k: (v.decode("utf-8", "replace") if v is not None else "") for k, v in headers}


def get_attempt(headers: Headers | tuple | None) -> int:
    """Attempt number of the delivery we are holding (1-based)."""
    raw = to_dict(headers).get(ATTEMPT)
    try:
        return max(1, int(raw)) if raw else 1
    except ValueError:
        return 1


def get_not_before(headers: Headers | tuple | None) -> float:
    raw = to_dict(headers).get(NOT_BEFORE)
    try:
        return float(raw) if raw else 0.0
    except ValueError:
        return 0.0


def get_error(headers: Headers | tuple | None) -> str | None:
    return to_dict(headers).get(ERROR)


def get_origin_topic(headers: Headers | tuple | None) -> str | None:
    return to_dict(headers).get(ORIGIN_TOPIC)
