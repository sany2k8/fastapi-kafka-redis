"""Topic bootstrap.

Auto-topic-creation is disabled on the broker on purpose: topics are part of
the contract, so partition count and retention are declared here rather than
being whatever the first producer happened to trigger.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError

from app.config import settings

log = logging.getLogger(__name__)

WEEK_MS = str(7 * 24 * 60 * 60 * 1000)


def _desired_topics() -> list[NewTopic]:
    return [
        NewTopic(
            name=settings.topic_orders,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication_factor,
            topic_configs={"retention.ms": WEEK_MS},
        ),
        NewTopic(
            name=settings.topic_retry,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication_factor,
            topic_configs={"retention.ms": WEEK_MS},
        ),
        NewTopic(
            # The DLQ is a single partition: strict arrival order matters more
            # than throughput for something a human will read.
            name=settings.topic_dlq,
            num_partitions=1,
            replication_factor=settings.topic_replication_factor,
            topic_configs={"retention.ms": str(30 * 24 * 60 * 60 * 1000)},
        ),
    ]


async def ensure_topics(retries: int = 30, delay: float = 2.0) -> None:
    """Create the topics, waiting for the broker to come up if necessary."""
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        admin = AIOKafkaAdminClient(bootstrap_servers=settings.bootstrap_list)
        try:
            await admin.start()
            existing = set(await admin.list_topics())
            missing = [t for t in _desired_topics() if t.name not in existing]
            if missing:
                try:
                    await admin.create_topics(missing)
                except TopicAlreadyExistsError:
                    pass  # another replica won the race — fine
                log.info("created topics: %s", ", ".join(t.name for t in missing))
            else:
                log.info("topics already present: %s", ", ".join(sorted(existing)))
            return
        except (KafkaConnectionError, OSError) as exc:
            last_error = exc
            log.warning("kafka not reachable (attempt %d/%d): %s", attempt, retries, exc)
            await asyncio.sleep(delay)
        finally:
            try:
                await admin.close()
            except Exception:  # pragma: no cover - best effort
                pass

    raise RuntimeError(f"could not reach Kafka to create topics: {last_error}")
