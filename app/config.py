"""Application settings, loaded from the environment (12-factor style)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "orderflow"
    log_level: str = "INFO"

    # --- Source of truth -------------------------------------------------
    postgres_dsn: str = "postgresql+asyncpg://orderflow:orderflow@localhost:5433/orderflow"

    # --- Redis -----------------------------------------------------------
    redis_url: str = "redis://localhost:6380/0"
    # Cache-aside TTL for order reads. Short enough that a missed
    # invalidation self-heals, long enough to absorb read bursts.
    cache_ttl_seconds: int = 60
    # Must comfortably exceed the worst-case processing time of one message,
    # otherwise the guard can expire mid-flight and admit a duplicate.
    idempotency_lock_ttl_seconds: int = 60
    # How long we remember "this event was already handled".
    idempotency_done_ttl_seconds: int = 86_400
    api_idempotency_ttl_seconds: int = 86_400

    # --- Kafka -----------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:29092"
    topic_orders: str = "orders.created"
    topic_retry: str = "orders.retry"
    topic_dlq: str = "orders.dlq"
    group_processors: str = "order-processors"
    group_retry: str = "order-retry-scheduler"
    group_dlq: str = "order-dlq-archiver"
    topic_partitions: int = 3
    topic_replication_factor: int = 1

    # --- Retry policy ----------------------------------------------------
    max_attempts: int = 4  # 1 initial delivery + 3 retries
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 60.0

    # --- Demo knobs ------------------------------------------------------
    processing_delay_seconds: float = 0.3

    @property
    def bootstrap_list(self) -> list[str]:
        return [s.strip() for s in self.kafka_bootstrap_servers.split(",") if s.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
