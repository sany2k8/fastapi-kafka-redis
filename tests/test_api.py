"""API tests with Kafka stubbed out — the HTTP contract, not the broker."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from app.db import get_session
from app.main import app
from app.redis_client import get_redis
from app.schemas import OrderEvent


class FakeProducer:
    """Captures what would have gone onto the topic."""

    def __init__(self) -> None:
        self.published: list[OrderEvent] = []

    async def __call__(self, event: OrderEvent, **kwargs) -> None:
        self.published.append(event)


@pytest.fixture
async def producer(monkeypatch) -> FakeProducer:
    fake = FakeProducer()
    monkeypatch.setattr("app.service.publish_event", fake)
    return fake


@pytest.fixture
async def client(session, redis, producer) -> AsyncIterator[AsyncClient]:
    async def _session() -> AsyncIterator:
        yield session

    app.dependency_overrides[get_session] = _session
    app.dependency_overrides[get_redis] = lambda: redis
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


ORDER = {"customer_id": "c1", "sku": "WIDGET-1", "quantity": 2, "amount_cents": 4999}


async def test_post_returns_202_and_publishes(client, producer):
    resp = await client.post("/orders", json=ORDER)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    assert body["duplicate"] is False

    assert len(producer.published) == 1
    event = producer.published[0]
    assert str(event.order_id) == body["id"]
    assert event.event_type == "order.created"


async def test_get_falls_back_to_the_database_then_serves_from_cache(client):
    order_id = (await client.post("/orders", json=ORDER)).json()["id"]

    # POST warms the cache, so the first read is already a hit.
    first = await client.get(f"/orders/{order_id}")
    assert first.status_code == 200
    assert first.json()["source"] == "cache"

    # Evict, and the read must still succeed — from Postgres this time.
    await client.post("/stats/reset")
    from app.redis_client import Keys

    redis = app.dependency_overrides[get_redis]()
    await redis.delete(Keys.order(order_id))

    second = await client.get(f"/orders/{order_id}")
    assert second.status_code == 200
    assert second.json()["source"] == "database"

    # And the DB read repopulated the cache.
    assert (await client.get(f"/orders/{order_id}")).json()["source"] == "cache"


async def test_unknown_order_is_404(client):
    resp = await client.get("/orders/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


async def test_idempotency_key_collapses_client_retries(client, producer):
    headers = {"Idempotency-Key": "checkout-abc-123"}

    first = await client.post("/orders", json=ORDER, headers=headers)
    second = await client.post("/orders", json=ORDER, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert first.json()["id"] == second.json()["id"]
    # Crucially: one order, one event.
    assert len(producer.published) == 1


async def test_different_idempotency_keys_create_different_orders(client, producer):
    a = await client.post("/orders", json=ORDER, headers={"Idempotency-Key": "a"})
    b = await client.post("/orders", json=ORDER, headers={"Idempotency-Key": "b"})
    assert a.json()["id"] != b.json()["id"]
    assert len(producer.published) == 2


@pytest.mark.parametrize(
    "bad",
    [
        {**ORDER, "quantity": 0},
        {**ORDER, "quantity": -1},
        {**ORDER, "amount_cents": -5},
        {**ORDER, "customer_id": ""},
    ],
)
async def test_invalid_payloads_are_rejected_at_the_edge(client, producer, bad):
    assert (await client.post("/orders", json=bad)).status_code == 422
    assert producer.published == []


async def test_list_and_filter_orders(client):
    for _ in range(3):
        await client.post("/orders", json=ORDER)

    assert len((await client.get("/orders")).json()) == 3
    assert len((await client.get("/orders?status=pending")).json()) == 3
    assert (await client.get("/orders?status=completed")).json() == []


async def test_stats_expose_the_pipeline_counters(client):
    await client.post("/orders", json=ORDER)
    body = (await client.get("/stats")).json()

    assert body["pipeline"]["published"] == 1
    assert body["orders_by_status"] == {"pending": 1}


async def test_dead_letter_endpoint_starts_empty(client):
    assert (await client.get("/dead-letters")).json() == []
