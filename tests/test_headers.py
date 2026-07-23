from __future__ import annotations

import time

from app.kafka import headers as H


def test_build_and_read_round_trip():
    not_before = time.time() + 12.5
    built = H.build(attempt=3, not_before=not_before, error="boom", origin_topic="orders.created")

    assert H.get_attempt(built) == 3
    assert H.get_not_before(built) == not_before  # repr() keeps full precision
    assert H.get_error(built) == "boom"
    assert H.get_origin_topic(built) == "orders.created"


def test_defaults_for_a_first_delivery():
    built = H.build(attempt=1)
    assert H.get_attempt(built) == 1
    assert H.get_not_before(built) == 0.0
    assert H.get_error(built) is None


def test_missing_or_garbage_headers_do_not_explode():
    assert H.get_attempt(None) == 1
    assert H.get_attempt([]) == 1
    assert H.get_attempt([(H.ATTEMPT, b"not-a-number")]) == 1
    assert H.get_not_before([(H.NOT_BEFORE, b"nope")]) == 0.0


def test_error_text_is_truncated():
    built = H.build(attempt=2, error="x" * 5000)
    assert len(H.get_error(built)) == 500
