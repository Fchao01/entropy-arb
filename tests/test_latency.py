"""Timestamp units, feed demultiplexing, and timing independent of heartbeats."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from entropy_arb.book import OrderBook
from entropy_arb.feeds import HLBookFeed, LighterBookFeed
from entropy_arb.latency import BookLatency, timestamp_seconds


STAMP = 1792022400.125


@pytest.mark.parametrize("value", [STAMP, STAMP * 1000, STAMP * 1e6,
                                   STAMP * 1e9, str(STAMP * 1000),
                                   "2026-10-15T00:00:00.125Z"])
def test_timestamp_units(value):
    assert timestamp_seconds(value) == pytest.approx(STAMP)


@pytest.mark.parametrize("value", [None, True, 0, 1234, float("nan"), float("inf"),
                                   "bad", "2026-10-15T00:00:00", {}, -1])
def test_missing_or_invalid_timestamp_is_unknown(value):
    assert timestamp_seconds(value) is None


def test_latency_separates_publish_commit_and_processing_and_keeps_negative(monkeypatch):
    monkeypatch.setattr("entropy_arb.latency.time.perf_counter", lambda: 10.004)
    timing = BookLatency()
    timing.record(STAMP * 1000, received_at=STAMP + .080, received_mono=10,
                  timestamp_field="timestamp", data_timestamp=(STAMP - .020) * 1e6)
    info = timing.snapshot(STAMP + .180)
    assert info["delay_ms"] == pytest.approx(80, abs=.001)
    assert info["data_delay_ms"] == pytest.approx(100, abs=.001)
    assert info["publish_gap_ms"] == pytest.approx(20, abs=.001)
    assert info["apply_ms"] == pytest.approx(4)
    assert info["local_wait_ms"] == pytest.approx(4)
    assert info["source_age_ms"] == pytest.approx(180, abs=.001)
    timing.record((STAMP + 1) * 1000, received_at=STAMP)
    assert timing.snapshot()["delay_ms"] == -1000
    assert timing.snapshot()["clock_warning"]
    timing.record(None, received_at=STAMP)
    assert timing.snapshot()["delay_ms"] is None
    assert timing.snapshot()["timestamp_field"] is None
    assert timing.snapshot()["samples"] == 2


def test_latency_history_is_bounded():
    timing = BookLatency()
    for index in range(600):
        timing.record(STAMP, received_at=STAMP + index / 1000)
    result = timing.snapshot()
    assert result["samples"] == 512 and result["messages"] == 600
    assert result["p95_ms"] > result["p50_ms"]
    assert result["max_ms"] == pytest.approx(599, abs=.001)


def test_lighter_uses_envelope_and_committer_timestamps_only_on_accepted_books():
    async def run():
        book = OrderBook()
        feed = LighterBookFeed("RH", "unused", 26, book, lambda: None)
        ws = AsyncMock()
        msg = dict(channel="order_book:26", timestamp=STAMP * 1000,
                   order_book=dict(nonce=100, last_updated_at=(STAMP - .02) * 1e6,
                                   bids=[dict(price="100", size="1")],
                                   asks=[dict(price="101", size="1")]))
        await feed._handle_book(ws, msg, True, received_at=STAMP + .08)
        assert book.latency.snapshot()["delay_ms"] == pytest.approx(80, abs=.001)
        assert book.latency.snapshot()["data_delay_ms"] == pytest.approx(100, abs=.001)
        msg["channel"] = "order_book:27"
        await feed._handle_book(ws, msg, False, received_at=STAMP + 99)
        assert book.latency.snapshot()["messages"] == 1
        msg["channel"] = "order_book:26"
        msg["order_book"]["begin_nonce"] = 200
        await feed._handle_book(ws, msg, False, received_at=STAMP + 99)
        assert not book.ready and book.latency.snapshot()["received_at"] is None
    asyncio.run(run())


def test_hl_heartbeat_and_other_coins_do_not_refresh_book_timing():
    book = OrderBook()
    feed = HLBookFeed("ENTROPY", "unused", "io:SPY", book, lambda: None)
    msg = dict(channel="l2Book", data=dict(coin="io:SPY", time=STAMP * 1000,
                                          levels=[[dict(px="100", sz="1")],
                                                  [dict(px="101", sz="1")]]))
    feed._on_frame(msg, received_at=STAMP + .15)
    feed._on_frame(dict(channel="pong"), received_at=STAMP + 10)
    msg["data"]["coin"] = "BTC"
    feed._on_frame(msg, received_at=STAMP + 10)
    assert book.latency.snapshot()["messages"] == 1
    assert book.latency.snapshot()["delay_ms"] == pytest.approx(150, abs=.001)
    book.clear()
    assert book.latency.snapshot()["delay_ms"] is None
