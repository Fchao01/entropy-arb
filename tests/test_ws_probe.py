"""Read-only probe statistics include slow data and exclude unsynced books."""
import asyncio
import json

import pytest

from entropy_arb.book import OrderBook
from tools import ws_probe


STAMP = 1792022400.0


def test_probe_reports_all_samples_and_preserves_clock_errors():
    samples, book = ws_probe.Samples(), OrderBook()
    book.ready = True
    for age in (-10, 20, 30, 40, 200):
        book.latency.record(STAMP * 1000, received_at=STAMP + age / 1000,
                            data_timestamp=(STAMP - .02) * 1e6)
        samples.observe(book)
    result = samples.report()
    assert result["samples"] == 5
    assert result["message_delay_ms"]["p50"] == pytest.approx(30, abs=.001)
    assert result["data_age_at_receive_ms"]["p50"] == pytest.approx(50, abs=.001)
    assert result["data_age_at_receive_ms"]["p99"] == pytest.approx(220, abs=.001)
    assert result["data_over_100ms_pct"] == 20
    assert result["ws_over_100ms_pct"] == 20
    assert result["clock_warning"]
    book.clear()
    samples.observe(book)
    assert samples.report()["samples"] == 5
    assert ws_probe.Samples().report()["data_over_100ms_pct"] is None


def test_probe_handles_diffs_and_resubscription_without_trading(monkeypatch):
    sent, options = [], []

    def frame(kind, nonce, begin=None, age=10):
        return json.dumps(dict(type=kind, channel="order_book:1",
                               timestamp=ws_probe.time.time() * 1000 - age,
                               order_book=dict(nonce=nonce, begin_nonce=begin,
                                               bids=[dict(price="100", size="1")],
                                               asks=[dict(price="101", size="1")])))

    class WS:
        async def send(self, raw):
            sent.append(json.loads(raw))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def __aiter__(self):
            return self.read()

        async def read(self):
            yield json.dumps(dict(type="connected"))
            yield frame("subscribed/order_book", 100)
            yield frame("update/order_book", 101, 100, age=200)
            yield frame("update/order_book", 200, 150)  # gap: never sampled
            yield frame("subscribed/order_book", 201)
            yield json.dumps(dict(type="ping"))
            await asyncio.Event().wait()

    def connect(url, **kwargs):
        options.append(kwargs)
        return WS()

    monkeypatch.setattr(ws_probe, "ws_connect", connect)
    result = asyncio.run(ws_probe.probe("RH", "unused", 1, seconds=.03, warmup=0,
                                       compression=None, max_queue=4))
    assert result["error"] is None and result["samples"] == 3
    assert result["data_age_at_receive_ms"]["p99"] >= 200
    assert result["data_over_100ms_pct"] == pytest.approx(100 / 3, abs=.001)
    assert result["source_gap_ms"] is not None and result["receive_gap_ms"] is not None
    assert options[0]["compression"] is None and options[0]["max_queue"] == 4
    assert [row["type"] for row in sent] == ["subscribe", "unsubscribe", "subscribe", "pong"]
