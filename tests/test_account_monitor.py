"""Public account statistics must not manufacture history or strategy PnL."""
import asyncio
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from entropy_arb.account_monitor import (AccountMonitor, AccountStore, account_id,
                                       hedge_matches, observed_cycles, symbols_config, timestamp, trade_detail)
from entropy_arb.web import ROOT, TaskManager, create_app


def trade(tid="1", *, venue="lighter-rh", ts=100, ask=10, bid=20, price=100,
          size=1, bps=None, ask_before=None, bid_before=None):
    return (venue, 0, tid, "BTC", ts, ts + .1, price, size, price * size,
            ask, bid, 1, "trade", bps, ask_before, bid_before)


@pytest.mark.parametrize("value", [1792200000, 1792200000000, 1792200000000000, 1792200000000000000])
def test_timestamp_units(value):
    assert timestamp(value) == 1792200000


@pytest.mark.parametrize("value", [True, -1, "1.0", "1 OR 1=1", 2**48])
def test_invalid_account_ids(value):
    with pytest.raises(ValueError):
        account_id(value)


def test_symbols_are_explicit_and_validated():
    assert symbols_config("btc, ETH btc") == ["BTC", "ETH"]
    assert symbols_config("*") == []
    for value in ("", [], "BTC;DROP", "https://private-host"):
        with pytest.raises(ValueError):
            symbols_config(value)


def test_deduplication_volume_and_platform_identity(tmp_path):
    store = AccountStore(tmp_path / "accounts.db")
    try:
        row = trade()
        assert store.ingest([row, row]) == 1
        assert store.ingest([trade(venue="lighter")]) == 1
        result = store.overview(0)
        assert result["summary"]["trades"] == 2
        assert result["summary"]["market_volume"] == 200
        assert result["total"] == 4
        assert sum(r["volume"] for r in result["accounts"]) == 400
        rh = store.overview(0, "lighter-rh")
        assert rh["summary"]["market_volume"] == 100
        assert rh["total"] == 2
        assert store.overview(101)["total"] == 0
    finally:
        store.close()


def test_watch_settings_and_fills_survive_restart(tmp_path):
    path = tmp_path / "accounts.db"
    store = AccountStore(path)
    store.watch("lighter", 42, True)
    store.setting("config", {"symbols": "ETH"})
    store.ingest([trade()])
    store.close()
    store = AccountStore(path)
    try:
        assert store.settings()["config"] == {"symbols": "ETH"}
        assert store.overview(0)["watches"] == [{"venue": "lighter", "id": 42}]
        assert store.detail("lighter", 42, 0)["account"]["fetched"] == 0
        assert store.overview(0, search="% OR 1=1")["total"] == 0
        assert store.overview(0, search="20")["total"] == 1
    finally:
        store.close()


def fill(tid, ts, side, price, before, **extras):
    return dict(venue="lighter-rh", market=0, id=str(tid), symbol="BTC", ts=ts,
                price=price, size=1, usd=price, side=side, maker=1, kind="trade",
                before_position=before, bps=-7.0, account=20, **extras)


def test_round_trips_need_confirmed_flat_boundaries_and_no_gaps():
    rows = [fill(1, 10, 1, 100, 0), fill(2, 20, -1, 101, 1)]
    cycle = observed_cycles(rows)[0]
    assert cycle["cash"] == 1
    assert cycle["hold_sec"] == 10
    assert observed_cycles(rows, [15]) == []
    rows[0]["before_position"] = None
    assert observed_cycles(rows) == []
    rows[0]["before_position"] = 3
    assert observed_cycles(rows) == []
    shorts = [fill(1, 10, -1, 101, 0), fill(2, 20, 1, 100, -1)]
    assert observed_cycles(shorts)[0]["cash"] == 1
    rows = [fill(1, 10, 1, 100, 0), fill(2, 20, -1, 101, 2)]
    assert observed_cycles(rows) == []


def test_same_address_peers_only_and_link_does_not_overwrite_snapshot(tmp_path):
    store = AccountStore(tmp_path / "accounts.db")
    addr = "0x" + "1" * 40
    try:
        store.ingest([trade(bid_before=0), trade("2", venue="lighter", ask_before=0, ts=101)])
        store.save_account("lighter-rh", 20, {"l1_address": addr, "positions": []})
        assert store.detail("lighter-rh", 20, 0)["matches"] == []
        store.save_account("lighter", 10, {"l1_address": addr, "positions": [{"position": "1"}]})
        store.link_account("lighter", 10, addr)
        detail = store.detail("lighter-rh", 20, 0)
        assert len(detail["matches"]) == 1
        assert detail["peer_analysis_fills"] == 1
        assert detail["peer_fills"][0]["account"] == 10
        assert detail["peer_fills"][0]["position_action"] == "开空"
        peer = store.detail("lighter", 10, 0)["account"]
        assert peer["snapshot"]["positions"] == [{"position": "1"}]
        store.link_account("lighter", 30, addr)
        assert store.detail("lighter", 30, 0)["account"]["fetched"] == 0
    finally:
        store.close()


def test_cross_venue_matching_does_not_reuse_peer_or_match_wrong_size():
    rows = [fill(1, 10, 1, 100, 0), fill(2, 10.1, 1, 100, 1)]
    peer = fill(3, 10.2, -1, 101, 0)
    peer.update(venue="lighter", account=99)
    matches = hedge_matches(rows, [peer])
    assert len(matches) == 1
    assert matches[0]["fill_bps"] == pytest.approx((100 / 101 - 1) * 10000)
    peer["size"] = .5
    assert hedge_matches(rows, [peer]) == []
    peer["size"], peer["ts"] = 1, 14
    assert hedge_matches(rows, [peer]) == []


def test_pair_contains_both_actual_fills_when_viewed_from_either_venue():
    own = fill("9007199254740993", 10.123, 1, 100, 0)
    own.update(size=1.01, usd=101, maker=0)
    peer = fill("9007199254740995", 10.345, -1, 102, 0)
    peer.update(venue="lighter", market=8, account=99, usd=102, maker=1)
    pair = hedge_matches([own], [peer])[0]
    assert pair["total_volume"] == 203
    assert pair["delay_ms"] == pytest.approx(222)
    assert pair["legs"][0]["id"] == "9007199254740993"
    assert pair["legs"][0]["size"] == 1.01
    assert pair["legs"][1]["size"] == 1
    assert pair["legs"][1]["usd"] == 102
    assert pair["legs"][1]["price"] == 102
    assert pair["legs"][1]["maker"] == 1
    assert pair["legs"][0]["position_action"] == "开多"
    assert pair["legs"][1]["position_action"] == "开空"
    reverse = hedge_matches([peer], [own])[0]
    assert reverse["legs"][0]["venue"] == "lighter"
    assert reverse["fill_bps"] == pair["fill_bps"]
    assert reverse["delay_ms"] == pytest.approx(-222)


def test_unmatched_peer_trades_are_still_visible(tmp_path):
    store = AccountStore(tmp_path / "accounts.db")
    address = "0x" + "2" * 40
    try:
        store.ingest([trade(), trade("2", venue="lighter", ts=102, size=.5, ask_before=0)])
        for venue, aid in (("lighter-rh", 20), ("lighter", 10)):
            store.link_account(venue, aid, address)
        result = store.detail("lighter-rh", 20, 0)
        assert result["matches"] == []
        assert len(result["peer_fills"]) == 1
        assert result["peer_fills"][0]["size"] == .5
        assert result["peer_fills"][0]["usd"] == 50
        assert store.detail("lighter-rh", 20, 103)["peer_fills"] == []
    finally:
        store.close()


@pytest.mark.parametrize("before,side,size,expected", [
    (None, 1, 1, "开平未知"), (0, 1, 1, "开多"), (0, -1, 1, "开空"),
    (1, -1, 1, "平多"), (-1, 1, 1, "平空"), (1, 1, 1, "加多"),
    (-1, -1, 1, "加空"), (2, -1, 1, "减多"), (-2, 1, 1, "减空"),
    (-.5, 1, 1, "平空转多"), (.5, -1, 1, "平多转空"),
])
def test_public_position_change_labels(before, side, size, expected):
    row = fill(1, 10, side, 100, before)
    row["size"] = size
    assert trade_detail(row)["position_action"] == expected


def test_quotes_are_asof_not_future_or_stale(tmp_path):
    async def scenario():
        m = AccountMonitor(tmp_path / "accounts.db")
        try:
            now = time.time()
            m.quotes[("lighter-rh", "BTC")].extend([(now - 1, 99, now), (now + 1, 200, now)])
            m.quotes[("lighter", "BTC")].append((now - .5, 100, now))
            assert m.premium_at("BTC", now) == pytest.approx(-100)
            assert m.premium_at("BTC", now - 1) is None
            assert m.premium_at("BTC", now + 5) is None
        finally:
            await m.close()
    asyncio.run(scenario())


def test_consumes_only_valid_trade_fields_and_flushes_on_stop(tmp_path):
    async def scenario():
        m = AccountMonitor(tmp_path / "accounts.db")
        m.markets = {"lighter-rh": {0: "BTC"}, "lighter": {0: "BTC"}}
        now = time.time()
        row = {"trade_id_str": "9007199254740993", "market_id": 0,
               "timestamp": int(now * 1000), "price": "100", "size": "1", "usd_amount": "100",
               "ask_account_id": 10, "bid_account_id": 20, "is_maker_ask": True,
               "maker_position_size_before": "-1", "taker_position_size_before": "0"}
        m.consume("lighter-rh", {"channel": "trade:0", "trades": [row, row]})
        m.consume("lighter-rh", {"channel": "trade:0", "trades": [{**row, "price": "NaN"},
                    {**row, "is_maker_ask": "false"}, {**row, "market_id": 99}]})
        assert m.dropped == 3
        await m.stop()
        d = m.store.detail("lighter-rh", 20, 0)
        assert len(d["fills"]) == 1
        assert d["fills"][0]["id"] == "9007199254740993"
        assert d["fills"][0]["before_position"] == 0
        assert d["fills"][0]["bps"] is None
        await m.close()
    asyncio.run(scenario())


def test_start_resume_and_stop_never_create_signer_or_submit_orders(tmp_path, monkeypatch):
    async def scenario():
        m = AccountMonitor(tmp_path / "accounts.db")
        calls = []
        async def request(venue, path, **params):
            calls.append((venue, path))
            return {"order_books": [{"market_id": 0, "symbol": "BTC", "status": "active", "market_type": "perp"},
                                    {"market_id": 1, "symbol": "BTC", "status": "active", "market_type": "spot"}]}
        async def idle(*args):
            await asyncio.Event().wait()
        monkeypatch.setattr(m, "request", request)
        monkeypatch.setattr(m, "_stream", idle)
        monkeypatch.setattr(m, "_enrich", idle)
        result = await m.start({"symbols": "BTC"})
        assert result["markets"] == {"lighter-rh": 1, "lighter": 1}
        assert all(c[1] == "orderBooks" for c in calls)
        await m.close()
        restored = AccountMonitor(tmp_path / "accounts.db")
        monkeypatch.setattr(restored, "request", request)
        monkeypatch.setattr(restored, "_stream", idle)
        monkeypatch.setattr(restored, "_enrich", idle)
        await restored.resume()
        assert restored.running
        await restored.stop()
        assert restored.store.settings()["enabled"] is False
        with pytest.raises(ValueError, match="公开查询"):
            await AccountMonitor.request(restored, "lighter", "sendTx")
        await restored.close()
    asyncio.run(scenario())


def test_failed_discovery_closes_session(tmp_path, monkeypatch):
    async def scenario():
        m = AccountMonitor(tmp_path / "accounts.db")
        async def request(*args, **kwargs):
            raise TimeoutError("unavailable")
        monkeypatch.setattr(m, "request", request)
        with pytest.raises(ValueError, match="没有找到"):
            await m.start({"symbols": "BTC"})
        assert m.session is None
        assert m.tasks == []
        await m.close()
    asyncio.run(scenario())


def test_account_api_is_authenticated_and_validates_mutations(tmp_path):
    async def scenario():
        manager = TaskManager(ROOT, tmp_path / "data")
        client = TestClient(TestServer(create_app(manager, "test-password-123456")))
        await client.start_server()
        headers = {"X-Web-Request": "1"}
        try:
            assert (await client.get("/api/account-monitor")).status == 401
            assert (await client.post("/api/login", json={"password": "test-password-123456"}, headers=headers)).status == 200
            assert (await client.get("/accounts")).status == 200
            assert (await client.get("/assets/accounts.js")).status == 200
            assert (await client.post("/api/account-monitor/lighter/123/watch", json={"watched": True})).status == 403
            assert (await client.post("/api/account-monitor/lighter/123/watch", json={"watched": True}, headers=headers)).status == 200
            result = await (await client.get("/api/account-monitor/lighter/123")).json()
            assert result["account"]["watched"] == 1
            assert result["fills"] == []
            assert (await client.post("/api/account-monitor/lighter/123/refresh", json={}, headers=headers)).status == 400
            for url in ("/api/account-monitor?period=100", "/api/account-monitor?page=0", "/api/account-monitor/arcus/123", "/api/account-monitor/lighter/-1"):
                assert (await client.get(url)).status == 400
        finally:
            await client.close()
    asyncio.run(scenario())
