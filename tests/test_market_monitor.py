import asyncio
import time

import pytest

from entropy_arb import market_monitor
from entropy_arb.book import OrderBook
from entropy_arb.market_monitor import MonitorRules, MultiVenueMonitor, TripleVenueMonitor, _net_edge, normalize_symbol


def venue(key, label, bid, ask):
    class Stub:
        pass
    value = Stub()
    value.key, value.name, value.fee_bps = key, label, 0.0
    value.conf = Stub()
    value.conf.symbol = key
    value.book = OrderBook()
    value.book.apply_hl([[{"px": str(bid), "sz": "1"}],
                         [{"px": str(ask), "sz": "1"}]])
    return value


def test_symbol_normalization_keeps_base_asset():
    assert normalize_symbol("io:ETH") == "ETH"
    assert normalize_symbol("ETH-USD") == "ETH"
    assert normalize_symbol("ETH-USDC") == "ETH"


def test_net_edge_matches_fee_aware_execution_inequality():
    assert round(_net_edge(101, 100, 0, 0), 6) == 100.0
    assert _net_edge(100, 100, 5, 5) < 0


def test_triple_snapshot_follows_rh_primary_semantics():
    monitor = TripleVenueMonitor()
    monitor.symbol = "ETH"
    monitor.rules = MonitorRules(primary_key="rh")
    monitor.venues = {
        "entropy": venue("entropy", "ENTROPY", 101, 102),
        "rh": venue("rh", "RH", 99, 100),
        "arcus": venue("arcus", "ARCUS", 98, 99),
    }
    pairs = monitor.snapshot()["pairs"]
    assert [pair["key"] for pair in pairs] == ["rh-entropy", "entropy-arcus", "rh-arcus"]
    assert pairs[0]["script_compatible"] is True
    assert pairs[1]["script_compatible"] is False
    assert pairs[2]["script_compatible"] is True
    assert pairs[0]["left"] == "RH" and pairs[0]["right"] == "ENTROPY"
    assert "sell_hurdle_bps" not in pairs[0]
    assert "buy_hurdle_bps" not in pairs[0]
    assert "sell_signal" not in pairs[0]
    assert "buy_signal" not in pairs[0]


def test_discovery_includes_two_market_symbols_without_order_specs(monkeypatch):
    async def public_json(session, method, url, **kwargs):
        if url.endswith("/orderBooks"):
            return {"order_books": [
                {"symbol": "BTC", "status": "active", "min_base_amount": "unknown"},
                {"symbol": "ETH", "status": "active"},
                {"symbol": "SNDK", "status": "active", "supported_size_decimals": 3,
                 "min_base_amount": 0.001},
                {"symbol": "RH_ONLY", "status": "active"},
                {"symbol": "INACTIVE", "status": "inactive"},
            ]}
        if url.endswith("/v1/markets"):
            return {"markets": [
                {"marketDisplayName": symbol + "-USD", "status": "ONLINE",
                 "type": "PERPETUAL", "quoteAsset": "USD", "stepSize": "0.001",
                 "minOrderSize": "0.001"}
                for symbol in ("BTC", "SNDK", "NBIS", "INACTIVE")
            ]}
        return {"universe": [
            {"name": "io:" + symbol, "szDecimals": 3}
            for symbol in ("ETH", "SNDK", "NBIS", "ENTROPY_ONLY")
        ]}

    monkeypatch.setattr(market_monitor, "_json", public_json)
    result = asyncio.run(market_monitor.discover_candidates(None))
    rows = {row["symbol"]: row for row in result["symbols"]}
    assert set(rows) == {"BTC", "ETH", "SNDK", "NBIS"}
    assert set(rows["BTC"]["venues"]) == {"lighter_rh", "arcus"}
    assert rows["BTC"]["compatible"] is False
    assert rows["BTC"]["monitorable"] is True
    assert result["compatible_symbols"] == ["SNDK"]
    assert result["monitorable_symbols"] == ["BTC", "ETH", "NBIS", "SNDK"]


def test_discovery_keeps_other_pairs_when_one_market_list_fails(monkeypatch):
    async def public_json(session, method, url, **kwargs):
        if url.endswith("/orderBooks"):
            raise TimeoutError("RH unavailable")
        if url.endswith("/v1/markets"):
            return {"markets": [{"marketDisplayName": "NBIS-USD", "status": "ONLINE",
                                 "type": "PERPETUAL", "quoteAsset": "USD"}]}
        return {"universe": [{"name": "io:NBIS", "szDecimals": 3}]}

    monkeypatch.setattr(market_monitor, "_json", public_json)
    result = asyncio.run(market_monitor.discover_candidates(None))
    assert result["monitorable_symbols"] == ["NBIS"]
    assert "lighter_rh" in result["errors"]


def test_two_venue_snapshot_displays_positive_negative_and_zero_edges():
    monitor = TripleVenueMonitor()
    monitor.venues = {"rh": venue("rh", "RH", 100, 101),
                      "arcus": venue("arcus", "ARCUS", 101, 102)}
    pairs = monitor.snapshot()["pairs"]
    assert len(pairs) == 1
    assert pairs[0]["key"] == "rh-arcus"
    assert pairs[0]["mid_spread_bps"] < 0
    assert pairs[0]["sell_left_buy_right_bps"] < 0
    assert pairs[0]["buy_left_sell_right_bps"] == 0
    monitor.venues["arcus"] = venue("arcus", "ARCUS", 102, 103)
    assert monitor.snapshot()["pairs"][0]["buy_left_sell_right_bps"] > 0
    monitor.venues["arcus"].book.alive_ts = time.time() - 11
    pair = monitor.snapshot()["pairs"][0]
    assert pair["mid_spread_bps"] is None
    assert pair["sell_left_buy_right_bps"] is None
    assert pair["buy_left_sell_right_bps"] is None


def test_rh_spread_direction_does_not_change_when_order_specs_are_missing():
    monitor = TripleVenueMonitor()
    monitor.venues = {"rh": venue("rh", "RH", 100, 101),
                      "entropy": venue("entropy", "ENTROPY", 102, 103)}
    monitor.market_specs = {"rh": {"step": None}, "entropy": {"step": None}}
    pair = monitor.snapshot()["pairs"][0]
    assert pair["left"] == "RH"
    assert pair["right"] == "ENTROPY"
    assert pair["script_compatible"] is False
    assert pair["mid_spread_bps"] < 0


@pytest.mark.parametrize("failed_keys", [set(), {"entropy"}, {"entropy", "rh"}])
def test_monitor_connects_available_venues_and_tolerates_third_failure(monkeypatch, failed_keys):
    created = {}

    class PublicVenue:
        def __init__(self, conf, *args):
            self.conf, self.key, self.name = conf, conf.key, conf.label
            self.book, self.fee_bps = OrderBook(), 0
            self.closed = False
            created[self.key] = self

        async def load_market(self):
            if self.key in failed_keys:
                raise RuntimeError("unavailable")

        def start_tasks(self, stop, touch, trade):
            assert trade is False
            return []

        async def close(self):
            self.closed = True

    for name in ("HLVenue", "LighterVenue", "ArcusVenue"):
        monkeypatch.setattr(market_monitor, name, PublicVenue)

    async def run():
        monitor = TripleVenueMonitor()
        sources = {key: {"symbol": "BTC", "step": 0.001, "min_base": 0.001}
                   for key in ("lighter_rh", "arcus")}
        if failed_keys:
            sources["entropy"] = {"symbol": "io:BTC", "step": 0.001, "min_base": 0.001}
        if len(failed_keys) > 1:
            with pytest.raises(RuntimeError, match="至少需要两个"):
                await monitor.start({"symbol": "BTC", "venues": sources}, MonitorRules())
            assert monitor.session is None
        else:
            result = await monitor.start({"symbol": "BTC", "venues": sources}, MonitorRules())
            assert result["running"] is True
            assert {row["key"] for row in result["venues"]} == {"rh", "arcus"}
            assert len(result["pairs"]) == 1
            if failed_keys:
                assert result["errors"] == {"entropy": "unavailable"}
            await monitor.stop()
        assert created["rh"].closed and created["arcus"].closed

    asyncio.run(run())


def test_overview_starts_all_two_market_symbols_without_spec_filter_or_limit(monkeypatch):
    class PublicMonitor:
        def __init__(self, *args):
            self.errors = {}

        async def start(self, candidate, rules):
            self.symbol = candidate["symbol"]

        def snapshot(self):
            return {"symbol": self.symbol}

        async def stop(self):
            pass

    monkeypatch.setattr(market_monitor, "TripleVenueMonitor", PublicMonitor)

    async def run():
        monitor = MultiVenueMonitor()
        candidates = [{"symbol": f"TEST{index}", "compatible": False,
                       "venues": {"lighter_rh": {}, "arcus": {}}}
                      for index in range(41)]
        result = await monitor.start_all(candidates, MonitorRules())
        assert result["count"] == 41
        assert result["selected_symbol"] in result["symbols"]
        await monitor.stop()

    asyncio.run(run())
