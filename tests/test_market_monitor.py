from entropy_arb.book import OrderBook
from entropy_arb.market_monitor import MonitorRules, TripleVenueMonitor, _net_edge, normalize_symbol


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
    monitor.rules = MonitorRules(midline_bps=2, upper_bps=4, lower_bps=3,
                                 primary_key="rh")
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
    assert pairs[0]["sell_hurdle_bps"] == 6
    assert pairs[0]["buy_hurdle_bps"] == 1
