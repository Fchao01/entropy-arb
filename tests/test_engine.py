"""Engine signal math: midline band directions, inventory ladder, scan.

Run:  python3 -m pytest tests/  (or  python3 tests/test_engine.py)
"""
import asyncio
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def make_cfg(midline=5.0, upper=4.0, lower=3.0):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(f"""
thresholds:
  midline_bps: {midline}
  upper_bps: {upper}
  lower_bps: {lower}
execution:
  premium_persist_sec: 0.0
""")
    f.close()
    return load_config(f.name, NO_ENV,
                       symbol="SNDK", hedge_venue="lighter")


class StubVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash = 0.0, 0.0
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])


def make_engine(**thr):
    cfg = make_cfg(**thr)
    eng = Engine(cfg)
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def approx(a, b, tol=1e-9):
    assert abs(a - b) <= tol, f"{a} != {b}"


def test_eff_threshold_directions():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    e, h = eng.entropy, eng.hedge
    # sell entropy: hurdle = midline + upper = 9
    approx(eng._eff_threshold(buy=h, sell=e), 9.0)
    # buy entropy: hurdle = lower - midline = -2 (unwind side of a positive
    # midline is deliberately cheap — that's what completes the round trip)
    approx(eng._eff_threshold(buy=e, sell=h), -2.0)
    # round trip nets upper + lower regardless of midline sign
    for m in (-7.0, 0.0, 12.5):
        eng.cfg.midline_bps = m
        total = eng._eff_threshold(buy=h, sell=e) + eng._eff_threshold(buy=e, sell=h)
        approx(total, 7.0)


def test_rh_primary_uses_primary_premium_and_parallel_orders():
    from unittest.mock import AsyncMock, patch
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.cfg.primary_venue = "lighter-rh"
    eng.entropy.name, eng.hedge.name = "RH", "ARCUS"
    eng.entropy.volume_usd = eng.hedge.volume_usd = 0.0
    for v in (eng.entropy, eng.hedge):
        v.px_round = lambda px, round_up: px
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.hedge.fee_bps = 2.25
    approx(eng.premium_bps(), 15.0)
    assert eng.direction_key(True) == "sell_primary"
    eng.entropy.position, eng.hedge.position = -1.0, 1.0
    approx(eng._eff_threshold(eng.entropy, eng.hedge), 1.0)
    eng.entropy.position = eng.hedge.position = 0
    plan, reason = eng._plan(eng.hedge, eng.entropy, 100)
    assert plan is not None, reason
    async def run():
        eng.ensure_async_state()
        gate = asyncio.Event()
        started = []
        async def send(v, *, is_buy, qty, limit_px):
            started.append((v.name, is_buy, qty))
            if len(started) == 2:
                gate.set()
            await asyncio.wait_for(gate.wait(), 1)
            return {"status": "filled", "filled_base": qty, "avg_px": limit_px,
                    "err": None, "unresolved": False}, 1.0
        eng._timed_send = AsyncMock(side_effect=send)
        with patch.object(eng, "_log_csv"):
            assert not await eng._execute(eng.hedge, eng.entropy, plan)
        assert {row[:2] for row in started} == {("RH", False), ("ARCUS", True)}
        assert eng.recent_trades[-1]["direction"] == "sell_primary"
        approx(eng.entropy.position + eng.hedge.position, 0)
    asyncio.run(run())


def test_live_non_hl_primary_initializes_without_hl_methods():
    from unittest.mock import AsyncMock, Mock
    eng = make_engine()
    eng.cfg.primary_venue = "lighter-rh"
    eng.cfg.entropy.kind, eng.cfg.hedge.kind = "lighter", "arcus"
    from entropy_arb.config import LighterCreds, ArcusCreds
    eng.cfg.entropy.lighter_creds = LighterCreds(1, 1, "fixture")
    eng.cfg.hedge.arcus_creds = ArcusCreds("0x" + "01" * 20, 0, "02" * 32)
    eng.cfg.recorder_enabled = False
    created = []
    def make(conf):
        v = StubVenue(conf.key, "RH" if conf.key == "entropy" else "ARCUS")
        v.kind, v.conf = conf.kind, conf
        v.load_market = AsyncMock()
        v.init_signer = Mock()
        v.fetch_position = AsyncMock(return_value=0.0)
        v.close = AsyncMock()
        def start(stop, notify, live):
            assert live
            stop.set()
            return []
        v.start_tasks = start
        created.append(v)
        return v
    eng._make_venue = make
    async def run():
        eng.ensure_async_state()
        await eng._run_inner()
    asyncio.run(run())
    assert len(created) == 2
    for v in created:
        v.init_signer.assert_called_once()
        v.fetch_position.assert_awaited_once()
        v.close.assert_awaited_once()


def test_inventory_ladder():
    eng = make_engine()
    eng.cfg.inventory_scale_bps, eng.cfg.inventory_floor_frac = 10.0, 0.5
    e, h = eng.entropy, eng.hedge
    e.set_book(99.9, 100.1)   # mid 100
    h.set_book(99.9, 100.1)
    approx(eng._inv_add_bps(e, h), 0.0)          # flat: dead zone
    e.position = 90.0                             # long $9k of $10k cap
    v = eng._inv_add_bps(e, h)                    # buying entropy adds long
    assert 7.5 < v < 8.5, v                       # u=0.9 -> ~+8
    approx(eng._inv_add_bps(h, e), 0.0)           # selling entropy reduces
    h.position = -90.0                            # hedge short $9k too
    v2 = eng._inv_add_bps(e, h)                   # both legs add -> max()
    assert abs(v2 - v) < 0.6, (v, v2)             # max, not sum


def run_scan(eng):
    async def go():
        # first pass arms the direction, second passes the persistence gate
        # (premium_persist_sec is 0 in the test config)
        eng._scan(__import__("time").time())
        return eng._scan(__import__("time").time())
    return asyncio.run(go())


def test_scan_fires_sell_entropy_above_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 15 bps rich vs hedge: above midline+upper=9 -> sell entropy
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert sell.key == "entropy" and buy.key == "hedge"
    assert plan.exp_edge_usd > 0


def test_scan_quiet_inside_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps rich = exactly on the midline: inside the band, no trade
    eng.entropy.set_book(100.04, 100.06)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is None


def test_scan_fires_buy_entropy_below_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps CHEAP (premium -5): below midline-lower=+2 -> buy entropy
    eng.entropy.set_book(99.94, 99.96)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert buy.key == "entropy" and sell.key == "hedge"


def test_scan_respects_position_caps():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.entropy.position = -100.0   # entropy already short at its cap
    eng.entropy.cap_usd = 10000.0
    eng.hedge.position = 100.0
    eng.hedge.cap_usd = 10000.0
    assert run_scan(eng) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
