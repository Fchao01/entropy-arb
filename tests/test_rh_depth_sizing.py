"""Arcus maker size follows executable RH depth without changing signals."""
import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.maker import MakerOrder, rh_hedge_depth
from test_bps_strategy import execute
from test_maker_rh_hedge import rh_engine


@pytest.fixture(autouse=True)
def event_loop_for_synchronous_planning():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    loop.close()
    asyncio.set_event_loop(None)


def depth_engine(tmp_path, primary_buy=True):
    engine = rh_engine(tmp_path, primary_buy)
    engine.cfg.max_order_notional = 500
    engine.cfg.leg_slippage_bps = 50
    engine.cfg.take_fraction = 0.5
    if not primary_buy:
        engine.entropy.set_book(100.2, 100.21)
    engine.hedge.set_book(100, 100.01)
    engine.hedge.book.bids = {100: 1, 99.98: 0.5, 99.9: 100}
    engine.hedge.book.asks = {100.01: 1, 100.03: 0.5, 100.11: 100}
    return engine


@pytest.mark.parametrize("primary_buy", [True, False])
@pytest.mark.parametrize("fraction", [0.25, 0.5, 1])
def test_rh_depth_caps_opening_size_using_correct_side_and_existing_fraction(tmp_path, primary_buy, fraction):
    engine = depth_engine(tmp_path, primary_buy)
    engine.cfg.take_fraction = fraction
    depth = rh_hedge_depth(engine, primary_buy)
    assert depth["depth_base"] == 1.5
    assert depth["capacity_base"] == 1.5 * fraction
    buy, sell = (engine.entropy, engine.hedge) if primary_buy else (engine.hedge, engine.entropy)
    before = engine.cfg.bps_entry, engine.cfg.bps_exit, engine.cfg.max_order_notional
    plan = engine.bps_strategy.plan(buy, sell)
    assert plan.qty == 1.5 * fraction
    assert (engine.cfg.bps_entry, engine.cfg.bps_exit, engine.cfg.max_order_notional) == before
    assert plan.buy_notional <= 500 and plan.sell_notional <= 500
    engine.hedge.book.bids = {100: 0.8} if primary_buy else {100: 1000}
    engine.hedge.book.asks = {100.01: 1000} if primary_buy else {100.01: 0.8}
    assert engine.bps_strategy.plan(buy, sell).qty == pytest.approx(0.8 * fraction)


@pytest.mark.parametrize("primary_buy", [True, False])
def test_rh_depth_cap_applies_to_close_without_exceeding_positions(tmp_path, primary_buy):
    engine = depth_engine(tmp_path, primary_buy)
    engine.entropy.position = -2 if primary_buy else 2
    engine.hedge.position = -engine.entropy.position
    buy, sell = (engine.entropy, engine.hedge) if primary_buy else (engine.hedge, engine.entropy)
    plan = engine.bps_strategy.plan(buy, sell, closing=True)
    assert plan.qty == 0.75 and plan.action == "bps_close"
    engine.entropy.position = -0.2 if primary_buy else 0.2
    engine.hedge.position = -engine.entropy.position
    assert engine.bps_strategy.plan(buy, sell, closing=True).qty == 0.2


@pytest.mark.parametrize("blocker", ["stale", "readiness", "empty", "minimum", "limited"])
def test_no_primary_order_when_rh_depth_is_unavailable_or_below_minimum(tmp_path, blocker):
    async def run():
        engine = depth_engine(tmp_path)
        candidate = engine._scan(time.time())
        assert candidate is not None
        if blocker == "stale":
            engine.hedge.book.alive_ts = 0
        elif blocker == "readiness":
            engine.hedge.ready_to_trade = lambda: False
        elif blocker == "empty":
            engine.hedge.book.bids.clear()
        elif blocker == "minimum":
            engine.hedge.book.bids = {100: 0.001}
        else:
            engine._mark_limited(engine.hedge)
        await execute(engine, candidate)
        engine.entropy.place_maker.assert_not_awaited()
        engine.hedge.send_taker.assert_not_awaited()
        assert engine.entropy.position == engine.hedge.position == 0
        assert not engine.halted

    asyncio.run(run())


@pytest.mark.parametrize("primary_buy", [True, False])
def test_rh_size_is_rechecked_immediately_before_primary_submission(tmp_path, primary_buy, caplog):
    async def run():
        engine = depth_engine(tmp_path, primary_buy)
        engine.hedge.book.bids = {100: 100}
        engine.hedge.book.asks = {100.01: 100}
        candidate = engine._scan(time.time())
        assert candidate[2].qty > 1
        engine.hedge.book.bids = {100: 0.4}
        engine.hedge.book.asks = {100.01: 0.4}
        await execute(engine, candidate)
        assert engine.entropy.place_maker.call_args.kwargs["qty"] == 0.2
        assert engine.hedge.send_taker.call_args.kwargs["qty"] == 0.2
        assert sum(venue.position for venue in engine.venues.values()) == pytest.approx(0)
        assert engine.hedges == 0 and not engine.halted
        engine.entropy.send_taker.assert_not_awaited()

    caplog.set_level("INFO", logger="engine")
    asyncio.run(run())
    assert "[MAKER SIZE]" in caplog.text
    assert "capacity=0.2" in caplog.text


@pytest.mark.parametrize("primary_buy", [True, False])
def test_resting_quote_checks_remaining_unhedged_size_not_original_quantity(tmp_path, primary_buy):
    engine = depth_engine(tmp_path, primary_buy)
    engine._maker_quote_valid = type(engine)._maker_quote_valid.__get__(engine)
    buy, sell = (engine.entropy, engine.hedge) if primary_buy else (engine.hedge, engine.entropy)
    plan = engine.bps_strategy.plan(buy, sell)
    engine.bps_strategy.before_execution(plan)
    price = plan.buy_limit if primary_buy else plan.sell_limit
    assert engine._maker_quote_valid(buy, sell, plan, price)
    engine.hedge.book.bids = {100: 0.7}
    engine.hedge.book.asks = {100.01: 0.7}
    assert not engine._maker_quote_valid(buy, sell, plan, price)
    engine.active_maker = {"filled": 0.4, "hedged": 0.4}
    assert engine._maker_quote_valid(buy, sell, plan, price)
    engine.active_maker["hedged"] = 0.3
    assert not engine._maker_quote_valid(buy, sell, plan, price)


def test_resting_quote_cancels_on_depth_drop_and_hedges_last_confirmed_fill(tmp_path):
    async def run():
        engine = depth_engine(tmp_path)
        engine._maker_quote_valid = type(engine)._maker_quote_valid.__get__(engine)
        candidate = engine._scan(time.time())

        async def place(**kwargs):
            engine.hedge.book.bids = {100: 0.2}
            return MakerOrder("depth-drop", kwargs["limit_px"], time.time())

        async def state(order):
            canceled = bool(engine.entropy.cancel_maker.await_count)
            return dict(filled_base=0.1 if canceled else 0, avg_px=order.limit_px if canceled else None,
                        terminal=canceled, status="CANCELED" if canceled else "OPEN", err=None)

        engine.entropy.place_maker = AsyncMock(side_effect=place)
        engine.entropy.maker_state = AsyncMock(side_effect=state)
        await execute(engine, candidate)
        engine.entropy.cancel_maker.assert_awaited()
        engine.entropy.place_maker.assert_awaited_once()
        engine.hedge.send_taker.assert_awaited_once()
        assert engine.hedge.send_taker.call_args.kwargs["qty"] == 0.1
        assert engine.entropy.position == pytest.approx(-engine.hedge.position)
        assert not engine.halted

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["threshold", "spread"])
@pytest.mark.parametrize("primary_buy", [True, False])
def test_non_bps_arcus_rh_maker_plans_also_respect_rh_depth(tmp_path, mode, primary_buy):
    engine = depth_engine(tmp_path, primary_buy)
    engine.cfg.strategy_mode = mode
    engine.bps_strategy = None
    engine.cfg.midline_bps = engine.cfg.upper_bps = engine.cfg.lower_bps = -100
    engine.cfg.spread_bps = -100
    buy, sell = (engine.entropy, engine.hedge) if primary_buy else (engine.hedge, engine.entropy)
    plan, _ = engine._plan(buy, sell, 500)
    assert plan is not None
    assert plan.qty <= 0.75


def test_other_routes_keep_existing_depth_sizing(tmp_path):
    engine = depth_engine(tmp_path)
    engine.cfg.hedge_venue = "lighter"
    assert rh_hedge_depth(engine, True) is None
    plan = engine.bps_strategy.plan(engine.entropy, engine.hedge)
    assert plan.qty > 0.75


@pytest.mark.parametrize("slippage", [0, 5])
def test_rh_depth_respects_tick_rounded_price_boundary(tmp_path, slippage):
    engine = depth_engine(tmp_path, False)
    engine.cfg.hedge_slippage_bps = slippage
    engine.hedge.book.asks = {100: 1, 100.05: 2, 100.06: 100}
    depth = rh_hedge_depth(engine, False)
    assert depth["limit_px"] == (100 if slippage == 0 else 100.05)
    assert depth["depth_base"] == (1 if slippage == 0 else 3)


@pytest.mark.parametrize("price,size", [(float("nan"), 1), (100, float("inf")), (100, -1)])
def test_invalid_rh_depth_cannot_create_a_positive_capacity(tmp_path, price, size):
    engine = depth_engine(tmp_path)
    engine.hedge.book.bids = {price: size}
    assert rh_hedge_depth(engine, True)["capacity_base"] == 0


def test_depth_sizing_keeps_order_and_position_limits(tmp_path):
    engine = depth_engine(tmp_path)
    engine.cfg.max_order_notional = 20
    engine.cfg.bps_max_position_usd = 15
    plan = engine.bps_strategy.plan(engine.entropy, engine.hedge)
    assert plan is not None and plan.qty <= 0.15
    assert plan.buy_notional <= 15 and plan.sell_notional <= 15
