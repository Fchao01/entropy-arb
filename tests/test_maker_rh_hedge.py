"""Confirmed Arcus fills drive bounded RH hedges; no exchange access."""
import asyncio
import math
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.maker import execute_primary_maker
from test_arcus_maker_only import arcus_engine
from test_bps_strategy import books, execute


def rh_engine(tmp_path, long=True):
    engine = arcus_engine(tmp_path, long=long)
    engine.cfg.hedge_venue = "lighter-rh"
    engine.cfg.hedge_slippage_bps = 5
    engine.cfg.maker_timeout_sec = 5
    engine.cfg.settle_timeout_sec = 1
    engine.hedge.name = "RH"
    engine.hedge.px_round = lambda price, round_up: (
        math.ceil(price * 100 - 1e-9) if round_up else math.floor(price * 100 + 1e-9)) / 100
    engine._maker_quote_valid = lambda *args: True
    return engine


def result(status="filled", qty=0, price=None, unresolved=False, err=None):
    return dict(status=status, filled_base=qty, avg_px=price, unresolved=unresolved, err=err)


@pytest.mark.parametrize("long", [True, False])
@pytest.mark.parametrize("closing", [True, False])
def test_rh_hedges_with_own_slippage_and_retries_confirmed_zero(tmp_path, long, closing, caplog):
    async def run():
        engine = rh_engine(tmp_path, long)
        if closing:
            await execute(engine, engine._scan(time.time()))
            books(engine, -5.5 if long else 5.5)
        candidate = engine._scan(time.time())
        buy, sell, plan = candidate
        plan.slippage_bps = 0
        primary_buy = buy is engine.entropy
        attempts = []

        async def send(**kwargs):
            assert engine.entropy.maker_state.await_count > 0
            reference = engine.hedge.book.best_bid() if primary_buy else engine.hedge.book.best_ask()
            expected_bound = engine.hedge.px_round(
                reference * (1 - 5 / 1e4 if primary_buy else 1 + 5 / 1e4), round_up=primary_buy)
            assert kwargs["limit_px"] == expected_bound
            assert kwargs["is_buy"] is not primary_buy
            assert kwargs.get("reduce_only", False) is closing
            attempts.append(kwargs)
            if len(attempts) == 1:
                engine.hedge.set_book(100.01, 100.02)
                return result("canceled-too-much-slippage")
            return result(qty=kwargs["qty"], price=100.02 if kwargs["is_buy"] else 100.01)

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        await execute(engine, candidate)
        assert len(attempts) == 2
        assert attempts[0]["qty"] == attempts[1]["qty"] == plan.qty
        engine.entropy.send_taker.assert_not_awaited()
        assert sum(venue.position for venue in engine.venues.values()) == pytest.approx(0)
        assert engine.hedges == 0
        assert not engine.halted
        assert engine.execution_stats.openings == 1
        assert engine.execution_stats.snapshot(engine)["volume_usd"] == pytest.approx(engine._total_volume())
        assert "retry" in caplog.text.lower()

    asyncio.run(run())


def test_rh_zero_fill_retries_beyond_three_attempts_until_confirmed_fill(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)

        async def send(**kwargs):
            if engine.hedge.send_taker.await_count < 6:
                return result("canceled-too-much-slippage")
            return result(qty=kwargs["qty"], price=100)

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        _, hedge = await asyncio.wait_for(execute_primary_maker(engine, buy, sell, plan), 2)
        assert engine.hedge.send_taker.await_count == 6
        assert hedge[0]["filled_base"] == plan.qty
        assert not hedge[0]["err"]
        assert not hedge[0]["unresolved"]
        assert len({call.kwargs["limit_px"] for call in engine.hedge.send_taker.call_args_list}) == 1
        engine.entropy.send_taker.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("response", [
    result("timeout", unresolved=True),
    result("canceled-too-much-slippage", unresolved=True),
    result("rate-limited", err="RATE_LIMITED: rejected"),
    result("rejected", err="insufficient margin"),
    result("acknowledged"),
    result("canceled", qty=0.2, price=100),
])
def test_rh_does_not_retry_unknown_rejected_or_partial_results(tmp_path, response):
    async def run():
        engine = rh_engine(tmp_path)
        engine.hedge.send_taker = AsyncMock(return_value=response)
        buy, sell, plan = engine._scan(time.time())
        _, hedge = await execute_primary_maker(engine, buy, sell, plan)
        engine.hedge.send_taker.assert_awaited_once()
        assert hedge[0]["filled_base"] == response["filled_base"]
        if response["unresolved"]:
            assert engine.halted
        engine.entropy.send_taker.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("blocker", ["stale", "depth", "readiness", "budget", "limited"])
@pytest.mark.parametrize("long", [True, False])
def test_rh_retry_rechecks_market_and_stops_when_unavailable(tmp_path, blocker, long):
    async def run():
        engine = rh_engine(tmp_path, long)

        async def send(**kwargs):
            if blocker == "stale":
                engine.hedge.book.last_update_ts = engine.hedge.book.alive_ts = 0
            elif blocker == "depth":
                engine.hedge.set_book(100, 100.01, sz=0.001)
            elif blocker == "readiness":
                engine.hedge.ready_to_trade = lambda: False
            elif blocker == "limited":
                engine._mark_limited(engine.hedge)
            else:
                engine._venue_rate_ok = lambda venue: False
            return result("canceled-too-much-slippage")

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        results = await execute_primary_maker(engine, buy, sell, plan)
        hedge = results[1 if long else 0][0]
        engine.hedge.send_taker.assert_awaited_once()
        assert hedge["filled_base"] == 0 and hedge["err"]
        assert not hedge["unresolved"]
        engine.entropy.send_taker.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("long", [True, False])
def test_rh_retry_reprices_from_latest_bbo_instead_of_first_price_boundary(tmp_path, long):
    async def run():
        engine = rh_engine(tmp_path, long)
        calls = []

        async def send(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                engine.hedge.set_book(99.9 if long else 100.1, 99.91 if long else 100.11)
                return result("canceled-too-much-slippage")
            return result(qty=kwargs["qty"], price=kwargs["limit_px"])

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        plan.slippage_bps = 0
        results = await execute_primary_maker(engine, buy, sell, plan)
        assert len(calls) == 2
        assert calls[1]["limit_px"] != calls[0]["limit_px"]
        reference = engine.hedge.book.best_bid() if long else engine.hedge.book.best_ask()
        assert calls[1]["limit_px"] == engine.hedge.px_round(
            reference * (1 - 5 / 1e4 if long else 1 + 5 / 1e4), long)
        assert results[1 if long else 0][0]["filled_base"] == plan.qty

    asyncio.run(run())


@pytest.mark.parametrize("control", ["stop", "pause"])
def test_rh_continuous_zero_fill_retries_can_be_stopped(tmp_path, control):
    async def run():
        engine = rh_engine(tmp_path)

        async def send(**kwargs):
            if engine.hedge.send_taker.await_count == 4:
                if control == "stop":
                    engine.stop.set()
                else:
                    engine.manual_paused = True
            return result("canceled-too-much-slippage")

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        _, hedge = await asyncio.wait_for(execute_primary_maker(engine, buy, sell, plan), 2)
        assert engine.hedge.send_taker.await_count == 4
        assert hedge[0]["filled_base"] == 0 and hedge[0]["err"]
        assert not hedge[0]["unresolved"]

    asyncio.run(run())


def test_rh_retry_cancels_resting_primary_and_hedges_cancel_race_fills_once(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)
        engine.entropy.maker_state = AsyncMock(side_effect=[
            dict(filled_base=0.3, avg_px=100, terminal=False, status="OPEN", err=None),
            dict(filled_base=0.3, avg_px=100, terminal=False, status="OPEN", err=None),
            dict(filled_base=0.5, avg_px=100, terminal=True, status="CANCELED", err=None),
        ])
        attempts = []

        async def send(**kwargs):
            attempts.append(kwargs["qty"])
            if len(attempts) == 1:
                return result("canceled-too-much-slippage")
            engine.entropy.cancel_maker.assert_awaited()
            return result(qty=kwargs["qty"], price=100)

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        primary, hedge = await execute_primary_maker(engine, buy, sell, plan)
        assert attempts == pytest.approx([0.3, 0.3, 0.2])
        assert primary[0]["filled_base"] == hedge[0]["filled_base"] == 0.5
        assert engine.active_maker is None and not engine.halted

    asyncio.run(run())


def test_rh_unknown_retry_result_stops_without_third_send(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)
        engine.hedge.send_taker = AsyncMock(side_effect=[
            result("canceled-too-much-slippage"), result("timeout", unresolved=True)])
        buy, sell, plan = engine._scan(time.time())
        _, hedge = await execute_primary_maker(engine, buy, sell, plan)
        assert engine.hedge.send_taker.await_count == 2
        assert hedge[0]["unresolved"] and engine.halted

    asyncio.run(run())


def test_rh_retry_stops_when_primary_reports_a_terminal_error(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)
        engine.entropy.maker_state = AsyncMock(side_effect=[
            dict(filled_base=0.3, avg_px=100, terminal=False, status="OPEN", err=None),
            dict(filled_base=0.3, avg_px=100, terminal=True, status="CANCELED", err="primary error"),
        ])
        engine.hedge.send_taker = AsyncMock(return_value=result("canceled-too-much-slippage"))
        buy, sell, plan = engine._scan(time.time())
        await asyncio.wait_for(execute_primary_maker(engine, buy, sell, plan), 2)
        engine.hedge.send_taker.assert_awaited_once()

    asyncio.run(run())


def test_rh_retry_stops_if_price_change_puts_remaining_order_below_minimum(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)
        engine.entropy.maker_state = AsyncMock(return_value=dict(
            filled_base=0.2, avg_px=100, terminal=True, status="CANCELED", err=None))

        async def send(**kwargs):
            engine.hedge.set_book(10, 10.01)
            return result("canceled-too-much-slippage")

        engine.hedge.send_taker = AsyncMock(side_effect=send)
        buy, sell, plan = engine._scan(time.time())
        _, hedge = await asyncio.wait_for(execute_primary_maker(engine, buy, sell, plan), 2)
        engine.hedge.send_taker.assert_awaited_once()
        assert "minimum" in hedge[0]["err"]

    asyncio.run(run())


def test_other_venue_routes_keep_existing_hedge_slippage_policy(tmp_path):
    async def run():
        engine = rh_engine(tmp_path)
        engine.cfg.hedge_venue = "lighter"
        buy, sell, plan = engine._scan(time.time())
        plan.slippage_bps = 0
        reference = engine.hedge.book.best_bid()
        engine.hedge.send_taker = AsyncMock(return_value=result("canceled-too-much-slippage"))
        await execute_primary_maker(engine, buy, sell, plan)
        engine.hedge.send_taker.assert_awaited_once()
        assert engine.hedge.send_taker.call_args.kwargs["limit_px"] == engine.hedge.px_round(reference, True)

    asyncio.run(run())
