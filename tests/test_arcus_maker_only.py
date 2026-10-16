"""Arcus maker-only opening, closing and residual orders use fake venues."""
import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.maker import MakerOrder
from entropy_arb.signals import execution_signals
from test_arcus import signed_venue
from test_bps_strategy import books, engine_at, execute


def arcus_engine(tmp_path, *, long=True):
    engine = engine_at(tmp_path, maker=True, long=long)
    engine.cfg.primary_venue = "arcus"
    engine.cfg.primary.kind = "arcus"
    engine.cfg.primary.maker_fee_bps = 0.2
    engine.cfg.primary.fee_bps = engine.entropy.fee_bps = 20
    engine.cfg.max_order_notional = 100
    engine.cfg.maker_cancel_policy = "timeout"
    engine.cfg.maker_timeout_sec = 0.005
    engine.entropy.name = "ARCUS"
    engine.entropy.reduce_only_min_base = engine.entropy.min_base
    engine.entropy.reduce_only_min_quote = 0
    engine.entropy.size_decimals = 7
    engine.hedge.reduce_only_min_base = engine.hedge.min_base
    engine.hedge.reduce_only_min_quote = 0

    async def place(**kwargs):
        order = MakerOrder("own-order", kwargs["limit_px"], time.time())
        order.qty = kwargs["qty"]
        return order

    async def filled(order):
        return dict(filled_base=order.qty, avg_px=order.limit_px,
                    terminal=True, status="FILLED", err=None)

    engine.entropy.place_maker = AsyncMock(side_effect=place)
    engine.entropy.maker_state = AsyncMock(side_effect=filled)
    engine.entropy.cancel_maker = AsyncMock()
    engine.ensure_async_state()
    return engine


@pytest.mark.parametrize("long", [True, False])
@pytest.mark.parametrize("executable", [True, False])
def test_arcus_maker_mode_opens_and_closes_without_any_arcus_ioc(tmp_path, long, executable):
    async def run():
        engine = arcus_engine(tmp_path, long=long)
        engine.cfg.bps_price_basis = "executable" if executable else "mid"
        assert engine.cfg.primary_maker_only
        await execute(engine, engine._scan(time.time()))
        assert engine.bps_strategy.entries == 1
        engine.entropy.send_taker.assert_not_awaited()
        assert not engine.entropy.place_maker.call_args.kwargs.get("reduce_only", False)
        close_calls = engine.hedge.send_taker.await_count
        for _ in range(4):
            if engine.bps_strategy.flat():
                break
            books(engine, -5.5 if long else -6.5 if executable else 5.5)
            closing = engine._scan(time.time())
            assert closing is not None and closing[2].action == "bps_close"
            primary_fee = closing[2].sell_fee if long else closing[2].buy_fee
            assert primary_fee == pytest.approx(0.2 / 1e4)
            await execute(engine, closing)
        assert engine.bps_strategy.flat()
        assert not engine.halted
        assert engine.bps_strategy.entries == 0
        engine.entropy.send_taker.assert_not_awaited()
        assert all(call.kwargs["reduce_only"] for call in engine.entropy.place_maker.call_args_list[1:])
        assert all(call.kwargs["is_buy"] is (not long)
                   for call in engine.entropy.place_maker.call_args_list[1:])
        assert all(call.kwargs["reduce_only"]
                   for call in engine.hedge.send_taker.call_args_list[close_calls:])
        assert engine.total_fees_usd < 0.01

    asyncio.run(run())


def test_arcus_resting_close_rechecks_exit_signal_and_hedge_readiness(tmp_path):
    async def run():
        engine = arcus_engine(tmp_path)
        await execute(engine, engine._scan(time.time()))
        books(engine, -5.5)
        buy, sell, plan = engine._scan(time.time())
        price = plan.sell_limit
        assert engine._maker_quote_valid(buy, sell, plan, price)
        books(engine, -8)
        assert not engine._maker_quote_valid(buy, sell, plan, price)
        books(engine, -5.5)
        engine.hedge.book.alive_ts = 0
        assert not engine._maker_quote_valid(buy, sell, plan, price)

    asyncio.run(run())


def test_arcus_rejected_close_never_sends_the_other_leg_first(tmp_path):
    async def run():
        engine = arcus_engine(tmp_path)
        await execute(engine, engine._scan(time.time()))
        engine.hedge.send_taker.reset_mock()
        engine.entropy.place_maker = AsyncMock(return_value=MakerOrder(
            "rejected-close", 100, time.time(), rejected=True))
        books(engine, -5.5)
        await execute(engine, engine._scan(time.time()))
        engine.entropy.send_taker.assert_not_awaited()
        engine.hedge.send_taker.assert_not_awaited()
        assert not engine.halted

    asyncio.run(run())


def test_arcus_shutdown_settles_tracked_residual_without_starting_another_order(tmp_path):
    async def run():
        engine = arcus_engine(tmp_path)
        engine.cfg.maker_timeout_sec = 1
        engine.entropy.position = 0.5
        canceled = asyncio.Event()

        async def state(order):
            await canceled.wait()
            return dict(filled_base=0, avg_px=None, terminal=True, status="CANCELED", err=None)

        async def cancel(order):
            canceled.set()

        engine.entropy.maker_state = AsyncMock(side_effect=state)
        engine.entropy.cancel_maker = AsyncMock(side_effect=cancel)
        controller = asyncio.create_task(engine._maybe_hedge())
        for _ in range(20):
            if engine.active_maker is not None:
                break
            await asyncio.sleep(0.001)
        assert engine.active_maker is not None
        tracked = list(engine._exec_tasks)
        assert tracked
        controller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await controller
        engine.stop.set()
        await asyncio.wait_for(asyncio.gather(*tracked), 1)
        engine.entropy.cancel_maker.assert_awaited()
        engine.entropy.place_maker.assert_awaited_once()
        assert engine.active_maker is None
        assert engine.halted and engine.bps_strategy.pending
        engine.entropy.send_taker.assert_not_awaited()
        engine.hedge.send_taker.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("long", [True, False])
def test_arcus_close_hedges_only_after_each_new_confirmed_fill(tmp_path, long):
    async def run():
        engine = arcus_engine(tmp_path, long=long)
        await execute(engine, engine._scan(time.time()))
        engine.hedge.send_taker.reset_mock()
        engine.cfg.maker_timeout_sec = 5
        engine.cfg.settle_timeout_sec = 1
        books(engine, -5.5 if long else 5.5)
        candidate = engine._scan(time.time())
        assert candidate[2].qty > 0.4
        observations = iter([
            (0.0, False, []),
            (0.2, False, []),
            (0.2, False, [0.2]),
            (0.4, True, [0.2]),
        ])

        async def state(order):
            filled, terminal, expected_hedges = next(observations)
            assert [call.kwargs["qty"] for call in engine.hedge.send_taker.call_args_list] == pytest.approx(expected_hedges)
            return dict(filled_base=filled, avg_px=order.limit_px if filled else None,
                        terminal=terminal, status="CANCELED" if terminal else "OPEN", err=None)

        engine.entropy.maker_state = AsyncMock(side_effect=state)
        await execute(engine, candidate)
        assert engine.entropy.maker_state.await_count == 4
        assert [call.kwargs["qty"] for call in engine.hedge.send_taker.call_args_list] == pytest.approx([0.2, 0.2])
        assert all(call.kwargs["is_buy"] is long and call.kwargs["reduce_only"]
                   for call in engine.hedge.send_taker.call_args_list)
        engine.entropy.send_taker.assert_not_awaited()
        assert not engine.halted
        assert sum(venue.position for venue in engine.venues.values()) == pytest.approx(0)

    asyncio.run(run())


@pytest.mark.parametrize("long", [True, False])
def test_arcus_partial_close_and_cancel_race_only_hedges_confirmed_reductions(tmp_path, long):
    async def run():
        engine = arcus_engine(tmp_path, long=long)
        await execute(engine, engine._scan(time.time()))
        engine.hedge.send_taker.reset_mock()
        books(engine, -5.5 if long else 5.5)
        candidate = engine._scan(time.time())
        assert candidate[2].qty > 0.4
        engine.entropy.maker_state = AsyncMock(side_effect=[
            dict(filled_base=0.2, avg_px=100, terminal=False, status="OPEN", err=None),
            dict(filled_base=0.4, avg_px=100, terminal=True, status="CANCELED", err=None),
        ])
        engine.manual_paused = True
        await execute(engine, candidate)
        assert [call.kwargs["qty"] for call in engine.hedge.send_taker.call_args_list] == pytest.approx([0.2, 0.2])
        assert all(call.kwargs["reduce_only"] for call in engine.hedge.send_taker.call_args_list)
        assert engine.entropy.cancel_maker.await_count >= 1
        assert engine.active_maker is None
        assert not engine.halted
        engine.entropy.send_taker.assert_not_awaited()
        assert sum(venue.position for venue in engine.venues.values()) == pytest.approx(0)

    asyncio.run(run())


@pytest.mark.parametrize("unknown", [True, False])
def test_arcus_unfilled_or_unconfirmed_close_never_falls_back_to_taker(tmp_path, unknown):
    async def run():
        engine = arcus_engine(tmp_path)
        await execute(engine, engine._scan(time.time()))
        positions = [venue.position for venue in engine.venues.values()]
        engine.hedge.send_taker.reset_mock()
        engine.entropy.maker_state = AsyncMock(return_value=dict(
            filled_base=0, avg_px=None, terminal=not unknown,
            status="OPEN" if unknown else "CANCELED", err=None))
        books(engine, -5.5)
        await execute(engine, engine._scan(time.time()))
        assert [venue.position for venue in engine.venues.values()] == positions
        engine.entropy.send_taker.assert_not_awaited()
        engine.hedge.send_taker.assert_not_awaited()
        assert bool(engine.halted) is unknown
        if unknown:
            assert engine.active_maker["state"] == "unresolved"
            assert engine.bps_strategy.pending

    asyncio.run(run())


@pytest.mark.parametrize("net", [0.0400394, -0.0400394])
def test_arcus_residual_repair_is_post_only_reduce_only_without_other_leg_orders(tmp_path, net):
    async def run():
        engine = arcus_engine(tmp_path)
        engine.entropy.position = net
        engine.hedge.position = 0
        assert await engine._hedge(net)
        kwargs = engine.entropy.place_maker.call_args.kwargs
        assert kwargs["reduce_only"] is True
        assert kwargs["is_buy"] is (net < 0)
        assert kwargs["qty"] == pytest.approx(abs(net))
        expected = engine.entropy.book.best_bid() if net < 0 else engine.entropy.book.best_ask()
        assert kwargs["limit_px"] == expected
        assert engine.entropy.position == pytest.approx(0)
        assert engine.active_maker is None
        assert not engine.bps_strategy.pending
        assert not engine.halted
        engine.entropy.send_taker.assert_not_awaited()
        engine.hedge.send_taker.assert_not_awaited()
        assert engine.total_fees_usd < 0.001

    asyncio.run(run())


def test_arcus_ioc_is_rejected_by_maker_only_execution_guard(tmp_path):
    async def run():
        engine = arcus_engine(tmp_path)
        for reduce_only in (True, False):
            info, _ = await engine._timed_send(engine.entropy, is_buy=True, qty=1,
                                              limit_px=100, reduce_only=reduce_only)
            assert info["status"] == "maker-only-blocked"
            assert info["filled_base"] == 0 and info["unresolved"] is False
        engine.entropy.send_taker.assert_not_awaited()

    asyncio.run(run())


@pytest.mark.parametrize("is_buy", [True, False])
def test_arcus_maker_reduce_only_wire_keeps_alo_and_signed_reduction_flag(is_buy):
    async def run():
        venue = signed_venue()
        venue._rpc = AsyncMock(return_value={"status": 200, "result": {"orderId": "own-order"}})
        await venue.place_maker(is_buy=is_buy, qty=0.001, limit_px=100, reduce_only=True)
        request = venue._rpc.call_args.args[0]
        payload = request["payload"]
        assert payload["timeInForce"] == "ALO"
        assert payload["reduceOnly"] is True
        assert payload["orderSide"] == ("BUY" if is_buy else "SELL")
        from entropy_arb.venue_arcus import canonical
        signed = dict(ad=venue.address, ai=venue.account_index, c=payload["clientId"],
                      ct=payload["timestamp"], g=int(payload["goodTilTime"]) * 1000,
                      m=venue.market_id, op=1, p=10000, q=10000, r=1,
                      s=0 if is_buy else 1, t=3, v=1)
        venue._signer.public_key().verify(bytes.fromhex(request["signature"]), canonical(signed))

    asyncio.run(run())


def test_arcus_monitor_reports_maker_for_both_bps_directions(tmp_path):
    async def run():
        engine = arcus_engine(tmp_path)
        engine.markets_ready = True
        signals = execution_signals(engine)
        assert [row["order_type"] for row in signals["rows"]] == ["maker", "maker"]
        engine.cfg.primary.kind = "lighter"
        assert not engine.cfg.primary_maker_only
        signals = execution_signals(engine)
        assert [row["order_type"] for row in signals["rows"]] == ["maker", "taker"]

    asyncio.run(run())
