"""Executable BPS references must not override post-only execution selection."""
import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.config import load_config
from entropy_arb.maker import MakerOrder
from entropy_arb.signals import execution_signals
from test_bps_executable import executable_engine, quotes
from test_bps_strategy import execute


def enable_maker(eng):
    eng.cfg.primary_order_type = 'maker'
    eng.cfg.maker_cancel_policy = 'best_price'
    eng.cfg.maker_timeout_sec = .001  # Must not be a total deadline in this policy.
    eng.entropy.name, eng.hedge.name = 'ARCUS', 'RH'
    eng.cfg.primary.maker_fee_bps = .5
    eng.entropy.fee_bps = 2.25
    async def place(**kwargs):
        return MakerOrder('fixture-post-only', kwargs['limit_px'], time.time())
    eng.entropy.place_maker = AsyncMock(side_effect=place)
    eng.entropy.cancel_maker = AsyncMock()


def maker_state(filled, price, terminal):
    return dict(status='filled' if terminal else 'open', filled_base=filled, avg_px=price if filled else None,
                terminal=terminal, err=None)


@pytest.mark.parametrize('long', [True, False])
def test_maker_open_hedges_only_confirmed_increments_and_close_stays_taker(tmp_path, long):
    async def run():
        eng = executable_engine(tmp_path, long)
        eng.ensure_async_state()
        enable_maker(eng)
        quotes(eng, opening=True, long=long)
        candidate = eng._scan(time.time())
        plan = candidate[2]
        price = plan.buy_limit if long else plan.sell_limit
        assert price == (eng.entropy.book.best_bid() if long else eng.entropy.book.best_ask())
        assert (plan.buy_fee if long else plan.sell_fee) == .5 / 10000
        signals = execution_signals(eng)['rows']
        assert signals[0]['order_type'] == 'maker' and signals[1]['order_type'] == 'taker'
        assert signals[0]['trigger_premium_bps'] == eng.bps_strategy.signal()
        assert signals[0]['executable_premium_bps'] >= signals[0]['trigger_premium_bps']
        count = 0
        async def state(order):
            nonlocal count
            count += 1
            assert eng._maker_quote_valid(candidate[0], candidate[1], plan, price)
            if count == 1:
                eng.hedge.send_taker.assert_not_awaited()
                return maker_state(0, price, False)
            if count == 2:
                eng.hedge.send_taker.assert_not_awaited()
                return maker_state(.2, price, False)
            assert eng.hedge.send_taker.await_count == 1
            return maker_state(plan.qty, price, True)
        eng.entropy.maker_state = AsyncMock(side_effect=state)
        await execute(eng, candidate)
        eng.entropy.place_maker.assert_awaited_once()
        eng.entropy.send_taker.assert_not_awaited()
        eng.entropy.cancel_maker.assert_not_awaited()
        amounts = [c.kwargs['qty'] for c in eng.hedge.send_taker.call_args_list]
        assert amounts == pytest.approx([.2, plan.qty - .2])
        assert not eng.halted and not eng.bps_strategy.pending
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        quotes(eng, opening=False, long=long)
        closing = eng._scan(time.time())
        assert closing[2].action == 'bps_close'
        await execute(eng, closing)
        eng.entropy.place_maker.assert_awaited_once()  # No maker exit.
        assert eng.entropy.send_taker.call_args.kwargs['reduce_only']
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
    asyncio.run(run())


@pytest.mark.parametrize('long', [True, False])
def test_bps_failure_cancels_even_when_the_maker_quote_is_still_best(tmp_path, long):
    async def run():
        eng = executable_engine(tmp_path, long)
        eng.ensure_async_state()
        enable_maker(eng)
        quotes(eng, opening=True, long=long)
        candidate = eng._scan(time.time())
        price = candidate[2].buy_limit if long else candidate[2].sell_limit
        count = 0
        async def state(order):
            nonlocal count
            count += 1
            if count == 1:
                eng.hedge.set_book(99.94, 99.95) if long else eng.hedge.set_book(100.07, 100.08)
                assert eng._maker_at_best_price(long, price)
                assert eng.bps_strategy.signal() < eng.bps_strategy.locked_entry
                assert not eng._maker_quote_valid(candidate[0], candidate[1], candidate[2], price)
                return maker_state(0, price, False)
            return maker_state(0, price, True)
        eng.entropy.maker_state = AsyncMock(side_effect=state)
        await execute(eng, candidate)
        eng.entropy.cancel_maker.assert_awaited_once()
        eng.entropy.send_taker.assert_not_awaited()
        eng.hedge.send_taker.assert_not_awaited()
        assert not eng.halted and not eng.bps_strategy.pending
    asyncio.run(run())


@pytest.mark.parametrize('mode', ['maker', 'taker'])
def test_explicit_executable_config_honors_execution_and_fee_selection(tmp_path, mode):
    path = tmp_path / 'config.yaml'
    path.write_text('strategy:\n  mode: bps\nbps:\n  price_basis: executable\n  levels: manual\n'
                    f'execution:\n  primary_order_type: {mode}\n'
                    'primary:\n  maker_fee_bps: 0\n  taker_fee_bps: 2.25\n')
    cfg = load_config(str(path), symbol='SPY', primary_venue='arcus', hedge_venue='lighter-rh', credential_env={})
    assert cfg.primary_order_type == mode and cfg.bps_price_basis == 'executable'
    assert cfg.primary_fee_bps == (0 if mode == 'maker' else 2.25)
