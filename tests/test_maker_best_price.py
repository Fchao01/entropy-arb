"""Keep queue priority only while the strategy and hedge remain valid."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.config import ConfigError, load_config
from entropy_arb.maker import MakerOrder, execute_primary_maker
from test_bps_strategy import engine_at
from test_maker import setup_engine, state


@pytest.mark.parametrize('primary_buy', [True, False])
def test_best_quote_survives_many_timeout_periods(primary_buy):
    async def run():
        e, buy, sell, plan = setup_engine(primary_buy)
        e.cfg.maker_cancel_policy, e.cfg.maker_timeout_sec = 'best_price', 5
        tick = [100.0]
        calls = [0]
        async def query(order):
            tick[0] += 10
            calls[0] += 1
            return state(.3, True) if calls[0] >= 4 else state(0)
        e.entropy.maker_state = AsyncMock(side_effect=query)
        real_sleep = asyncio.sleep
        async def yield_only(seconds):
            await real_sleep(0)
        with patch('entropy_arb.maker.time', SimpleNamespace(monotonic=lambda: tick[0], perf_counter=lambda: tick[0])), \
                patch('entropy_arb.maker.asyncio.sleep', yield_only):
            await execute_primary_maker(e, buy, sell, plan)
        assert tick[0] >= 140
        e.entropy.cancel_maker.assert_not_awaited()
        assert e.hedge.send_taker.call_args.kwargs['qty'] == .3
        assert not e.halted
    asyncio.run(run())


@pytest.mark.parametrize('primary_buy', [True, False])
def test_quote_behind_best_is_canceled_before_replacing(primary_buy):
    async def run():
        e, buy, sell, plan = setup_engine(primary_buy)
        e.cfg.maker_cancel_policy = 'best_price'
        e.cfg.maker_reprice_delay_sec = .01
        e.cfg.settle_timeout_sec = .5
        if primary_buy:
            e.entropy.set_book(100.01, 101)  # our bid is 100
        else:
            e.entropy.set_book(99, 99.99)  # our ask is 100
        e.entropy.maker_state = AsyncMock(side_effect=lambda order: state(
            0, bool(e.entropy.cancel_maker.await_count), 'canceled'))
        await asyncio.wait_for(execute_primary_maker(e, buy, sell, plan), 2)
        e.entropy.cancel_maker.assert_awaited_once()
        e.entropy.place_maker.assert_awaited_once()  # no overlapping replacement
        e.hedge.send_taker.assert_not_awaited()
        assert not e.halted
    asyncio.run(run())


@pytest.mark.parametrize('long', [True, False])
def test_bps_failure_cancels_even_at_best_and_during_slow_state_query(tmp_path, long):
    async def run():
        e = engine_at(tmp_path, maker=True, long=long)
        e.ensure_async_state()
        e.cfg.maker_cancel_policy = 'best_price'
        e.cfg.maker_reprice_delay_sec = 60
        e.cfg.maker_timeout_sec = .005
        e.cfg.settle_timeout_sec = .5
        buy, sell, plan = e._scan(time.time())
        e.bps_strategy.before_execution(plan)
        price = plan.buy_limit if long else plan.sell_limit
        e.entropy.place_maker = AsyncMock(return_value=MakerOrder('test', price, time.time()))
        canceled = asyncio.Event()
        async def cancel(order):
            assert e._maker_at_best_price(long, price)
            assert not e._maker_quote_valid(buy, sell, plan, price)
            canceled.set()
        async def query(order):
            await canceled.wait()
            return state(0, True, 'canceled')
        async def invalidate_bps():
            await asyncio.sleep(.02)
            assert not canceled.is_set()  # timeout expired, but BPS was still valid
            hedge_mid = 99.8 if long else 100.2
            e.hedge.set_book(hedge_mid - .0005, hedge_mid + .0005)
        e.entropy.cancel_maker = AsyncMock(side_effect=cancel)
        e.entropy.maker_state = AsyncMock(side_effect=query)
        update = asyncio.create_task(invalidate_bps())
        try:
            await asyncio.wait_for(execute_primary_maker(e, buy, sell, plan), 2)
            await update
        finally:
            update.cancel()
        e.entropy.cancel_maker.assert_awaited_once()
        e.hedge.send_taker.assert_not_awaited()
        assert not e.halted
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['signal', 'hedge-down', 'hedge-limited', 'stale', 'stop', 'pause'])
def test_best_price_never_overrides_cancel_conditions(failure):
    async def run():
        e, buy, sell, plan = setup_engine()
        e.cfg.maker_cancel_policy = 'best_price'
        e.cfg.settle_timeout_sec = .5
        if failure == 'stop':
            e.stop.set()
        elif failure == 'pause':
            e.manual_paused = True
        else:
            # Use the real validity function; alter just the failed input.
            e._maker_quote_valid = type(e)._maker_quote_valid.__get__(e)
            if failure == 'signal':
                e.hedge.set_book(98, 99)
            elif failure == 'hedge-down':
                e._venue_down['hedge'] = time.time()
            elif failure == 'hedge-limited':
                e._venue_limited_until['hedge'] = time.time() + 60
            else:
                e.hedge.book.alive_ts = 0
        assert e._maker_at_best_price(True, plan.buy_limit)
        e.entropy.maker_state = AsyncMock(side_effect=lambda order: state(
            0, bool(e.entropy.cancel_maker.await_count), 'canceled'))
        await asyncio.wait_for(execute_primary_maker(e, buy, sell, plan), 2)
        e.entropy.cancel_maker.assert_awaited_once()
        assert not e.halted
    asyncio.run(run())


@pytest.mark.parametrize('primary_buy', [True, False])
def test_brief_loss_of_best_resets_when_quote_returns(primary_buy):
    async def run():
        e, buy, sell, plan = setup_engine(primary_buy)
        e.cfg.maker_cancel_policy = 'best_price'
        e.cfg.maker_reprice_delay_sec = .3
        e.cfg.settle_timeout_sec = 1
        tick, calls = [100.0], [0]
        async def query(order):
            calls[0] += 1
            tick[0] += .1
            behind = calls[0] % 2
            if primary_buy:
                e.entropy.set_book(100.01 if behind else 100, 101)
            else:
                e.entropy.set_book(99, 99.99 if behind else 100)
            return state(.3, True) if calls[0] >= 10 else state(0)
        e.entropy.maker_state = AsyncMock(side_effect=query)
        real_sleep = asyncio.sleep
        async def yield_only(seconds):
            await real_sleep(0)
        with patch('entropy_arb.maker.time', SimpleNamespace(monotonic=lambda: tick[0], perf_counter=lambda: tick[0])), \
                patch('entropy_arb.maker.asyncio.sleep', yield_only):
            await execute_primary_maker(e, buy, sell, plan)
        e.entropy.cancel_maker.assert_not_awaited()
        assert not e.halted
    asyncio.run(run())


def test_best_quote_cannot_use_expired_rolling_bps_levels(tmp_path):
    async def run():
        e = engine_at(tmp_path, maker=True)
        e.ensure_async_state()
        e.cfg.maker_cancel_policy = 'best_price'
        e.cfg.settle_timeout_sec = .5
        buy, sell, plan = e._scan(time.time())
        e.bps_strategy.before_execution(plan)
        e.cfg.bps_levels = 'rolling'
        e.bps_strategy.window_end = time.time() - 180
        e.entropy.place_maker = AsyncMock(return_value=MakerOrder('test', plan.buy_limit, time.time()))
        e.entropy.cancel_maker = AsyncMock()
        e.entropy.maker_state = AsyncMock(side_effect=lambda order: state(
            0, bool(e.entropy.cancel_maker.await_count), 'canceled'))
        assert e._maker_at_best_price(True, plan.buy_limit)
        await asyncio.wait_for(execute_primary_maker(e, buy, sell, plan), 2)
        e.entropy.cancel_maker.assert_awaited_once()
        e.hedge.send_taker.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('policy,delay,valid', [('best_price', .3, True), ('timeout', 0, True),
    ('invalid', .3, False), ('best_price', -1, False), ('best_price', float('inf'), False)])
def test_config_default_and_validation(tmp_path, policy, delay, valid):
    p = tmp_path / 'config.yaml'
    p.write_text(f'strategy: {{mode: bps}}\nexecution:\n  maker_cancel_policy: {policy}\n'
                 f'  maker_reprice_delay_sec: {delay if delay != float("inf") else ".inf"}\n')
    if valid:
        cfg = load_config(str(p), symbol='ETH', hedge_venue='lighter', credential_env={})
        assert cfg.maker_cancel_policy == policy and cfg.maker_reprice_delay_sec == delay
    else:
        with pytest.raises(ConfigError):
            load_config(str(p), symbol='ETH', hedge_venue='lighter', credential_env={})
    p.write_text('strategy: {mode: bps}')
    assert load_config(str(p), symbol='ETH', hedge_venue='lighter', credential_env={}).maker_cancel_policy == 'best_price'
