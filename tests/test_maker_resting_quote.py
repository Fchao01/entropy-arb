"""Resting orders are checked for remaining risk, not resized as new orders."""
import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

from entropy_arb.config import ConfigError, load_config
from entropy_arb.maker import MakerOrder, execute_primary_maker
from test_bps_executable import executable_engine, quotes
from test_bps_strategy import execute


def resting_engine(tmp_path, long=True):
    eng = executable_engine(tmp_path, long)
    eng.ensure_async_state()
    eng.cfg.primary_order_type = 'maker'
    eng.cfg.primary.kind = 'arcus'
    eng.cfg.hedge_venue = 'lighter-rh'
    eng.cfg.take_fraction = .5
    eng.cfg.max_order_notional = 100
    eng.cfg.settle_timeout_sec = .5
    eng.entropy.name, eng.hedge.name = 'ARCUS', 'RH'
    quotes(eng, opening=True, long=long)
    eng.hedge.set_book(100, 100.01, 2)
    candidate = eng._scan(time.time())
    eng.bps_strategy.before_execution(candidate[2])
    price = candidate[2].buy_limit if long else candidate[2].sell_limit
    eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('resting', price, time.time()))
    eng.entropy.cancel_maker = AsyncMock()
    return eng, candidate, price


@pytest.mark.parametrize('long', [True, False])
@pytest.mark.parametrize('partial', [False, True])
def test_valid_resting_depth_is_not_halved_again_or_subject_to_new_order_minimum(tmp_path, long, partial):
    async def run():
        eng, (buy, sell, plan), price = resting_engine(tmp_path, long)
        filled = plan.qty - .0001 if partial else 0
        required = plan.qty - filled
        eng.active_maker = dict(filled=filled, hedged=filled)
        eng.hedge.set_book(100, 100.01, required * 1.1)
        assert eng._maker_quote_valid(buy, sell, plan, price)
        assert eng._maker_validation['hedge_required_base'] == pytest.approx(required)
        assert eng.bps_strategy.plan(buy, sell, quote=price) is None if partial else (
            eng.bps_strategy.plan(buy, sell, quote=price).qty < plan.qty)
        # Losing actually required depth still cancels; no relaxed BPS gate.
        eng.hedge.set_book(100, 100.01, required * .9)
        assert not eng._maker_quote_valid(buy, sell, plan, price)
        assert eng._maker_validation['reason'] == 'hedge:qualified-depth'
    asyncio.run(run())


@pytest.mark.parametrize('failure,reason', [('signal', 'bps:signal-below-target'),
    ('stale', 'RH:book-stale'), ('budget', 'RH:order-budget'),
    ('position', 'position-unconfirmed'), ('cap', 'ARCUS:position-cap')])
def test_invalidity_is_specific_and_not_hidden_by_best_price(tmp_path, failure, reason):
    async def run():
        eng, (buy, sell, plan), price = resting_engine(tmp_path)
        if failure == 'signal':
            eng.hedge.set_book(99.9, 99.91)
        elif failure == 'stale':
            eng.hedge.book.alive_ts = 0
        elif failure == 'budget':
            eng.hedge.orders_per_min = 0
        elif failure == 'position':
            eng._position_uncertain.add('hedge')
        else:
            eng.entropy.cap_usd = 1
        assert eng._maker_at_best_price(True, price)
        assert not eng._maker_quote_valid(buy, sell, plan, price)
        assert eng._maker_validation['reason'] == reason
    asyncio.run(run())


@pytest.mark.parametrize('long', [True, False])
def test_partial_fill_can_remain_at_best_until_all_confirmed_increments_hedged(tmp_path, long):
    async def run():
        eng, (buy, sell, plan), price = resting_engine(tmp_path, long)
        calls = 0
        async def state(order):
            nonlocal calls
            calls += 1
            if calls == 1:
                return dict(status='OPEN', filled_base=plan.qty / 2, avg_px=price, terminal=False, err=None)
            if calls == 2:
                remaining = plan.qty / 2
                eng.hedge.set_book(100, 100.01, remaining * 1.1)
                assert eng._maker_quote_valid(buy, sell, plan, price)
                return dict(status='OPEN', filled_base=plan.qty / 2, avg_px=price, terminal=False, err=None)
            return dict(status='FILLED', filled_base=plan.qty, avg_px=price, terminal=True, err=None)
        eng.entropy.maker_state = AsyncMock(side_effect=state)
        await execute(eng, (buy, sell, plan))
        eng.entropy.cancel_maker.assert_not_awaited()
        assert eng.hedge.send_taker.await_count == 2
        assert sum(c.kwargs['qty'] for c in eng.hedge.send_taker.call_args_list) == pytest.approx(plan.qty)
        assert not eng.halted
    asyncio.run(run())


def test_zero_fill_requote_wait_resets_signal_and_does_not_create_a_completed_cycle(tmp_path, caplog, monkeypatch):
    async def run():
        eng, candidate, price = resting_engine(tmp_path)
        eng.bps_strategy.last_cycle_net_usd = 12.3
        eng.entropy.maker_state = AsyncMock(return_value=dict(
            status='CANCELED', filled_base=0, avg_px=None, terminal=True, err=None))
        eng._notify_trade = AsyncMock()
        await execute(eng, candidate)
        eng._notify_trade.assert_not_called()
        assert eng.bps_strategy.last_cycle_net_usd == 12.3
        assert eng.bps_strategy.last_closed_at == 0
        assert eng.bps_strategy.entries == 0
        quotes(eng, opening=True)
        assert eng._scan(time.time()) is None
        assert '短暂等待' in eng.bps_strategy.reason
        assert 'BPS CYCLE' not in caplog.text
        future = eng._maker_requote_until + .01
        monkeypatch.setattr('entropy_arb.engine.time.time', lambda: future)
        quotes(eng, opening=True)
        assert eng._scan(time.time())
    asyncio.run(run())


def test_rejected_zero_fill_with_submission_error_still_notifies_immediately(tmp_path):
    async def run():
        eng, candidate, _ = resting_engine(tmp_path)
        eng.entropy.place_maker.return_value = MakerOrder('rejected', 99.9, time.time(),
                                                         rejected=True, err='Forbidden')
        eng._notify_trade = Mock()
        await execute(eng, candidate)
        eng._notify_trade.assert_called_once()
        assert not hasattr(eng, '_zero_fill_summary')
    asyncio.run(run())


def test_signal_failure_logs_actual_metric_and_cancels_before_next_quote(tmp_path, caplog):
    async def run():
        eng, (buy, sell, plan), price = resting_engine(tmp_path)
        eng.hedge.set_book(99.9, 99.91)
        eng.entropy.maker_state = AsyncMock(side_effect=lambda order: dict(
            status='CANCELED' if eng.entropy.cancel_maker.await_count else 'OPEN',
            filled_base=0, avg_px=None, terminal=bool(eng.entropy.cancel_maker.await_count), err=None))
        import logging
        with caplog.at_level(logging.INFO):
            await execute_primary_maker(eng, buy, sell, plan)
        assert 'quote-invalid:bps:signal-below-target' in caplog.text
        assert 'signal_bps' in caplog.text and 'target_bps' in caplog.text
        eng.entropy.place_maker.assert_awaited_once()
        eng.entropy.cancel_maker.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('delay', [-1, float('inf'), float('nan'), 61])
def test_requote_delay_validated(tmp_path, delay):
    path = tmp_path / 'config.yaml'
    value = '.inf' if delay == float('inf') else '.nan' if delay != delay else delay
    path.write_text(f'strategy: {{mode: bps}}\nexecution: {{maker_requote_delay_sec: {value}}}\n')
    with pytest.raises(ConfigError):
        load_config(str(path), symbol='SPY', hedge_venue='lighter', credential_env={})
