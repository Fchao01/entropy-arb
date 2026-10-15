"""Executable-price triggers, depth sizing and risk limits without BPS cycles."""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.config import ConfigError, load_config
from entropy_arb.signals import execution_signals
from entropy_arb.web import TaskManager, ConsoleError
from test_engine import make_engine


def spread_engine():
    engine = make_engine(midline=500, upper=500, lower=500)
    engine.cfg.strategy_mode = 'spread'
    engine.cfg.spread_bps = 1
    engine.cfg.premium_persist_sec = 100
    engine.cfg.inventory_scale_bps = 100
    engine.cfg.bps_take_profit_bps = 0
    engine.cfg.take_fraction = 1
    engine.cfg.leg_slippage_bps = 0
    engine.markets_ready = True
    engine.hedge.fee_bps = 2.25
    for venue in engine.venues.values():
        venue.volume_usd = 0
        venue.px_round = lambda price, round_up: price
        venue.send_taker = AsyncMock(side_effect=lambda **kw: dict(
            filled_base=kw['qty'], avg_px=kw['limit_px'], status='filled', err=None))
    return engine


@pytest.mark.parametrize('primary_buy', [True, False])
@pytest.mark.parametrize('sell_bid,qualifies', [(100.009, False), (100.01, False), (100.011, True)])
def test_strict_gross_spread_both_directions(primary_buy, sell_bid, qualifies):
    async def run():
        engine = spread_engine()
        engine.ensure_async_state()
        buy, sell = (engine.entropy, engine.hedge) if primary_buy else (engine.hedge, engine.entropy)
        buy.set_book(99.99, 100)
        sell.set_book(sell_bid, sell_bid + .01)
        candidate = engine._scan(time.time())
        assert bool(candidate) is qualifies
        if qualifies:
            assert candidate[:2] == (buy, sell)
            assert candidate[2].exp_edge_usd < 0  # configured fees exceed the price spread
        rows = execution_signals(engine)['rows']
        row = next(row for row in rows if row['primary_buy'] is primary_buy)
        assert row['hurdle_bps'] == 1
        assert row['comparison'] == '>'
    asyncio.run(run())


def test_only_qualifying_depth_counts_and_position_caps_win():
    async def run():
        engine = spread_engine()
        engine.ensure_async_state()
        engine.entropy.set_book(99.99, 100)
        engine.hedge.set_book(100.02, 100.03)
        engine.hedge.book.bids = {100.02: .2, 100.01: 50}
        candidate = engine._scan(time.time())
        assert candidate[2].qty == pytest.approx(.2)
        engine.entropy.position = engine.entropy.cap_usd / 100
        engine.hedge.position = -engine.entropy.position
        assert engine._scan(time.time()) is None
    asyncio.run(run())


def test_repeated_trades_ignore_batch_profit_and_history_settings(tmp_path):
    async def run():
        engine = spread_engine()
        engine.cfg.trades_csv = str(tmp_path / 'trades.csv')
        engine.cfg.recorder_csv = str(tmp_path / 'does-not-exist.csv')
        engine.cfg.max_order_notional = 20
        engine.ensure_async_state()
        for _ in range(3):
            engine.entropy.set_book(99.99, 100)
            engine.hedge.set_book(100.02, 100.03)
            candidate = engine._scan(time.time())
            assert candidate
            with patch.object(engine, '_log_csv'):
                await engine._execute(*candidate)
        assert engine.trades == 3  # legacy max_entries=1 cannot block this mode
        assert engine.total_fill_edge < 0
        assert engine.total_fees_usd > 0
        assert engine.entropy.position == pytest.approx(.6)
        assert engine.hedge.position == pytest.approx(-.6)
    asyncio.run(run())


def test_price_is_rechecked_before_sending():
    async def run():
        engine = spread_engine()
        engine.ensure_async_state()
        engine.entropy.set_book(99.99, 100)
        engine.hedge.set_book(100.02, 100.03)
        candidate = engine._scan(time.time())
        assert candidate
        engine.hedge.set_book(100, 100.01)
        await engine._execute(*candidate)
        assert engine.trades == 0
        for venue in engine.venues.values():
            venue.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_spread_config_needs_no_thresholds_or_batches(tmp_path):
    path = tmp_path / 'spread.yaml'
    path.write_text('strategy:\n  mode: spread\n  spread_bps: 1\nbps:\n  max_entries: 0\n  take_profit_bps: 0\n')
    cfg = load_config(str(path), symbol='MU', hedge_venue='lighter', credential_env={})
    assert cfg.strategy_mode == 'spread' and cfg.spread_bps == 1
    path.write_text(path.read_text() + 'execution:\n  primary_order_type: maker\n')
    with pytest.raises(ConfigError, match='requires taker'):
        load_config(str(path), symbol='MU', hedge_venue='lighter', credential_env={})


def test_levels_mode_allows_unused_zero_profit_target(tmp_path):
    path = tmp_path / 'levels.yaml'
    path.write_text('strategy:\n  mode: bps\nbps:\n  take_profit_bps: 0\n')
    cfg = load_config(str(path), symbol='MU', hedge_venue='lighter', credential_env={})
    assert cfg.bps_take_profit_bps == 0


def test_web_spread_task_skips_daily_thresholds(tmp_path):
    async def run():
        manager = TaskManager(tmp_path, tmp_path / 'web-data')
        try:
            task = await manager.put(dict(name='MU spread', symbol='MU', primary='lighter-rh',
                hedge='lighter', mode='record', config='strategy:\n  mode: spread\n  spread_bps: 1\n'))
            assert task['strategy_mode'] == 'spread'
            with pytest.raises(ConsoleError, match='实际价差'):
                manager._threshold_suggestion(task['id'])
            from entropy_arb.thresholds import daily_window
            await manager.auto_update_task(task['id'], daily_window())
            assert manager._read_auto_status(task['id'])['status'] == 'skipped'
        finally:
            await manager.close()
    asyncio.run(run())
