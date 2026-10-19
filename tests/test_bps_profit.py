"""Legacy profit configurations use BPS levels; accounting remains independent."""
import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.config import load_config
from entropy_arb.signals import execution_signals
from test_bps_strategy import engine_at, execute, books


def legacy_engine(tmp_path, long=True, maker=False):
    eng = engine_at(tmp_path, long=long, maker=maker)
    eng.cfg.bps_exit_mode = 'profit'
    eng.cfg.bps_entry_confirm_sec = eng.cfg.bps_cycle_cooldown_sec = 0
    eng.cfg.bps_min_gap_bps = eng.cfg.bps_add_step_bps = 0
    eng.cfg.bps_execution_slippage_bps = 0
    return eng


@pytest.mark.parametrize('long', [True, False])
def test_old_profit_guards_cannot_override_point_exits(tmp_path, long):
    async def run():
        eng = legacy_engine(tmp_path, long)
        eng.cfg.bps_take_profit_bps = 10000
        eng.cfg.bps_min_gap_bps = eng.cfg.bps_entry_confirm_sec = 10000
        eng.cfg.bps_cost_buffer_bps = eng.cfg.bps_add_step_bps = 10000
        await execute(eng, eng._scan(time.time()))
        strategy = eng.bps_strategy
        strategy.costs.started_at = time.time() - 100000
        books(eng, -8 if long else 8)  # Neither entry nor exit point reached.
        assert eng._scan(time.time()) is None  # No timeout/stop-loss exit.
        strategy.closing, strategy.close_reason = True, '净收益止损'
        assert eng._scan(time.time()) is None  # No restored/latching bypass.
        assert not strategy.closing
        books(eng, -5.5 if long else 5.5)
        candidate = eng._scan(time.time())
        assert candidate[2].action == 'bps_close'
        assert strategy.close_reason == 'bps 平仓点位'
        await execute(eng, candidate)
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
    asyncio.run(run())


def test_costs_fees_and_partial_close_survive_restart(tmp_path):
    async def run():
        eng = legacy_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.primary.fee_bps = eng.entropy.fee_bps = .2
        eng.cfg.hedge.fee_bps = eng.hedge.fee_bps = .2
        await execute(eng, eng._scan(time.time()))
        original_start = eng.bps_strategy.costs.started_at
        books(eng, -3)
        for venue in eng.venues.values():
            async def partial(**args):
                return dict(status='partial', filled_base=args['qty'] / 2,
                            avg_px=args['limit_px'], err=None, unresolved=False)
            venue.send_taker = AsyncMock(side_effect=partial)
        await execute(eng, eng._scan(time.time()))
        strategy = eng.bps_strategy
        costs_before = strategy.costs.dump()
        assert strategy.closing and not strategy.flat()
        fresh = legacy_engine(tmp_path)
        fresh.cfg.primary.fee_bps = fresh.entropy.fee_bps = .2
        fresh.cfg.hedge.fee_bps = fresh.hedge.fee_bps = .2
        for key in fresh.venues:
            fresh.venues[key].position = eng.venues[key].position
        books(fresh, -3)
        fresh.bps_strategy.restore()
        assert fresh.bps_strategy.costs.started_at == original_start
        assert fresh.bps_strategy.closing and fresh.bps_strategy.close_reason == 'bps 平仓点位'
        assert fresh.bps_strategy.costs.dump() == costs_before
        assert fresh.execution_stats.lots  # Known inventory cost restored, turnover still zero.
        assert fresh.execution_stats.volumes == {}
        data = json.loads(strategy.path.read_text())
        assert data['cycle_costs']['known'] is True
        assert data['cycle_costs']['cash'] == pytest.approx(strategy.costs.cash)
    asyncio.run(run())


def test_multiple_entries_preserve_costs_and_no_removed_cycle_cooldown(tmp_path):
    async def run():
        eng = legacy_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.max_order_notional = 100
        await execute(eng, eng._scan(time.time()))
        first_start = eng.bps_strategy.costs.started_at
        books(eng, -10)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.entries == 2
        assert eng.bps_strategy.costs.started_at == first_start
        assert eng.bps_strategy.costs.open_notional > 190
        books(eng, -3)
        while not eng.bps_strategy.flat():
            await execute(eng, eng._scan(time.time()))
            books(eng, -3)
        eng.cfg.bps_cycle_cooldown_sec = 30
        books(eng, -10)
        assert eng._scan(time.time())[2].action == 'bps_open'
    asyncio.run(run())


def test_unknown_legacy_cost_does_not_block_bps_adds_or_invent_profit(tmp_path):
    async def run():
        eng = legacy_engine(tmp_path)
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.bps_strategy.restore()
        assert not eng.bps_strategy.costs.known
        assert eng._scan(time.time())[2].action == 'bps_open'
        books(eng, -5)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat()
        assert eng.bps_strategy.last_cycle_net_usd is None
    asyncio.run(run())


def test_bps_signal_has_only_price_target_and_snapshot_has_no_profit_controls(tmp_path):
    async def run():
        eng = legacy_engine(tmp_path)
        await execute(eng, eng._scan(time.time()))
        eng.markets_ready = True
        books(eng, -3)
        closing = execution_signals(eng)['rows'][1]
        assert closing['trigger_basis'] != 'cycle_net_return_bps'
        assert closing['hurdle_bps'] == eng.bps_strategy.locked_exit
        assert closing['trigger_premium_bps'] == pytest.approx(eng.premium_bps())
        snapshot = eng.bps_strategy.snapshot()
        assert snapshot['exit_mode'] == 'levels'
        assert not {'exit_quote', 'round_trip_reference', 'take_profit_bps', 'stop_loss_bps', 'max_hold_sec'} & snapshot.keys()
    asyncio.run(run())


@pytest.mark.parametrize('basis', [None, 'mid', 'executable'])
def test_profit_config_is_migrated_without_changing_explicit_basis(tmp_path, basis):
    import yaml
    bps = {'exit_mode': 'profit', 'take_profit_bps': 0, 'entry_bps': -9, 'exit_bps': -6,
           'entry_percentile': 10, 'exit_percentile': 75, 'stop_loss_bps': 0, 'max_hold_sec': 0}
    if basis:
        bps['price_basis'] = basis
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump({'strategy': {'mode': 'bps'}, 'bps': bps}))
    cfg = load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={})
    assert cfg.bps_exit_mode == 'levels'
    assert cfg.bps_price_basis == (basis or 'mid')
    assert (cfg.bps_entry, cfg.bps_exit) == (-9, -6)
    assert (cfg.bps_entry_percentile, cfg.bps_exit_percentile) == (10, 75)


def test_flat_restart_keeps_accounting_and_halt_reason_survives_refresh(tmp_path):
    async def run():
        eng = legacy_engine(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        books(eng, -3)
        while not eng.bps_strategy.flat():
            await execute(eng, eng._scan(time.time()))
            books(eng, -3)
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.last_closed_at == eng.bps_strategy.last_closed_at
        assert restored.last_cycle_net_usd == eng.bps_strategy.last_cycle_net_usd
        eng.bps_strategy = restored
        eng.cfg.bps_levels = 'rolling'
        eng.halted = True
        restored.reason = '成交或对冲未确认，暂停交易'
        async def stop(awaitable, timeout):
            awaitable.close()
            eng.stop.set()
        with patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=stop):
            await restored.refresh_loop()
        assert restored.reason == '成交或对冲未确认，暂停交易'
    asyncio.run(run())


def test_maker_partial_fill_cost_and_bps_exit_survive_legacy_guards(tmp_path):
    from entropy_arb.maker import MakerOrder
    async def run():
        eng = legacy_engine(tmp_path, maker=True)
        eng.ensure_async_state()
        eng.cfg.primary.maker_fee_bps = .1
        eng.cfg.primary.fee_bps = eng.entropy.fee_bps = 5
        eng.cfg.hedge.fee_bps = eng.hedge.fee_bps = .2
        eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('fixture', 99.9045, time.time()))
        eng.entropy.cancel_maker = AsyncMock()
        eng.entropy.maker_state = AsyncMock(return_value=dict(
            filled_base=.5, avg_px=99.9045, terminal=True, status='filled', err=None))
        eng.cfg.bps_exit = -3
        eng.bps_strategy.exit = -3
        candidate = eng._scan(time.time())
        assert candidate
        await execute(eng, candidate)
        assert eng.bps_strategy.costs.open_notional == pytest.approx(.5 * 99.9045)
        assert eng.entropy.position == .5 and eng.hedge.position == -.5
        eng.entropy.send_taker.assert_not_awaited()
        books(eng, -3)
        assert eng._scan(time.time())[2].action == 'bps_close'
    asyncio.run(run())


def test_profit_preset_is_loadable_and_legacy_configs_keep_level_exits():
    from entropy_arb.web import ROOT
    cfg = load_config(str(ROOT / 'configs/bps-profit-rh-lighter.yaml'), symbol='GOOGL',
                      primary_venue='lighter-rh', hedge_venue='lighter', credential_env={})
    assert cfg.bps_exit_mode == 'levels'
    assert cfg.bps_max_hold_sec == 900
    old = load_config(str(ROOT / 'configs/bps-rh-arcus.yaml'), symbol='GOOGL',
                      primary_venue='lighter-rh', hedge_venue='arcus', credential_env={})
    assert old.bps_exit_mode == 'levels'
