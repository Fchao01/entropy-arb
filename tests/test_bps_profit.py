"""Full-cycle BPS accounting and exits using synthetic confirmed fills only."""
import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.config import ConfigError, load_config
from entropy_arb.signals import execution_signals
from test_bps_strategy import engine_at, execute, books


def profit_engine(tmp_path, long=True, maker=False):
    eng = engine_at(tmp_path, long=long, maker=maker)
    eng.cfg.bps_exit_mode = 'profit'
    eng.cfg.bps_entry_confirm_sec = eng.cfg.bps_cycle_cooldown_sec = 0
    eng.cfg.bps_min_gap_bps = eng.cfg.bps_add_step_bps = 0
    eng.cfg.bps_execution_slippage_bps = 0
    return eng


@pytest.mark.parametrize('long', [True, False])
def test_midpoint_exit_does_not_override_net_profit_exit(tmp_path, long):
    async def run():
        eng = profit_engine(tmp_path, long)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        eng.cfg.bps_take_profit_bps = 100
        books(eng, -5.5 if long else 5.5)
        assert eng.bps_strategy.exit_reached(eng.premium_bps())
        assert eng._scan(time.time()) is None  # Old point reached, profit target not reached.
        eng.cfg.bps_take_profit_bps = 1
        candidate = eng._scan(time.time())
        assert candidate[2].action == 'bps_close'
        assert eng.bps_strategy.close_reason == '净收益止盈'
        await execute(eng, candidate)
        for _ in range(3):
            if eng.bps_strategy.flat():
                break
            books(eng, -5.5 if long else 5.5)
            await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat()
        assert eng.bps_strategy.last_cycle_net_usd > 0
        assert eng.bps_strategy.last_cycle_net_usd == pytest.approx(eng.total_fill_edge)
        assert eng.execution_stats.snapshot(eng)['net_realized_usd'] == pytest.approx(eng.total_fill_edge)
    asyncio.run(run())


def test_losing_round_is_not_closed_as_a_profit_at_midpoint_target(tmp_path):
    eng = profit_engine(tmp_path)
    eng.cfg.bps_stop_loss_bps = 30
    strategy = eng.bps_strategy
    qty = 1
    primary_price = 100.27
    strategy.record_fill(eng.entropy, True, qty, primary_price, 0, opening=True)
    eng.entropy.position = qty
    strategy.record_fill(eng.hedge, False, qty, 100, 0, opening=True)
    eng.hedge.position = -qty
    strategy.entries, strategy.locked_entry, strategy.locked_exit = 1, 15, 20
    eng.entropy.set_book(100.11, 100.75)
    eng.hedge.set_book(99.99, 100)
    assert strategy.exit_reached(eng.premium_bps())
    assert strategy.costs.cash == pytest.approx(-.27)
    quote = strategy.exit_quote()
    assert quote['net_usd'] == pytest.approx(-.16)
    assert strategy.exit_decision(time.time(), quote) is None
    eng.entropy.set_book(100.30, 100.32)
    assert strategy.exit_decision(time.time()) == '净收益止盈'


@pytest.mark.parametrize('cause', ['stop_loss', 'timeout'])
def test_stop_loss_and_timeout_can_exit_before_midpoint_target(tmp_path, cause):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        eng.cfg.bps_take_profit_bps = 100
        books(eng, -30)
        if cause == 'timeout':
            eng.cfg.bps_stop_loss_bps = 0
            eng.bps_strategy.costs.started_at = time.time() - 901
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].action == 'bps_close'
        assert eng.bps_strategy.close_reason == ('净收益止损' if cause == 'stop_loss' else '最长持仓到期')
        await execute(eng, candidate)
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
    asyncio.run(run())


def test_costs_fees_and_partial_close_survive_restart(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
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
        quote_before = strategy.exit_quote()
        assert strategy.closing and not strategy.flat()
        fresh = profit_engine(tmp_path)
        fresh.cfg.primary.fee_bps = fresh.entropy.fee_bps = .2
        fresh.cfg.hedge.fee_bps = fresh.hedge.fee_bps = .2
        for key in fresh.venues:
            fresh.venues[key].position = eng.venues[key].position
        books(fresh, -3)
        fresh.bps_strategy.restore()
        assert fresh.bps_strategy.costs.started_at == original_start
        assert fresh.bps_strategy.closing and fresh.bps_strategy.close_reason == '净收益止盈'
        assert fresh.bps_strategy.exit_quote()['net_usd'] == pytest.approx(quote_before['net_usd'])
        assert fresh.execution_stats.lots  # Known inventory cost restored, turnover still zero.
        assert fresh.execution_stats.volumes == {}
        data = json.loads(strategy.path.read_text())
        assert data['cycle_costs']['known'] is True
        assert data['cycle_costs']['cash'] == pytest.approx(strategy.costs.cash)
    asyncio.run(run())


def test_multiple_entries_do_not_reset_holding_clock_and_cooldown_is_enforced(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.max_order_notional = 100
        eng.cfg.bps_max_entries = 2
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
        assert eng._scan(time.time()) is None
        assert eng.bps_strategy.reason == '平仓后冷却中'
    asyncio.run(run())


def test_unknown_legacy_cost_is_not_zero_cost_profit_and_cannot_be_added(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.cfg.bps_max_entries = 100
        eng.bps_strategy.restore()
        assert eng.bps_strategy.exit_quote() is None
        assert eng._scan(time.time()) is None
        assert '旧仓成本未知' in eng.bps_strategy.reason
        books(eng, -5)
        candidate = eng._scan(time.time())
        assert candidate[2].action == 'bps_close'
        await execute(eng, candidate)
        assert eng.bps_strategy.last_cycle_net_usd is None
    asyncio.run(run())


def test_entry_guard_rejects_too_narrow_or_unexecutable_signal(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.bps_min_gap_bps = 4
        assert eng._scan(time.time()) is None
        assert '开仓空间不足' in eng.bps_strategy.reason
        eng.cfg.bps_min_gap_bps = 0
        # Mid -9.5 qualifies, but buying this wide spread does not.
        eng.entropy.set_book(99.86, 99.95)
        assert eng._scan(time.time()) is None
        assert '可成交开仓报价未达点位' in eng.bps_strategy.reason
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_profit_signal_reports_net_return_basis(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        eng.markets_ready = True
        books(eng, -3)
        result = execution_signals(eng)
        closing = result['rows'][1]
        assert closing['trigger_basis'] == 'cycle_net_return_bps'
        assert closing['hurdle_bps'] == eng.cfg.bps_take_profit_bps
        assert closing['trigger_premium_bps'] == eng.bps_strategy.exit_quote()['net_bps']
    asyncio.run(run())


@pytest.mark.parametrize('field,value', [('exit_mode', 'invalid'), ('take_profit_bps', 0),
                                       ('stop_loss_bps', -1), ('max_hold_sec', -1),
                                       ('entry_confirm_sec', -1), ('execution_slippage_bps', 10000),
                                       ('min_gap_bps', float('nan'))])
def test_profit_config_validation(tmp_path, field, value):
    import yaml
    path = tmp_path / 'config.yaml'
    path.write_text(yaml.safe_dump({'strategy': {'mode': 'bps'}, 'bps': {field: value}}))
    with pytest.raises(ConfigError):
        load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={})



def test_flat_restart_keeps_cooldown_and_halt_reason_survives_refresh(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
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


def test_full_exit_quote_requires_depth_and_deducts_slippage_reserve(tmp_path):
    async def run():
        eng = profit_engine(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        books(eng, -3)
        direct = eng.bps_strategy.exit_quote()['net_usd']
        eng.cfg.leg_slippage_bps = eng.cfg.bps_execution_slippage_bps = 1
        reserved = eng.bps_strategy.exit_quote()['net_usd']
        assert reserved < direct
        # Full-cycle TP cannot be asserted when only part of the inventory has depth.
        eng.hedge.set_book(99.9995, 100.0005, sz=.01)
        assert eng.bps_strategy.exit_quote() is None
        eng.bps_strategy.costs.started_at = time.time() - 901
        assert eng._scan(time.time()) is None  # Min order depth unavailable, but remains closing.
        assert eng.bps_strategy.closing and not eng.halted
    asyncio.run(run())


def test_maker_partial_fill_cost_is_hedged_then_exit_uses_taker_fees(tmp_path):
    from entropy_arb.maker import MakerOrder
    async def run():
        eng = profit_engine(tmp_path, maker=True)
        eng.ensure_async_state()
        eng.cfg.primary.maker_fee_bps = .1
        eng.cfg.primary.fee_bps = eng.entropy.fee_bps = 5
        eng.cfg.hedge.fee_bps = eng.hedge.fee_bps = .2
        eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('fixture', 99.9045, time.time()))
        eng.entropy.cancel_maker = AsyncMock()
        eng.entropy.maker_state = AsyncMock(return_value=dict(
            filled_base=.5, avg_px=99.9045, terminal=True, status='filled', err=None))
        eng.cfg.bps_exit = -3  # Enough entry space for taker exit fees.
        eng.bps_strategy.exit = -3
        candidate = eng._scan(time.time())
        assert candidate
        await execute(eng, candidate)
        assert eng.bps_strategy.costs.open_notional == pytest.approx(.5 * 99.9045)
        assert eng.entropy.position == .5 and eng.hedge.position == -.5
        eng.entropy.send_taker.assert_not_awaited()
        books(eng, -3)
        quoted = eng.bps_strategy.exit_quote()['net_usd']
        eng.entropy.fee_bps = 0
        assert eng.bps_strategy.exit_quote()['net_usd'] > quoted
    asyncio.run(run())


def test_profit_preset_is_loadable_and_legacy_configs_keep_level_exits():
    from entropy_arb.web import ROOT
    cfg = load_config(str(ROOT / 'configs/bps-profit-rh-lighter.yaml'), symbol='GOOGL',
                      primary_venue='lighter-rh', hedge_venue='lighter', credential_env={})
    assert cfg.bps_exit_mode == 'profit' and cfg.bps_take_profit_bps == 1
    assert cfg.bps_max_hold_sec == 900
    old = load_config(str(ROOT / 'configs/bps-rh-arcus.yaml'), symbol='GOOGL',
                      primary_venue='lighter-rh', hedge_venue='arcus', credential_env={})
    assert old.bps_exit_mode == 'levels'
