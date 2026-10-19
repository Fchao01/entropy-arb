"""Confirmed exchange precision tails: retain real inventory, never unknown orders."""
import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.config import ConfigError, load_config
from test_bps_strategy import engine_at, execute


def spy_engine(tmp_path):
    eng = engine_at(tmp_path, long=False)
    eng.cfg.bps_price_basis = 'executable'
    eng.cfg.bps_entry, eng.cfg.bps_exit = -8, 9
    eng.cfg.max_order_notional = 50
    eng.cfg.take_fraction = 1
    eng._min_base = eng.hedge.min_base = .01
    eng.entropy.min_base, eng.entropy.min_quote = .001, 5
    eng.hedge.min_quote = 10
    eng.entropy.name, eng.hedge.name = 'ARCUS', 'RH'
    eng.bps_strategy = BpsStrategy(eng)
    opening_quotes(eng)
    return eng


def opening_quotes(eng):
    eng.entropy.set_book(771.71, 771.72)
    eng.hedge.set_book(772.29, 772.30)


async def recorded_partial_fill(eng):
    eng.ensure_async_state()
    eng.entropy.send_taker.side_effect = None
    eng.entropy.send_taker.return_value = dict(status='canceled', filled_base=.0249306,
                                             avg_px=771.71, err=None, unresolved=False)
    candidate = eng._scan(time.time())
    assert candidate[2].qty == .0647
    await execute(eng, candidate)


def test_log_partial_fill_repairs_tradeable_excess_and_keeps_precision_tail(tmp_path):
    async def run():
        eng = spy_engine(tmp_path)
        await recorded_partial_fill(eng)
        strategy = eng.bps_strategy
        assert [c.kwargs['qty'] for c in eng.hedge.send_taker.call_args_list] == pytest.approx([.0647, .0397])
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
        assert eng.entropy.position == pytest.approx(-.0249306)
        assert eng.hedge.position == pytest.approx(.025)
        assert not eng.halted and not strategy.pending
        assert strategy.entries == 1 and not strategy.flat()
        residual = strategy.snapshot()['position_residual']
        assert residual['accepted_dust']
        assert residual['net_base'] == pytest.approx(.0000694)
        assert residual['notional_usd'] == pytest.approx(.053597273)
        checkpoint = json.loads(strategy.path.read_text())
        assert checkpoint['positions']['hedge'] == pytest.approx(.025)
        assert not checkpoint['pending']
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.costs.dump() == strategy.costs.dump()
        # A new entry is still possible, with the actual tail included in caps.
        opening_quotes(eng)
        assert eng._scan(time.time())[2].action == 'bps_open'
    asyncio.run(run())


@pytest.mark.parametrize('fractional_fill', [.0249306, .0249694])
def test_close_keeps_subminimum_tail_in_costs_and_can_add_again(tmp_path, fractional_fill):
    async def run():
        eng = spy_engine(tmp_path)
        await recorded_partial_fill(eng)
        if fractional_fill != .0249306:
            # Opposite rounding side: a fractional paired remainder > half a
            # common step must also be closable without inventing a full step.
            eng.entropy.position = -fractional_fill
            eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        async def full(**args):
            return dict(status='filled', filled_base=args['qty'], avg_px=args['limit_px'], err=None, unresolved=False)
        eng.entropy.send_taker.side_effect = full
        eng.entropy.set_book(771.40, 771.41)
        eng.hedge.set_book(772.29, 772.30)
        closing = eng._scan(time.time())
        assert closing[2].qty == .0249
        await execute(eng, closing)
        strategy = eng.bps_strategy
        assert not eng.halted and not strategy.pending
        assert strategy.only_dust_positions() and not strategy.flat()
        assert eng.entropy.position == pytest.approx(.0249 - fractional_fill)
        assert eng.hedge.position == pytest.approx(.0001)
        assert strategy.last_cycle_net_usd is None  # Not a fully closed cycle.
        for v in eng.venues.values():
            assert sum(lot[0] for lot in strategy.costs.stats.lots[v.key]) == pytest.approx(v.position)
        # A close signal alone cannot send an under-minimum order or halt.
        eng.entropy.set_book(771.40, 771.41)
        eng.hedge.set_book(772.29, 772.30)
        assert eng._scan(time.time()) is None
        assert not eng.halted
        opening_quotes(eng)
        assert eng._scan(time.time())[2].action == 'bps_open'
    asyncio.run(run())


@pytest.mark.parametrize('price,net,cap', [(100000, .0000694, .1), (772, .0001001, .1),
                                        (772, .0000694, 0), (772, .0000694, .01)])
def test_larger_or_disabled_dust_does_not_bypass_pause(tmp_path, price, net, cap):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        plan = eng._scan(time.time())[2]
        eng.bps_strategy.before_execution(plan)
        eng.cfg.bps_max_dust_notional_usd = cap
        eng.entropy.set_book(price, price + .01)
        eng.hedge.set_book(price, price + .01)
        eng.entropy.position, eng.hedge.position = -.025, .025 + net
        eng.bps_strategy.after_execution(plan, 0, False)
        assert eng.halted and eng.bps_strategy.pending
        assert not eng.bps_strategy.snapshot()['position_residual']['accepted_dust']
    asyncio.run(run())


@pytest.mark.parametrize('unknown', ['order', 'hedge', 'position', 'maker'])
def test_small_net_never_clears_uncertainty(tmp_path, unknown):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        plan = eng._scan(time.time())[2]
        eng.bps_strategy.before_execution(plan)
        eng.entropy.position, eng.hedge.position = -.0249306, .025
        if unknown == 'hedge':
            eng.halted = True
        elif unknown == 'position':
            eng._position_uncertain.add('entropy')
        elif unknown == 'maker':
            eng.active_maker = dict(venue='ARCUS', client_id='saved-order')
        eng.bps_strategy.after_execution(plan, 0, unknown == 'order')
        assert eng.halted and eng.bps_strategy.pending
        assert not eng.bps_strategy.snapshot()['position_residual']['accepted_dust']
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_not_awaited()
        eng.hedge.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_tradeable_small_residual_and_stale_prices_are_not_dust(tmp_path):
    eng = spy_engine(tmp_path)
    eng.entropy.position, eng.hedge.position = -.0249, .025
    eng.hedge.min_base = eng.hedge.min_quote = eng.cfg.min_order_notional = 0
    assert not eng.bps_strategy.precision_dust()  # A full common step is orderable.
    eng.hedge.position = .0249694
    assert eng.bps_strategy.precision_dust()
    eng.hedge.book.alive_ts = 0
    assert not eng.bps_strategy.precision_dust()


@pytest.mark.parametrize('pending', [True, False])
def test_restart_verifies_orders_positions_and_prices_before_carrying_tail(tmp_path, monkeypatch, pending):
    async def run():
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
        eng = spy_engine(tmp_path)
        await recorded_partial_fill(eng)
        costs = eng.bps_strategy.costs.dump()
        eng.bps_strategy.last_add_premium = -7.5
        eng.bps_strategy.last_cycle_net_usd = .12
        eng.bps_strategy.pending = pending
        eng.bps_strategy.save()
        for v in eng.venues.values():
            v.has_open_orders = AsyncMock(return_value=False)
            v.fetch_position = AsyncMock(side_effect=lambda v=v: v.position)
            v.send_taker.reset_mock()
        eng.bps_strategy = BpsStrategy(eng)
        assert await eng.bps_strategy.recover_pending()
        assert eng.bps_strategy.pending
        await eng.bps_strategy.finish_startup_recovery()
        eng._check_starting_positions()
        eng.bps_strategy.restore()
        assert not eng.bps_strategy.pending and not eng.halted
        assert eng.bps_strategy.snapshot()['position_residual']['accepted_dust']
        assert eng.bps_strategy.costs.known is (not pending)
        if not pending:
            assert eng.bps_strategy.costs.dump() == costs
            assert eng.bps_strategy.last_add_premium == -7.5
            assert eng.bps_strategy.last_cycle_net_usd == .12
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
            assert v.has_open_orders.await_count == 4
            assert sum(lot[0] for lot in eng.execution_stats.lots[v.key]) == pytest.approx(v.position)
            if pending:
                assert all(lot[1] is None for lot in eng.execution_stats.lots[v.key])
    asyncio.run(run())


def test_external_position_change_is_not_excused_as_precision_dust(tmp_path):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = -.025, .025
        eng.entropy.fetch_position = AsyncMock(return_value=-.0249306)
        with patch('entropy_arb.engine.asyncio.sleep', new=AsyncMock()):
            await eng._reconcile_venue(eng.entropy, False)
        assert eng.halted and eng.bps_strategy.pending
        assert not eng.bps_strategy.residual_status()['accepted_dust']
    asyncio.run(run())


def test_restart_dust_with_open_order_stays_pending(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
        eng = spy_engine(tmp_path)
        await recorded_partial_fill(eng)
        eng.bps_strategy.pending = True
        eng.bps_strategy.save()
        for v in eng.venues.values():
            v.has_open_orders = AsyncMock(return_value=False)
            v.fetch_position = AsyncMock(side_effect=lambda v=v: v.position)
            v.send_taker.reset_mock()
        eng.hedge.has_open_orders.return_value = True
        eng.bps_strategy = BpsStrategy(eng)
        with pytest.raises(RuntimeError, match='存在未成交订单'):
            await eng.bps_strategy.recover_pending()
        assert json.loads(eng.bps_strategy.path.read_text())['pending']
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_dust_configuration_defaults_and_disable(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('strategy:\n  mode: bps\n')
    assert load_config(str(path), symbol='SPY', hedge_venue='lighter', credential_env={}).bps_max_dust_notional_usd == .1
    path.write_text('strategy:\n  mode: bps\nbps:\n  max_dust_notional_usd: 0\n')
    assert load_config(str(path), symbol='SPY', hedge_venue='lighter', credential_env={}).bps_max_dust_notional_usd == 0


def test_legacy_profit_dust_does_not_latch_an_exit(tmp_path):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.bps_exit_mode = 'profit'
        strategy = eng.bps_strategy
        eng.entropy.position, eng.hedge.position = -.0000306, .0001
        strategy.entries = 1
        strategy.locked_entry, strategy.locked_exit = -8, 9
        strategy.closing, strategy.close_reason = True, '净收益止损'
        strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        assert eng._scan(time.time())[2].action == 'bps_open'
        assert not strategy.closing and strategy.close_reason is None
        assert not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('value', ['-1', '.nan', '.inf'])
def test_dust_cap_must_be_finite_nonnegative(tmp_path, value):
    path = tmp_path / 'config.yaml'
    path.write_text(f'strategy:\n  mode: bps\nbps:\n  max_dust_notional_usd: {value}\n')
    with pytest.raises(ConfigError):
        load_config(str(path), symbol='SPY', hedge_venue='lighter', credential_env={})
