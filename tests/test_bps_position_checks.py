"""Position verification and residual hedging, without exchange access."""
import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.maker import MakerOrder
from test_bps_strategy import books, engine_at, execute


@pytest.mark.parametrize('long', [True, False])
def test_restart_mid_exit_rechecks_level_and_can_add(tmp_path, long):
    async def run():
        eng = engine_at(tmp_path, long=long)
        eng.ensure_async_state()
        eng.cfg.max_order_notional = 100
        for _ in range(2):
            books(eng, -10 if long else 10)
            await execute(eng, eng._scan(time.time()))
        books(eng, -5 if long else 5)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.closing and not eng.bps_strategy.flat()
        eng.bps_strategy = BpsStrategy(eng)
        eng.bps_strategy.restore()
        books(eng, -8 if long else 8)
        assert eng._scan(time.time()) is None
        assert not eng.bps_strategy.closing
        assert not json.loads(eng.bps_strategy.path.read_text())['closing']
        books(eng, -10 if long else 10)
        await execute(eng, eng._scan(time.time()))
        assert not eng.halted
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        assert eng.bps_strategy.costs.known
        restored = BpsStrategy(eng)
        restored.restore()  # Re-adds must preserve the remaining inventory basis.
        assert restored.locked_exit == (-6 if long else 6)
    asyncio.run(run())


@pytest.mark.parametrize('positions,halted', [([.5, 1], False), ([.5, .5], True)])
def test_discrepant_rest_position_is_verified_before_adoption(tmp_path, positions, halted):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.entropy.fetch_position = AsyncMock(side_effect=positions)
        with patch('entropy_arb.engine.asyncio.sleep', new=AsyncMock()):
            await eng._reconcile_venue(eng.entropy, False)
        assert eng.entropy.fetch_position.await_count == 2
        assert eng.halted is halted
        assert eng.entropy.position == positions[-1]
        assert eng.bps_strategy.pending is halted
        assert not eng._position_uncertain
    asyncio.run(run())


def test_changing_or_unavailable_rest_positions_block_orders_without_overwriting_fills(tmp_path):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.entropy.fetch_position = AsyncMock(side_effect=[.5, .6, RuntimeError('429'), 1])
        with patch('entropy_arb.engine.asyncio.sleep', new=AsyncMock()):
            await eng._reconcile_venue(eng.entropy, False)
        assert not eng.halted and eng.entropy.position == 1
        assert eng._scan(time.time()) is None
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_not_awaited()
        await eng._reconcile_venue(eng.entropy, False)
        assert eng._position_uncertain
        await eng._reconcile_venue(eng.entropy, False)
        assert not eng._position_uncertain
        assert eng._scan(time.time()) is not None
    asyncio.run(run())


def test_confirmed_partial_residual_hedge_retries_without_overhedging(tmp_path):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = 2, -1
        eng.entropy.send_taker = AsyncMock(side_effect=[
            dict(status='partial', filled_base=.6, avg_px=100, err=None, unresolved=False),
            dict(status='filled', filled_base=.4, avg_px=100, err=None, unresolved=False)])
        await eng._maybe_hedge()
        assert [c.kwargs['qty'] for c in eng.entropy.send_taker.call_args_list] == pytest.approx([1, .4])
        assert all(c.kwargs['reduce_only'] for c in eng.entropy.send_taker.call_args_list)
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        assert not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['unknown', 'exception', 'invalid'])
def test_unknown_residual_hedge_is_not_retried_or_cleared(tmp_path, failure):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.entropy.position, eng.hedge.position = 2, -1
        eng.entropy.send_taker = AsyncMock(return_value=dict(
            status='timeout', filled_base=0, avg_px=None, err=None, unresolved=True))
        if failure == 'exception':
            eng.entropy.send_taker.side_effect = RuntimeError('connection closed')
        elif failure == 'invalid':
            eng.entropy.send_taker.return_value = dict(
                status='filled', filled_base=2, avg_px=100, err=None, unresolved=False)
        await eng._maybe_hedge()
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_awaited_once()
        assert eng.halted and eng.bps_strategy.pending
    asyncio.run(run())


def test_confirmed_maker_partial_hedge_repairs_excess_primary_before_resuming(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        eng.ensure_async_state()
        eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('test', 99.9, time.time()))
        eng.entropy.maker_state = AsyncMock(return_value=dict(
            filled_base=.5, avg_px=99.9, terminal=True, status='filled', err=None))
        eng.hedge.send_taker = AsyncMock(return_value=dict(
            filled_base=.3, avg_px=100, status='partial', err=None, unresolved=False))
        await execute(eng, eng._scan(time.time()))
        eng.hedge.send_taker.assert_awaited_once()
        eng.entropy.send_taker.assert_awaited_once()
        repair = eng.entropy.send_taker.call_args.kwargs
        assert repair['qty'] == pytest.approx(.2) and repair['reduce_only'] and not repair['is_buy']
        assert not eng.halted and not eng.bps_strategy.pending
        assert eng.entropy.position == pytest.approx(.3)
        assert eng.hedge.position == pytest.approx(-.3)
        assert eng.active_maker is None
        BpsStrategy(eng).restore()  # Includes the repair fill in the cost ledger.
    asyncio.run(run())
