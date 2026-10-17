"""Recorded BABA/QQQ margin cancellations and SPY dust-level lock regression."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.maker import MakerOrder
from test_bps_executable import executable_engine, quotes
from test_bps_strategy import execute
from test_bps_precision_dust import spy_engine


def margin_engine(tmp_path):
    eng = executable_engine(tmp_path)
    eng.ensure_async_state()
    eng.cfg.primary_order_type = 'maker'
    eng.cfg.primary.kind = 'arcus'
    eng.telegram.token = eng.telegram.chat_id = ''
    for v in eng.venues.values():
        v.free, v.equity = 100, 200
    quotes(eng, opening=True)
    eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('margin-test', 99.9, time.time()))
    eng.entropy.cancel_maker = AsyncMock()
    eng.entropy.maker_state = AsyncMock(return_value=dict(
        status='MARGIN_CANCELED', filled_base=0, avg_px=None, terminal=True, err=None))
    return eng


def test_margin_cancellation_does_not_become_a_ten_second_rate_limit_or_retry(tmp_path, monkeypatch):
    async def run():
        eng = margin_engine(tmp_path)
        await execute(eng, eng._scan(time.time()))
        assert not eng._venue_limited(eng.entropy)
        assert not eng.halted and not eng.bps_strategy.pending
        future = time.time() + 120
        monkeypatch.setattr('entropy_arb.engine.time.time', lambda: future)
        quotes(eng, opening=True)
        assert eng._scan(future) is None
        assert '保证金' in eng.bps_strategy.reason
        # A new successful snapshot of unchanged funds does not re-arm opens.
        eng._observe_margin_balance(eng.entropy, future)
        assert eng.entropy.key in eng._margin_blocks
        eng.entropy.free = 110
        eng._observe_margin_balance(eng.entropy, future)
        assert not eng._margin_blocks
        quotes(eng, opening=True)
        assert eng._scan(future)
        eng.entropy.place_maker.assert_awaited_once()
    asyncio.run(run())


def test_margin_block_allows_reduce_only_exit_and_survives_missing_snapshot(tmp_path):
    async def run():
        eng = margin_engine(tmp_path)
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.bps_strategy.entries = 1
        eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = 5, 4
        eng.entropy.free = None
        eng._mark_margin_block(eng.entropy)
        quotes(eng, opening=False)
        assert eng._scan(time.time())[2].action == 'bps_close'
        eng.entropy.free = 100
        eng._observe_margin_balance(eng.entropy, time.time() + 120)
        assert eng.entropy.key in eng._margin_blocks  # Establish a real baseline first.
    asyncio.run(run())


@pytest.mark.parametrize('unknown', [None, 'order', 'position', 'maker'])
def test_precision_tail_uses_current_levels_without_erasing_cost_or_position(tmp_path, unknown):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        strategy = eng.bps_strategy
        eng.entropy.position, eng.hedge.position = -.0000084, .0001
        strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        strategy.entries = 2
        strategy.locked_entry, strategy.locked_exit = -6.338201, 8.01885
        strategy.locked_price_basis = 'executable'
        strategy.entry, strategy.exit = -6.846, 8.165
        eng.entropy.set_book(766.01, 766.02)
        eng.hedge.set_book(766.49, 766.52)
        if unknown == 'order':
            strategy.pending = True
        elif unknown == 'position':
            eng._position_uncertain.add('hedge')
        elif unknown == 'maker':
            eng.active_maker = {'client_id': 'unresolved'}
        original = strategy.costs.dump()
        positions = [v.position for v in eng.venues.values()]
        if unknown == 'maker':
            strategy._refresh_dust_levels()
            candidate = None
        else:
            candidate = eng._scan(time.time())
        if unknown:
            assert candidate is None and strategy.locked_entry == -6.338201
        else:
            assert candidate and candidate[2].action == 'bps_open'
            assert strategy.locked_entry == -6.846
            assert strategy.locked_exit == 8.165
            assert strategy.snapshot()['dust_levels_current']
            assert strategy.last_cycle_net_usd is None
            assert json.loads(strategy.path.read_text())['positions'] == dict(entropy=-.0000084, hedge=.0001)
            restored = BpsStrategy(eng)
            restored.restore()
            assert restored.costs.dump() == original
            assert restored.locked_entry == -6.846
        assert strategy.costs.dump() == original
        assert [v.position for v in eng.venues.values()] == positions
    asyncio.run(run())


def test_meaningful_inventory_keeps_frozen_targets(tmp_path):
    async def run():
        eng = spy_engine(tmp_path)
        eng.ensure_async_state()
        strategy = eng.bps_strategy
        eng.entropy.position, eng.hedge.position = -.5, .5
        strategy.entries = 2
        strategy.locked_entry, strategy.locked_exit = -6.338201, 8.01885
        strategy.entry, strategy.exit = -6.846, 8.165
        strategy._refresh_dust_levels()
        assert strategy.locked_entry == -6.338201 and strategy.locked_exit == 8.01885
    asyncio.run(run())


def test_baba_negative_round_trip_reference_is_visible_without_new_profit_gate(tmp_path):
    async def run():
        eng = executable_engine(tmp_path, long=False)
        eng.ensure_async_state()
        eng.cfg.bps_entry, eng.cfg.bps_exit = -10.0627556593, 8.2387403881
        eng.bps_strategy = BpsStrategy(eng)
        eng.entropy.set_book(109.32, 109.33)
        eng.hedge.set_book(109.41, 109.42)
        candidate = eng._scan(time.time())
        assert candidate
        estimate = eng.bps_strategy.round_trip_reference(candidate[2])
        assert estimate['nonpositive'] and estimate['net_bps'] < -.89
        assert '主腿价格保持不变' in estimate['assumption']
        assert eng.bps_strategy.snapshot()['round_trip_reference']['nonpositive']
    asyncio.run(run())
