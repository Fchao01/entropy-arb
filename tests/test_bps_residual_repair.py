"""Reproduce the ETH partial-close log; all fills and account reads are fake."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.venue_lighter import LighterVenue
from test_bps_strategy import engine_at, execute


def eth_engine(tmp_path, long=True):
    eng = engine_at(tmp_path, long=long)
    eng.cfg.symbol = 'ETH'
    eng.cfg.bps_price_basis = 'executable'
    eng.cfg.bps_entry, eng.cfg.bps_exit = 1.27, -1.5078831178596546
    eng.cfg.take_fraction = 1
    eng.cfg.max_order_notional = 15
    eng._min_base = .005
    for venue, minimum, name in zip(eng.venues.values(), (.005, .002), ('RH', 'LIGHTER')):
        venue.min_base, venue.min_quote, venue.name = minimum, 10, name
        venue.reduce_only_min_base = LighterVenue.reduce_only_min_base.fget(venue)
        venue.reduce_only_min_quote = LighterVenue.reduce_only_min_quote
    eng.bps_strategy = BpsStrategy(eng)
    eng.ensure_async_state()
    set_quotes(eng, long)
    return eng


def set_quotes(eng, primary_cheap):
    eng.entropy.set_book(2441, 2441.02) if primary_cheap else eng.entropy.set_book(2442.16, 2442.18)
    eng.hedge.set_book(2442.16, 2442.18) if primary_cheap else eng.hedge.set_book(2441, 2441.02)


def fill(qty, status='filled', price=2441.84):
    return dict(status=status, filled_base=qty, avg_px=price if qty else None, err=None, unresolved=False)


@pytest.mark.parametrize('long', [True, False])
@pytest.mark.parametrize('partial_leg', ['entropy', 'hedge'])
def test_partial_close_automatically_clears_0006_eth_tail(tmp_path, long, partial_leg):
    async def run():
        eng = eth_engine(tmp_path, long)
        opening = eng._scan(time.time())
        assert opening[2].qty == .0061
        await execute(eng, opening)
        eng.cfg.max_order_notional = 100
        set_quotes(eng, not long)
        partial = eng.venues[partial_leg]
        partial.send_taker.side_effect = [fill(.0055, 'canceled-too-much-slippage'), fill(.0006)]
        closing = eng._scan(time.time())
        assert closing[2].action == 'bps_close' and closing[2].qty == .0061
        await execute(eng, closing)
        calls = partial.send_taker.call_args_list
        assert [c.kwargs['qty'] for c in calls[-2:]] == pytest.approx([.0061, .0006])
        assert all(c.kwargs['reduce_only'] for c in calls[-2:])
        assert eng.bps_strategy.flat() and eng.bps_strategy.entries == 0
        assert not eng.halted and not eng.bps_strategy.pending
        assert not json.loads(eng.bps_strategy.path.read_text())['pending']
        assert eng.bps_strategy.last_cycle_net_usd is not None
    asyncio.run(run())


def test_paired_small_inventory_can_exit_but_opening_still_has_minimums(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.cfg.max_order_notional = 1.5
        assert eng._scan(time.time()) is None  # Opening minimums were not relaxed.
        eng.entropy.position, eng.hedge.position = .0006, -.0006
        eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        eng.bps_strategy.entries = 1
        eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = 1.27, -1.5078831178596546
        set_quotes(eng, False)
        closing = eng._scan(time.time())
        assert closing[2].qty == .0006
        await execute(eng, closing)
        assert eng.bps_strategy.flat() and not eng.halted
    asyncio.run(run())


def test_nonlighter_close_keeps_venue_minimum_instead_of_opening_floor(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.cfg.min_order_notional = 100
        for venue in eng.venues.values():
            del venue.reduce_only_min_base
            del venue.reduce_only_min_quote
        eng.entropy.position, eng.hedge.position = .0061, -.0061
        strategy = eng.bps_strategy
        strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        strategy.entries = 1
        strategy.locked_entry, strategy.locked_exit = 1.27, -1.5078831178596546
        set_quotes(eng, False)
        closing = eng._scan(time.time())
        assert closing[2].qty == .0061  # ~$15 clears venue $10, below opening floor $100.
        await execute(eng, closing)
        assert strategy.flat() and not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('side', ['entropy', 'hedge'])
def test_old_pending_tail_is_repaired_on_restart(tmp_path, monkeypatch, side):
    async def run():
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
        eng = eth_engine(tmp_path)
        eng.entropy.position, eng.hedge.position = ((.0006, 6.07153e-18) if side == 'entropy' else (0, -.0006))
        strategy = eng.bps_strategy
        strategy.entries, strategy.pending = 1, True
        strategy.locked_entry, strategy.locked_exit = 1.27, -1.5078831178596546
        strategy.save()
        for v in eng.venues.values():
            v.has_open_orders = AsyncMock(return_value=False)
            v.fetch_position = AsyncMock(side_effect=lambda v=v: v.position)
        eng.bps_strategy = BpsStrategy(eng)
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        eng.bps_strategy.restore()
        assert eng.bps_strategy.flat() and not eng.bps_strategy.pending
        assert not eng.halted
        assert eng.venues[side].send_taker.call_args.kwargs['qty'] == .0006
        assert eng.venues[side].send_taker.call_args.kwargs['reduce_only']
    asyncio.run(run())


def test_confirmed_zero_fill_correction_retries_without_duplicate_unknown_order(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.entropy.position = .0006
        eng.entropy.send_taker.side_effect = [fill(0, 'canceled-too-much-slippage'), fill(.0006)]
        await eng._maybe_hedge()
        assert eng.entropy.send_taker.await_count == 2
        assert eng.bps_strategy.flat() and not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('unknown', [True, False])
def test_failed_correction_reason_is_preserved(tmp_path, unknown):
    async def run():
        eng = eth_engine(tmp_path)
        plan = eng._scan(time.time())[2]
        eng.bps_strategy.before_execution(plan)
        eng.entropy.position = .0006
        eng.entropy.send_taker.return_value = dict(status='timeout' if unknown else 'send-failed',
                                                  filled_base=0, avg_px=None,
                                                  err=None if unknown else 'rejected', unresolved=unknown)
        eng.entropy.send_taker.side_effect = None
        await eng._maybe_hedge()
        eng.bps_strategy.after_execution(plan, 0, False)
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_awaited_once()
        assert eng.halted and eng.bps_strategy.pending
        assert ('结果未确认' in eng.bps_strategy.reason) is unknown
        assert ('提交失败' in eng.bps_strategy.reason) is (not unknown)
        assert '成交已确认' not in eng.bps_strategy.reason
    asyncio.run(run())


def test_other_venue_minimums_are_not_bypassed_and_reason_is_explicit(tmp_path, caplog):
    async def run():
        eng = eth_engine(tmp_path)
        del eng.entropy.reduce_only_min_base
        del eng.entropy.reduce_only_min_quote
        plan = eng._scan(time.time())[2]
        eng.bps_strategy.before_execution(plan)
        eng.entropy.position = .0006
        await eng._maybe_hedge()
        eng.bps_strategy.after_execution(plan, 0, False)
        assert eng.halted and eng.bps_strategy.pending
        assert '成交已确认' in eng.bps_strategy.reason
        assert '+0.0006 ETH' in eng.bps_strategy.reason
        assert '最小数量 0.005' in eng.bps_strategy.reason
        assert '最小金额 $10.0000' in eng.bps_strategy.reason
        assert '未确认' not in eng.bps_strategy.reason
        assert '[BPS PAUSED]' in caplog.text
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_error_count_pause_still_repairs_a_confirmed_one_leg_fill(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.cfg.max_consecutive_errors = 1
        eng.entropy.send_taker.side_effect = None
        eng.entropy.send_taker.return_value = dict(status='send-failed', filled_base=0,
                                                  avg_px=None, err='explicit rejection', unresolved=False)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat()  # Successful leg was reduced despite the pause.
        assert eng.hedge.send_taker.await_count == 2
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
        assert eng.halted and eng.bps_strategy.pending  # Do not remove the error-count protection.
        assert '连续 1 次下单失败' in eng.bps_strategy.reason
    asyncio.run(run())
