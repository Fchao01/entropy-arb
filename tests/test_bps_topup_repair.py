"""Repair log (10)'s accumulated Arcus precision tail using a verified RH IOC."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.venue_lighter import LighterVenue
from test_arcus_maker_only import arcus_engine
from test_bps_reconnect_repair import inventory, retry


def log10_engine(tmp_path, sign=1):
    eng = arcus_engine(tmp_path, long=sign > 0)
    eng.cfg.symbol = 'SPY'
    eng.entropy.min_base = eng.entropy.reduce_only_min_base = .001
    eng.hedge.min_base, eng.hedge.min_quote = .01, 10
    eng.hedge.taker_min_base = LighterVenue.taker_min_base.fget(eng.hedge)
    eng.hedge.taker_min_quote = LighterVenue.taker_min_quote
    eng.hedge.reduce_only_min_base = LighterVenue.reduce_only_min_base.fget(eng.hedge)
    eng.hedge.reduce_only_min_quote = LighterVenue.reduce_only_min_quote
    eng.entropy.set_book(768.53, 768.54)
    eng.hedge.set_book(769.01, 769.02)
    eng.bps_strategy = BpsStrategy(eng)
    inventory(eng, sign * .7772325, -sign * .7771)
    eng.bps_strategy.entries = 8
    eng.bps_strategy.locked_entry = 6.09029659489968
    eng.bps_strategy.locked_exit = -6.50262706133331
    return eng


@pytest.mark.parametrize('sign', [1, -1])
def test_log10_accumulated_tail_tops_up_deficient_leg_without_arcus_ioc(tmp_path, sign, caplog):
    async def run():
        eng = log10_engine(tmp_path, sign)
        strategy = eng.bps_strategy
        assert not strategy.net_balanced()
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_not_awaited()
        eng.entropy.place_maker.assert_not_awaited()
        eng.hedge.send_taker.assert_awaited_once()
        call = eng.hedge.send_taker.call_args.kwargs
        assert call['qty'] == pytest.approx(.0001)
        assert call['is_buy'] is (sign < 0) and not call.get('reduce_only', False)
        assert eng.hedge.position == pytest.approx(-sign * .7772)
        assert sum(v.position for v in eng.venues.values()) == pytest.approx(sign * .0000325)
        assert strategy.precision_dust() and not strategy.pending and not eng.halted
        assert strategy.entries == 8
        assert strategy.locked_exit == -6.50262706133331
        assert not strategy.costs.known  # Inherited prices stay unknown.
        assert 'mode=top-up' in caplog.text
        for v in eng.venues.values():
            v.has_open_orders.assert_awaited_once()
            v.fetch_position.assert_awaited_once()
        assert json.loads(strategy.path.read_text())['positions']['hedge'] == pytest.approx(-sign * .7772)
        await eng._maybe_hedge()
        eng.hedge.send_taker.assert_awaited_once()
    asyncio.run(run())


def test_recent_partial_fill_defers_until_rest_grace_then_counts_entry_once(tmp_path):
    async def run():
        eng = log10_engine(tmp_path)
        strategy = eng.bps_strategy
        from entropy_arb.book import ArbPlan
        plan = ArbPlan(.65, 768.53, 769.01, 500, 500, 1, 1000, 6.2, 6.2, 0, 0,
                       action='bps_open')
        strategy.before_execution(plan)
        eng.hedge.last_traded_ts = time.time()
        await eng._maybe_hedge()
        strategy.after_execution(plan, .6501569, False)
        assert strategy.repair_waiting and strategy.pending and not eng.halted
        eng.hedge.send_taker.assert_not_awaited()
        eng.hedge.last_traded_ts = 0
        await retry(eng)
        assert strategy.entries == 9 and not strategy.pending and not eng.halted
        await eng._maybe_hedge()
        assert strategy.entries == 9
        eng.hedge.send_taker.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['query', 'open_order', 'different_position', 'stale', 'manual_pause', 'stop'])
def test_topup_requires_clean_actual_orders_positions_and_running_engine(tmp_path, failure):
    async def run():
        eng = log10_engine(tmp_path)
        if failure == 'query':
            eng.hedge.fetch_position.side_effect = OSError('account unavailable')
        elif failure == 'open_order':
            eng.hedge.has_open_orders.return_value = True
        elif failure == 'different_position':
            eng.hedge.fetch_position.side_effect = None
            eng.hedge.fetch_position.return_value = -.7
        elif failure == 'stale':
            eng.hedge.book.alive_ts = 0
        elif failure == 'manual_pause':
            eng.manual_paused = True
        else:
            eng.stop.set()
        await eng._maybe_hedge()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
        eng.entropy.place_maker.assert_not_awaited()
        assert eng.entropy.position == .7772325 and eng.hedge.position == -.7771
        if failure == 'different_position':
            assert eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('constraint', ['min_base', 'min_quote', 'position_cap', 'order_cap', 'depth', 'wrong_direction'])
def test_topup_never_rounds_up_bypasses_caps_or_changes_direction(tmp_path, constraint):
    async def run():
        eng = log10_engine(tmp_path)
        if constraint == 'min_base':
            eng.hedge.taker_min_base = .001
        elif constraint == 'min_quote':
            eng.hedge.taker_min_quote = 10
        elif constraint == 'position_cap':
            eng.cfg.bps_max_position_usd = .77715 * 769
        elif constraint == 'order_cap':
            eng.cfg.max_order_notional = .05
        elif constraint == 'depth':
            eng.hedge.set_book(769.01, 769.02, sz=.00001)
        else:
            eng.cfg.bps_direction = 'short_primary'
        qty, _, blocked = eng._topup_order(eng.hedge, .0001325)
        assert qty == 0 and blocked
        await eng._maybe_hedge()
        eng.hedge.send_taker.assert_not_awaited()
        assert eng.halted and eng.bps_strategy.pending
    asyncio.run(run())


def test_tradeable_excess_is_reduced_before_any_topup(tmp_path):
    async def run():
        eng = log10_engine(tmp_path)
        eng.entropy.position = .8
        eng.cfg.max_order_notional = 100
        await eng._maybe_hedge()
        assert eng.entropy.position == pytest.approx(.7771)
        eng.entropy.place_maker.assert_awaited_once()
        assert eng.entropy.place_maker.call_args.kwargs['reduce_only']
        eng.hedge.send_taker.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('outcome', ['unknown', 'rejected', 'invalid_quantity', 'invalid_price'])
def test_failed_or_unknown_topup_cannot_repeat_or_resume(tmp_path, outcome):
    async def run():
        eng = log10_engine(tmp_path)
        result = dict(status='filled', filled_base=.0001, avg_px=769.01, err=None, unresolved=False)
        if outcome == 'unknown':
            result.update(status='timeout', filled_base=0, avg_px=None, unresolved=True)
        elif outcome == 'rejected':
            result.update(status='rejected', filled_base=0, avg_px=None, err='minimum rejected')
        elif outcome == 'invalid_quantity':
            result['filled_base'] = .00012  # Even less than half a step overfill is invalid.
        else:
            result['avg_px'] = float('nan')
        eng.hedge.send_taker = AsyncMock(return_value=result)
        await eng._maybe_hedge()
        await eng._maybe_hedge()
        await retry(eng)
        eng.hedge.send_taker.assert_awaited_once()
        assert eng.halted and eng.bps_strategy.pending
        assert eng.hedge.position == -.7771
        assert eng.execution_stats.unresolved is (outcome != 'rejected')
    asyncio.run(run())


def test_old_paused_log10_checkpoint_recovers_without_deleting_history(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
        eng = log10_engine(tmp_path)
        eng.bps_strategy.pending = True
        eng.bps_strategy.save()
        original = eng.bps_strategy.path.read_text()
        eng.bps_strategy = BpsStrategy(eng)
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        eng.hedge.send_taker.assert_awaited_once()
        assert eng.hedge.send_taker.call_args.kwargs['qty'] == pytest.approx(.0001)
        assert not eng.bps_strategy.pending and not eng.halted
        assert eng.bps_strategy.entries == 8 and eng.bps_strategy.precision_dust()
        assert not eng.bps_strategy.costs.known
        archive, = eng.bps_strategy.path.parent.glob('*.recovered-*.bak')
        assert archive.read_text() == original
    asyncio.run(run())


def test_topup_checkpoint_failure_sends_nothing(tmp_path, monkeypatch):
    async def run():
        eng = log10_engine(tmp_path)
        monkeypatch.setattr(eng.bps_strategy, 'save', lambda: (_ for _ in ()).throw(OSError('disk full')))
        with pytest.raises(OSError, match='disk full'):
            await eng._maybe_hedge()
        eng.hedge.send_taker.assert_not_awaited()
        assert eng.halted and eng.bps_strategy.pending
    asyncio.run(run())


def test_partial_topup_uses_remaining_net_after_confirmation(tmp_path):
    async def run():
        eng = log10_engine(tmp_path)
        # Use the documented IOC step, with a larger total net. Each confirmed
        # partial fill changes the next request; unknown outcomes never do.
        eng.entropy.position = .77795
        results = [dict(status='partial', filled_base=.0004, avg_px=769.01, err=None, unresolved=False),
                   dict(status='filled', filled_base=.0004, avg_px=769.01, err=None, unresolved=False)]
        eng.hedge.send_taker = AsyncMock(side_effect=results)
        await eng._maybe_hedge()
        assert eng.bps_strategy.repair_waiting and not eng.halted
        assert eng.hedge.send_taker.await_count == 1
        eng.hedge.last_traded_ts = 0
        await retry(eng)
        assert [c.kwargs['qty'] for c in eng.hedge.send_taker.call_args_list] == pytest.approx([.0008, .0004])
        assert eng.bps_strategy.precision_dust() and not eng.bps_strategy.pending
    asyncio.run(run())


def test_explicit_topup_rate_limit_waits_and_retries_only_after_cooldown(tmp_path):
    async def run():
        eng = log10_engine(tmp_path)
        eng.hedge.send_taker = AsyncMock(return_value=dict(status='rejected', filled_base=0,
                                                          avg_px=None, err='RATE_LIMITED: 429', unresolved=False))
        await eng._maybe_hedge()
        assert not eng.halted and eng.bps_strategy.pending and eng.bps_strategy.repair_waiting
        eng.hedge.send_taker.assert_awaited_once()
        await retry(eng)
        eng.hedge.send_taker.assert_awaited_once()
        eng._venue_limited_until.clear()
        eng.hedge.rate_limit_until = 0
        eng.hedge.last_traded_ts = 0
        eng.hedge.send_taker.return_value = dict(status='filled', filled_base=.0001, avg_px=769.01,
                                                err=None, unresolved=False)
        await retry(eng)
        assert eng.hedge.send_taker.await_count == 2
        assert not eng.bps_strategy.pending and not eng.halted
    asyncio.run(run())


def test_known_cycle_keeps_actual_topup_costs_and_fees(tmp_path):
    async def run():
        eng = log10_engine(tmp_path)
        strategy = eng.bps_strategy
        from entropy_arb.bps_pnl import CycleCosts
        strategy.costs = CycleCosts()
        for v, buy in ((eng.entropy, True), (eng.hedge, False)):
            strategy.costs.record(v.key, buy, abs(v.position), 769, 0, 0, eng._step,
                                  v is eng.entropy, time.time())
        before_cash, before_basis = strategy.costs.cash, strategy.costs.open_notional
        eng.hedge.fee_bps = 2
        await eng._maybe_hedge()
        actual_price = eng.hedge.send_taker.call_args.kwargs['limit_px']
        assert strategy.costs.known
        assert strategy.costs.cash == pytest.approx(before_cash + .0001 * actual_price * (1 - .0002))
        assert strategy.costs.open_notional == before_basis
        assert eng.total_fees_usd == pytest.approx(.0001 * actual_price * .0002)
        assert eng.hedge.volume_usd == pytest.approx(.0001 * actual_price)
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.costs.dump() == strategy.costs.dump()
    asyncio.run(run())
