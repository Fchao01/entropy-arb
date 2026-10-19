"""Confirmed residuals across outages: fake fills/REST only, no exchange access."""
import asyncio
import json
import time
from collections import deque
from unittest.mock import AsyncMock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from test_bps_precision_dust import spy_engine
from test_bps_strategy import engine_at
from test_arcus_maker_only import arcus_engine


def account_reads(eng):
    for venue in eng.venues.values():
        venue.has_open_orders = AsyncMock(return_value=False)
        venue.fetch_position = AsyncMock(side_effect=lambda v=venue: v.position)


def inventory(eng, primary, hedge):
    eng.ensure_async_state()
    eng.entropy.position, eng.hedge.position = primary, hedge
    eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
    account_reads(eng)


def quotes(eng):
    eng.entropy.set_book(761.73, 761.74)
    eng.hedge.set_book(762.12, 762.15)


async def retry(eng):
    # Reach the retry deadline without real sleeps.
    eng.bps_strategy._repair_retry_at = 0
    await eng._evaluate()


def test_log_four_cent_tail_waits_for_reconnect_and_resumes_without_order(tmp_path, caplog):
    async def run():
        eng = spy_engine(tmp_path)
        eng.cfg.primary_order_type = 'maker'
        inventory(eng, .8999478, -.9)
        strategy = eng.bps_strategy
        strategy.entries, strategy.closing = 3, True
        strategy.locked_entry, strategy.locked_exit = 6.9456, -7.4376
        quotes(eng)
        assert strategy.precision_dust()
        eng.hedge.book.alive_ts = 0
        await eng._maybe_hedge()
        assert strategy.repair_waiting and strategy.pending and not eng.halted
        assert json.loads(strategy.path.read_text())['pending']
        assert eng._scan(time.time()) is None
        # Repeated evaluations during the outage neither send nor query REST.
        await retry(eng)
        eng.hedge.fetch_position.assert_not_awaited()
        quotes(eng)
        await retry(eng)
        assert not strategy.repair_waiting and not strategy.pending and not eng.halted
        assert strategy.entries == 3 and strategy.closing
        assert strategy.last_cycle_net_usd is None
        assert eng.entropy.position == .8999478 and eng.hedge.position == -.9
        assert strategy.residual_status()['accepted_dust']
        assert not json.loads(strategy.path.read_text())['pending']
        for v in eng.venues.values():
            v.has_open_orders.assert_awaited_once()
            v.fetch_position.assert_awaited_once()
            v.send_taker.assert_not_awaited()
        assert '[BPS REPAIR RESUMED]' in caplog.text
    asyncio.run(run())


@pytest.mark.parametrize('resume_via', ['market_event', 'periodic_reconcile'])
def test_log8_positive_arcus_tail_periodic_check_during_rh_disconnect(tmp_path, resume_via, caplog):
    async def run():
        eng = arcus_engine(tmp_path)
        eng.entropy.min_base = eng.entropy.reduce_only_min_base = .001
        inventory(eng, 1.3032569, -1.3032)
        strategy = eng.bps_strategy
        strategy.entries = 7
        strategy.locked_entry, strategy.locked_exit = 6.287527512848445, -6.706714078499099
        eng.entropy.set_book(759.72, 759.73)
        eng.hedge.set_book(760.29, 760.31)
        costs = strategy.costs.dump()
        assert strategy.precision_dust()
        # No order is executing: the periodic REST reconciliation, rather
        # than after_execution, encounters a stale RH book and a positive
        # tail on Arcus below its .001 minimum (engine (8).log 03:16:48).
        eng.hedge.book.alive_ts = 0
        await eng._reconcile_positions(hedge=True)
        assert not eng.halted
        assert strategy.repair_waiting and strategy.pending
        assert '[BPS REPAIR WAIT]' in caplog.text
        eng.hedge.set_book(760.29, 760.31)
        strategy._repair_retry_at = 0
        if resume_via == 'market_event':
            await eng._evaluate()
        else:
            await eng._reconcile_positions(hedge=True)
        assert not eng.halted and not strategy.pending and not strategy.repair_waiting
        assert strategy.entries == 7 and strategy.locked_exit == -6.706714078499099
        assert eng.entropy.position == 1.3032569 and eng.hedge.position == -1.3032
        assert strategy.costs.dump() == costs and strategy.last_cycle_net_usd is None
        assert strategy.residual_status()['notional_usd'] == pytest.approx(.04326107)
        eng.entropy.place_maker.assert_not_awaited()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
            v.has_open_orders.assert_awaited_once()
        assert '[BPS REPAIR RESUMED]' in caplog.text
    asyncio.run(run())


def test_log8_upgrade_recovers_old_halted_checkpoint_without_deleting_it(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
        eng = arcus_engine(tmp_path)
        eng.bps_strategy = BpsStrategy(eng)  # Fixture selects Arcus after constructing its first strategy.
        eng.entropy.min_base = eng.entropy.reduce_only_min_base = .001
        inventory(eng, 1.3032569, -1.3032)
        strategy = eng.bps_strategy
        strategy.entries, strategy.pending = 7, True
        strategy.locked_entry, strategy.locked_exit = 6.287527512848445, -6.706714078499099
        strategy.save()
        original = strategy.path.read_text()
        eng.bps_strategy = BpsStrategy(eng)
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        eng._check_starting_positions()
        eng.bps_strategy.restore()
        assert not eng.halted and not eng.bps_strategy.pending
        assert eng.bps_strategy.entries == 7
        assert eng.bps_strategy.residual_status()['accepted_dust']
        archives = list(strategy.path.parent.glob(strategy.path.name + '.recovered-*.bak'))
        assert len(archives) == 1 and archives[0].read_text() == original
        eng.entropy.place_maker.assert_not_awaited()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('sign', [1, -1])
def test_orderable_imbalance_reduces_excess_after_reconnect(tmp_path, sign):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2 * sign, -sign)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        assert eng.bps_strategy.repair_waiting and not eng.halted
        eng.entropy.set_book(100, 100.01)
        await retry(eng)
        call = eng.entropy.send_taker.call_args.kwargs
        assert call['qty'] == 1 and call['reduce_only']
        assert call['is_buy'] is (sign < 0)
        assert eng.entropy.position == sign and eng.hedge.position == -sign
        assert not eng.bps_strategy.pending and not eng.halted
        eng.hedge.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_deferred_open_batch_accounting_happens_exactly_once(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        eng.ensure_async_state()
        strategy = eng.bps_strategy
        plan = eng._scan(time.time())[2]
        strategy.before_execution(plan)
        inventory(eng, plan.qty, -plan.qty + .2)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        strategy.after_execution(plan, 0, False)
        assert strategy.entries == 0 and strategy.pending and not eng.halted
        eng.entropy.set_book(100, 100.01)
        await retry(eng)
        assert strategy.entries == 1 and not strategy.pending
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        await eng._maybe_hedge()
        assert strategy.entries == 1
        eng.entropy.send_taker.assert_awaited_once()
    asyncio.run(run())


def test_repeated_confirmed_zero_fills_retry_later_without_hard_pause(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.send_taker.side_effect = None
        eng.entropy.send_taker.return_value = dict(status='canceled', filled_base=0,
                                                  avg_px=None, err=None, unresolved=False)
        await eng._maybe_hedge()
        assert eng.entropy.send_taker.await_count == 3
        assert eng.bps_strategy.repair_waiting and not eng.halted
        eng.entropy.send_taker.return_value = dict(status='filled', filled_base=1,
                                                  avg_px=100, err=None, unresolved=False)
        eng.entropy.last_traded_ts = 0
        await retry(eng)
        assert eng.entropy.send_taker.await_count == 4
        assert not eng.bps_strategy.pending and not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('unavailable', ['query_error', 'open_order', 'invalid_position'])
def test_failed_account_verification_waits_then_retries(tmp_path, unavailable):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        if unavailable == 'query_error':
            eng.hedge.fetch_position.side_effect = RuntimeError('transport failure')
        elif unavailable == 'open_order':
            eng.hedge.has_open_orders.return_value = True
        else:
            eng.hedge.fetch_position.side_effect = None
            eng.hedge.fetch_position.return_value = float('nan')
        await retry(eng)
        assert eng.bps_strategy.pending and not eng.halted
        eng.entropy.send_taker.assert_not_awaited()
        account_reads(eng)
        await retry(eng)
        assert not eng.bps_strategy.pending and not eng.halted
        eng.entropy.send_taker.assert_awaited_once()
    asyncio.run(run())


def test_external_position_change_is_not_repaired_from_stale_ledger(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        eng.entropy.fetch_position.side_effect = None
        eng.entropy.fetch_position.return_value = 3
        await retry(eng)
        assert eng.halted and eng.bps_strategy.pending
        assert eng.entropy.position == 2
        eng.entropy.send_taker.assert_not_awaited()
        account_reads(eng)
        await retry(eng)
        assert eng.halted
    asyncio.run(run())


def test_unknown_repair_fill_remains_paused_after_reconnect(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        eng.entropy.send_taker.side_effect = None
        eng.entropy.send_taker.return_value = dict(status='timeout', filled_base=0,
                                                  avg_px=None, err=None, unresolved=True)
        await retry(eng)
        assert eng.halted and eng.bps_strategy.pending
        await retry(eng)
        await eng._maybe_hedge()
        eng.entropy.send_taker.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('blocker', ['manual', 'stop', 'hard_pause', 'unknown_position', 'live_maker'])
def test_reconnect_does_not_clear_other_blocks(tmp_path, blocker):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        if blocker == 'manual':
            eng.manual_paused = True
        elif blocker == 'stop':
            eng.stop.set()
        elif blocker == 'hard_pause':
            eng.halted = True
        elif blocker == 'unknown_position':
            eng._position_uncertain.add(eng.hedge.key)
        else:
            eng.active_maker = {'client_id': 'unknown'}
        await retry(eng)
        await eng._maybe_hedge()
        assert eng.bps_strategy.pending
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_overlapping_recovery_callers_send_only_one_repair(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        eng.bps_strategy._repair_retry_at = 0
        await asyncio.gather(eng._evaluate(), eng._maybe_hedge())
        eng.entropy.send_taker.assert_awaited_once()
        assert eng.entropy.position == 1 and not eng.bps_strategy.pending
    asyncio.run(run())


def test_market_ticks_do_not_bypass_retry_backoff(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        for _ in range(30):
            await eng._evaluate()
        eng.entropy.fetch_position.assert_not_awaited()
        eng.hedge.fetch_position.side_effect = RuntimeError('unavailable')
        await retry(eng)
        assert eng.hedge.fetch_position.await_count == 1
        for _ in range(30):
            await eng._evaluate()
        assert eng.hedge.fetch_position.await_count == 1
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_recent_fills_wait_for_account_snapshot_grace(tmp_path, monkeypatch):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        now = time.time()
        eng.entropy.last_traded_ts = now
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        await retry(eng)
        eng.entropy.fetch_position.assert_not_awaited()
        eng.entropy.send_taker.assert_not_awaited()
        monkeypatch.setattr('entropy_arb.engine.time.time', lambda: now + 6)
        await retry(eng)
        eng.entropy.send_taker.assert_awaited_once()
        assert not eng.bps_strategy.pending and not eng.halted
    asyncio.run(run())


def test_send_budget_waits_then_repairs_without_exceeding_limit(tmp_path):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng._sends[eng.entropy.key] = deque([time.time()] * eng.entropy.orders_per_min)
        await eng._maybe_hedge()
        assert eng.bps_strategy.repair_waiting and not eng.halted
        eng.entropy.send_taker.assert_not_awaited()
        eng._sends[eng.entropy.key].clear()
        await retry(eng)
        eng.entropy.send_taker.assert_awaited_once()
        assert not eng.bps_strategy.pending and not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('sign', [1, -1])
def test_arcus_reconnect_repair_keeps_post_only_and_never_sends_other_leg(tmp_path, sign):
    async def run():
        eng = arcus_engine(tmp_path)
        inventory(eng, sign * .04, 0)
        strategy = eng.bps_strategy
        strategy.entries = 1
        eng.entropy.book.alive_ts = 0
        await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        await retry(eng)
        call = eng.entropy.place_maker.call_args.kwargs
        assert call['reduce_only'] and call['is_buy'] is (sign < 0)
        assert call['qty'] == .04
        eng.entropy.send_taker.assert_not_awaited()
        eng.hedge.send_taker.assert_not_awaited()
        assert eng.active_maker is None and strategy.flat()
        assert strategy.entries == 0 and not strategy.pending and not eng.halted
    asyncio.run(run())


def test_disk_failure_during_wait_prevents_automatic_repair(tmp_path, monkeypatch):
    async def run():
        eng = engine_at(tmp_path, maker=True)
        inventory(eng, 2, -1)
        eng.entropy.book.alive_ts = 0
        monkeypatch.setattr(eng.bps_strategy, 'save', lambda: (_ for _ in ()).throw(OSError('disk full')))
        with pytest.raises(OSError):
            await eng._maybe_hedge()
        eng.entropy.set_book(100, 100.01)
        await retry(eng)
        assert eng.halted and eng.bps_strategy.pending
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())
