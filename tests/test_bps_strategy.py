"""Paired bps position lifecycle: synthetic prices/orders, no exchange access."""
import asyncio
import csv
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy, rolling_levels, recorder_source
from entropy_arb.config import ConfigError, load_config
from entropy_arb.maker import MakerOrder
from test_engine import make_engine


def engine_at(tmp_path, maker=False, long=True):
    eng = make_engine()
    eng.cfg.strategy_mode = 'bps'
    eng.cfg.bps_levels = 'manual'
    eng.cfg.bps_direction = 'long_primary' if long else 'short_primary'
    eng.cfg.bps_entry, eng.cfg.bps_exit = (-9, -6) if long else (9, 6)
    eng.cfg.bps_add_interval_sec = 0
    eng.cfg.leg_slippage_bps = 0
    eng.cfg.staleness_sec = 60
    eng.cfg.primary_order_type = 'maker' if maker else 'taker'
    eng.cfg.maker_timeout_sec = .001
    eng.cfg.settle_timeout_sec = .02
    eng.cfg.log_file = str(tmp_path / 'engine.log')
    eng.cfg.recorder_csv = str(tmp_path / 'minutes.csv')
    for venue in eng.venues.values():
        venue.volume_usd = 0
        venue.px_round = lambda price, round_up: price
        async def send(**args):
            return dict(status='filled', filled_base=args['qty'], avg_px=args['limit_px'], err=None, unresolved=False)
        venue.send_taker = AsyncMock(side_effect=send)
    eng.bps_strategy = BpsStrategy(eng)
    books(eng, -9.5 if long else 9.5)
    return eng


def books(eng, premium):
    primary = 100 * (1 + premium / 1e4)
    eng.entropy.set_book(primary - .0005, primary + .0005)
    eng.hedge.set_book(99.9995, 100.0005)


async def execute(eng, candidate):
    buy, sell, plan = candidate
    await eng._vlock(buy.key).acquire()
    await eng._vlock(sell.key).acquire()
    with patch.object(eng, '_log_csv'):
        await eng._execute_locked(buy, sell, plan)


@pytest.mark.parametrize('long', [True, False])
def test_open_frozen_target_and_reduce_only_close(tmp_path, long):
    async def scenario():
        eng = engine_at(tmp_path, long=long)
        eng.ensure_async_state()
        opening = eng._scan(time.time())
        assert opening and opening[2].action == 'bps_open'
        await execute(eng, opening)
        strategy = eng.bps_strategy
        assert strategy.entries == 1
        sign = 1 if long else -1
        assert eng.entropy.position * sign > 0
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        assert not eng._scan(time.time())  # stale pre-trade book, no duplicate add
        strategy.entry, strategy.exit = (-20, -1) if long else (20, 1)
        assert strategy.locked_exit == (-6 if long else 6)
        books(eng, -5.5 if long else 5.5)
        closing = eng._scan(time.time())
        assert closing and closing[2].action == 'bps_close'
        await execute(eng, closing)
        # Price movement can make the full position exceed the per-order USD
        # cap. The first slice must leave a tradable tail, then finish it.
        for _ in range(3):
            if strategy.flat():
                break
            books(eng, -5.5 if long else 5.5)
            await execute(eng, eng._scan(time.time()))
        assert strategy.entries == 0 and strategy.flat()
        for venue in eng.venues.values():
            calls = venue.send_taker.call_args_list
            assert calls[0].kwargs.get('reduce_only', False) is False
            assert calls[-1].kwargs['reduce_only'] is True
        assert json.loads(strategy.path.read_text())['pending'] is False
    asyncio.run(scenario())


def test_bps_opens_by_levels_without_estimated_return_guard(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        assert eng._scan(time.time())
        eng.hedge.fee_bps = 2.25
        assert eng._scan(time.time())  # fees do not alter the level trigger
        eng.hedge.fee_bps = 0
        eng.cfg.leg_slippage_bps = 1
        assert eng._scan(time.time())  # slippage remains execution protection
        assert 'projected_net_bps' not in eng.bps_strategy.snapshot()
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(scenario())


@pytest.mark.parametrize('long', [True, False])
@pytest.mark.parametrize('profit', [True, False])
def test_repeated_entries_are_limited_by_position_not_batches(tmp_path, long, profit):
    async def scenario():
        eng = engine_at(tmp_path, long=long)
        eng.ensure_async_state()
        eng.cfg.max_order_notional = 100
        eng.cfg.bps_max_position_usd = 220
        if profit:
            eng.cfg.bps_exit_mode = 'profit'
            eng.cfg.bps_entry_confirm_sec = eng.cfg.bps_cycle_cooldown_sec = 0
            eng.cfg.bps_min_gap_bps = eng.cfg.bps_add_step_bps = 0
            eng.cfg.bps_execution_slippage_bps = 0
        for _ in range(3):
            books(eng, -9.5 if long else 9.5)
            candidate = eng._scan(time.time())
            assert candidate
            await execute(eng, candidate)
        assert eng.bps_strategy.entries == 3
        assert abs(eng.entropy.position) * eng.entropy.book.mid() <= 220.001
        books(eng, -9.5 if long else 9.5)
        assert eng._scan(time.time()) is None
    asyncio.run(scenario())


@pytest.mark.parametrize('legacy_limit', [0, 1])
def test_old_yaml_batch_limit_does_not_block_repeated_entries(tmp_path, legacy_limit):
    async def scenario():
        path = tmp_path / 'legacy.yaml'
        path.write_text('strategy:\n  mode: bps\nbps:\n  levels: manual\n'
                        f'  max_entries: {legacy_limit}\n  add_interval_sec: 0\n'
                        'execution:\n  premium_persist_sec: 0\n  leg_slippage_bps: 0\n')
        eng = engine_at(tmp_path)
        eng.cfg = load_config(str(path), symbol='SNDK', hedge_venue='lighter', credential_env={})
        eng.cfg.log_file = str(tmp_path / 'engine.log')
        eng.cfg.max_order_notional = 100
        eng.bps_strategy = BpsStrategy(eng)
        eng.ensure_async_state()
        for _ in range(3):
            books(eng, -9.5)
            candidate = eng._scan(time.time())
            assert candidate
            await execute(eng, candidate)
        assert eng.bps_strategy.entries == 3
    asyncio.run(scenario())


def test_partial_close_latches_and_maker_open_is_hedged_only_after_fill(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path, maker=True)
        eng.ensure_async_state()
        eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('test', 99.9, time.time()))
        eng.entropy.cancel_maker = AsyncMock()
        eng.entropy.maker_state = AsyncMock(return_value=dict(filled_base=.5, avg_px=99.9045,
            terminal=True, status='filled', err=None))
        await execute(eng, eng._scan(time.time()))
        eng.entropy.send_taker.assert_not_awaited()
        assert eng.hedge.send_taker.call_args.kwargs['qty'] == .5
        assert eng.bps_strategy.entries == 1
        books(eng, -5.5)
        for venue in eng.venues.values():
            async def partial(**args):
                return dict(status='partial', filled_base=args['qty'] / 2, avg_px=args['limit_px'], err=None)
            venue.send_taker = AsyncMock(side_effect=partial)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.closing
        books(eng, -10)  # still close the residual, do not reopen on price retreat
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].action == 'bps_close'
        assert eng.entropy.send_taker.call_args.kwargs['reduce_only'] is True
    asyncio.run(scenario())


def test_zero_maker_fill_does_not_send_hedge_or_consume_entry(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path, maker=True)
        eng.ensure_async_state()
        eng.entropy.place_maker = AsyncMock(return_value=MakerOrder('test', 99.9, time.time()))
        eng.entropy.maker_state = AsyncMock(return_value=dict(filled_base=0, avg_px=None,
            terminal=True, status='canceled', err=None))
        await execute(eng, eng._scan(time.time()))
        eng.hedge.send_taker.assert_not_awaited()
        assert eng.bps_strategy.entries == 0
    asyncio.run(scenario())


def test_restart_restores_frozen_levels_but_rejects_unconfirmed_or_foreign_positions(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.entries == 1 and restored.locked_exit == -6
        restored.pending = True
        restored.save()
        eng.entropy.position = eng.hedge.position = 0
        with pytest.raises(RuntimeError, match='未完成确认'):
            BpsStrategy(eng).restore()  # flat may still have a resting unknown order
        restored.path.unlink()
        eng.entropy.position, eng.hedge.position = 1, -1
        adopted = BpsStrategy(eng)
        adopted.restore()
        assert adopted.entries == 1 and adopted.adopted_position
        assert adopted.locked_exit == -6
        eng.entropy.position, eng.hedge.position = 2, -2
        with pytest.raises(RuntimeError, match='旧仓位'):
            BpsStrategy(eng).restore()
    asyncio.run(scenario())


def test_rolling_reads_yaml_csv_only_completed_minutes_and_checks_coverage(tmp_path):
    eng = engine_at(tmp_path)
    cfg = eng.cfg
    cfg.bps_window_minutes = 4
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 0, 100
    now = 6000
    from pathlib import Path
    Path(cfg.recorder_csv + '.source.json').write_text(json.dumps(dict(source=recorder_source(cfg), since=0)))
    rows = [(now - 240, -10), (now - 180, -9), (now - 120, -7), (now - 60, -6),
            (now, 999), (now + 60, 999), (now - 300, -999)]
    def write(values):
        with open(cfg.recorder_csv, 'w') as handle:
            writer = csv.writer(handle)
            writer.writerow(['minute_ts', 'premium_close_bps', 'samples'])
            writer.writerows((ts, premium, 60) for ts, premium in values)
    write(rows)
    assert rolling_levels(cfg, now + 20) == (-10, -6, now)
    write(rows[1:])
    with pytest.raises(ValueError, match='3/4'):
        rolling_levels(cfg, now + 20)


def test_recorder_marks_source_change_without_relabelling_old_bars(tmp_path):
    from entropy_arb.recorder import MinuteRecorder
    eng = engine_at(tmp_path)
    cfg = eng.cfg
    recorder = MinuteRecorder(cfg.recorder_csv, eng.entropy.book, eng.hedge.book, 60,
                              source=recorder_source(cfg))
    recorder._open()
    recorder._fh.close()
    from pathlib import Path
    meta = Path(cfg.recorder_csv + '.source.json')
    original = json.loads(meta.read_text())
    next_recorder = MinuteRecorder(cfg.recorder_csv, eng.entropy.book, eng.hedge.book, 60,
                                   source=recorder_source(cfg))
    next_recorder._open()
    next_recorder._fh.close()
    assert json.loads(meta.read_text())['since'] == original['since']
    cfg.primary_venue = 'arcus'
    with pytest.raises(ValueError, match='来源'):
        rolling_levels(cfg, time.time())


@pytest.mark.parametrize('legacy_marker', [False, True])
def test_rolling_reuses_recent_legacy_history_without_recorder_warmup(tmp_path, legacy_marker):
    from pathlib import Path
    cfg = engine_at(tmp_path).cfg
    cfg.bps_window_minutes = 4
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 0, 100
    now = 6000
    csv_path = Path(cfg.recorder_csv)
    csv_path.write_text('minute_ts,premium_close_bps,samples\n'
                        '5760,-10,60\n5820,-9,60\n5880,-7,60\n5940,-6,60\n')
    original_csv = csv_path.read_bytes()
    metadata_path = Path(cfg.recorder_csv + '.source.json')
    if legacy_marker:
        # The previous release discarded all pre-upgrade history at startup.
        metadata_path.write_text(json.dumps(dict(source=recorder_source(cfg), since=now)))
    assert rolling_levels(cfg, now + 20) == (-10, -6, now)
    assert json.loads(metadata_path.read_text()) == dict(version=2, source=recorder_source(cfg), since=0)
    assert csv_path.read_bytes() == original_csv
    # Historical rows outside the configured wall-clock window are not backfilled.
    with pytest.raises(ValueError, match='0/4'):
        rolling_levels(cfg, now + 300)


def test_rolling_preserves_known_source_boundary_across_restart(tmp_path):
    from pathlib import Path
    from entropy_arb.recorder import MinuteRecorder
    eng = engine_at(tmp_path)
    cfg = eng.cfg
    cfg.bps_window_minutes = 4
    csv_path = Path(cfg.recorder_csv)
    from entropy_arb.recorder import HEADER
    csv_path.write_text(','.join(HEADER) + '\n')
    source = recorder_source(cfg)
    previous_source = dict(source, primary='other_venue')
    metadata_path = Path(cfg.recorder_csv + '.source.json')
    metadata_path.write_text(json.dumps(dict(version=2, source=previous_source, since=0)))
    with patch('entropy_arb.recorder.time.time', return_value=6000):
        recorder = MinuteRecorder(cfg.recorder_csv, eng.entropy.book, eng.hedge.book, 60, source=source)
        recorder._open()
        recorder.close()
    expected = dict(version=2, source=source, since=6000)
    assert json.loads(metadata_path.read_text()) == expected
    csv_path.write_text('minute_ts,premium_close_bps,samples\n'
                        '5760,-10,60\n5820,-9,60\n5880,-7,60\n5940,-6,60\n')
    for now in (6020, 6080):
        with pytest.raises(ValueError, match='0/4'):
            rolling_levels(cfg, now)
        assert json.loads(metadata_path.read_text()) == expected


@pytest.mark.parametrize('marker', ['{invalid json', 'null', '[]', '{}', '{"source": {}, "since": "nan"}'])
def test_rolling_rejects_corrupt_source_marker_instead_of_adopting_history(tmp_path, marker):
    from pathlib import Path
    cfg = engine_at(tmp_path).cfg
    Path(cfg.recorder_csv).write_text('minute_ts,premium_close_bps,samples\n')
    metadata_path = Path(cfg.recorder_csv + '.source.json')
    metadata_path.write_text(marker)
    with pytest.raises(ValueError, match='来源记录'):
        rolling_levels(cfg, 6000)
    assert metadata_path.read_text() == marker


def test_web_bps_skips_daily_threshold_without_restart_or_prune(tmp_path):
    from test_web import manager_at, payload
    from entropy_arb.thresholds import daily_window
    from entropy_arb.web import ConsoleError
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            data = payload()
            data['mode'] = 'live'
            data['config'] += 'strategy:\n  mode: bps\n'
            task = await manager.put(data)
            assert task['strategy_mode'] == 'bps'
            previous = manager.get(task['id'])['config']
            await manager.auto_update_task(task['id'], daily_window())
            assert manager.get(task['id'])['config'] == previous
            assert manager._read_auto_status(task['id'])['status'] == 'skipped'
            with pytest.raises(ConsoleError, match='独立开平仓'):
                manager._threshold_suggestion(task['id'])
        finally:
            await manager.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('extra', ['  direction: long_primary\n  entry_bps: -6\n  exit_bps: -9',
                                   '  window_minutes: 1',
                                   '  cost_buffer_bps: -1', '  exit_percentile: .nan',
                                   '  min_coverage_pct: 0', '  min_coverage_pct: -1',
                                   '  min_coverage_pct: 101', '  min_coverage_pct: .nan',
                                   '  min_valid_minutes: 1', '  min_valid_minutes: 10081',
                                   '  min_valid_minutes: .nan', '  min_valid_minutes: 2.5'])
def test_config_rejects_invalid_bps(tmp_path, extra):
    path = tmp_path / 'strategy.yaml'
    path.write_text('strategy:\n  mode: bps\nbps:\n  levels: manual\n' + extra + '\n')
    with pytest.raises(ConfigError):
        load_config(str(path), symbol='ETH', hedge_venue='arcus', credential_env={})


def test_levels_are_frozen_from_the_validated_plan_and_close_survives_restart(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        candidate = eng._scan(time.time())
        eng.bps_strategy.entry, eng.bps_strategy.exit = -20, -1  # refresh before task dispatch
        await execute(eng, candidate)
        assert eng.bps_strategy.locked_entry == -9
        assert eng.bps_strategy.locked_exit == -6
        books(eng, -5.5)
        assert eng._scan(time.time())[2].action == 'bps_close'
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.closing
        eng.bps_strategy = restored
        books(eng, -10)
        assert eng._scan(time.time())[2].action == 'bps_close'
    asyncio.run(scenario())


def test_unknown_fill_pauses_and_keeps_pending_checkpoint(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        async def uncertain(**args):
            return dict(status='unknown', filled_base=0, avg_px=None, unresolved=True, err='timeout')
        eng.entropy.send_taker = AsyncMock(side_effect=uncertain)
        eng.hedge.send_taker = AsyncMock(side_effect=uncertain)
        await execute(eng, eng._scan(time.time()))
        assert eng.halted
        assert json.loads(eng.bps_strategy.path.read_text())['pending'] is True
        with pytest.raises(RuntimeError, match='未完成确认'):
            BpsStrategy(eng).restore()
    asyncio.run(scenario())


def test_net_tolerance_does_not_reclassify_small_paired_positions_as_flat(tmp_path):
    eng = engine_at(tmp_path)
    eng.cfg.net_tolerance_base = .001
    eng.entropy.position, eng.hedge.position = .0009, -.0009
    assert not eng.bps_strategy.flat()
    eng.bps_strategy.restore()
    assert eng.bps_strategy.entries == 1
    assert eng.bps_strategy.adopted_position


def test_unmatched_fill_below_legacy_net_tolerance_is_not_a_paired_cycle(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.entropy.send_taker = AsyncMock(return_value=dict(status='partial', filled_base=.0005,
                                                           avg_px=99.9, err=None, unresolved=False))
        eng.hedge.send_taker = AsyncMock(return_value=dict(status='canceled', filled_base=0,
                                                         avg_px=None, err=None, unresolved=False))
        await execute(eng, eng._scan(time.time()))
        assert eng.halted
        assert eng.bps_strategy.pending
        assert eng.bps_strategy.entries == 0
    asyncio.run(scenario())


def test_close_depth_repricing_leaves_a_tradable_tail(tmp_path):
    eng = engine_at(tmp_path)
    eng.cfg.max_order_notional = 100
    eng.cfg.leg_slippage_bps = 20
    eng.entropy.position, eng.hedge.position = 1, -1
    eng.entropy.set_book(99.98, 99.981)
    eng.hedge.set_book(99.97, 99.99)
    eng.hedge.book.asks = {99.99: .1, 100.1: 50}
    plan = eng.bps_strategy.plan(eng.hedge, eng.entropy, closing=True)
    assert plan
    assert max(plan.buy_notional, plan.sell_notional) <= 100
    assert plan.qty < 1
    residual = 1 - plan.qty
    assert residual * eng.entropy.book.best_bid() * (1 - 20 / 1e4) >= eng.entropy.min_quote


@pytest.mark.parametrize('long', [True, False])
def test_missing_checkpoint_adopts_paired_old_positions_and_closes_reduce_only(tmp_path, long):
    async def scenario():
        eng = engine_at(tmp_path, long=long)
        eng.ensure_async_state()
        sign = 1 if long else -1
        eng.entropy.position, eng.hedge.position = sign, -sign
        eng.bps_strategy.restore()
        strategy = eng.bps_strategy
        assert strategy.adopted_position and strategy.entries == 1
        assert strategy.locked_exit == (-6 if long else 6)
        # Above the opening point but before the exit point: existing pair is held.
        books(eng, -7 if long else 7)
        assert eng._scan(time.time()) is None
        books(eng, -5 if long else 5)
        candidate = eng._scan(time.time())
        assert candidate[2].action == 'bps_close'
        await execute(eng, candidate)
        assert strategy.flat() and not strategy.adopted_position
        for venue in eng.venues.values():
            assert venue.send_taker.call_count == 1
            assert venue.send_taker.call_args.kwargs['reduce_only'] is True
    asyncio.run(scenario())


@pytest.mark.parametrize('positions', [(1, 0), (1, -.5), (-1, 1), (1, 1)])
def test_unpaired_or_opposite_old_positions_are_not_silently_adopted(tmp_path, positions):
    eng = engine_at(tmp_path)
    eng.entropy.position, eng.hedge.position = positions
    with pytest.raises(RuntimeError, match='未配对或方向'):
        eng.bps_strategy.restore()
    assert not eng.bps_strategy.path.exists()


def test_adopted_rolling_positions_wait_for_data_then_freeze_exit_across_restart(tmp_path):
    async def scenario():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.cfg.bps_levels = 'rolling'
        strategy = eng.bps_strategy = BpsStrategy(eng)
        eng.entropy.position, eng.hedge.position = 1, -1
        strategy.restore()
        assert strategy.awaiting_levels and strategy.locked_exit is None
        books(eng, 1)  # Must not use the arbitrary configured manual exit while waiting.
        assert eng._scan(time.time()) is None
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.awaiting_levels
        eng.bps_strategy = restored
        now = int(time.time() // 60) * 60
        with open(eng.cfg.recorder_csv, 'w') as handle:
            writer = csv.writer(handle)
            writer.writerow(['minute_ts', 'premium_close_bps', 'samples'])
            for index in range(30):
                writer.writerow([now - (30 - index) * 60, -10 if index < 15 else -4, 60])
        async def stop_after_refresh(awaitable, timeout):
            awaitable.close()
            eng.stop.set()
        with patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=stop_after_refresh):
            await restored.refresh_loop()
        assert not restored.awaiting_levels and restored.locked_exit == -4
        second_restart = BpsStrategy(eng)
        second_restart.restore()
        assert second_restart.locked_exit == -4 and not second_restart.awaiting_levels
        eng.bps_strategy = second_restart
        books(eng, -3)
        assert eng._scan(time.time())[2].action == 'bps_close'
    asyncio.run(scenario())
