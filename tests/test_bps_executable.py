"""Directional executable BPS, independent of exchange names and midpoints."""
import asyncio
import csv
import json
import math
import time

import pytest

from entropy_arb.bps_strategy import BpsStrategy, rolling_levels
from entropy_arb.config import load_config
from entropy_arb.signals import execution_signals
from test_bps_strategy import books, engine_at, execute


def executable_engine(tmp_path, long=True):
    eng = engine_at(tmp_path, long=long)
    eng.cfg.bps_price_basis = 'executable'
    eng.cfg.bps_entry, eng.cfg.bps_exit = 5, 4
    eng.cfg.take_fraction = 1
    eng.cfg.max_order_notional = 100
    eng.bps_strategy = BpsStrategy(eng)
    eng.markets_ready = True
    return eng


def quotes(eng, *, opening, long=True):
    cheap = opening == long
    eng.entropy.set_book(99.90 if cheap else 100.10, 99.91 if cheap else 100.11)
    eng.hedge.set_book(100, 100.01)


@pytest.mark.parametrize('names', [('RH', 'LIGHTER'), ('ARCUS', 'RH'), ('Entropy', 'ARCUS')])
@pytest.mark.parametrize('long', [True, False])
def test_both_directions_use_sell_bid_over_buy_ask_for_any_pair(tmp_path, names, long):
    async def run():
        eng = executable_engine(tmp_path, long)
        eng.ensure_async_state()
        eng.entropy.name, eng.hedge.name = names
        quotes(eng, opening=True, long=long)
        signals = execution_signals(eng)['rows']
        assert all(row['comparison'] == '>=' and row['order_type'] == 'taker' for row in signals)
        assert signals[0]['trigger_premium_bps'] == signals[0]['executable_premium_bps']
        assert signals[0]['trigger_basis'] == 'executable_premium_bps'
        opening = eng._scan(time.time())
        assert opening and opening[2].action == 'bps_open'
        assert opening[2].marginal_premium_bps >= 5
        await execute(eng, opening)
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        quotes(eng, opening=False, long=long)
        closing = eng._scan(time.time())
        assert closing and closing[2].action == 'bps_close'
        assert closing[2].marginal_premium_bps >= 4
        await execute(eng, closing)
        if not eng.bps_strategy.flat():
            quotes(eng, opening=False, long=long)
            await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat()
    asyncio.run(run())


def test_wide_spread_is_not_hidden_by_a_zero_midpoint_premium(tmp_path):
    async def run():
        eng = executable_engine(tmp_path)
        eng.ensure_async_state()
        eng.entropy.set_book(100, 100.1)
        eng.hedge.set_book(100, 100.1)
        assert eng.premium_bps() == pytest.approx(0)
        assert eng._scan(time.time()) is None
        assert eng.bps_strategy.signal() < -9
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.bps_strategy.entries = 1
        eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = 5, 0
        eng.bps_strategy.locked_price_basis = 'executable'
        eng.bps_strategy.closing = True
        assert eng._scan(time.time()) is None
        assert not eng.bps_strategy.closing
    asyncio.run(run())


def test_only_qualified_depth_is_used_and_limits_preserve_bps_threshold(tmp_path):
    async def run():
        eng = executable_engine(tmp_path)
        eng.ensure_async_state()
        eng.cfg.leg_slippage_bps = 50
        quotes(eng, opening=True)
        eng.entropy.book.asks = {99.91: .4, 100.5: 100}
        candidate = eng._scan(time.time())
        plan = candidate[2]
        assert plan.qty == .4
        assert plan.slippage_bps < 50
        await execute(eng, candidate)
        buy = eng.entropy.send_taker.call_args.kwargs
        sell = eng.hedge.send_taker.call_args.kwargs
        assert (sell['limit_px'] / buy['limit_px'] - 1) * 1e4 >= 5 - 1e-8
        assert buy['qty'] == sell['qty'] == .4
    asyncio.run(run())


@pytest.mark.parametrize('long', [True, False])
def test_price_tick_rounding_preserves_joint_threshold_and_fees_are_reported_separately(tmp_path, long):
    async def run():
        eng = executable_engine(tmp_path, long)
        eng.ensure_async_state()
        eng.cfg.leg_slippage_bps = 50
        eng.entropy.fee_bps = 100  # Fees must not become a new signal gate.
        eng.entropy.px_round = lambda px, round_up: (math.ceil(px * 100 - 1e-9) if round_up else math.floor(px * 100 + 1e-9)) / 100
        quotes(eng, opening=True, long=long)
        candidate = eng._scan(time.time())
        assert candidate
        await execute(eng, candidate)
        buy, sell, _ = candidate
        bp = buy.send_taker.call_args.kwargs['limit_px']
        sp = sell.send_taker.call_args.kwargs['limit_px']
        assert (sp / bp - 1) * 1e4 >= 5 - 1e-8
        assert eng.total_fees_usd > 0
    asyncio.run(run())


def test_history_rebuilds_two_independent_directional_series_from_old_quotes(tmp_path):
    eng = executable_engine(tmp_path)
    cfg = eng.cfg
    cfg.bps_window_minutes = cfg.bps_min_valid_minutes = 2
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 100, 100
    with open(cfg.recorder_csv, 'w') as handle:
        writer = csv.writer(handle)
        writer.writerow(['minute_ts', 'entropy_bid', 'entropy_ask', 'hedge_bid', 'hedge_ask', 'premium_close_bps', 'samples'])
        writer.writerows([(600, 99.9, 99.91, 100, 100.01, 9999, 60),
                          (720, 100.1, 100.11, 100, 100.01, 9999, 60)])
    original = open(cfg.recorder_csv, 'rb').read()
    entry, exit, _ = rolling_levels(cfg, 900)
    assert entry == pytest.approx((100 / 99.91 - 1) * 1e4)
    assert exit == pytest.approx((100.1 / 100.01 - 1) * 1e4)
    cfg.bps_direction = 'short_primary'
    assert rolling_levels(cfg, 900)[:2] == pytest.approx((exit, entry))
    assert open(cfg.recorder_csv, 'rb').read() == original


def test_missing_quote_history_never_falls_back_to_midpoint_thresholds(tmp_path):
    cfg = executable_engine(tmp_path).cfg
    cfg.bps_min_valid_minutes = 2
    with open(cfg.recorder_csv, 'w') as handle:
        handle.write('minute_ts,premium_close_bps,samples\n600,-10,60\n720,-6,60\n')
    with pytest.raises(ValueError, match='等待有效分钟数据'):
        rolling_levels(cfg, 900)


@pytest.mark.parametrize('long,entry,exit,expected', [(True, 10, 75, (90, 75)), (False, 80, 20, (80, 80))])
def test_legacy_rolling_config_migrates_historical_tails_and_preserves_maker(tmp_path, long, entry, exit, expected):
    path = tmp_path / 'config.yaml'
    path.write_text(f'strategy:\n  mode: bps\nbps:\n  levels: rolling\n  direction: {"long_primary" if long else "short_primary"}\n'
                    f'  entry_percentile: {entry}\n  exit_percentile: {exit}\nexecution:\n  primary_order_type: maker\n')
    cfg = load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={})
    assert cfg.bps_price_basis == 'executable'
    assert cfg.primary_order_type == 'maker'
    assert (cfg.bps_entry_percentile, cfg.bps_exit_percentile) == expected


def test_explicit_executable_manual_targets_are_independent_greater_equal_levels(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('strategy:\n  mode: bps\nbps:\n  price_basis: executable\n  levels: manual\n'
                    '  entry_bps: 1\n  exit_bps: 1\n')
    cfg = load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={})
    assert cfg.bps_entry == cfg.bps_exit == 1
    assert cfg.bps_entry_percentile == 90


def test_existing_paired_cycle_adopts_current_basis_without_flattening(tmp_path):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(time.time()))
        old = json.loads(eng.bps_strategy.path.read_text())
        old.pop('locked_price_basis')  # Real pre-upgrade checkpoint.
        eng.bps_strategy.path.write_text(json.dumps(old))
        eng.cfg.bps_price_basis = 'executable'
        eng.cfg.bps_entry, eng.cfg.bps_exit = 5, 4
        positions = [v.position for v in eng.venues.values()]
        eng.bps_strategy = BpsStrategy(eng)
        eng.bps_strategy.restore()
        assert eng.bps_strategy.price_basis == 'executable'
        assert [v.position for v in eng.venues.values()] == positions
        assert eng.bps_strategy.locked_entry == 5 and eng.bps_strategy.locked_exit == 4
        quotes(eng, opening=True)
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].action == 'bps_open'
        await execute(eng, candidate)
        quotes(eng, opening=False)
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].action == 'bps_close'
        assert candidate[2].cycle_exit_bps == 4
        await execute(eng, candidate)
        for _ in range(3):
            if eng.bps_strategy.flat():
                break
            quotes(eng, opening=False)
            await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat() and eng.bps_strategy.price_basis == 'executable'
        quotes(eng, opening=True)
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].cycle_entry_bps == 5
        assert candidate[2].marginal_premium_bps >= 5
    asyncio.run(run())


def test_new_basis_partial_close_can_wait_and_add_again_within_caps(tmp_path):
    async def run():
        eng = executable_engine(tmp_path)
        eng.ensure_async_state()
        for _ in range(2):
            quotes(eng, opening=True)
            await execute(eng, eng._scan(time.time()))
        quotes(eng, opening=False)
        await execute(eng, eng._scan(time.time()))
        assert not eng.bps_strategy.flat()
        eng.entropy.set_book(100, 100.01)
        eng.hedge.set_book(100, 100.01)
        assert eng._scan(time.time()) is None
        assert not eng.bps_strategy.closing
        quotes(eng, opening=True)
        candidate = eng._scan(time.time())
        assert candidate and candidate[2].action == 'bps_open'
        await execute(eng, candidate)
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
        assert abs(eng.entropy.position) * eng.entropy.book.mid() <= eng.cfg.bps_max_position_usd
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.price_basis == 'executable'
    asyncio.run(run())
