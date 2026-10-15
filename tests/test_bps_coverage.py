"""Explicit time-window mode allows gaps without backfilling older bars."""
import asyncio
import csv
import logging
from unittest.mock import patch

import pytest

from entropy_arb.bps_strategy import rolling_levels
from entropy_arb.config import load_config
from test_bps_strategy import engine_at as make_engine


def time_window_engine(tmp_path):
    engine = make_engine(tmp_path)
    engine.cfg.bps_history_mode = "time"
    return engine


def write_bars(cfg, rows):
    with open(cfg.recorder_csv, 'w') as handle:
        writer = csv.writer(handle)
        writer.writerow(['minute_ts', 'premium_close_bps', 'samples'])
        writer.writerows(rows)


def bars(now, count):
    # Deliberate gaps within the 120-minute time window.
    return [(now - (120 - index) * 60, -10 if index < count // 2 else -6, 60)
            for index in range(count)]


def test_119_of_120_bars_work_immediately_with_existing_yaml_default(tmp_path):
    cfg = time_window_engine(tmp_path).cfg
    now = 60000
    write_bars(cfg, bars(now, 119))
    assert rolling_levels(cfg, now + 10) == (-10, -6, now)
    coverage = rolling_levels(cfg, now + 10, include_coverage=True)[3]
    assert coverage['valid_minutes'] == 119
    assert coverage['required_minutes'] == 30
    assert coverage['missing_minutes'] == 1
    # Restarting/re-reading the same history does not reset the coverage.
    assert rolling_levels(cfg, now + 40) == (-10, -6, now)


def test_accepts_30_and_rejects_29_without_old_or_future_backfill(tmp_path):
    cfg = time_window_engine(tmp_path).cfg
    now = 60000
    valid = bars(now, 30)
    extras = [(now - 121 * 60, -999, 60), (now, 999, 60), (now + 60, 999, 60),
              (now - 60, -999, 9), (now - 119 * 60 + 1, -999, 60),
              (now - 120 * 60, 'nan', 60)]
    write_bars(cfg, valid + extras)
    assert rolling_levels(cfg, now + 20) == (-10, -6, now)
    # Many duplicate rows must not turn 29 valid minutes into a full window.
    write_bars(cfg, valid[1:] + [valid[1]] * 100 + extras)
    with pytest.raises(ValueError, match=r'29/120.*至少 30'):
        rolling_levels(cfg, now + 20)


def test_minimum_minutes_is_configurable_and_short_window_requires_all_bars(tmp_path):
    cfg = time_window_engine(tmp_path).cfg
    now = 60000
    cfg.bps_min_valid_minutes = 120
    write_bars(cfg, bars(now, 119))
    with pytest.raises(ValueError, match=r'119/120.*至少 120'):
        rolling_levels(cfg, now + 20)
    cfg.bps_window_minutes = 2
    write_bars(cfg, [(now - 60, -10, 60)])
    with pytest.raises(ValueError, match=r'1/2.*至少 2'):
        rolling_levels(cfg, now + 20)


def test_yaml_loads_count_default_and_override(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('strategy:\n  mode: bps\n')
    assert load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={}).bps_min_valid_minutes == 30
    path.write_text('strategy:\n  mode: bps\nbps:\n  min_valid_minutes: 45\n')
    assert load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={}).bps_min_valid_minutes == 45


def test_refresh_deduplicates_waiting_logs_even_when_scan_changes_reason(tmp_path, caplog):
    async def scenario():
        engine = time_window_engine(tmp_path)
        engine.ensure_async_state()
        engine.cfg.bps_levels = 'rolling'
        engine.cfg.bps_min_valid_minutes = 120
        now = 60000
        write_bars(engine.cfg, bars(now, 119))
        attempts = 0

        async def next_attempt(awaitable, timeout):
            nonlocal attempts
            awaitable.close()
            attempts += 1
            engine.bps_strategy.reason = '等待可交易的双腿行情'
            if attempts == 3:
                engine.stop.set()
            raise asyncio.TimeoutError

        with patch('entropy_arb.bps_strategy.time.time', return_value=now + 20), \
                patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=next_attempt):
            await engine.bps_strategy.refresh_loop()
        assert attempts == 3
        assert engine.bps_strategy.entry is None

    caplog.set_level(logging.INFO, logger='bps')
    asyncio.run(scenario())
    waits = [record.message for record in caplog.records if '等待有效分钟数据' in record.message]
    assert len(waits) == 1


def test_refresh_reports_coverage_in_log_and_snapshot(tmp_path, caplog):
    async def scenario():
        engine = time_window_engine(tmp_path)
        engine.ensure_async_state()
        engine.cfg.bps_levels = 'rolling'
        now = 60000
        write_bars(engine.cfg, bars(now, 119))

        async def stop_after_refresh(awaitable, timeout):
            awaitable.close()
            engine.stop.set()

        with patch('entropy_arb.bps_strategy.time.time', return_value=now + 20), \
                patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=stop_after_refresh):
            await engine.bps_strategy.refresh_loop()
        state = engine.bps_strategy.snapshot()
        assert state['entry_bps'] == -10 and state['exit_bps'] == -6
        assert state['coverage']['valid_minutes'] == 119
        assert state['min_valid_minutes'] == 30

    caplog.set_level(logging.INFO, logger='bps')
    asyncio.run(scenario())
    assert 'valid=119/120 coverage=99.2%' in caplog.text


def test_82_bars_are_ready_even_with_legacy_80_percent_setting(tmp_path):
    cfg = time_window_engine(tmp_path).cfg
    cfg.bps_min_coverage_pct = 80
    now = 60000
    write_bars(cfg, bars(now, 82))
    entry, exit, _, coverage = rolling_levels(cfg, now + 20, include_coverage=True)
    assert (entry, exit) == (-10, -6)
    assert coverage['valid_minutes'] == 82
    assert coverage['required_minutes'] == 30
    # A new bar replaces an expired bar: cumulative writes increase, count stays 82.
    write_bars(cfg, bars(now, 82) + [(now, -6, 60)])
    assert rolling_levels(cfg, now + 60, include_coverage=True)[3]['valid_minutes'] == 82


def test_waiting_message_explains_missing_and_low_sample_minutes(tmp_path):
    cfg = time_window_engine(tmp_path).cfg
    now = 60000
    write_bars(cfg, bars(now, 25) + [(now - 60, -6, 9), (now - 120, 'nan', 60)])
    with pytest.raises(ValueError, match=r'25/120.*至少 30.*缺行 93.*采样不足 1.*无效溢价 1'):
        rolling_levels(cfg, now + 20)
