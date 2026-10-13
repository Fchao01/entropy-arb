"""Missing minutes are allowed inside the rolling window, never backfilled."""
import asyncio
import csv
import logging
from unittest.mock import patch

import pytest

from entropy_arb.bps_strategy import rolling_levels
from entropy_arb.config import load_config
from test_bps_strategy import engine_at


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
    cfg = engine_at(tmp_path).cfg
    now = 60000
    write_bars(cfg, bars(now, 119))
    assert rolling_levels(cfg, now + 10) == (-10, -6, now)
    assert rolling_levels(cfg, now + 10, include_coverage=True)[3] == dict(
        valid_minutes=119, required_minutes=96, coverage_pct=119 / 120 * 100)
    # Restarting/re-reading the same history does not reset the coverage.
    assert rolling_levels(cfg, now + 40) == (-10, -6, now)


def test_coverage_accepts_96_and_rejects_95_without_old_or_future_backfill(tmp_path):
    cfg = engine_at(tmp_path).cfg
    now = 60000
    valid = bars(now, 96)
    extras = [(now - 121 * 60, -999, 60), (now, 999, 60), (now + 60, 999, 60),
              (now - 60, -999, 9), (now - 119 * 60 + 1, -999, 60),
              (now - 120 * 60, 'nan', 60)]
    write_bars(cfg, valid + extras)
    assert rolling_levels(cfg, now + 20) == (-10, -6, now)
    # Many duplicate rows must not turn 95 valid minutes into a full window.
    write_bars(cfg, valid[1:] + [valid[1]] * 100 + extras)
    with pytest.raises(ValueError, match=r'95/120.*至少 96'):
        rolling_levels(cfg, now + 20)


def test_100_percent_restores_full_window_and_sparse_mode_still_needs_two_bars(tmp_path):
    cfg = engine_at(tmp_path).cfg
    now = 60000
    cfg.bps_min_coverage_pct = 100
    write_bars(cfg, bars(now, 119))
    with pytest.raises(ValueError, match=r'119/120.*至少 120'):
        rolling_levels(cfg, now + 20)
    cfg.bps_min_coverage_pct = .01
    write_bars(cfg, bars(now, 1))
    with pytest.raises(ValueError, match=r'1/120.*至少 2'):
        rolling_levels(cfg, now + 20)


def test_yaml_loads_coverage_default_and_override(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('strategy:\n  mode: bps\n')
    assert load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={}).bps_min_coverage_pct == 80
    path.write_text('strategy:\n  mode: bps\nbps:\n  min_coverage_pct: 95\n')
    assert load_config(str(path), symbol='ETH', hedge_venue='lighter', credential_env={}).bps_min_coverage_pct == 95


def test_refresh_deduplicates_waiting_logs_even_when_scan_changes_reason(tmp_path, caplog):
    async def scenario():
        engine = engine_at(tmp_path)
        engine.ensure_async_state()
        engine.cfg.bps_levels = 'rolling'
        engine.cfg.bps_min_coverage_pct = 100
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
        engine = engine_at(tmp_path)
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
        assert state['min_coverage_pct'] == 80

    caplog.set_level(logging.INFO, logger='bps')
    asyncio.run(scenario())
    assert 'valid=119/120 coverage=99.2%' in caplog.text
