"""Existing valid minute bars can span gaps and predate the current session."""
import asyncio
import csv
import json
from unittest.mock import patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy, recorder_source, rolling_levels
from entropy_arb.config import ConfigError, load_config
from test_bps_strategy import engine_at


def write_history(cfg, rows):
    with open(cfg.recorder_csv, 'w') as handle:
        writer = csv.writer(handle)
        writer.writerow(['minute_ts', 'premium_close_bps', 'samples'])
        writer.writerows(rows)


def test_default_uses_latest_120_valid_bars_across_gaps_and_downtime(tmp_path):
    cfg = engine_at(tmp_path).cfg
    cfg.bps_min_valid_minutes = 120
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 0, 100
    now = 6_000_000
    start = now - 86400 - 300 * 120
    rows = [(start + i * 120, i, 60) for i in range(300)]
    write_history(cfg, rows)
    original = open(cfg.recorder_csv, 'rb').read()
    entry, exit, refreshed, coverage = rolling_levels(cfg, now + 20, include_coverage=True)
    assert (entry, exit, refreshed) == (180, 299, now)
    assert coverage['valid_minutes'] == 120
    assert coverage['missing_minutes'] == 119
    assert coverage['history_start_ts'] == rows[180][0]
    assert coverage['history_end_ts'] == rows[-1][0] + 60
    assert coverage['latest_data_age_sec'] > 86400
    # Restarting later reuses these bars, without rewriting or re-warming them.
    assert rolling_levels(cfg, now + 3600)[:2] == (180, 299)
    assert open(cfg.recorder_csv, 'rb').read() == original


def test_records_skip_future_unfinished_invalid_and_duplicate_minutes(tmp_path):
    cfg = engine_at(tmp_path).cfg
    cfg.bps_window_minutes = cfg.bps_min_valid_minutes = 4
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 0, 100
    now = 60000
    valid = [(now - 3600 + i * 180, i, 60) for i in range(6)]
    extras = [(now, 999, 60), (now + 60, 999, 60),
              (now - 60, 999, 9), (now - 120, 'nan', 60),
              (now - 180 + 1, 999, 60)]
    write_history(cfg, valid + [valid[-1]] * 20 + extras)
    assert rolling_levels(cfg, now + 20) == (2, 5, now)
    write_history(cfg, valid[-3:] + [valid[-1]] * 20 + extras)
    with pytest.raises(ValueError, match='3/4.*至少 4'):
        rolling_levels(cfg, now + 20)


def test_record_search_expands_past_duplicate_tail(tmp_path):
    cfg = engine_at(tmp_path).cfg
    cfg.bps_window_minutes = cfg.bps_min_valid_minutes = 4
    cfg.bps_entry_percentile, cfg.bps_exit_percentile = 0, 100
    now = 60000
    rows = [(now - 3600 + i * 60, i, 60) for i in range(4)]
    write_history(cfg, rows + [rows[-1]] * 5000)
    assert rolling_levels(cfg, now) == (0, 3, now)


def test_record_history_respects_known_market_source_boundary(tmp_path):
    cfg = engine_at(tmp_path).cfg
    cfg.bps_window_minutes = cfg.bps_min_valid_minutes = 4
    now = 60000
    write_history(cfg, [(now - 600 + i * 60, i, 60) for i in range(6)])
    from pathlib import Path
    Path(cfg.recorder_csv + '.source.json').write_text(json.dumps(dict(
        version=2, source=recorder_source(cfg), since=now - 420)))
    with pytest.raises(ValueError, match='3/4.*来源过滤 3'):
        rolling_levels(cfg, now)


def test_refresh_publishes_history_range_without_new_collection(tmp_path):
    async def run():
        engine = engine_at(tmp_path)
        engine.cfg.bps_levels = 'rolling'
        engine.cfg.bps_window_minutes = engine.cfg.bps_min_valid_minutes = 4
        engine.bps_strategy = BpsStrategy(engine)
        engine.ensure_async_state()
        write_history(engine.cfg, [(3000, -10, 60), (3180, -10, 60),
                                   (3300, -6, 60), (3540, -6, 60)])
        async def stop(awaitable, timeout):
            awaitable.close()
            engine.stop.set()
        with patch('entropy_arb.bps_strategy.time.time', return_value=60020), \
                patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=stop):
            await engine.bps_strategy.refresh_loop()
        state = engine.bps_strategy.snapshot()
        assert state['entry_bps'] == -10 and state['exit_bps'] == -6
        assert state['window_end'] == 60000
        assert state['history_mode'] == 'records'
        assert state['coverage']['history_start_ts'] == 3000
        assert state['coverage']['history_end_ts'] == 3600
    asyncio.run(run())


@pytest.mark.parametrize('mode', ['records', 'time', 'invalid'])
def test_history_mode_config(tmp_path, mode):
    path = tmp_path / 'strategy.yaml'
    path.write_text(f'strategy:\n  mode: bps\nbps:\n  history_mode: {mode}\n')
    if mode == 'invalid':
        with pytest.raises(ConfigError, match='history_mode'):
            load_config(str(path), symbol='MU', hedge_venue='lighter', credential_env={})
    else:
        cfg = load_config(str(path), symbol='MU', hedge_venue='lighter', credential_env={})
        assert cfg.bps_history_mode == mode
