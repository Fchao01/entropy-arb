"""Verified paired inventory can use current BPS levels without a flat cycle."""
import asyncio
import csv
import json
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from test_bps_executable import executable_engine, quotes


def paired_engine(tmp_path, monkeypatch):
    monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
    eng = executable_engine(tmp_path)
    eng.ensure_async_state()
    eng.entropy.position, eng.hedge.position = 1, -1
    eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
    for venue in eng.venues.values():
        venue.has_open_orders = AsyncMock(return_value=False)
        venue.fetch_position = AsyncMock(side_effect=lambda venue=venue: venue.position)
    return eng


@pytest.mark.parametrize('checkpoint', [False, True])
def test_verified_pair_without_matching_checkpoint_is_adopted_at_current_levels(tmp_path, monkeypatch, checkpoint):
    async def run():
        eng = paired_engine(tmp_path, monkeypatch)
        if checkpoint:
            eng.bps_strategy.entries = 1
            eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = 5, 4
            eng.bps_strategy.locked_price_basis = 'executable'
            eng.bps_strategy.save()
            raw = json.loads(eng.bps_strategy.path.read_text())
            raw['positions'] = {eng.entropy.key: .5, eng.hedge.key: -.5}
            eng.bps_strategy.path.write_text(json.dumps(raw))
        await eng.bps_strategy.verify_existing_pair()
        eng.bps_strategy.restore()
        assert not eng.bps_strategy.pending
        assert eng.bps_strategy.price_basis == 'executable'
        assert eng.bps_strategy.locked_entry == 5 and eng.bps_strategy.locked_exit == 4
        assert not eng.bps_strategy.costs.known
        for venue in eng.venues.values():
            venue.send_taker.assert_not_awaited()
            assert venue.has_open_orders.await_count == venue.fetch_position.await_count == 2
        eng.bps_strategy.last_entry_at = 0  # Existing configured add interval still applies.
        quotes(eng, opening=True)
        assert eng._scan(time.time())[2].action == 'bps_open'
        quotes(eng, opening=False)
        assert eng._scan(time.time())[2].action == 'bps_close'
    asyncio.run(run())


@pytest.mark.parametrize('problem', ['orders', 'position', 'query', 'pending', 'identity'])
def test_unconfirmed_or_foreign_old_inventory_never_gets_rebased(tmp_path, monkeypatch, problem):
    async def run():
        eng = paired_engine(tmp_path, monkeypatch)
        eng.bps_strategy.entries = 1
        eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = -9, -6
        eng.bps_strategy.locked_price_basis = 'mid'
        eng.bps_strategy.save()
        eng.bps_strategy = BpsStrategy(eng)
        if problem == 'orders':
            eng.entropy.has_open_orders.return_value = True
        elif problem == 'position':
            eng.entropy.fetch_position.return_value = 2
            eng.entropy.fetch_position.side_effect = None
        elif problem == 'query':
            eng.entropy.has_open_orders.side_effect = RuntimeError('query failed')
        else:
            raw = json.loads(eng.bps_strategy.path.read_text())
            if problem == 'pending':
                raw['pending'] = True
            else:
                raw['identity'][0] = 'foreign'
            eng.bps_strategy.path.write_text(json.dumps(raw))
        original = eng.bps_strategy.path.read_text()
        with pytest.raises(RuntimeError):
            await eng.bps_strategy.verify_existing_pair()
            eng.bps_strategy.restore()
        assert eng.bps_strategy.path.read_text() == original
        for venue in eng.venues.values():
            venue.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_rolling_rebase_uses_existing_history_without_waiting_for_flat_positions(tmp_path, monkeypatch):
    async def run():
        eng = paired_engine(tmp_path, monkeypatch)
        eng.cfg.bps_levels = 'rolling'
        eng.cfg.bps_window_minutes = eng.cfg.bps_min_valid_minutes = 2
        eng.bps_strategy.entries = 1
        eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = -9, -6
        eng.bps_strategy.locked_price_basis = 'mid'
        eng.bps_strategy.save()
        eng.bps_strategy = BpsStrategy(eng)
        await eng.bps_strategy.verify_existing_pair()
        eng.bps_strategy.restore()
        assert eng.bps_strategy.awaiting_levels and eng.bps_strategy.locked_exit is None
        assert eng._scan(time.time()) is None
        now = int(time.time() // 60) * 60
        with open(eng.cfg.recorder_csv, 'w') as output:
            writer = csv.writer(output)
            writer.writerow(['minute_ts', 'entropy_bid', 'entropy_ask', 'hedge_bid', 'hedge_ask', 'samples'])
            writer.writerows([(now - 120, 99.90, 99.91, 100, 100.01, 60),
                              (now - 60, 100.10, 100.11, 100, 100.01, 60)])
        async def stop(awaitable, timeout):
            awaitable.close()
            eng.stop.set()
        with patch('entropy_arb.bps_strategy.asyncio.wait_for', side_effect=stop):
            await eng.bps_strategy.refresh_loop()
        assert not eng.bps_strategy.awaiting_levels
        assert eng.bps_strategy.locked_entry is not None and eng.bps_strategy.locked_exit is not None
        assert eng.entropy.position == 1 and eng.hedge.position == -1
        for venue in eng.venues.values():
            venue.send_taker.assert_not_awaited()
    asyncio.run(run())
