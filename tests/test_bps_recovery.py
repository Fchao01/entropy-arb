"""Pending recovery uses fresh, complete account reads before any trading."""
import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from test_bps_strategy import engine_at


@pytest.fixture
def pending_engine(tmp_path, monkeypatch):
    monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
    eng = engine_at(tmp_path)
    eng.bps_strategy.pending = True
    eng.bps_strategy.entries = 10
    eng.bps_strategy.locked_entry, eng.bps_strategy.locked_exit = -9, -6
    eng.bps_strategy.save()
    eng.bps_strategy = BpsStrategy(eng)
    for v in eng.venues.values():
        v.has_open_orders = AsyncMock(return_value=False)
        v.fetch_position = AsyncMock(return_value=0.0)
    return eng


def test_verified_flat_accounts_archive_pending_and_resume(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        strategy = eng.bps_strategy
        previous = strategy.path.read_text()
        await strategy.recover_pending()
        archive, = strategy.path.parent.glob('*.recovered-*.bak')
        assert archive.read_text() == previous
        assert json.loads(strategy.path.read_text())['pending'] is False
        assert strategy.entries == 0 and not strategy.pending
        assert strategy.costs.open_notional == 0
        strategy.restore()
        assert strategy.flat()
        for v in eng.venues.values():
            assert v.has_open_orders.await_count == v.fetch_position.await_count == 2
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


@pytest.mark.parametrize('venue_key', ['entropy', 'hedge'])
@pytest.mark.parametrize('second_pass', [False, True])
@pytest.mark.parametrize('failure', ['orders', 'position', 'tiny_position', 'invalid_position',
                                     'query', 'timeout', 'unknown_orders'])
def test_recovery_keeps_pending_on_any_uncertainty(pending_engine, venue_key,
                                                  second_pass, failure):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        v = eng.venues[venue_key]
        if failure in ('orders', 'unknown_orders', 'query', 'timeout'):
            result = {'orders': True, 'unknown_orders': None,
                      'query': RuntimeError('auth=synthetic-secret'),
                      'timeout': asyncio.TimeoutError()}[failure]
            v.has_open_orders.side_effect = ([False] if second_pass else []) + [result]
        else:
            result = {'position': .6471, 'tiny_position': .000001,
                      'invalid_position': float('nan')}[failure]
            v.fetch_position.side_effect = ([0.0] if second_pass else []) + [result]
        previous = eng.bps_strategy.path.read_text()
        with pytest.raises(RuntimeError, match='保留 pending') as error:
            await eng.bps_strategy.recover_pending()
        assert 'synthetic-secret' not in str(error.value)
        assert eng.bps_strategy.path.read_text() == previous
        assert not list(eng.bps_strategy.path.parent.glob('*.recovered-*.bak'))
    asyncio.run(run())


def test_pending_identity_mismatch_cannot_clear_another_accounts_state(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        data = json.loads(eng.bps_strategy.path.read_text())
        data['identity'][-1] = 'different-account'
        eng.bps_strategy.path.write_text(json.dumps(data))
        with pytest.raises(RuntimeError, match='账户或交易对'):
            await eng.bps_strategy.recover_pending()
        for v in eng.venues.values():
            v.has_open_orders.assert_not_awaited()
        assert json.loads(eng.bps_strategy.path.read_text())['pending'] is True
    asyncio.run(run())


def test_shutdown_during_recheck_preserves_pending(pending_engine, monkeypatch):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 1)
        async def stop():
            await asyncio.sleep(.01)
            eng.stop.set()
        task = asyncio.create_task(stop())
        with pytest.raises(RuntimeError, match='自动核验已停止'):
            await eng.bps_strategy.recover_pending()
        await task
        assert json.loads(eng.bps_strategy.path.read_text())['pending'] is True
    asyncio.run(run())


def test_normal_startup_does_not_query_pending_recovery(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        eng.bps_strategy.path.unlink()
        await eng.bps_strategy.recover_pending()
        eng.bps_strategy.save()
        await eng.bps_strategy.recover_pending()
        for v in eng.venues.values():
            v.has_open_orders.assert_not_awaited()
    asyncio.run(run())


def test_recovery_does_not_erase_checkpoint_changed_during_queries(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        def change():
            data = json.loads(eng.bps_strategy.path.read_text())
            data['entries'] += 1
            eng.bps_strategy.path.write_text(json.dumps(data))
            return False
        eng.hedge.has_open_orders.side_effect = change
        with pytest.raises(RuntimeError, match='状态文件变化'):
            await eng.bps_strategy.recover_pending()
        assert json.loads(eng.bps_strategy.path.read_text())['pending'] is True
        assert not list(eng.bps_strategy.path.parent.glob('*.recovered-*.bak'))
    asyncio.run(run())


def test_profit_recovery_preserves_a_new_cooldown_without_old_costs(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        eng.cfg.bps_exit_mode = 'profit'
        await eng.bps_strategy.recover_pending()
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.last_closed_at > 0
        assert restored.entries == 0 and restored.costs.open_notional == 0
        assert not restored.pending
    asyncio.run(run())


def test_failed_checkpoint_save_preserves_original_pending(pending_engine):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        previous = eng.bps_strategy.path.read_text()
        eng.bps_strategy.save = Mock(side_effect=OSError('disk full'))
        with pytest.raises(RuntimeError, match='状态归档/保存失败'):
            await eng.bps_strategy.recover_pending()
        assert eng.bps_strategy.path.read_text() == previous
        assert json.loads(previous)['pending'] is True
    asyncio.run(run())


@pytest.mark.parametrize('has_orders', [False, True])
def test_engine_queries_before_streams_or_execution(pending_engine, monkeypatch, has_orders):
    async def run():
        eng = pending_engine
        eng.ensure_async_state()
        monkeypatch.setattr(type(eng.cfg), 'creds_complete', property(lambda cfg: True))
        original_venues = dict(eng.venues)
        eng._make_venue = lambda conf: original_venues[conf.key]
        for conf in (eng.cfg.primary, eng.cfg.hedge):
            v = original_venues[conf.key]
            v.conf, v.kind = conf, conf.kind
            v.load_market = AsyncMock()
            v.init_signer = Mock()
            v.close = AsyncMock()
            def start(*args):
                assert not eng.bps_strategy.pending
                assert list(eng.bps_strategy.path.parent.glob('*.recovered-*.bak'))
                eng.stop.set()
                return []
            v.start_tasks = Mock(side_effect=start)
        eng.hedge.has_open_orders.return_value = has_orders
        eng._strategy_loop = AsyncMock()
        if has_orders:
            with pytest.raises(RuntimeError, match='存在未成交订单'):
                await eng._run_inner()
            for v in original_venues.values():
                v.start_tasks.assert_not_called()
            eng._strategy_loop.assert_not_called()
        else:
            await eng._run_inner()
            for v in original_venues.values():
                v.start_tasks.assert_called_once()
        for v in original_venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())
