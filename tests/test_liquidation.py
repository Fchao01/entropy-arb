"""Liquidation protection with synthetic accounts and orders only."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.liquidation import DEFAULTS, LiquidationGuard, LiquidationProtection, margin_snapshot
from test_bps_strategy import engine_at
from test_arcus import venue as arcus_venue
from test_lighter_taker_recheck import venue_fixture
from test_web import manager_at, payload, credentials
from entropy_arb.web import ConsoleError


def guard_at(tmp_path, ids=('one',)):
    g = LiquidationGuard(tmp_path / 'guard.json')
    g.initialize()
    g.enroll(ids)
    g.configure({**DEFAULTS, 'enabled': True})
    return g


def report(g, task='one', risk=20):
    g.report(task, [{'venue': 'RH', **margin_snapshot(1000, risk * 10)}])


@pytest.mark.parametrize('equity,mm,exposed,ratio', [(1000,800,True,80), (0,10,True,100),
    (-2,0,True,100), (0,0,False,0), (1000,0,False,0), (1e-300,1e300,True,10000)])
def test_margin_metric(equity, mm, exposed, ratio):
    assert margin_snapshot(equity, mm, exposed=exposed)['risk_pct'] == ratio


@pytest.mark.parametrize('bad', [None, True, 'NaN', float('inf')])
def test_missing_risk_is_not_zero(bad):
    with pytest.raises(ValueError):
        margin_snapshot(bad, 5)


def test_latch_is_global_persistent_and_cannot_be_disabled(tmp_path):
    g = guard_at(tmp_path, ('one', 'two'))
    assert g.view()['blocked']  # No data, no entry permission.
    report(g)
    report(g, 'two')
    assert not g.view()['blocked']
    report(g, risk=70)
    assert g.view()['blocked'] and not g.view()['latched']
    report(g, risk=80)
    report(g, risk=1)
    assert LiquidationGuard(g.path).view()['latched']
    with pytest.raises(ValueError, match='锁定'):
        g.configure(dict(DEFAULTS))
    g.enroll([])
    assert set(g.read()['tasks']) == {'one', 'two'}


def test_stale_and_failed_query_block_entries(tmp_path):
    g = guard_at(tmp_path)
    report(g)
    with g.edit() as value:
        value['tasks']['one']['updated_at'] -= 25
    assert g.view()['blocked']
    g.report('one', [], 'HTTP 429')
    assert g.view()['blocked'] and not g.view()['latched']


def test_reset_requires_fresh_flat_no_unknown_orders(tmp_path):
    g = guard_at(tmp_path)
    report(g, risk=90)
    g.progress('one', flat=True, pending_entropy={'unknown': True})
    with pytest.raises(ValueError):
        g.reset()
    g.progress('one', pending_entropy=None)
    with g.edit() as state:
        state['tasks']['one']['progress_at'] -= 25
    with pytest.raises(ValueError):
        g.reset()
    g.progress('one', flat=True)
    result = g.reset()
    assert not result['latched'] and not result['settings']['enabled']


@pytest.mark.parametrize('field,value', [('warn_pct',80), ('close_pct',100), ('poll_sec',1),
    ('stale_sec',2), ('slippage_bps',0), ('enabled',1)])
def test_bad_settings(tmp_path, field, value):
    g = guard_at(tmp_path)
    with pytest.raises(ValueError):
        g.configure({**DEFAULTS, field: value})


def engine_guard(tmp_path, *, maker=False, watch=False):
    e = engine_at(tmp_path, maker=maker)
    e.ensure_async_state()
    e.bps_strategy = None
    e.cfg.max_order_notional = 1000
    g = guard_at(tmp_path)
    e.liquidation = LiquidationProtection(e, g.path, 'one', watch)
    e.liquidation.refresh()
    e.entropy.position, e.hedge.position = 1., -1.
    for v in e.venues.values():
        v.has_open_orders = AsyncMock(return_value=False)
        async def position(venue=v):
            return venue.position
        v.fetch_position = AsyncMock(side_effect=position)
    return e, g


def test_emergency_flattens_both_directions_reduce_only_and_verifies(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        await e.liquidation.flatten()
        assert e.entropy.position == e.hedge.position == 0
        assert not g.read()['tasks']['one']['flat']  # Wait for separate authoritative verification.
        await e.liquidation.flatten()
        assert g.read()['tasks']['one']['flat']
        for v in e.venues.values():
            assert v.send_taker.await_count == 1
            assert v.send_taker.call_args.kwargs['reduce_only']
        g.reset()
        e.liquidation.refresh()
        assert e.liquidation.blocked  # Never resumes trading from a reset automatically.
    asyncio.run(run())


def test_arcus_maker_override_only_for_latched_emergency_reduce_only(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path, maker=True)
        e.cfg.primary.kind = 'arcus'
        report(g)
        result, _ = await e._timed_send(e.entropy, is_buy=False, qty=1, limit_px=99,
                                         reduce_only=True, emergency=True)
        assert result['filled_base'] == 0
        e.entropy.send_taker.assert_not_awaited()
        report(g, risk=90)
        result, _ = await e._timed_send(e.entropy, is_buy=False, qty=1, limit_px=99,
                                         reduce_only=True)
        assert result['filled_base'] == 0  # Normal maker policy remains intact.
        await e.liquidation.flatten()
        assert e.entropy.send_taker.await_count == 1
        assert e.entropy.send_taker.call_args.kwargs['reduce_only'] is True
    asyncio.run(run())


def test_unknown_order_is_journaled_before_send_and_never_retried(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        async def unknown(**kwargs):
            assert g.read()['tasks']['one']['pending_entropy']['qty'] > 0
            return dict(status='unknown', filled_base=0, avg_px=None, unresolved=True)
        e.entropy.send_taker.side_effect = unknown
        await e.liquidation.flatten()
        await e.liquidation.flatten()
        assert e.entropy.send_taker.await_count == 1
        assert e.hedge.position == 0
        assert g.read()['tasks']['one']['pending_entropy']['uncertain']
        with pytest.raises(ValueError):
            g.reset()
        e._position_uncertain.clear()  # Even a fresh process cannot erase the persistent journal.
        await e.liquidation.flatten()
        assert e.entropy.send_taker.await_count == 1
    asyncio.run(run())


def test_active_maker_waits_for_settlement(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        e.active_maker = {'state': 'canceling'}
        await e.liquidation.flatten()
        e.entropy.send_taker.assert_not_awaited()
        assert '等待' in g.read()['tasks']['one']['message']
    asyncio.run(run())


def test_external_position_and_open_orders_not_claimed(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        e.entropy.fetch_position.return_value = 2
        e.entropy.fetch_position.side_effect = None
        e.hedge.has_open_orders.return_value = True
        await e.liquidation.flatten()
        assert not g.read()['tasks']['one']['flat']
        for v in e.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_watch_only_has_no_strategy_or_topups_and_ownership_cap(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path, watch=True)
        report(g)
        e._scan = lambda *args: pytest.fail('watcher evaluated trading signals')
        await e._evaluate()
        await e._maybe_hedge(allow_halted=True)
        report(g, risk=90)
        e._risk_expected_positions = {'entropy': .5, 'hedge': -1}
        await e.liquidation.flatten()
        e.entropy.send_taker.assert_not_awaited()
        assert e.hedge.position == 0
    asyncio.run(run())


def test_small_residual_and_budget_reject_keep_retryable_not_flat(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        e.entropy.position = .00001
        e.hedge.send_taker.return_value = dict(status='rate-limited', filled_base=0, avg_px=None, unresolved=False)
        e.hedge.send_taker.side_effect = None
        await e.liquidation.flatten()
        await e.liquidation.flatten()
        assert e.hedge.send_taker.await_count == 2
        e.entropy.send_taker.assert_not_awaited()
        row = g.read()['tasks']['one']
        assert not row['flat'] and not row.get('pending_hedge')
    asyncio.run(run())


def test_risky_leg_trips_without_waiting_for_other_leg(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        e.entropy.fetch_liquidation_risk = AsyncMock(return_value=margin_snapshot(100,90))
        async def slow():
            await asyncio.sleep(30)
        e.hedge.fetch_liquidation_risk = slow
        await asyncio.wait_for(e.liquidation.poll(), .2)
        assert g.read()['latched']
    asyncio.run(run())


def test_lighter_uses_cross_equity_not_total_assets_and_rejects_isolated(tmp_path, monkeypatch):
    async def run():
        v = venue_fixture(monkeypatch)
        v.conf.lighter_creds.account_index = 20
        acct = dict(account_index=20, cross_asset_value='100', total_asset_value='10000',
                    cross_maintenance_margin_requirement='80', positions=[{'position':'1', 'margin_mode':0}])
        v._get = AsyncMock(return_value={'accounts':[acct]})
        assert (await v.fetch_liquidation_risk())['risk_pct'] == 80
        acct['positions'][0]['margin_mode'] = 1
        with pytest.raises(ValueError, match='isolated'):
            await v.fetch_liquidation_risk()
        acct['positions'] = []
        del acct['cross_maintenance_margin_requirement']
        with pytest.raises(KeyError):
            await v.fetch_liquidation_risk()
    asyncio.run(run())


def test_arcus_maintenance_includes_all_markets_not_only_task_symbol(tmp_path):
    async def run():
        v = arcus_venue()
        async def get(path, **kwargs):
            return {'/v1/account': {'equity':'100', 'address':v.address, 'accountIndex':0},
                '/v1/positions': {'positions':{'2':{'size':'1'}, '3':{'size':'-2'}}},
                '/v1/markets': {'markets':[
                    {'marketId':2, 'markPrice':'100', 'maintenanceMarginFraction':'.1'},
                    {'marketId':3, 'markPrice':'100', 'maintenanceMarginFraction':'.2'}]}}[path]
        v._get = AsyncMock(side_effect=get)
        result = await v.fetch_liquidation_risk()
        assert result['maintenance'] == 50 and result['risk_pct'] == 50
    asyncio.run(run())


def test_web_lock_blocks_start_edit_delete_and_auto_update(tmp_path):
    async def run():
        m = manager_at(tmp_path)
        try:
            credentials(m, 'test')
            task = await m.put({**payload(), 'mode':'live', 'profile':'test'})
            await m.start(task['id'], True)
            await m.stop(task['id'])
            await m.running[task['id']]['watcher']
            await m.configure_liquidation({**DEFAULTS, 'enabled':True})
            with pytest.raises(ConsoleError, match='风控'):
                await m.put({**payload(), 'mode':'live', 'profile':'test'}, task['id'])
            with pytest.raises(ConsoleError, match='风控'):
                await m.delete(task['id'])
            m.liquidation.report(task['id'], [{'venue':'RH', **margin_snapshot(100,90)}])
            with pytest.raises(ConsoleError, match='风控'):
                await m.start(task['id'], True)
            with pytest.raises(ConsoleError, match='风控'):
                await m.auto_update_task(task['id'], None)
        finally:
            await m.close()
    asyncio.run(run())


def test_http_settings_auth_validation_and_reset(tmp_path):
    import aiohttp
    from aiohttp.test_utils import TestClient, TestServer
    from entropy_arb.web import create_app
    async def run():
        m = manager_at(tmp_path)
        client = TestClient(TestServer(create_app(m, 'test-password-long')),
                            cookie_jar=aiohttp.CookieJar(unsafe=True))
        await client.start_server()
        try:
            headers = {'X-Web-Request':'1'}
            assert (await client.get('/api/liquidation')).status == 401
            await client.post('/api/login', json={'password':'test-password-long'}, headers=headers)
            assert (await client.get('/api/liquidation')).status == 200
            assert (await client.put('/api/liquidation', json={**DEFAULTS, 'enabled':True}, headers=headers)).status == 200
            assert (await client.put('/api/liquidation', json={**DEFAULTS, 'close_pct':100}, headers=headers)).status == 400
            assert (await client.put('/api/liquidation', json=dict(DEFAULTS))).status == 403
            assert (await client.post('/api/liquidation/reset', json={}, headers=headers)).status == 400
            result = await (await client.get('/api/tasks')).json()
            assert result['liquidation']['settings']['enabled']
        finally:
            await client.close()
    asyncio.run(run())


def test_stopped_task_protector_uses_saved_positions_and_identity(tmp_path):
    from entropy_arb.web import write_json
    async def run():
        m = manager_at(tmp_path)
        try:
            credentials(m, 'test')
            task = await m.put({**payload(), 'mode':'live', 'profile':'test'})
            task_id = task['id']
            await m.start(task_id, True)
            await m.stop(task_id)
            await m.running[task_id]['watcher']
            write_json(m.directory(task_id) / 'status.json', {
                'updated_at':time.time(), 'active_maker':None,
                'venues':[{'key':'entropy','position':1.}, {'key':'hedge','position':-1.}]})
            await m.configure_liquidation({**DEFAULTS, 'enabled':True})
            watcher = await m.start(task_id, True, risk_watch=True)
            assert watcher['risk_watch_only']
            assert m.running[task_id]['risk_watch']
            assert m.liquidation.read()['tasks'][task_id]['owned_positions'] == {'entropy':1, 'hedge':-1}
            await m.stop(task_id)
            await m.running[task_id]['watcher']
            # A mismatching newer status must not expand the durable ownership cap.
            write_json(m.directory(task_id) / 'status.json', {
                'venues':[{'key':'entropy','position':10.}, {'key':'hedge','position':-10.}]})
            await m.start(task_id, True, risk_watch=True)
            assert m.liquidation.read()['tasks'][task_id]['owned_positions']['entropy'] == 1
            await m.stop(task_id)
            await m.running[task_id]['watcher']
            path = m.root / 'credentials/test.env'
            path.write_text(path.read_text().replace('LIGHTER_ACCOUNT_INDEX=123', 'LIGHTER_ACCOUNT_INDEX=124'))
            with pytest.raises(ConsoleError, match='身份'):
                await m.start(task_id, True, risk_watch=True)
        finally:
            await m.close()
    asyncio.run(run())


def test_watch_position_send_journal_survives_cancellation(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        began = asyncio.Event()
        async def slow(**kwargs):
            began.set()
            await asyncio.sleep(30)
        e.entropy.send_taker.side_effect = slow
        task = asyncio.create_task(e.liquidation.flatten())
        await began.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert g.read()['tasks']['one']['pending_entropy']
        await e.liquidation.flatten()
        assert e.entropy.send_taker.await_count == 1
    asyncio.run(run())


def test_explicit_order_confirmation_only_resumes_bounded_reduction(tmp_path):
    async def run():
        e, g = engine_guard(tmp_path)
        report(g, risk=90)
        g.progress('one', pending_entropy=dict(position=1., qty=1., uncertain=True))
        e._position_uncertain.add('entropy')
        with pytest.raises(ValueError):
            g.acknowledge_orders(False)
        g.acknowledge_orders(True)
        # The user checked terminal order history; read the actual remaining position again.
        e.entropy.fetch_position.side_effect = None
        e.entropy.fetch_position.return_value = .4
        await e.liquidation.flatten()
        assert e.entropy.send_taker.await_count == 1
        assert e.entropy.send_taker.call_args.kwargs['qty'] <= .4
        assert e.execution_stats.unresolved  # No fabricated historical profit.
        assert g.read()['latched']  # This is not a trading resume or a reset.
        assert not g.read()['tasks']['one']['flat']
    asyncio.run(run())


def test_nonzero_exposure_with_zero_mmr_is_not_safe():
    with pytest.raises(ValueError, match='zero maintenance'):
        margin_snapshot(100,0,exposed=True)
