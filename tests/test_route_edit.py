"""Stopped task route edits preserve old market files; workers are fake."""
import asyncio
import fcntl
import json
from pathlib import Path

import pytest
import yaml

from entropy_arb.config import ConfigError
from entropy_arb.web import ConsoleError, TaskManager
from test_web import manager_at, payload


def strategy(primary='lighter-rh', **extra):
    raw = dict(thresholds=dict(midline_bps=0, upper_bps=4, lower_bps=4),
               primary=dict(venue=primary, taker_fee_bps=0), hedge=dict(taker_fee_bps=0),
               recorder=dict(csv='logs/{symbol}/minutes.csv'),
               logging=dict(file='logs/{symbol}/engine.log', trades_csv='logs/{symbol}/trades.csv'))
    raw.update(extra)
    return yaml.safe_dump(raw, sort_keys=False)


def request(**changes):
    return {**payload(), 'config': strategy(), **changes}


def previous_run(manager, task, *, failed=False):
    old = manager.get(task['id'])
    old.update(started_at=100, ended_at=101, state='failed' if failed else 'stopped',
               exit_code=2 if failed else 0)
    directory = manager.directory(task['id'])
    manager.write_config(directory / 'run-config.yaml', (directory / 'config.yaml').read_text())
    cfg = manager.task_config(old)
    outputs = manager.route_outputs(cfg)
    contents = {}
    for key, value in outputs.items():
        path = Path(value)
        path.parent.mkdir(parents=True, exist_ok=True)
        contents[path] = (json.dumps({'pending': True, 'old-market': True}) if key == 'bps_state'
                          else f'old route {key}\n').encode()
        path.write_bytes(contents[path])
    source = Path(outputs['minutes_csv'] + '.source.json')
    contents[source] = b'{"source": "old-source"}'
    source.write_bytes(contents[source])
    (directory / 'status.json').write_text(json.dumps({'updated_at': 100, 'halted': True,
        'active_maker': None, 'session_volume_usd': 500, 'session_profit_usd': 1}))
    (directory / 'events.log').write_text('old route START / EXIT\n')
    (directory / 'auto_threshold.json').write_text('{"status":"applied","window":"old-day"}')
    manager.save()
    return old.copy(), outputs, contents


def test_failed_started_task_can_change_exchange_without_losing_old_files(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(request(mode='live'))
            old, outputs, contents = previous_run(manager, task, failed=True)
            updated = await manager.put(request(mode='live', hedge='lighter'), task['id'])
            assert updated['id'] == task['id'] and updated['hedge'] == 'lighter'
            assert updated['route_changed'] and updated['state'] == 'stopped'
            assert updated['started_at'] is None and updated['ended_at'] is None
            assert updated['exit_code'] is None and updated['status'] is None
            assert updated['auto_threshold']['status'] == 'pending'
            assert not manager.running  # Saving never places orders or launches a worker.
            for path, value in contents.items():
                assert path.read_bytes() == value
            new_cfg = manager.task_config(manager.get(task['id']))
            assert all(value not in outputs.values() for value in manager.route_outputs(new_cfg).values())
            archive = Path(updated['route_history'][0]['archive'])
            assert json.loads((archive / 'task.json').read_text())['hedge'] == 'arcus'
            assert json.loads((archive / 'outputs.json').read_text()) == outputs
            assert (archive / 'status.json').is_file()
            assert (archive / 'auto_threshold.json').is_file()
            assert 'old route' in (archive / 'events.log').read_text()
            assert 'ROUTE CHANGED' in (manager.directory(task['id']) / 'events.log').read_text()
        finally:
            await manager.close()
    asyncio.run(run())


@pytest.mark.parametrize('change', ['swap', 'hedge', 'symbol', 'primary_market', 'hedge_market', 'network'])
def test_each_market_identity_change_gets_independent_output_paths(tmp_path, change):
    async def run():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(request())
            _, _, files = previous_run(manager, task)
            new = request()
            raw = yaml.safe_load(new['config'])
            if change == 'swap':
                new.update(primary='arcus', hedge='lighter-rh')
                raw['primary']['venue'] = 'arcus'
            elif change == 'hedge':
                new['hedge'] = 'lighter'
            elif change == 'symbol':
                new['symbol'] = 'BTC'
            elif change == 'primary_market':
                raw['primary']['symbol'] = 'BTC'
            elif change == 'hedge_market':
                raw['hedge']['symbol'] = 'BTC-USD'
            else:
                raw['arcus'] = {'network': 'testnet'}
            new['config'] = yaml.safe_dump(raw)
            saved = await manager.put(new, task['id'])
            assert saved['route_changed'] and saved['started_at'] is None
            assert len(saved['route_history']) == 1
            for path, value in files.items():
                assert path.read_bytes() == value
        finally:
            await manager.close()
    asyncio.run(run())


def test_new_route_starts_and_survives_manager_restart(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        root, data = manager.root, manager.data
        task = await manager.put(request())
        await manager.start(task['id'])
        watcher = manager.running[task['id']]['watcher']
        await asyncio.sleep(.1)
        old_log = manager.log_path(manager.get(task['id']))
        await manager.stop(task['id'])
        await asyncio.wait_for(watcher, 3)
        content = old_log.read_bytes()
        changed = await manager.put(request(hedge='lighter'), task['id'])
        await manager.start(task['id'])
        watcher = manager.running[task['id']]['watcher']
        await asyncio.sleep(.1)
        assert old_log.read_bytes() == content
        new_log = manager.log_path(manager.get(task['id']))
        assert new_log != old_log and 'fake worker ready ETH' in new_log.read_text()
        await manager.stop(task['id'])
        await asyncio.wait_for(watcher, 3)
        await manager.close()
        restored = TaskManager(root, data)
        try:
            current = restored.get(task['id'])
            assert current['hedge'] == 'lighter'
            assert current['route_history'] == changed['route_history']
            await restored.start(task['id'])
        finally:
            await restored.close()
    asyncio.run(run())


def test_same_route_parameter_edit_keeps_trading_state_and_run_timestamps(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(request())
            old, outputs, files = previous_run(manager, task)
            edited = request(config=strategy().replace('upper_bps: 4', 'upper_bps: 6'))
            saved = await manager.put(edited, task['id'])
            assert not saved['route_changed'] and saved['started_at'] == old['started_at']
            assert 'route_history' not in saved
            assert manager.route_outputs(manager.task_config(manager.get(task['id']))) == outputs
            for path, value in files.items():
                assert path.read_bytes() == value
        finally:
            await manager.close()
    asyncio.run(run())


def test_already_route_specific_paths_are_not_replaced(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy().replace('logs/{symbol}', 'logs/{primary}-{hedge}/{symbol}')
            task = await manager.put(request(config=text))
            previous_run(manager, task)
            saved = await manager.put(request(hedge='lighter', config=text), task['id'])
            assert saved['route_changed']
            assert 'route-' not in saved['config']
            assert yaml.safe_load(saved['config'])['recorder']['csv'] == 'logs/{primary}-{hedge}/{symbol}/minutes.csv'
        finally:
            await manager.close()
    asyncio.run(run())


def test_returning_to_a_previously_used_path_preserves_all_history(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(request())
            _, first_outputs, files = previous_run(manager, task)
            first = await manager.put(request(hedge='lighter'), task['id'])
            # Even before the new route has run, another edit cannot reuse
            # the archived first route's CSV, checkpoint or logs.
            saved = await manager.put(request(), task['id'])
            assert len(saved['route_history']) == 2
            assert all(value not in first_outputs.values() for value in
                       manager.route_outputs(manager.task_config(manager.get(task['id']))).values())
            for path, value in files.items():
                assert path.read_bytes() == value
        finally:
            await manager.close()
    asyncio.run(run())


def test_shared_yaml_route_edit_detaches_only_the_edited_task(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy()
            await manager.save_strategy('shared.yaml', {'config': text})
            first = await manager.put(request(strategy_file='shared.yaml'))
            second = await manager.put(request(symbol='BTC', strategy_file='shared.yaml'))
            previous_run(manager, first)
            saved = await manager.put(request(hedge='lighter', strategy_file='shared.yaml',
                                               save_strategy=True), first['id'])
            assert saved['strategy_detached'] and saved['strategy_file'] is None
            assert manager.strategy('shared.yaml') == text
            assert manager.get(second['id'])['strategy_file'] == 'shared.yaml'
            assert manager.get(second['id'])['hedge'] == 'arcus'
        finally:
            await manager.close()
    asyncio.run(run())


def test_stopped_yaml_refresh_does_not_erase_the_actual_previous_route_snapshot(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy()
            await manager.save_strategy('linked.yaml', {'config': text})
            task = await manager.put(request(strategy_file='linked.yaml'))
            old, outputs, files = previous_run(manager, task)
            original = manager.task_snapshot_config(old).read_text()
            newer = text.replace('venue: lighter-rh', 'venue: lighter-rh\n  symbol: BTC')
            await manager.save_strategy('linked.yaml', {'config': newer, 'overwrite': True})
            manager.ensure_task_config(manager.get(task['id']))
            assert manager.task_snapshot_config(old).read_text() == original
            saved = await manager.put(request(strategy_file='linked.yaml'), task['id'])
            assert saved['route_changed'] and saved['strategy_detached']
            archive = Path(saved['route_history'][0]['archive'])
            assert (archive / 'config.yaml').read_text() == original
            assert (archive / 'run-config.yaml').read_text() == original
            assert saved['route_history'][0]['outputs'] == outputs
        finally:
            await manager.close()
    asyncio.run(run())


def test_invalid_route_does_not_modify_the_linked_yaml_or_task(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy()
            await manager.save_strategy('linked.yaml', {'config': text})
            task = await manager.put(request(strategy_file='linked.yaml'))
            old, _, files = previous_run(manager, task)
            manifest = (manager.data / 'tasks.json').read_bytes()
            with pytest.raises(ConfigError, match='same venue'):
                await manager.put(request(strategy_file='linked.yaml', save_strategy=True,
                                           config=text.replace('upper_bps: 4', 'upper_bps: 7'),
                                           hedge='lighter-rh'), task['id'])
            assert manager.strategy('linked.yaml') == text
            assert (manager.data / 'tasks.json').read_bytes() == manifest
            assert not (manager.directory(task['id']) / 'history').exists()
            for path, value in files.items():
                assert path.read_bytes() == value
        finally:
            await manager.close()
    asyncio.run(run())


def test_task_instance_lock_still_blocks_route_changes(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(request())
            with (manager.directory(task['id']) / 'instance.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with pytest.raises(ConsoleError, match='先停止'):
                    await manager.put(request(hedge='lighter'), task['id'])
            assert manager.get(task['id'])['hedge'] == 'arcus'
        finally:
            await manager.close()
    asyncio.run(run())


def test_snapshot_write_failure_restores_linked_yaml(tmp_path, monkeypatch):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy()
            await manager.save_strategy('linked.yaml', {'config': text})
            task = await manager.put(request(strategy_file='linked.yaml'))
            old, _, files = previous_run(manager, task)
            snapshot = manager.directory(task['id']) / 'config.yaml'
            before = snapshot.read_bytes()
            write = manager.write_config
            def fail_snapshot(path, value):
                if path == snapshot:
                    raise OSError('simulated snapshot write failure')
                write(path, value)
            monkeypatch.setattr(manager, 'write_config', fail_snapshot)
            with pytest.raises(OSError):
                await manager.put(request(hedge='lighter', strategy_file='linked.yaml',
                                           save_strategy=True), task['id'])
            assert manager.strategy('linked.yaml') == text
            assert snapshot.read_bytes() == before
            assert manager.get(task['id'])['hedge'] == old['hedge']
            for path, value in files.items():
                assert path.read_bytes() == value
        finally:
            await manager.close()
    asyncio.run(run())


def test_archive_failure_does_not_change_task_or_source(tmp_path, monkeypatch):
    async def run():
        manager = manager_at(tmp_path)
        try:
            text = strategy()
            await manager.save_strategy('linked.yaml', {'config': text})
            task = await manager.put(request(strategy_file='linked.yaml'))
            old, _, _ = previous_run(manager, task)
            manifest = (manager.data / 'tasks.json').read_bytes()
            def fail(*args):
                raise OSError('simulated archive failure')
            monkeypatch.setattr(manager, 'archive_route', fail)
            with pytest.raises(OSError):
                await manager.put(request(hedge='lighter', strategy_file='linked.yaml',
                                           save_strategy=True), task['id'])
            assert manager.strategy('linked.yaml') == text
            assert manager.get(task['id'])['hedge'] == old['hedge']
            assert (manager.data / 'tasks.json').read_bytes() == manifest
        finally:
            await manager.close()
    asyncio.run(run())


def test_upgrade_freezes_legacy_run_config_before_yaml_refresh(tmp_path):
    async def run():
        manager = manager_at(tmp_path)
        task = await manager.put(request())
        previous_run(manager, task)
        directory = manager.directory(task['id'])
        actual = (directory / 'run-config.yaml').read_text()
        (directory / 'run-config.yaml').unlink()  # Older versions saved only config.yaml.
        root, data = manager.root, manager.data
        await manager.close()
        restored = TaskManager(root, data)
        try:
            assert (directory / 'run-config.yaml').read_text() == actual
            (directory / 'config.yaml').write_text(strategy(primary='arcus'))
            assert restored.task_snapshot_config(restored.get(task['id'])).read_text() == actual
        finally:
            await restored.close()
    asyncio.run(run())
