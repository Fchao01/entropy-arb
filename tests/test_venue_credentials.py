"""Deployment-bound ENV references; fixture API keys only, no exchange calls."""
import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from entropy_arb.config import ConfigError, load_config
from entropy_arb.web import TaskManager, missing_credentials
from entropy_arb.venue_lighter import LighterVenue


def configured(tmp_path, primary, hedge, values, binding=''):
    path = tmp_path / 'strategy.yaml'
    path.write_text('thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n'
                    'primary:\n  taker_fee_bps: 0\nhedge:\n  taker_fee_bps: 0\n' + binding)
    return load_config(str(path), symbol='SPY', primary_venue=primary,
                       hedge_venue=hedge, credential_env=values)


def group(name, account, key):
    return {name + '_ACCOUNT_INDEX': str(account), name + '_API_KEY_INDEX': '4',
            name + '_API_PRIVATE_KEY': key}


@pytest.mark.parametrize('primary,hedge', [('arcus', 'lighter-rh'), ('lighter-rh', 'arcus'),
                                        ('lighter', 'lighter-rh'), ('lighter-rh', 'lighter')])
def test_existing_two_api_groups_are_bound_by_network_in_either_role(tmp_path, primary, hedge):
    # Reproduce the startup failure: legacy PRIMARY holds the mainnet account,
    # while the base group holds RH. No third API/key is required.
    values = {**group('LIGHTER', 20408, 'rh-fixture'),
              **group('PRIMARY_LIGHTER', 45447, 'mainnet-fixture')}
    cfg = configured(tmp_path, primary, hedge, values,
                     'credentials:\n  lighter-rh: LIGHTER\n  lighter: PRIMARY_LIGHTER\n')
    for venue in (cfg.primary, cfg.hedge):
        if venue.label == 'RH':
            assert venue.lighter_creds.account_index == 20408
            assert venue.lighter_creds.api_private_key == 'rh-fixture'
            assert venue.lighter_creds.env_prefix == ''
            assert venue.lighter_profile.api_url == 'https://api.rh.lighter.xyz'
        elif venue.label == 'LIGHTER':
            assert venue.lighter_creds.account_index == 45447
            assert venue.lighter_creds.api_private_key == 'mainnet-fixture'


@pytest.mark.parametrize('primary,hedge', [('arcus', 'lighter-rh'), ('lighter-rh', 'arcus'),
                                        ('lighter', 'lighter-rh'), ('lighter-rh', 'lighter'),
                                        ('lighter', 'arcus'), ('arcus', 'lighter')])
def test_dedicated_rh_and_mainnet_groups_ignore_stale_primary_group(tmp_path, primary, hedge):
    values = {**group('RH_LIGHTER', 20408, 'rh-fixture'),
              **group('LIGHTER', 45447, 'mainnet-fixture'),
              **group('PRIMARY_LIGHTER', 777, 'stale-fixture')}
    cfg = configured(tmp_path, primary, hedge, values)
    for venue in (cfg.primary, cfg.hedge):
        if venue.kind != 'lighter':
            continue
        creds = venue.lighter_creds
        assert creds.account_index == (20408 if venue.label == 'RH' else 45447)
        assert creds.api_private_key != 'stale-fixture'


@pytest.mark.parametrize('primary,hedge', [('arcus', 'lighter-rh'), ('lighter-rh', 'arcus'),
                                        ('lighter', 'lighter-rh'), ('lighter-rh', 'lighter')])
def test_ambiguous_legacy_groups_fail_without_selecting_an_account(tmp_path, primary, hedge):
    values = {**group('LIGHTER', 20408, 'rh-secret-fixture'),
              **group('PRIMARY_LIGHTER', 45447, 'main-secret-fixture')}
    with pytest.raises(ConfigError, match='凭据归属不明确') as error:
        configured(tmp_path, primary, hedge, values)
    assert 'rh-secret-fixture' not in str(error.value)
    assert 'main-secret-fixture' not in str(error.value)


def test_partial_rh_group_never_borrows_fields_from_another_network(tmp_path):
    cfg = configured(tmp_path, 'arcus', 'lighter-rh', {
        'RH_LIGHTER_API_PRIVATE_KEY': 'rh-fixture',
        **group('PRIMARY_LIGHTER', 777, 'stale-fixture'),
        **group('LIGHTER', 45447, 'mainnet-fixture')})
    creds = cfg.hedge.lighter_creds
    assert creds.account_index is None and creds.api_key_index is None
    assert creds.api_private_key == 'rh-fixture' and not creds.complete
    assert 'RH_LIGHTER_ACCOUNT_INDEX' in missing_credentials(cfg)
    assert 'RH_LIGHTER_API_KEY_INDEX' in missing_credentials(cfg)


def test_explicit_missing_group_is_not_replaced_with_a_configured_group(tmp_path):
    cfg = configured(tmp_path, 'arcus', 'lighter-rh', group('PRIMARY_LIGHTER', 45447, 'other-fixture'),
                     'credentials:\n  lighter-rh: RH_LIGHTER\n')
    assert not cfg.hedge.lighter_creds.complete
    assert cfg.hedge.lighter_creds.account_index is None


def test_credential_mapping_cannot_read_arbitrary_environment_or_store_a_key(tmp_path):
    with pytest.raises(ConfigError, match='ENV group name only') as error:
        configured(tmp_path, 'arcus', 'lighter-rh', {}, 'credentials:\n  lighter-rh: secret-fixture\n')
    assert 'secret-fixture' not in str(error.value)


def test_web_profile_supports_rh_fields_without_leaking_or_inheriting_secrets(tmp_path, monkeypatch):
    async def run():
        root = tmp_path / 'project'
        root.mkdir()
        (root / 'credentials').mkdir()
        (root / 'credentials/rh.env').write_text('RH_LIGHTER_ACCOUNT_INDEX=20408\n'
            'RH_LIGHTER_API_KEY_INDEX=4\nRH_LIGHTER_API_PRIVATE_KEY=rh-fixture\n')
        monkeypatch.setenv('PRIMARY_LIGHTER_API_PRIVATE_KEY', 'ambient-fixture')
        monkeypatch.setenv('RH_LIGHTER_API_PRIVATE_KEY', 'ambient-rh-fixture')
        manager = TaskManager(root, tmp_path / 'data')
        values = manager.environment('rh')
        assert values['RH_LIGHTER_API_PRIVATE_KEY'] == 'rh-fixture'
        assert 'PRIMARY_LIGHTER_API_PRIVATE_KEY' not in values
        status = manager.credential_status('rh')
        assert next(x for x in status['fields'] if x['name'] == 'RH_LIGHTER_API_PRIVATE_KEY')['configured']
        assert 'rh-fixture' not in json.dumps(status)
        await manager.save_credentials('rh', {'overwrite': True,
            'values': {'RH_LIGHTER_API_KEY_INDEX': '5', 'RH_LIGHTER_API_PRIVATE_KEY': 'new-fixture'}})
        assert manager.environment('rh')['RH_LIGHTER_API_KEY_INDEX'] == '5'
        assert manager.environment('rh')['RH_LIGHTER_API_PRIVATE_KEY'] == 'new-fixture'
        assert 'new-fixture' not in json.dumps(manager.credential_status('rh'))
    asyncio.run(run())


@pytest.mark.parametrize('canonical', [True, False])
def test_arcus_primary_rh_signer_gets_rh_account_and_key_not_mainnet(tmp_path, monkeypatch, canonical):
    rh_group = 'RH_LIGHTER' if canonical else 'LIGHTER'
    main_group = 'LIGHTER' if canonical else 'PRIMARY_LIGHTER'
    cfg = configured(tmp_path, 'arcus', 'lighter-rh', {
        **group(rh_group, 20408, 'rh-fixture'), **group(main_group, 45447, 'main-fixture')},
        '' if canonical else 'credentials:\n  lighter-rh: LIGHTER\n  lighter: PRIMARY_LIGHTER\n')
    calls = []
    def signer(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(check_client=lambda: None)
    monkeypatch.setitem(sys.modules, 'lighter', SimpleNamespace(SignerClient=signer))
    venue = LighterVenue(cfg.hedge, None, cfg.settle_timeout_sec)
    venue.init_signer()
    assert len(calls) == 1
    assert calls[0]['url'] == 'https://api.rh.lighter.xyz'
    assert calls[0]['account_index'] == 20408
    assert calls[0]['api_private_keys'] == {4: 'rh-fixture'}


def test_record_validation_does_not_consult_ambiguous_legacy_api_groups(tmp_path, monkeypatch):
    from test_web import manager_at, payload
    async def run():
        manager = manager_at(tmp_path)
        for name, value in {**group('LIGHTER', 20408, 'rh-fixture'),
                            **group('PRIMARY_LIGHTER', 45447, 'main-fixture')}.items():
            monkeypatch.setenv(name, value)
        _, _, cfg, _ = manager.validate(payload(), 'a' * 32)
        assert cfg.primary.lighter_creds.account_index is None
        assert cfg.primary.lighter_creds.api_private_key is None
        await manager.close()
    asyncio.run(run())


def test_cli_record_only_does_not_load_trading_credentials(monkeypatch):
    import main
    class BeforeEngine(Exception):
        pass
    seen = []
    def load(*args, **kwargs):
        seen.append(kwargs['credential_env'])
        raise BeforeEngine()
    monkeypatch.setattr(main, 'load_config', load)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--symbol', 'SPY', '--hedge', 'arcus', '--record-only'])
    with pytest.raises(BeforeEngine):
        main.main()
    assert seen == [{}]
