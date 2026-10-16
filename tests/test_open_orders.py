"""Authenticated/scope-correct order checks; malformed replies cannot mean empty."""
import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from entropy_arb.config import load_config
from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import LighterVenue
from test_arcus import venue as arcus_venue


@pytest.mark.parametrize('data,expected', [
    ({'orders': []}, False), ({'orders': [{'status': 'OPEN'}]}, True),
    ({'orders': [{'status': 'UNTRIGGERED'}]}, True),
    ({}, None), ({'orders': None}, None), ({'orders': {}, 'total': 0}, None),
    ({'orders': [], 'error': 'unavailable'}, None),
])
def test_arcus_open_orders_use_subaccount_and_include_trigger_orders(data, expected):
    async def run():
        v = arcus_venue()
        v.conf.arcus_creds.account_index = 3
        v._get = AsyncMock(return_value=data)
        if expected is None:
            with pytest.raises(RuntimeError):
                await v.has_open_orders()
        else:
            assert await v.has_open_orders() is expected
        v._get.assert_awaited_once_with('/v1/openOrders', address=v.address,
                                       accountIndex=3, market='ETH-USD', limit=1)
    asyncio.run(run())


@pytest.mark.parametrize('auth_field', ['auth', 'authorization'])
@pytest.mark.parametrize('data,expected', [
    ({'code': 200, 'orders': []}, False),
    ({'code': 200, 'orders': [{'status': 'pending'}]}, True),
    ({'code': 401, 'orders': []}, None), ({'code': 200, 'orders': None}, None),
    ({'code': 200}, None), ({'orders': []}, None),
])
def test_lighter_open_orders_use_selected_account_market_and_auth(tmp_path, monkeypatch,
                                                                 auth_field, data, expected):
    async def run():
        path = tmp_path / 'config.yaml'
        path.write_text('strategy: {mode: bps}\nprimary: {taker_fee_bps: 2.25}\n')
        cfg = load_config(str(path), symbol='SPY', primary_venue='arcus',
                          hedge_venue='lighter-rh', credential_env={
                              'PRIMARY_LIGHTER_ACCOUNT_INDEX': '20408',
                              'PRIMARY_LIGHTER_API_KEY_INDEX': '4',
                              'PRIMARY_LIGHTER_API_PRIVATE_KEY': 'test-key'})
        v = LighterVenue(cfg.hedge, None, 1)
        v.market_id = 26
        v.signer = SimpleNamespace(api_client=object(),
                                  create_auth_token_with_expiry=lambda: ('test-token', None))
        calls = []
        async def auth_method(*, account_index, market_id, auth):
            calls.append((account_index, market_id, auth))
            return SimpleNamespace(to_dict=lambda: data)
        async def authorization_method(*, account_index, market_id, authorization):
            calls.append((account_index, market_id, authorization))
            return SimpleNamespace(to_dict=lambda: data)
        method = auth_method if auth_field == 'auth' else authorization_method
        monkeypatch.setitem(sys.modules, 'lighter', SimpleNamespace(
            OrderApi=lambda client: SimpleNamespace(account_active_orders=method)))
        if expected is None:
            with pytest.raises(RuntimeError):
                await v.has_open_orders()
        else:
            assert await v.has_open_orders() is expected
        assert calls == [(20408, 26, 'test-token')]
    asyncio.run(run())


@pytest.mark.parametrize('rows,expected', [
    ([], False), ([{'coin': 'io:SPY', 'isTrigger': True}], True),
    ([{'coin': 'io:OTHER'}], False), ({}, None), ([{}], None),
])
def test_hl_open_orders_include_triggers_and_filter_selected_market(tmp_path, rows, expected):
    async def run():
        path = tmp_path / 'config.yaml'
        path.write_text('strategy: {mode: bps}')
        cfg = load_config(str(path), symbol='SPY', primary_venue='entropy',
                          hedge_venue='lighter-rh', credential_env={})
        v = HLVenue(cfg.primary, cfg.hl_api_url, cfg.hl_ws_url, None, 1)
        v.account = SimpleNamespace(query_address='0x' + 'ab' * 20)
        v.coin = 'io:SPY'
        v._info = AsyncMock(return_value=rows)
        if expected is None:
            with pytest.raises(RuntimeError):
                await v.has_open_orders()
        else:
            assert await v.has_open_orders() is expected
        v._info.assert_awaited_once_with({'type': 'frontendOpenOrders',
                                          'user': v.account.query_address, 'dex': 'io'})
    asyncio.run(run())


@pytest.mark.parametrize('data', [{}, {'positions': None}, {'positions': {'2': {}}}])
def test_arcus_incomplete_position_is_never_flat(data):
    async def run():
        v = arcus_venue()
        v._get = AsyncMock(return_value=data)
        with pytest.raises((RuntimeError, KeyError, TypeError)):
            await v.fetch_position()
    asyncio.run(run())


@pytest.mark.parametrize('data,expected', [
    ({'positions': []}, 0),
    ({'positions': [{'market_id': 26, 'sign': -1, 'position': '.6471'}]}, -.6471),
    ({}, None), ({'positions': None}, None),
    ({'positions': [{'market_id': 26, 'sign': 1}]}, None),
    ({'positions': [{'market_id': 26, 'sign': 0, 'position': '.6471'}]}, None),
    ({'positions': [{'market_id': 26, 'sign': 1, 'position': 'NaN'}]}, None),
])
def test_lighter_positions_cannot_default_missing_fields_to_zero(data, expected):
    async def run():
        v = object.__new__(LighterVenue)
        v.name, v.market_id = 'RH', 26
        v._account = AsyncMock(return_value=data)
        if expected is None:
            with pytest.raises((RuntimeError, KeyError, TypeError)):
                await v.fetch_position()
        else:
            assert await v.fetch_position() == pytest.approx(expected)
    asyncio.run(run())


@pytest.mark.parametrize('data,expected', [
    ({'assetPositions': []}, 0),
    ({'assetPositions': [{'position': {'coin': 'io:SPY', 'szi': '-.6471'}}]}, -.6471),
    ({}, None), ({'assetPositions': None}, None),
    ({'assetPositions': [{'position': {'coin': 'io:SPY'}}]}, None),
    ({'assetPositions': [{'position': {'coin': 'io:SPY', 'szi': 'NaN'}}]}, None),
])
def test_hl_positions_cannot_default_missing_fields_to_zero(data, expected):
    async def run():
        v = object.__new__(HLVenue)
        v.name, v.coin = 'ENTROPY', 'io:SPY'
        v.conf = SimpleNamespace(hl_dex='io')
        v._query_address = lambda: '0x' + 'ab' * 20
        v._info = AsyncMock(return_value=data)
        if expected is None:
            with pytest.raises((RuntimeError, KeyError, TypeError)):
                await v.fetch_position()
        else:
            assert await v.fetch_position() == pytest.approx(expected)
    asyncio.run(run())
