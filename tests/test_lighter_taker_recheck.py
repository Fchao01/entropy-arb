"""Delayed/ambiguous IOC submissions are queried by the same ID, never resent."""
import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue
from test_maker import setup_engine


def venue_fixture(monkeypatch):
    eng, *_ = setup_engine()
    venue = LighterVenue(eng.cfg.primary, None, .001)
    venue.market_id = 0
    venue.signer = SimpleNamespace(create_order=AsyncMock(return_value=(None, SimpleNamespace(code=200), None)))
    venue.orders_feed = AccountOrdersFeed('RH', '', 0, 1, None)
    monkeypatch.setitem(sys.modules, 'lighter', SimpleNamespace(SignerClient=SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0, DEFAULT_IOC_EXPIRY=0)))
    monkeypatch.setattr('entropy_arb.venue_lighter.TAKER_RECHECK_SEC', .03)
    return venue


@pytest.mark.parametrize('transport_error', [True, False])
def test_late_confirmation_uses_original_order_id_and_returns_actual_fill(monkeypatch, transport_error):
    async def run():
        venue = venue_fixture(monkeypatch)
        if transport_error:
            venue.signer.create_order.side_effect = ConnectionError('synthetic')
        venue.maker_state = AsyncMock(return_value=dict(terminal=True, status='canceled-too-much-slippage',
                                                       filled_base=.0003, avg_px=2441.84))
        info = await venue.send_taker(is_buy=False, qty=.0006, limit_px=2440, reduce_only=True)
        assert not info['unresolved'] and info['filled_base'] == .0003
        venue.signer.create_order.assert_awaited_once()
        original = venue.signer.create_order.call_args.kwargs
        assert original['base_amount'] == 6 and original['reduce_only']
        assert venue.maker_state.call_args.args[0].client_id == original['client_order_index']
    asyncio.run(run())


@pytest.mark.parametrize('result', ['missing', 'nonterminal', 'invalid_fill'])
def test_unknown_or_invalid_result_is_not_fabricated_or_resent(monkeypatch, result):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue.maker_state = AsyncMock(return_value=dict(terminal=result != 'nonterminal', status='open',
                                                       filled_base=.001 if result == 'invalid_fill' else 0,
                                                       avg_px=2440))
        if result == 'missing':
            venue.maker_state.side_effect = RuntimeError('not found')
        info = await venue.send_taker(is_buy=False, qty=.0006, limit_px=2440, reduce_only=True)
        assert info['unresolved'] and info['filled_base'] == 0
        venue.signer.create_order.assert_awaited_once()
    asyncio.run(run())


def test_api_rejection_is_known_zero_fill_without_status_poll(monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue.signer.create_order.return_value = (None, None, 'invalid nonce')
        venue.maker_state = AsyncMock()
        info = await venue.send_taker(is_buy=False, qty=.0006, limit_px=2440, reduce_only=True)
        assert not info['unresolved'] and info['filled_base'] == 0
        venue.maker_state.assert_not_awaited()
        venue.signer.create_order.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('still_rejected', [True, False])
def test_explicit_nonce_rejection_refreshes_and_retries_only_once(monkeypatch, still_rejected):
    async def run():
        venue = venue_fixture(monkeypatch)
        refresh = AsyncMock()
        venue.signer.nonce_manager = SimpleNamespace(async_hard_refresh_nonce=refresh)
        attempts = []
        async def submit(**kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1 or still_rejected:
                return None, None, 'code=21104 invalid nonce'
            venue.orders_feed._handle_orders({'orders': {'0': [dict(
                client_order_index=kwargs['client_order_index'], status='filled',
                filled_base_amount='.0006', filled_quote_amount='1.465104')]}})
            return None, SimpleNamespace(code=200), None
        venue.signer.create_order.side_effect = submit
        venue.maker_state = AsyncMock()
        info = await venue.send_taker(is_buy=False, qty=.0006, limit_px=2440, reduce_only=True)
        assert len(attempts) == 2
        refresh.assert_awaited_once_with(venue.conf.lighter_creds.api_key_index)
        assert all(a['reduce_only'] and a['base_amount'] == 6 for a in attempts)
        assert not info['unresolved']
        assert info['filled_base'] == (0 if still_rejected else .0006)
        venue.maker_state.assert_not_awaited()
    asyncio.run(run())
