"""REST backoff must not suppress confirmed cumulative WS fills."""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.maker import MakerOrder
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue
from test_maker import setup_engine


def test_rest_errors_back_off_but_late_ws_fill_is_read_immediately():
    async def run():
        eng, *_ = setup_engine()
        venue = LighterVenue(eng.cfg.primary, None, .02)
        venue.market_id = 2
        venue.orders_feed = AccountOrdersFeed('RH', '', 2, 1, None)
        venue._maker_lookup = AsyncMock(side_effect=RuntimeError('429'))
        handle = MakerOrder(4, 100, time.time())
        with patch('entropy_arb.venue_lighter.time.monotonic', return_value=100):
            with pytest.raises(RuntimeError, match='429'):
                await venue.maker_state(handle)
            for _ in range(20):
                assert not (await venue.maker_state(handle))['terminal']
            venue._maker_lookup.assert_awaited_once()
            venue.orders_feed._handle_orders({'orders': {'2': [{
                'client_order_index': 4, 'order_index': 99, 'status': 'canceled',
                'filled_base_amount': '.2', 'filled_quote_amount': '20'}]}})
            state = await venue.maker_state(handle)
            assert state['terminal'] and state['filled_base'] == .2
            assert handle.order_id == 99
            venue._maker_lookup.assert_awaited_once()
    asyncio.run(run())


def test_cancel_and_poll_share_one_lookup_and_keep_known_partial_fill():
    async def run():
        eng, *_ = setup_engine()
        venue = LighterVenue(eng.cfg.primary, None, .02)
        venue.orders_feed = None
        venue._maker_lookup = AsyncMock(return_value=dict(
            order_index=99, status='open', filled_base_amount='.2', filled_quote_amount='20'))
        handle = MakerOrder(4, 100, time.time())
        with patch('entropy_arb.venue_lighter.time.monotonic', return_value=100):
            states = await asyncio.gather(venue.maker_state(handle), venue.maker_state(handle))
            venue._maker_lookup.assert_awaited_once()
        assert all(s['filled_base'] == .2 for s in states)
        venue._maker_lookup.side_effect = RuntimeError('429')
        with patch('entropy_arb.venue_lighter.time.monotonic', return_value=102):
            state = await venue.maker_state(handle)
            assert state['filled_base'] == .2 and not state['terminal']
            await venue.maker_state(handle)
        assert venue._maker_lookup.await_count == 2
    asyncio.run(run())
