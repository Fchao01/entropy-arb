"""Deadline cancellation and authoritative final fills, without live orders."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.maker import MakerOrder, execute_primary_maker
from test_arcus import order as arcus_order, signed_venue
from test_maker import setup_engine, state


def test_deadline_cancels_while_order_query_is_waiting():
    async def run():
        e, buy, sell, plan = setup_engine()
        e.cfg.maker_timeout_sec, e.cfg.settle_timeout_sec = .02, .5
        canceled = asyncio.Event()
        query_finished = False
        async def query(order):
            nonlocal query_finished
            try:
                await canceled.wait()
                return state(0, True, 'canceled')
            finally:
                query_finished = True
        async def cancel(order):
            assert not query_finished  # cancel was not queued behind this query
            canceled.set()
        e.entropy.maker_state = AsyncMock(side_effect=query)
        e.entropy.cancel_maker = AsyncMock(side_effect=cancel)
        await execute_primary_maker(e, buy, sell, plan)
        e.entropy.cancel_maker.assert_awaited_once()
        e.hedge.send_taker.assert_not_awaited()
        assert not e.halted and e.active_maker is None
    asyncio.run(run())


@pytest.mark.parametrize('primary_buy', [True, False])
def test_deadline_cancels_during_slow_hedge_and_only_hedges_new_fills(primary_buy):
    async def run():
        e, buy, sell, plan = setup_engine(primary_buy)
        e.cfg.maker_timeout_sec, e.cfg.settle_timeout_sec = .02, .5
        canceled = asyncio.Event()
        hedge_finished = False
        async def cancel(order):
            assert not hedge_finished
            canceled.set()
        async def hedge(**args):
            nonlocal hedge_finished
            await canceled.wait()
            hedge_finished = True
            return dict(status='filled', filled_base=args['qty'], avg_px=102,
                        err=None, unresolved=False)
        e.entropy.cancel_maker = AsyncMock(side_effect=cancel)
        e.entropy.maker_state = AsyncMock(side_effect=[state(.3), state(.5, True, 'canceled')])
        e.hedge.send_taker = AsyncMock(side_effect=hedge)
        results = await execute_primary_maker(e, buy, sell, plan)
        pinfo, hinfo = (results[0][0], results[1][0]) if primary_buy else (results[1][0], results[0][0])
        assert pinfo['filled_base'] == hinfo['filled_base'] == pytest.approx(.5)
        assert [call.kwargs['qty'] for call in e.hedge.send_taker.call_args_list] == pytest.approx([.3, .2])
        assert all(call.kwargs['is_buy'] is not primary_buy for call in e.hedge.send_taker.call_args_list)
        e.entropy.cancel_maker.assert_awaited_once()
        assert not e.halted
    asyncio.run(run())


@pytest.mark.parametrize('filled', [0, .2])
def test_arcus_cancel_queries_http_when_live_ws_retains_open(filled):
    async def run():
        v = signed_venue()
        v._remember_order(arcus_order(status='OPEN', state='OPEN', filledSize='0',
                                      remainingSize='.02', avgFillPrice=None, timeInForce='ALO'))
        v._rpc = AsyncMock(return_value={'status': 202})
        handle = MakerOrder('client-1', 2500.01, time.time(), order_id='order-1',
                            cancel_requested_at=time.monotonic())
        v._get = AsyncMock(return_value=arcus_order(
            status='CANCELED', state='CANCELED', originalSize='1',
            filledSize=str(filled), remainingSize=str(1 - filled),
            avgFillPrice='2500.01' if filled else None, timeInForce='ALO'))
        await v.cancel_maker(handle)
        result = await v.maker_state(handle)
        assert result['terminal'] and result['filled_base'] == filled
        v._get.assert_awaited_once_with('/v1/order/order-1', address=v.address, accountIndex=0)
    asyncio.run(run())


def test_cancel_lookup_retries_at_one_second_and_accepts_ws_terminal_immediately():
    async def run():
        v = signed_venue()
        row = arcus_order(status='OPEN', state='OPEN', filledSize='0',
                          remainingSize='.02', avgFillPrice=None, timeInForce='ALO')
        v._remember_order(row)
        v._get = AsyncMock(return_value=row)
        tick = [100.0]
        handle = MakerOrder('client-1', 2500.01, time.time(), order_id='order-1',
                            cancel_requested_at=99)
        with patch('entropy_arb.venue_arcus.time', SimpleNamespace(monotonic=lambda: tick[0])):
            assert not (await v.maker_state(handle))['terminal']
            tick[0] = 100.1
            assert not (await v.maker_state(handle))['terminal']
            assert v._get.await_count == 1
            tick[0] = 101.1
            assert not (await v.maker_state(handle))['terminal']
            assert v._get.await_count == 2
            v._remember_order(arcus_order(status='CANCELED', state='CANCELED', filledSize='.01',
                                          remainingSize='.01', timeInForce='ALO'))
            assert (await v.maker_state(handle))['terminal']
            assert v._get.await_count == 2
    asyncio.run(run())


def test_real_arcus_lifecycle_hedges_cancel_race_from_http_without_ws_terminal():
    async def run():
        e, buy, sell, plan = setup_engine()
        e.cfg.maker_timeout_sec, e.cfg.settle_timeout_sec = .02, .5
        v = signed_venue()
        v.key = v.conf.key = 'entropy'
        # Reuse synthetic books; only order placement/state/cancel are real adapter code.
        v.book = e.entropy.book
        e.entropy = buy = e.venues['entropy'] = v
        client = []
        async def rpc(request):
            if request['type'] == 'placeOrder':
                cid = request['payload']['clientId']
                client.append(cid)
                v._remember_order(arcus_order(clientId=cid, status='OPEN', state='OPEN',
                                              originalSize=str(plan.qty), filledSize='.3',
                                              remainingSize=str(plan.qty - .3), avgFillPrice='100',
                                              timeInForce='ALO'))
                return {'status': 202, 'result': {'orderId': 'order-1'}}
            assert request['type'] == 'cancelOrder'
            return {'status': 202}  # No final order update over WS.
        async def get(path, **params):
            return arcus_order(clientId=client[0], status='CANCELED', state='CANCELED',
                               originalSize=str(plan.qty), filledSize='.5',
                               remainingSize=str(plan.qty - .5), avgFillPrice='100',
                               timeInForce='ALO')
        v._rpc, v._get = AsyncMock(side_effect=rpc), AsyncMock(side_effect=get)
        results = await execute_primary_maker(e, buy, sell, plan)
        assert results[0][0]['filled_base'] == results[1][0]['filled_base'] == pytest.approx(.5)
        assert [call.kwargs['qty'] for call in e.hedge.send_taker.call_args_list] == pytest.approx([.3, .2])
        v._get.assert_awaited_once()
        assert not e.halted and e.active_maker is None
    asyncio.run(run())
