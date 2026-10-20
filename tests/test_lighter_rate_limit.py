"""Explicit HTTP rejection is zero-fill; an order lookup's 429 proves no such thing."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from test_lighter_taker_recheck import venue_fixture
from test_bps_residual_repair import eth_engine, fill, set_quotes
from test_bps_strategy import execute


@pytest.mark.parametrize('history_error', [False, True])
def test_history_refresh_preserves_pending_repair_reason(tmp_path, monkeypatch, history_error):
    async def run():
        eng = eth_engine(tmp_path)
        strategy = eng.bps_strategy
        eng.cfg.bps_levels = 'rolling'
        strategy.pending = True
        strategy.reason = '限流冷却中，等待自动修正已确认仓位差额；暂不开新订单'
        expected = strategy.reason

        def levels(*args, **kwargs):
            eng.stop.set()  # One refresh, without waiting for a real minute.
            if history_error:
                raise ValueError('insufficient history')
            coverage = dict(valid_minutes=240, coverage_pct=100, required_minutes=240,
                            missing_minutes=0, low_sample_minutes=0,
                            invalid_premium_minutes=0, source_filtered_minutes=0)
            return 1.2, -1.5, time.time(), coverage

        monkeypatch.setattr('entropy_arb.bps_strategy.rolling_levels', levels)
        await strategy.refresh_loop()
        assert strategy.pending and strategy.reason == expected
    asyncio.run(run())


class Http429(Exception):
    status = 429

    def __init__(self, headers=None):
        super().__init__('Too Many Requests: 40 requests per 60 second is allowed')
        self.headers = headers or {}


@pytest.mark.parametrize('header,seconds', [(None, 60), ({'Retry-After': '120'}, 120)])
def test_rejected_taker_is_not_polled_and_cooldown_resyncs_nonce(monkeypatch, header, seconds):
    async def run():
        venue = venue_fixture(monkeypatch)
        now = [time.time()]
        monkeypatch.setattr('entropy_arb.venue_lighter.time.time', lambda: now[0])
        venue.signer.create_order.side_effect = Http429(header)
        venue.maker_state = AsyncMock()
        refresh = AsyncMock()
        venue.signer.nonce_manager = SimpleNamespace(async_hard_refresh_nonce=refresh)
        info = await venue.send_taker(is_buy=True, qty=.0058, limit_px=2445.23)
        assert info['err'].startswith('RATE_LIMITED') and not info['unresolved']
        assert info['filled_base'] == 0
        assert venue.rate_limit_until == now[0] + seconds
        assert not venue.orders_feed._pending
        venue.maker_state.assert_not_awaited()
        refresh.assert_not_awaited()
        assert (await venue.send_taker(is_buy=True, qty=.0058, limit_px=2445.23))['filled_base'] == 0
        venue.signer.create_order.assert_awaited_once()  # No retry during cooldown.
        assert venue.rate_limit_until == now[0] + seconds  # Polling doesn't extend it.
        now[0] += seconds + .01
        async def accepted(**kwargs):
            refresh.assert_awaited_once()
            venue.orders_feed._handle_orders({'orders': {'0': [dict(
                client_order_index=kwargs['client_order_index'], status='filled',
                filled_base_amount='.0058', filled_quote_amount='14.182276')]}})
            return None, SimpleNamespace(code=200), None
        venue.signer.create_order.side_effect = accepted
        result = await venue.send_taker(is_buy=True, qty=.0058, limit_px=2445.23)
        assert result['filled_base'] == .0058 and not result['unresolved']
        assert not venue._nonce_refresh_required
        assert venue.signer.create_order.await_count == 2
    asyncio.run(run())


def test_api_23000_rejection_is_known_zero_fill(monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue.signer.create_order.return_value = (None, SimpleNamespace(code=23000, message='Too Many Requests'), None)
        venue.maker_state = AsyncMock()
        result = await venue.send_taker(is_buy=True, qty=.0058, limit_px=2445.23)
        assert result['err'].startswith('RATE_LIMITED') and not result['unresolved']
        venue.maker_state.assert_not_awaited()
    asyncio.run(run())


def test_maker_http_429_is_rejected_without_an_unknown_live_order(monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue.signer.create_order.side_effect = Http429()
        order = await venue.place_maker(is_buy=True, qty=.0058, limit_px=2445.23)
        assert order.rejected and order.err.startswith('RATE_LIMITED')
        assert venue.rate_limit_until > time.time()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['lookup_429', 'untyped_429_text'])
def test_ambiguous_submission_is_not_cleared_by_rate_limit_text_or_failed_lookup(monkeypatch, failure):
    async def run():
        venue = venue_fixture(monkeypatch)
        if failure == 'untyped_429_text':
            venue.signer.create_order.side_effect = RuntimeError('connection lost (429)')
        venue.maker_state = AsyncMock(side_effect=Http429())
        result = await venue.send_taker(is_buy=True, qty=.0058, limit_px=2445.23)
        assert result['unresolved']
        assert venue.rate_limit_until == 0
        venue.signer.create_order.assert_awaited_once()
        assert venue.maker_state.await_count >= 1
    asyncio.run(run())


def test_log_rejection_keeps_balanced_inventory_and_resumes_after_cooldown(tmp_path, monkeypatch):
    async def run():
        eng = eth_engine(tmp_path)
        strategy = eng.bps_strategy
        eng.cfg.max_order_notional = 14.2
        eng.entropy.position, eng.hedge.position = .015, -.015
        strategy.entries = 7
        strategy.locked_entry, strategy.locked_exit = strategy.entry, strategy.exit
        strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        original_primary, original_hedge = eng.entropy.send_taker.side_effect, eng.hedge.send_taker.side_effect
        async def rejected(**kwargs):
            eng.entropy.rate_limit_until = time.time() + 60
            return dict(status='rate-limited', filled_base=0, avg_px=None,
                        err='RATE_LIMITED: HTTP 429 rejection', unresolved=False)
        eng.entropy.send_taker.side_effect = rejected
        eng.hedge.send_taker.side_effect = None
        eng.hedge.send_taker.return_value = fill(0, 'canceled-too-much-slippage')
        candidate = eng._scan(time.time())
        assert candidate[2].qty == .0058
        await execute(eng, candidate)
        assert not eng.halted and not strategy.pending
        assert not eng.execution_stats.unresolved
        assert eng.consec_errors == 0
        assert eng.entropy.position == .015 and eng.hedge.position == -.015
        assert eng._scan(time.time()) is None and '限流冷却' in strategy.reason
        assert eng._venue_limited_until[eng.entropy.key] >= eng.entropy.rate_limit_until
        future = eng.entropy.rate_limit_until + .01
        monkeypatch.setattr('entropy_arb.engine.time.time', lambda: future)
        eng.entropy.send_taker.side_effect = original_primary
        eng.hedge.send_taker.side_effect = original_hedge
        set_quotes(eng, True)
        next_order = eng._scan(time.time())
        assert next_order
        await execute(eng, next_order)
        assert not eng.halted and not strategy.pending
        assert strategy.net_balanced()
    asyncio.run(run())


def test_other_leg_fill_is_repaired_even_when_rejected_venue_is_cooling(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        async def rejected(**kwargs):
            eng.entropy.rate_limit_until = time.time() + 60
            return dict(status='rate-limited', filled_base=0, avg_px=None,
                        err='RATE_LIMITED: HTTP 429 rejection', unresolved=False)
        eng.entropy.send_taker.side_effect = rejected
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.flat() and not eng.halted and not eng.bps_strategy.pending
        assert eng.hedge.send_taker.await_count == 2
        assert eng.hedge.send_taker.call_args.kwargs['reduce_only']
    asyncio.run(run())


def test_repair_429_waits_and_completes_without_repeating_an_unknown_order(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.cfg.rate_limit_pause_sec = .02
        original = eng.entropy.send_taker.side_effect
        calls = 0
        async def primary(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                eng.entropy.rate_limit_until = time.time() + .02
                return dict(status='rate-limited', filled_base=0, avg_px=None,
                            err='RATE_LIMITED: explicit rejection', unresolved=False)
            return await original(**kwargs)
        eng.entropy.send_taker.side_effect = primary
        eng.hedge.send_taker.side_effect = None
        eng.hedge.send_taker.return_value = fill(0, 'canceled-too-much-slippage')
        async def observe():
            await asyncio.sleep(.005)
            assert eng.bps_strategy.pending and not eng.halted
            assert '限流' in eng.bps_strategy.reason
            assert eng._scan(time.time()) is None
            await eng._maybe_hedge()  # Periodic reconciliation must not start a second owner.
            assert calls == 2
        observer = asyncio.create_task(observe())
        await execute(eng, eng._scan(time.time()))
        await observer
        assert calls == 3
        assert eng.bps_strategy.flat() and not eng.halted and not eng.bps_strategy.pending
    asyncio.run(run())


def test_two_repair_callers_cannot_use_the_same_net_after_cooldown(tmp_path):
    async def run():
        eng = eth_engine(tmp_path)
        eng.cfg.primary_order_type = 'maker'
        eng.cfg.max_order_notional = 100
        eng.entropy.position, eng.hedge.position = .0211, -.015
        eng.entropy.rate_limit_until = time.time() + .02
        eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        async def periodic():
            await asyncio.sleep(.005)
            await eng._maybe_hedge()
        await asyncio.gather(eng._maybe_hedge(), periodic())
        eng.entropy.send_taker.assert_awaited_once()
        assert eng.entropy.send_taker.call_args.kwargs['qty'] == .0061
        assert eng.entropy.position == pytest.approx(.015)
        assert eng.hedge.position == -.015
        assert not eng.halted
    asyncio.run(run())


@pytest.mark.parametrize('message', [
    '{"ratelimit": "didn\'t use volume quota"}',
    '{"ratelimit": "used volume quota"}',
    'Rate limit remaining: 59',
])
def test_accepted_quota_metadata_never_starts_shared_cooldown(monkeypatch, message, caplog):
    async def run():
        from entropy_arb.rest_reads import NORMAL
        from entropy_arb.venue_lighter import LighterVenue
        v = venue_fixture(monkeypatch)
        response = SimpleNamespace(code=200, message=message)
        assert not LighterVenue._is_rate_limit_rejection(response, None)
        assert not LighterVenue._is_rate_limit_rejection({'code':200, 'message':message}, None)
        async def accepted(**kwargs):
            v.orders_feed._handle_orders({'orders': {'0': [dict(
                client_order_index=kwargs['client_order_index'], status='filled',
                filled_base_amount='.0058', filled_quote_amount='14.182276')]}})
            return None, response, None
        v.signer.create_order.side_effect = accepted
        for _ in range(2):
            result = await v.send_taker(is_buy=True, qty=.0058, limit_px=2445.23)
            assert result['status'] == 'filled' and result['filled_base'] == .0058
        assert v.signer.create_order.await_count == 2
        assert v._requests().stats['server_limits'] == 0
        assert v._requests().available(NORMAL)
        assert not v._requests().snapshot()['cooldown_until']
        assert '[REQUEST COOLDOWN]' not in caplog.text
        assert not v._nonce_refresh_required
    asyncio.run(run())
