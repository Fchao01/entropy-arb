"""Shared admission, authenticated reads and recovery; no exchange requests."""
import asyncio
import multiprocessing
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from entropy_arb.rest_reads import RestReads, RestReadCooldown, BACKGROUND, NORMAL, CRITICAL
from entropy_arb.account_monitor import AccountMonitor
from test_lighter_taker_recheck import venue_fixture
from test_bps_strategy import engine_at


def account_venue(monkeypatch):
    venue = venue_fixture(monkeypatch)
    venue.conf.lighter_creds.account_index = 20
    venue.conf.lighter_creds.api_key_index = 3
    venue.conf.lighter_creds.api_private_key = 'synthetic-key-only'
    return venue


class Session:
    def __init__(self, data=None, status=200):
        self.calls = []
        self.status = status
        self.data = data or {'code': 200, 'accounts': [{'positions': [
            {'market_id': 0, 'position': '2', 'sign': '-1'},
            {'market_id': 1, 'position': '3', 'sign': '1'}],
            'total_asset_value': '100', 'available_balance': '80'}]}

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outer = self
        class Response:
            status = outer.status
            headers = {'Retry-After': '120'}
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def json(self):
                return outer.data
            def raise_for_status(self):
                if self.status != 200:
                    import aiohttp
                    raise aiohttp.ClientResponseError(
                        SimpleNamespace(real_url=url, headers=kwargs.get('headers', {})), (),
                        status=self.status, headers=self.headers)
        return Response()


def test_budget_shared_across_keys_with_reserved_risk_slots_and_expiry(tmp_path, monkeypatch):
    async def run():
        clock = [time.time()]
        monkeypatch.setattr('entropy_arb.rest_reads.time.time', lambda: clock[0])
        a, b = RestReads('https://rh.example', tmp_path), RestReads('https://rh.example', tmp_path)
        for _ in range(36):
            await a.acquire(BACKGROUND)
        with pytest.raises(RestReadCooldown) as error:
            await b.acquire(BACKGROUND)
        assert error.value.source == 'budget'
        for _ in range(12):
            await b.acquire(NORMAL)
        assert not a.available() and a.available(CRITICAL)
        for _ in range(12):
            await a.acquire(CRITICAL)
        with pytest.raises(RestReadCooldown):
            await b.acquire(CRITICAL)
        assert a.snapshot()['shared_used'] == 60
        await RestReads('https://mainnet.example', tmp_path).acquire(CRITICAL)
        clock[0] += 61
        await b.acquire(BACKGROUND)
        assert b.snapshot()['shared_used'] == 1
    asyncio.run(run())


def _budget_worker(root, start, output):
    async def run():
        reads = RestReads('https://rh.example', root, limit=2, reserve=0)
        await asyncio.to_thread(start.wait)
        try:
            await reads.acquire(CRITICAL)
            output.put('admitted')
        except RestReadCooldown:
            output.put('waiting')
    asyncio.run(run())


def test_independent_processes_cannot_overbook_one_deployment(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    start, output = ctx.Event(), ctx.Queue()
    workers = [ctx.Process(target=_budget_worker, args=(tmp_path, start, output)) for _ in range(4)]
    try:
        for w in workers:
            w.start()
        start.set()
        results = [output.get(timeout=10) for _ in workers]
        assert results.count('admitted') == results.count('waiting') == 2
        for w in workers:
            w.join(timeout=10)
            assert w.exitcode == 0
    finally:
        for w in workers:
            if w.is_alive():
                w.terminate()
            w.join(timeout=2)
        output.close()


def test_account_snapshot_shared_by_different_markets_keys_and_monitor(tmp_path, monkeypatch):
    async def run():
        a = account_venue(monkeypatch)
        b = account_venue(monkeypatch)
        b.market_id = 1
        b.conf.lighter_creds.api_key_index = 19
        b.conf.lighter_creds.account_index = a.conf.lighter_creds.account_index
        a.session, b.session = Session(), Session()
        assert await a.fetch_position_background() == -2
        assert await b.fetch_position_background() == 3
        assert await b.fetch_equity() == (100, 80)
        assert len(a.session.calls) == 1 and not b.session.calls
        assert a.session.calls[0][1]['headers'] == {'Authorization': 'test-only-auth'}
        monitor = AccountMonitor(tmp_path / 'accounts.sqlite3')
        monitor.session = Session()
        # Use the same deployment; cache matches account integer/string params.
        monitor.requests['lighter-rh'] = a._requests()
        result = await monitor.request('lighter-rh', 'account', by='index',
                                       value=a.conf.lighter_creds.account_index)
        assert len(result['accounts'][0]['positions']) == 2
        assert not monitor.session.calls
    asyncio.run(run())


def test_verification_bypasses_old_background_cache_and_post_trade_snapshot(tmp_path, monkeypatch):
    async def run():
        venue = account_venue(monkeypatch)
        venue.session = Session()
        clock = [time.time()]
        monkeypatch.setattr('entropy_arb.rest_reads.time.time', lambda: clock[0])
        assert await venue.fetch_position_background() == -2
        clock[0] += 1
        assert await venue.fetch_position_background() == -2
        assert len(venue.session.calls) == 1
        assert await venue.fetch_position() == -2  # Critical verification is fresh.
        assert len(venue.session.calls) == 2
        clock[0] += .5
        await venue.fetch_position()
        assert len(venue.session.calls) == 3
        clock[0] += 1
        venue.last_traded_ts = clock[0]
        await venue.fetch_position_background()
        assert len(venue.session.calls) == 4
    asyncio.run(run())


def test_inflight_response_cannot_resurrect_invalidated_account_snapshot(tmp_path):
    async def run():
        reads = RestReads('https://rh.example', tmp_path)
        entered, release = asyncio.Event(), asyncio.Event()
        async def old():
            entered.set()
            await release.wait()
            return {'position': 1}
        task = asyncio.create_task(reads.get('/account', {}, old, 15))
        await entered.wait()
        await asyncio.wait_for(reads.invalidate('/account', {}), .1)
        release.set()
        assert await task == {'position': 1}
        fresh = AsyncMock(return_value={'position': 2})
        assert await reads.get('/account', {}, fresh, 15) == {'position': 2}
        fresh.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize('failure', ['token_error', 'token_exception', 'http_401'])
def test_authentication_failure_never_falls_back_or_exposes_token(monkeypatch, failure, caplog):
    async def run():
        venue = account_venue(monkeypatch)
        token = 'sensitive-token-must-not-appear'
        if failure == 'token_error':
            venue.signer.create_auth_token_with_expiry = lambda: (None, token)
        elif failure == 'token_exception':
            def broken():
                raise ValueError(token)
            venue.signer.create_auth_token_with_expiry = broken
        else:
            venue.signer.create_auth_token_with_expiry = lambda: (token, None)
        venue.session = Session(status=401 if failure == 'http_401' else 200)
        with pytest.raises(RuntimeError) as result:
            await venue.fetch_position()
        assert token not in str(result.value) + repr(result.value) + caplog.text
        assert len(venue.session.calls) == (1 if failure == 'http_401' else 0)
        assert venue._read_auth_until == 0
        venue.signer.create_order.assert_not_awaited()
    asyncio.run(run())


def test_cached_auth_is_renewed_without_network_token_calls(monkeypatch):
    async def run():
        venue = account_venue(monkeypatch)
        calls = []
        def token():
            calls.append(1)
            return ('auth-' + str(len(calls)), None)
        venue.signer.create_auth_token_with_expiry = token
        venue.session = Session()
        await venue._get('/first', cache_ttl=0)
        await venue._get('/second', cache_ttl=0)
        assert len(calls) == 1
        venue._read_auth_until = 0
        await venue._get('/third', cache_ttl=0)
        assert len(calls) == 2
        assert venue.session.calls[-1][1]['headers']['Authorization'] == 'auth-2'
    asyncio.run(run())


def test_monitor_429_cools_strategy_reads_and_sdk_submissions(monkeypatch, tmp_path):
    async def run():
        venue = account_venue(monkeypatch)
        venue.session = Session()
        monitor = AccountMonitor(tmp_path / 'accounts.sqlite3')
        monitor.session = Session(status=429)
        monitor.requests['lighter-rh'] = venue._requests()
        with pytest.raises(RestReadCooldown):
            await monitor.request('lighter-rh', 'orderBooks')
        with pytest.raises(RestReadCooldown):
            await venue.fetch_position()
        assert not venue.session.calls
        result = await venue.send_taker(is_buy=True, qty=.1, limit_px=100)
        assert result['filled_base'] == 0 and not result['unresolved']
        venue.signer.create_order.assert_not_awaited()
        assert venue.orders_feed._pending == {}
    asyncio.run(run())


def test_order_admission_failure_is_known_unsent_and_reserved_reduction_can_run(monkeypatch):
    async def run():
        venue = account_venue(monkeypatch)
        requests = venue._requests()
        for _ in range(48):
            await requests.acquire(NORMAL)
        result = await venue.send_taker(is_buy=True, qty=.1, limit_px=100)
        assert result['status'] == 'local-budget-wait' and not result['unresolved']
        assert not venue._nonce_refresh_required and not venue.orders_feed._pending
        venue.signer.create_order.assert_not_awaited()
        # A different strategy/key on the same host still has reserved risk slots.
        other = account_venue(monkeypatch)
        other.maker_state = AsyncMock(return_value=dict(terminal=True, status='filled', filled_base=.1, avg_px=100))
        result = await other.send_taker(is_buy=False, qty=.1, limit_px=100, reduce_only=True)
        assert result['filled_base'] == .1 and not result['unresolved']
        other.signer.create_order.assert_awaited_once()
        assert requests.snapshot()['shared_used'] == 49
    asyncio.run(run())


@pytest.mark.parametrize('mode', ['recover', 'exhaust', 'stop', 'non_rate_error'])
def test_startup_retries_only_query_limits_and_never_sends_orders(tmp_path, mode):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        count = []
        async def check(*args, **kwargs):
            count.append(1)
            if mode == 'recover' and len(count) == 2:
                return
            if mode == 'non_rate_error':
                raise RuntimeError('account mismatch')
            if mode == 'stop':
                eng.stop.set()
            error = RuntimeError('query limit')
            error.__cause__ = RestReadCooldown(time.time())
            raise error
        eng._reconcile_positions = check
        if mode == 'recover':
            await eng._reconcile_startup()
            assert len(count) == 2
        else:
            with pytest.raises(RuntimeError):
                await eng._reconcile_startup()
            assert len(count) == (3 if mode == 'exhaust' else 1)
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_429_diagnostics_classify_ip_without_logging_auth_or_server_echo(monkeypatch, caplog):
    async def run():
        venue = account_venue(monkeypatch)
        token = 'private-query-token'
        venue.signer.create_auth_token_with_expiry = lambda: (token, None)
        venue.session = Session({'code': 23000, 'message': 'Too many requests for this IP ' + token}, status=429)
        with pytest.raises(RestReadCooldown) as result:
            await venue.fetch_position()
        assert result.value.until > time.time() + 119
        assert 'scope_hint=ip' in caplog.text and 'authenticated=True' in caplog.text
        assert token not in caplog.text + repr(result.value)
    asyncio.run(run())


def test_sdk_lookup_429_is_shared_with_public_monitors(monkeypatch, tmp_path):
    async def run():
        venue = account_venue(monkeypatch)
        class Rejected(Exception):
            status = 429
            headers = {}
        lookup = AsyncMock(side_effect=Rejected())
        with pytest.raises(RestReadCooldown):
            await venue._sdk_request('/api/v1/accountActiveOrders', lookup)
        lookup.assert_awaited_once()
        monitor = AccountMonitor(tmp_path / 'accounts.sqlite3')
        monitor.session = Session()
        monitor.requests['lighter-rh'] = venue._requests()
        with pytest.raises(RestReadCooldown):
            await monitor.request('lighter-rh', 'account', by='index', value=20)
        assert not monitor.session.calls
    asyncio.run(run())


def test_websocket_handshake_429_sets_shared_cooldown(monkeypatch):
    async def run():
        from entropy_arb.feeds import LighterBookFeed
        from entropy_arb.book import OrderBook
        venue = account_venue(monkeypatch)
        stop = asyncio.Event()
        class WSRejected(Exception):
            response = SimpleNamespace(status_code=429, headers={'retry-after': '120'})
        def connect(*args, **kwargs):
            raise WSRejected()
        waits = []
        async def sleep(seconds):
            waits.append(seconds)
            stop.set()
        monkeypatch.setattr('entropy_arb.feeds.ws_connect', connect)
        monkeypatch.setattr('entropy_arb.feeds.asyncio.sleep', sleep)
        feed = LighterBookFeed('RH', '', 1, OrderBook(), lambda: None, requests=venue._requests())
        await feed.run(stop)
        assert waits and waits[0] >= 119
        with pytest.raises(RestReadCooldown):
            await venue._requests().acquire(CRITICAL)
        assert venue._requests().stats['server_limits'] == 1
    asyncio.run(run())


def test_lazy_sdk_nonce_fetch_is_counted_inside_order_submission(monkeypatch):
    async def run():
        import sys
        venue = account_venue(monkeypatch)
        fetched = AsyncMock(return_value=7)
        manager = SimpleNamespace(_fetch_nonce=fetched)
        signer = SimpleNamespace(check_client=lambda: None, nonce_manager=manager)
        monkeypatch.setitem(sys.modules, 'lighter', SimpleNamespace(SignerClient=lambda **kwargs: signer))
        venue.init_signer()
        assert manager._entropy_nonce_guarded
        async def order():
            await manager._fetch_nonce(3)
            return (None, SimpleNamespace(code=200), None)
        await venue._sdk_request('/api/v1/sendTx', order, submission=True, priority=NORMAL)
        assert venue._requests().snapshot()['shared_used'] == 2
        fetched.assert_awaited_once_with(3)
        assert venue._requests().stats['sent'] == 2
    asyncio.run(run())


@pytest.mark.parametrize('mode', ['bps', 'threshold', 'spread'])
def test_account_query_failure_blocks_new_orders_in_every_strategy(tmp_path, mode):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.cfg.strategy_mode = mode
        if mode != 'bps':
            eng.bps_strategy = None
        eng.entropy.fetch_position = AsyncMock(side_effect=RuntimeError('query authentication failed'))
        await eng._reconcile_venue(eng.entropy, strict=False)
        assert eng.entropy.key in eng._position_uncertain
        assert eng._scan(time.time()) is None
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())
