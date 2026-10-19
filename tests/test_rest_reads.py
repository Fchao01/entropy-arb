"""Read-only coordination: fake HTTP, real process locks, no exchange access."""
import asyncio
from email.utils import formatdate
import multiprocessing
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from entropy_arb.rest_reads import RestReadCooldown, RestReads, retry_delay
from test_bps_strategy import engine_at
from test_lighter_taker_recheck import venue_fixture


class Http429(Exception):
    status = 429
    headers = {'Retry-After': '120'}


def test_coalesces_account_reads_and_expires_before_second_position_verification(tmp_path):
    async def run():
        a, b = RestReads('https://rh.example', tmp_path), RestReads('https://rh.example/', tmp_path)
        fetch = AsyncMock(return_value={'positions': [1]})
        params = {'by': 'index', 'value': '20'}
        results = await asyncio.gather(*[r.get('/account', params, fetch, .2) for r in (a, b, a, b)])
        assert results == [{'positions': [1]}] * 4
        fetch.assert_awaited_once()
        await asyncio.sleep(.25)
        await b.get('/account', params, fetch, .2)
        assert fetch.await_count == 2
        await b.get('/account', params, fetch, 15)  # Balance reuses this position snapshot.
        assert fetch.await_count == 2
        await b.get('/account', {**params, 'value': '21'}, fetch, .2)
        await RestReads('https://mainnet.example', tmp_path).get('/account', params, fetch, .2)
        assert fetch.await_count == 4  # Account and deployment never mix.
    asyncio.run(run())


def test_429_blocks_all_reads_on_host_without_reusing_old_cached_position(tmp_path, monkeypatch):
    async def run():
        clock = [time.time()]
        monkeypatch.setattr('entropy_arb.rest_reads.time.time', lambda: clock[0])
        first, other = RestReads('https://rh.example', tmp_path), RestReads('https://rh.example', tmp_path)
        ok = AsyncMock(return_value={'position': 7})
        await first.get('/account', {}, ok, .2)
        rejected = AsyncMock(side_effect=Http429())
        with pytest.raises(RestReadCooldown) as error:
            await first.get('/status', {}, rejected, 10)
        until = error.value.until
        assert until == clock[0] + 120
        for path in ['/account', '/market', '/status']:
            with pytest.raises(RestReadCooldown) as error:
                await other.get(path, {}, ok, .2)
            assert error.value.until == until  # Repeated calls do not extend the wait.
        ok.assert_awaited_once()
        clock[0] = until + .01
        await other.get('/account', {}, ok, .2)
        assert ok.await_count == 2
    asyncio.run(run())


def test_retry_after_accepts_seconds_http_date_and_invalid_values():
    now = time.time()
    assert retry_delay('2', now) == 60
    assert retry_delay('120', now) == 120
    assert retry_delay(formatdate(now + 180, usegmt=True), now) == pytest.approx(180, abs=1)
    assert all(retry_delay(value, now) == 60 for value in (None, 'bad', 'nan', '-1'))


def test_cancellation_releases_coalescing_lock(tmp_path):
    async def run():
        reads = RestReads('https://rh.example', tmp_path)
        started = asyncio.Event()
        async def blocked():
            started.set()
            await asyncio.Event().wait()
        task = asyncio.create_task(reads.get('/account', {}, blocked, .2))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await asyncio.wait_for(reads.get('/account', {}, AsyncMock(return_value=3), .2), .5)
        assert result == 3
    asyncio.run(run())


def _worker(directory, start, count, results, reject):
    async def run():
        reads = RestReads('https://rh.example', directory)
        await asyncio.to_thread(start.wait)
        async def fetch():
            with count.get_lock():
                count.value += 1
            await asyncio.sleep(.05)
            if reject:
                raise Http429()
            return {'position': 5}
        try:
            results.put(await reads.get('/account', {'value': '20'}, fetch, 2))
        except RestReadCooldown:
            results.put('cooldown')
    asyncio.run(run())


@pytest.mark.parametrize('reject', [False, True])
def test_independent_processes_share_inflight_query_and_cooldown(tmp_path, reject):
    ctx = multiprocessing.get_context('spawn')
    start, count, results = ctx.Event(), ctx.Value('i', 0), ctx.Queue()
    workers = [ctx.Process(target=_worker, args=(tmp_path, start, count, results, reject)) for _ in range(3)]
    try:
        for worker in workers:
            worker.start()
        start.set()
        got = [results.get(timeout=10) for _ in workers]
        for worker in workers:
            worker.join(timeout=10)
            assert worker.exitcode == 0
        assert got == (['cooldown'] * 3 if reject else [{'position': 5}] * 3)
        assert count.value == 1
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=2)
        results.close()


def test_adapter_shares_equity_but_never_uses_it_as_a_stale_position(tmp_path, monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue.conf.lighter_creds.account_index = 20
        venue._rest_reads = RestReads('https://rh.example', tmp_path)
        params = {'by': 'index', 'value': str(venue.conf.lighter_creds.account_index)}
        account = {'accounts': [{'total_asset_value': '100', 'available_balance': '80',
                                 'positions': [{'market_id': 0, 'position': '2', 'sign': '-1'}]}]}
        await venue._rest_reads.get('/api/v1/account', params, AsyncMock(return_value=account), .2)
        assert await venue.fetch_position() == -2
        assert await venue.fetch_equity() == (100, 80)
        await asyncio.sleep(.25)
        # Equity may reuse it, but positions must attempt HTTP (fixture has no session).
        assert await venue.fetch_equity() == (100, 80)
        with pytest.raises(AttributeError):
            await venue.fetch_position()
    asyncio.run(run())


class FakeSession:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        session = self
        class Response:
            request_info, history, headers = None, (), {'Retry-After': '120'}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            async def json(self):
                return session.data
        return Response()


def test_keepalive_warms_actual_signer_without_duplicate_ping_and_obeys_query_cooldown(tmp_path, monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue._rest_reads = RestReads('https://rh.example', tmp_path)
        venue.session = FakeSession({'order_book_details': [{'market_id': 0, 'mark_price': '100'}]})
        signer_session = FakeSession({'code': 200})
        venue.signer.api_client = SimpleNamespace(rest_client=SimpleNamespace(pool_manager=signer_session))
        await venue.warm_http()
        await venue.warm_http()
        assert venue.mark_price == 100
        assert len(venue.session.calls) == 1  # Cached market metadata; no extra public status.
        assert all(url.endswith('/orderBookDetails') for url in venue.session.calls)
        assert len(signer_session.calls) == 2  # Distinct connection really stays warm.
        with pytest.raises(RestReadCooldown):
            await venue._rest_reads.get('/blocked', {}, AsyncMock(side_effect=Http429()), 0)
        await venue.warm_http()
        assert len(venue.session.calls) == 1 and len(signer_session.calls) == 2
    asyncio.run(run())


def test_query_api_code_23000_activates_shared_cooldown(tmp_path, monkeypatch):
    async def run():
        venue = venue_fixture(monkeypatch)
        venue._rest_reads = RestReads('https://rh.example', tmp_path)
        venue.session = FakeSession({'code': 23000})
        now = time.time()
        with pytest.raises(RestReadCooldown) as result:
            await venue._get('/api/v1/account')
        assert result.value.until >= now + 120
        with pytest.raises(RestReadCooldown):
            await venue._get('/api/v1/status')
        assert len(venue.session.calls) == 1
        venue.signer.create_order.assert_not_awaited()
    asyncio.run(run())


def test_reconcile_cooldown_retains_ledger_without_false_outage_and_recovers(tmp_path, monkeypatch):
    async def run():
        eng = engine_at(tmp_path)
        eng.entropy.position, eng.hedge.position = 1, -1
        eng.entropy.fetch_position = AsyncMock(side_effect=RestReadCooldown(time.time() + 120))
        eng.hedge.fetch_position = AsyncMock(return_value=-1)
        await eng._reconcile_positions(hedge=False)
        assert eng.entropy.position == 1
        assert eng.entropy.key in eng._position_uncertain
        assert not eng._venue_down and not any(eng._venue_fetch_fails.values())
        for _ in range(4):
            await eng._reconcile_positions(hedge=False)
        eng.entropy.fetch_position.assert_awaited_once()
        assert eng._scan(time.time()) is None and not eng.halted
        eng._read_cooldowns[eng.entropy.key] = 0
        eng.entropy.fetch_position.side_effect = None
        eng.entropy.fetch_position.return_value = 1
        await eng._reconcile_positions(hedge=False)
        assert not eng._position_uncertain and not eng._read_cooldowns
        assert not eng.halted
        eng.entropy.send_taker.assert_not_awaited()
    asyncio.run(run())
