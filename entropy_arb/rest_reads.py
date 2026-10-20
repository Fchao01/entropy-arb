"""Same-user, same-host Lighter request budgets, caches and cooldowns.

Conservative 60/min deployment-wide budget includes all wallets/API keys.
Background reads leave 24 slots, normal requests 12; risk requests can use
the remaining slots, never exceed the total. Orders are never queued/cached.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import math
import logging
import os
from pathlib import Path
import stat
import tempfile
import time

log = logging.getLogger('requests')
BACKGROUND, NORMAL, CRITICAL = 2, 1, 0
REQUESTS_PER_MIN = 60
RESERVED_REQUESTS = 12


def http_status(error):
    response = getattr(error, 'response', None)
    return getattr(error, 'status', None) or getattr(response, 'status_code', None)


def response_headers(error):
    return getattr(error, 'headers', None) or getattr(getattr(error, 'response', None), 'headers', None) or {}


class RestReadCooldown(RuntimeError):
    def __init__(self, until, source='server'):
        self.until = until
        self.source = source
        # Local admission rejection is not an HTTP response from the exchange.
        self.status = 429 if source == 'server' else None
        self.headers = {'Retry-After': str(max(0, until - time.time()))}
        label = '共享请求预算等待' if source == 'budget' else '交易所限流冷却'
        super().__init__(f'{label}至 {until:.3f}；订单状态不作推断')


def retry_delay(value, now):
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(value).timestamp() - now
        except (TypeError, ValueError, OverflowError):
            delay = 60.0
    return max(60.0, delay) if math.isfinite(delay) else 60.0


class RestReads:
    def __init__(self, api_url, directory=None, *, limit=REQUESTS_PER_MIN, reserve=RESERVED_REQUESTS):
        self.root = Path(directory or os.environ.get('ENTROPY_REST_DIR')
                         or Path(tempfile.gettempdir()) / f'entropy-arb-rest-{os.getuid()}')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RuntimeError('REST cache directory must be private and owned by current user')
        self.host = hashlib.sha256(api_url.rstrip('/').encode()).hexdigest()
        self.label = api_url.split('://')[-1].rstrip('/')
        self.limit, self.reserve = limit, reserve
        self.stats = dict(sent=0, cache_hits=0, budget_waits=0, server_limits=0)

    def _key(self, path, params):
        # Account monitor passes integers, strategy passes strings: same HTTP query.
        params = {k: str(v) for k, v in (params or {}).items()}
        return hashlib.sha256(json.dumps([self.host, path, params], sort_keys=True).encode()).hexdigest()

    def available(self, priority=NORMAL):
        """Advisory preflight; acquire() still enforces admission atomically."""
        now = time.time()
        if self._read(self.host).get('until', 0) > now:
            return False
        events = self._read(self.host + '-budget-rest').get('events', [])
        return sum(now - 60 < t <= now for t in events) < max(1, self.limit - priority * self.reserve)

    def snapshot(self):
        now = time.time()
        return dict(self.stats, shared_used=sum(now - 60 < t <= now for t in self._read(
            self.host + '-budget-rest').get('events', [])), shared_limit=self.limit,
                    background_limit=max(1, self.limit - 2 * self.reserve),
                    normal_limit=max(1, self.limit - self.reserve),
                    cooldown_until=self._read(self.host).get('until', 0))

    async def acquire(self, priority=NORMAL, *, cost=1, bucket='rest'):
        """Atomic admission only: do not hold a stale trading plan in a queue."""
        await self._check_cooldown()
        name = self.host + '-budget-' + bucket
        async with self._locked(name):
            now = time.time()
            state = self._read(name)
            events = [t for t in state.get('events', []) if now - 60 < t <= now]
            cap = (120 if bucket == 'ws' else
                   max(1, self.limit - priority * self.reserve))
            if len(events) + cost > cap:
                until = events[max(0, len(events) + cost - cap - 1)] + 60.01 if events else now + 1
                self.stats['budget_waits'] += 1
                if now - state.get('logged_at', 0) >= 10:
                    state.update(events=events, logged_at=now)
                    self._write(name, state)
                    log.warning('[REQUEST BUDGET] host=%s bucket=%s used=%d cap=%d priority=%d wait=%.1fs',
                                self.label, bucket, len(events), cap, priority, until - now)
                raise RestReadCooldown(until, 'budget')
            events.extend([now] * cost)
            self._write(name, {**state, 'events': events})

    async def cooldown(self, headers=None):
        now = time.time()
        headers = headers or {}
        async with self._locked(self.host):
            value = next((v for k, v in headers.items() if str(k).lower() == 'retry-after'), None)
            until = max(now + retry_delay(value, now),
                        self._read(self.host).get('until', 0))
            self._write(self.host, {'until': until})
        self.stats['server_limits'] += 1
        log.warning('[REQUEST COOLDOWN] host=%s wait=%.1fs; shared across strategies and monitors',
                    self.label, until - now)
        return until

    async def invalidate(self, path, params):
        key = self._key(path, params)
        # Do not wait behind an account HTTP request after submitting an order.
        # A late pre-submission response must not resurrect a reusable cache.
        self._write(key + '-invalid', {'at': time.time()})

    @asynccontextmanager
    async def _locked(self, name):
        fd = os.open(self.root / (name + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            deadline = time.monotonic() + 12
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('REST read coalescing timed out')
                    await asyncio.sleep(.02)
            yield
        finally:
            os.close(fd)  # Also releases the lock on cancellation/errors.

    def _read(self, name):
        try:
            return json.loads((self.root / (name + '.json')).read_text())
        except (FileNotFoundError, ValueError):
            return {}

    def _write(self, name, value):
        fd, temporary = tempfile.mkstemp(dir=self.root, prefix=name + '-')
        try:
            with os.fdopen(fd, 'w') as output:
                json.dump(value, output)
            os.replace(temporary, self.root / (name + '.json'))
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    async def _check_cooldown(self):
        async with self._locked(self.host):
            until = self._read(self.host).get('until', 0)
        if time.time() < until:
            raise RestReadCooldown(until)

    async def get(self, path, params, fetch, ttl, *, priority=NORMAL, not_before=0):
        key = self._key(path, params)
        await self._check_cooldown()
        async with self._locked(key):
            await self._check_cooldown()
            snapshot = self._read(key) if ttl else {}
            minimum = max(not_before, self._read(key + '-invalid').get('at', 0))
            if (0 <= time.time() - snapshot.get('at', 0) <= ttl and 'data' in snapshot
                    and snapshot.get('at', 0) >= minimum):
                self.stats['cache_hits'] += 1
                return snapshot['data']
            await self.acquire(priority)
            started = time.time()
            try:
                self.stats['sent'] += 1
                data = await fetch()
            except Exception as error:
                if isinstance(error, RestReadCooldown):
                    raise
                if http_status(error) not in (429, 405):
                    raise
                headers = response_headers(error)
                until = await self.cooldown(headers)
                raise RestReadCooldown(until) from None
            if ttl:
                self._write(key, {'at': started, 'data': data})
            return data
