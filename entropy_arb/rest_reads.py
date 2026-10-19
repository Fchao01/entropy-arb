"""Coalesce Lighter REST reads and share query cooldowns across local workers.

Only public queries use this coordinator. Never cache order confirmations or
submissions. Account reads have a 200ms position TTL; equity-only reads may
reuse a snapshot for 15s. Recovery's separate position checks stay separate.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import tempfile
import time


class RestReadCooldown(RuntimeError):
    status = 429

    def __init__(self, until):
        self.until = until
        super().__init__(f'账户/行情 REST 查询限流，冷却至 {until:.3f}；订单状态不作推断')


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
    def __init__(self, api_url, directory=None):
        self.root = Path(directory or Path(tempfile.gettempdir()) / f'entropy-arb-rest-{os.getuid()}')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise RuntimeError('REST cache directory must be private and owned by current user')
        self.host = hashlib.sha256(api_url.rstrip('/').encode()).hexdigest()

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

    async def get(self, path, params, fetch, ttl):
        key = hashlib.sha256(json.dumps([self.host, path, params], sort_keys=True).encode()).hexdigest()
        await self._check_cooldown()
        async with self._locked(key):
            await self._check_cooldown()
            snapshot = self._read(key) if ttl else {}
            if 0 <= time.time() - snapshot.get('at', 0) <= ttl and 'data' in snapshot:
                return snapshot['data']
            try:
                data = await fetch()
            except Exception as error:
                if getattr(error, 'status', None) != 429:
                    raise
                now = time.time()
                headers = getattr(error, 'headers', None) or {}
                until = now + retry_delay(headers.get('Retry-After'), now)
                async with self._locked(self.host):
                    until = max(until, self._read(self.host).get('until', 0))
                    self._write(self.host, {'until': until})
                raise RestReadCooldown(until) from error
            if ttl:
                self._write(key, {'at': time.time(), 'data': data})
            return data
