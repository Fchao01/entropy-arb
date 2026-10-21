"""zkLighter venue adapter (Lighter mainnet / Lighter Robinhood chain).

Market data and account state come from Lighter's public REST + websocket
APIs via plain aiohttp/websockets, so --record-only data collection works
without the SDK. Trading lazily imports the official `lighter` SDK
(https://github.com/elliottech/lighter-python) for transaction signing only.

Market orders carry mandatory avg-execution-price protection and settle
asynchronously on the authenticated account_orders websocket; send_taker()
hides that behind the same result shape the HL venue returns:
{status, filled_base, avg_px, err, unresolved}.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import time
from collections import OrderedDict
from typing import Optional

import aiohttp

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import VenueConf
from .feeds import LighterBookFeed
from .maker import MakerOrder
from .rest_reads import RestReads, RestReadCooldown, BACKGROUND, NORMAL, CRITICAL, http_status, response_headers

log = logging.getLogger("lighter")

AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0
TAKER_RECHECK_SEC = 10.0
RATE_LIMIT_COOLDOWN_SEC = 60.0


def terminal_status(status: str) -> bool:
    return status in {"filled", "expired", "rejected"} or status.startswith("canceled")


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer, requests=None) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
        self.requests = requests
        self.ready = asyncio.Event()
        self._pending: dict[int, asyncio.Future] = {}
        self._terminal: OrderedDict[int, dict] = OrderedDict()
        self.latest: OrderedDict[int, dict] = OrderedDict()

    def watch(self, coi: int) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        if coi in self._terminal:
            fut.set_result(self._terminal[coi])
            return fut
        self._pending[coi] = fut
        return fut

    def unwatch(self, coi: int) -> None:
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.cancel()

    def _resolve(self, coi: int, info: dict) -> None:
        self._terminal[coi] = info
        while len(self._terminal) > 512:
            self._terminal.popitem(last=False)
        fut = self._pending.pop(coi, None)
        if fut is not None and not fut.done():
            fut.set_result(info)

    def _handle_orders(self, msg: dict) -> None:
        for lst in (msg.get("orders") or {}).values():
            for o in lst or []:
                status = str(o.get("status", ""))
                try:
                    coi = int(o.get("client_order_index"))
                except (TypeError, ValueError):
                    continue
                old = self.latest.get(coi)
                if old and (terminal_status(str(old.get("status", "")))
                            or float(o.get("filled_base_amount") or 0) < float(old.get("filled_base_amount") or 0)):
                    continue
                self.latest[coi] = {**(old or {}), **o}
                self.latest.move_to_end(coi)
                while len(self.latest) > 1000:
                    self.latest.popitem(last=False)
                if not terminal_status(status):
                    continue
                fb = float(o.get("filled_base_amount") or 0.0)
                fq = float(o.get("filled_quote_amount") or 0.0)
                self._resolve(coi, {"status": status, "filled_base": fb,
                                    "filled_quote": fq,
                                    "avg_px": (fq / fb) if fb > 0 else None})

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                if self.requests:
                    await self.requests.acquire(CRITICAL, bucket='ws')
                auth, err = self.signer.create_auth_token_with_expiry()
                if err is not None:
                    raise RuntimeError(f"auth token: {err}")
                connected_at = time.time()
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        t = msg.get("type")
                        if t in ("subscribed/account_orders", "update/account_orders"):
                            if t.startswith("subscribed"):
                                log.info("[%s] account orders stream ready", self.name)
                            self.ready.set()
                            self._handle_orders(msg)
                        elif t == "connected":
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": f"account_orders/{self.market_id}/"
                                           f"{self.account_index}",
                                "auth": auth}))
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set():
                            break
                        if (time.time() - connected_at > AUTH_REFRESH_SEC
                                and not self._pending):
                            log.info("[%s] refreshing account ws auth", self.name)
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.requests and http_status(e) in (429, 405) and not isinstance(e, RestReadCooldown):
                    until = await self.requests.cooldown(response_headers(e))
                    backoff = max(backoff, until - time.time())
                elif isinstance(e, RestReadCooldown):
                    backoff = max(backoff, e.until - time.time())
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, type(e).__name__, backoff)
                self.ready.clear()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            self.ready.clear()


class LighterVenue:
    kind = "lighter"
    # Official core-concepts: market minimums apply to maker orders. IOC
    # reductions still require a positive integer base_amount, not min_base.
    # https://apidocs.lighter.xyz/docs/core-concepts#integer-prices-and-sizes
    reduce_only_min_quote = 0.0

    @property
    def reduce_only_min_base(self):
        return 10.0 ** -self.size_decimals

    # IOC orders do not rest on the book: maker minimums are not an opening
    # minimum for a confirmed-fill hedge top-up either. Keep normal strategy
    # opening minimums unchanged; only residual repairs use these rules.
    taker_min_quote = 0.0

    @property
    def taker_min_base(self):
        return 10.0 ** -self.size_decimals

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float) -> None:
        assert conf.lighter_profile is not None
        self.conf = conf
        self.key = conf.key
        self.name = conf.label
        self.session = session
        self.settle_timeout = settle_timeout_sec
        self.profile = conf.lighter_profile
        self.book = OrderBook()
        self.mark_price = None
        self.position = 0.0
        self.cash = 0.0
        self.volume_usd = 0.0     # cumulative filled notional this session
        self.equity = None
        self.free = None
        self.start_equity = None
        self.fee_bps = conf.fee_bps
        self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.price_decimals = 2
        self.size_decimals = 4
        self.min_base = 0.0
        self.min_quote = 10.0
        self.signer = None
        self.orders_feed: Optional[AccountOrdersFeed] = None
        self._coi = int(time.time() * 1000)
        self.rate_limit_until = 0.0
        self._nonce_refresh_required = False
        self._rest_reads = None
        self._read_auth = None
        self._read_auth_until = 0
        self._budget_view = {}

    # ------------------------------------------------------------------ REST

    def _requests(self):
        if self._rest_reads is None:
            self._rest_reads = RestReads(self.profile.api_url)
        return self._rest_reads

    def shared_request_ready(self, *, critical=False):
        # File reads stay off the high-frequency book-update path. The actual
        # submission still acquires an atomic slot, so this is advisory only.
        now = time.monotonic()
        until, ready = self._budget_view.get(critical, (0, False))
        if now >= until:
            ready = self._requests().available(CRITICAL if critical else NORMAL)
            self._budget_view[critical] = (now + .5, ready)
        return ready

    def _query_headers(self):
        if self.signer is None:
            return {}  # Credential-less collection, subject to public IP budget.
        if time.time() >= self._read_auth_until:
            try:
                token, err = self.signer.create_auth_token_with_expiry()
            except Exception:
                raise RuntimeError(f'[{self.name}] 查询认证令牌生成失败；未回退为公共查询') from None
            if err or not isinstance(token, str) or not token:
                raise RuntimeError(f'[{self.name}] 查询认证不可用；未回退为公共查询')
            self._read_auth, self._read_auth_until = token, time.time() + AUTH_REFRESH_SEC
        return {'Authorization': self._read_auth}

    async def _get(self, path: str, params: Optional[dict] = None, *, cache_ttl=None,
                   query_session=None, priority=BACKGROUND, not_before=0):
        reads = self._requests()
        headers = self._query_headers()  # Authentication errors must not use a cached success.
        ttl = {'/api/v1/account': .2, '/api/v1/status': 30,
               '/api/v1/orderBooks': 300, '/api/v1/orderBookDetails': 30}.get(path, 0)
        async def fetch():
            async with (query_session or self.session).get(
                    self.profile.api_url + path, params=params, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
                if getattr(r, 'status', 200) in (429, 405):
                    # Log response classification, never RequestInfo/Auth headers.
                    try:
                        body = await r.json()
                    except Exception:
                        body = {}
                    text = str(body.get('message', '')).lower() if isinstance(body, dict) else ''
                    hint = ('ip' if ' ip' in ' ' + text else
                            'account' if any(x in text for x in ('account', 'address', 'wallet')) else 'unknown')
                    code = body.get('code') if isinstance(body, dict) else None
                    log.warning('[REQUEST 429] host=%s path=%s authenticated=%s code=%s scope_hint=%s',
                                reads.label, path, bool(headers), code if isinstance(code, (int, float)) else None, hint)
                    raise RestReadCooldown(await reads.cooldown(r.headers))
                r.raise_for_status()
                data = await r.json()
                if isinstance(data, dict) and data.get('code') == 23000:
                    log.warning('[REQUEST 429] host=%s path=%s authenticated=%s code=23000 scope_hint=unknown response=API',
                                reads.label, path, bool(headers))
                    raise aiohttp.ClientResponseError(r.request_info, r.history, status=429,
                                                     message='REST query rate limited', headers=r.headers)
                return data
        try:
            return await reads.get(path, params, fetch, ttl if cache_ttl is None else cache_ttl,
                                   priority=priority, not_before=not_before)
        except RestReadCooldown:
            raise
        except aiohttp.ClientError as error:
            # ClientResponseError.__repr__ contains RequestInfo.headers/auth.
            status = http_status(error)
            if status in (401, 403):
                self._read_auth_until = 0
            raise RuntimeError(f'[{self.name}] REST {path} failed HTTP {status or "transport"}; '
                               'request credentials redacted') from None

    async def _sdk_request(self, path, fetch, *, priority=CRITICAL, params=None, submission=False):
        """SDK endpoints share admission/cooldown; accepted orders are never retried here."""
        reads = self._requests()
        async def send():
            try:
                result = await fetch()
            except RestReadCooldown:
                raise
            except Exception as error:
                if http_status(error) in (429, 405):
                    log.warning('[REQUEST 429] host=%s path=%s response=SDK-HTTP status=%s',
                                reads.label, path, http_status(error))
                    raise RestReadCooldown(await reads.cooldown(response_headers(error))) from None
                # Transport/SDK exceptions can embed authenticated request data.
                raise RuntimeError(f'SDK {path} {type(error).__name__}; request credentials redacted') from None
            response = result[1] if isinstance(result, tuple) and len(result) == 3 else result
            error = result[2] if isinstance(result, tuple) and len(result) == 3 else None
            if self._is_rate_limit_rejection(response, error):
                code = response.get('code') if isinstance(response, dict) else getattr(response, 'code', None)
                log.warning('[REQUEST 429] host=%s path=%s response=SDK-API code=%s',
                            reads.label, path, code if isinstance(code, (int, float)) else None)
                await reads.cooldown()
            return result
        if not submission:
            return await reads.get(path, params, send, 0, priority=priority)
        await reads.acquire(priority)  # Fail before signing; never queue a stale order.
        try:
            reads.stats['sent'] += 1
            return await send()
        finally:
            await reads.invalidate('/api/v1/account', self._account_params())

    async def prepare_signer(self):
        # SDK construction initializes nonce and check_client verifies the key.
        # These native calls cannot use the aiohttp cache: reserve both slots.
        await self._requests().acquire(CRITICAL, cost=2)
        self.init_signer()
        log.info('[REQUEST POLICY] host=%s shared_total=60/min reserve=12/24; '
                 'authenticated queries; account background cache=30s; equity cache=60s; fresh verification=0.2s; '
                 'bulk market metadata + shared keepalive cache=30s',
                 self._requests().label)

    # ------------------------------------------------------------- lifecycle

    async def load_market(self) -> None:
        data = await self._get("/api/v1/orderBooks")
        for ob in data.get("order_books") or []:
            if ob.get("symbol") != self.conf.symbol:
                continue
            if ob.get("status") != "active":
                raise RuntimeError(f"[{self.name}] market status={ob.get('status')}")
            self.market_id = int(ob["market_id"])
            self.price_decimals = int(ob["supported_price_decimals"])
            self.size_decimals = int(ob["supported_size_decimals"])
            self.min_base = float(ob["min_base_amount"])
            self.min_quote = float(ob["min_quote_amount"])
            log.info("[%s] %s market_id=%d px_dec=%d sz_dec=%d min_base=%s "
                     "min_quote=%s taker_fee=%s", self.name, ob["symbol"],
                     self.market_id, self.price_decimals, self.size_decimals,
                     ob["min_base_amount"], ob["min_quote_amount"],
                     ob.get("taker_fee"))
            return
        raise RuntimeError(f"[{self.name}] {self.conf.symbol} not found on "
                           f"{self.profile.name}")

    def init_signer(self) -> None:
        c = self.conf.lighter_creds
        assert c is not None and c.complete, f"[{self.name}] missing credentials"
        try:
            from lighter import SignerClient
        except ImportError as e:
            raise RuntimeError(
                "live trading on Lighter needs the official SDK — "
                "pip install -r requirements-live.txt "
                "(git+https://github.com/elliottech/lighter-python.git)") from e
        signer = SignerClient(
            url=self.profile.api_url,
            account_index=c.account_index,
            api_private_keys={c.api_key_index: c.api_private_key},
            chain_id=self.profile.chain_id,
        )
        self.signer = signer  # Ensure failed startup can close the SDK HTTP session.
        err = signer.check_client()
        if err is not None:
            raise RuntimeError(
                f"[{self.name}] API key check failed "
                f"(account={c.account_index}, api_key_index={c.api_key_index}, "
                f"credentials={c.env_prefix}LIGHTER_*, url={self.profile.api_url}): {err}")
        self.signer = signer
        manager = getattr(signer, 'nonce_manager', None)
        fetch_nonce = getattr(manager, '_fetch_nonce', None)
        if fetch_nonce is not None and asyncio.iscoroutinefunction(fetch_nonce):
            # SDK lazily initializes a nonce inside create_order, and can
            # refresh it internally after rejection. Count those HTTP reads,
            # too, rather than assuming one SDK call is always one request.
            async def guarded_nonce(index):
                return await self._sdk_request('/api/v1/nextNonce', lambda: fetch_nonce(index),
                                               params={'account': c.account_index, 'key': index})
            manager._fetch_nonce = guarded_nonce
            manager._entropy_nonce_guarded = True
        log.info("[%s] signer ready (account %d, api_key_index %d, credentials %sLIGHTER_*)",
                 self.name, c.account_index, c.api_key_index, c.env_prefix)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            LighterBookFeed(self.name, self.profile.ws_url, self.market_id,
                            self.book, notify, requests=self._requests()).run(stop),
            name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer, requests=self._requests())
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return self.orders_feed is not None and self.orders_feed.ready.is_set()

    async def warm_http(self) -> None:
        """Best-effort warming within one shared host snapshot per 30 seconds.

        Every worker used to ping its distinct signer socket every ten seconds:
        twelve workers would consume 72 requests/min on pings alone. Cached
        workers may have a cold SDK socket; account verification/orders take priority.
        """
        if self.signer is None:
            try:
                await self._get("/api/v1/status", cache_ttl=30)
            except Exception as e:
                log.debug("[%s] keepalive ping failed: %r", self.name, e)
        try:
            # market_id is optional in the official API: one shared response
            # contains all perp markets. Select this worker's market locally.
            data = await self._get("/api/v1/orderBookDetails", cache_ttl=30)
            for detail in data.get("order_book_details") or []:
                if int(detail.get("market_id", -1)) != self.market_id:
                    continue
                value = float(detail.get("mark_price"))
                if math.isfinite(value) and value > 0:
                    self.mark_price = value
                break
        except (TypeError, ValueError, KeyError):
            log.debug("[%s] invalid mark price response", self.name)
        except Exception as e:
            log.debug("[%s] mark price request failed: %r", self.name, e)
        if self.signer is None:
            return
        try:
            sess = self.signer.api_client.rest_client.pool_manager
            # A cache hit does not warm this process's socket. Do not bypass
            # the global cache merely to keep every optional connection hot.
            await self._get("/api/v1/status", cache_ttl=30, query_session=sess)
        except Exception as e:
            log.debug("[%s] signer keepalive failed: %r", self.name, e)

    # ------------------------------------------------------------ price grid

    def px_round(self, px: float, round_up: bool) -> float:
        f = 10 ** self.price_decimals
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    # ------------------------------------------------------------- execution

    def _next_coi(self) -> int:
        self._coi += 1
        return self._coi

    async def place_maker(self, *, is_buy: bool, qty: float, limit_px: float):
        from lighter import SignerClient
        order = MakerOrder(self._next_coi(), limit_px, time.time())
        if time.time() < self.rate_limit_until:
            order.rejected, order.err = True, self._rate_limit_result()['err']
            return order
        if self._nonce_refresh_required:
            if not await self._refresh_nonce():
                order.rejected, order.err = True, 'nonce resynchronization failed; no maker order sent'
                return order
            self._nonce_refresh_required = False
        try:
            _tx, resp, err = await asyncio.wait_for(self._sdk_request('/api/v1/sendTx', lambda: self.signer.create_order(
                market_index=self.market_id, client_order_index=order.client_id,
                base_amount=int(round(qty * 10 ** self.size_decimals)),
                price=int(round(limit_px * 10 ** self.price_decimals)), is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_LIMIT,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False, order_expiry=SignerClient.DEFAULT_28_DAY_ORDER_EXPIRY),
                priority=NORMAL, submission=True),
                timeout=self.settle_timeout)
            if err is not None or (getattr(resp, "code", 200) or 200) != 200:
                order.rejected = True
                order.err = (self._rate_limit_rejected()['err'] if self._is_rate_limit_rejection(resp, err)
                             else 'Lighter maker transaction rejected')
        except Exception as error:
            if isinstance(error, RestReadCooldown) and error.source == 'budget':
                order.rejected, order.err = True, 'RATE_LIMITED: shared request budget, no order sent'
                self.rate_limit_until = max(self.rate_limit_until, error.until)
            elif getattr(error, 'status', None) == 429:
                order.rejected, order.err = True, self._rate_limit_rejected(getattr(error, 'headers', None))['err']
            # Other exceptions are ambiguous: resolve by client index, never resend.
        return order

    async def _maker_lookup(self, order):
        from lighter import OrderApi
        api = OrderApi(self.signer.api_client)
        auth, err = self.signer.create_auth_token_with_expiry()
        if err:
            raise RuntimeError("Lighter order lookup authentication unavailable")
        for method in (api.account_active_orders, api.account_inactive_orders):
            params = inspect.signature(method).parameters
            kwargs = {"account_index": self.conf.lighter_creds.account_index,
                      "market_id": self.market_id,
                      "authorization" if "authorization" in params else "auth": auth}
            if method == api.account_inactive_orders:
                kwargs["limit"] = 100
            response = await self._sdk_request('/api/v1/' + getattr(method, '__name__', 'accountOrders'),
                                              lambda: method(**kwargs), params={
                                                  'account': self.conf.lighter_creds.account_index,
                                                  'market': self.market_id})
            for row in response.orders or []:
                row = row.to_dict() if hasattr(row, "to_dict") else row
                if int(row.get("client_order_index", -1)) == order.client_id:
                    return row
        raise RuntimeError("Lighter maker state unknown")

    async def maker_state(self, order):
        feed = self.orders_feed
        # A confirmed WS fill remains useful during a stream reconnect, and a
        # terminal WS update must never be discarded because REST is limited.
        row = feed.latest.get(order.client_id) if feed else None
        row = row or order.last_state
        if order.query_lock is None:
            order.query_lock = asyncio.Lock()
        async with order.query_lock:
            row = (feed.latest.get(order.client_id) if feed else None) or row
            if (not row or not terminal_status(str(row.get('status', '')))) and time.monotonic() >= order.next_query:
                # Set the deadline before awaiting: cancel_maker and the main
                # fill loop can request the same order concurrently.
                order.last_query = time.monotonic()
                order.next_query = order.last_query + 1
                try:
                    found = await self._maker_lookup(order)
                    order.query_failures = 0
                    if feed:
                        feed._handle_orders({"orders": {str(self.market_id): [found]}})
                        row = feed.latest[order.client_id]
                    else:
                        row = found
                except Exception:
                    order.query_failures += 1
                    order.next_query = time.monotonic() + min(8, 2 ** min(order.query_failures, 3))
                    # Use a known cumulative fill even if periodic REST fails.
                    row = (feed.latest.get(order.client_id) if feed else None) or row
                    if row is None:
                        raise
            if row is None:
                # No new confirmation during REST backoff. This is explicitly
                # nonterminal, never evidence that a cancel succeeded.
                return dict(status='pending-confirmation', filled_base=0.0,
                            avg_px=None, err=None, terminal=False)
            order.last_state = row
        order.order_id = row.get("order_index")
        filled = float(row["filled_base_amount"])
        quote = float(row["filled_quote_amount"])
        if filled and (not math.isfinite(quote) or quote <= 0):
            raise RuntimeError("Lighter maker fill price unavailable")
        status = str(row["status"])
        return {"status": status, "filled_base": filled,
                "avg_px": quote / filled if filled else None, "err": None,
                "terminal": terminal_status(status)}

    async def cancel_maker(self, order):
        if order.order_id is None:
            await self.maker_state(order)
        if order.order_id is None:
            raise RuntimeError("Lighter server order index unknown")
        _tx, resp, err = await self._sdk_request('/api/v1/sendTx', lambda: self.signer.cancel_order(
            market_index=self.market_id, order_index=int(order.order_id)), submission=True)
        if err or (getattr(resp, "code", 200) or 200) != 200:
            raise RuntimeError("Lighter cancellation rejected")

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False, risk: bool = False, _retry_nonce: bool = True) -> dict:
        """Market order with avg-price protection; settle via account ws."""
        assert self.signer is not None
        if time.time() < self.rate_limit_until:
            return self._rate_limit_result()
        if self._nonce_refresh_required:
            if not await self._refresh_nonce():
                return dict(status='send-failed', filled_base=0.0, avg_px=None,
                            err='nonce resynchronization failed after rate-limit rejection; no order sent', unresolved=False)
            self._nonce_refresh_required = False
        from lighter import SignerClient
        coi = self._next_coi()
        fut = self.orders_feed.watch(coi) if self.orders_feed else None
        base_amount = int(round(qty * 10 ** self.size_decimals))
        price = int(round(limit_px * 10 ** self.price_decimals))
        try:
            _tx, resp, err = await self._sdk_request('/api/v1/sendTx', lambda: self.signer.create_order(
                market_index=self.market_id,
                client_order_index=coi,
                base_amount=base_amount,
                price=price,
                is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_MARKET,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            ), submission=True, priority=CRITICAL if reduce_only or risk else NORMAL)
        except Exception as e:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            if isinstance(e, RestReadCooldown) and e.source == 'budget':
                self.rate_limit_until = max(self.rate_limit_until, e.until)
                return dict(status='local-budget-wait', filled_base=0., avg_px=None,
                            err='RATE_LIMITED: shared request budget, no order sent', unresolved=False)
            if getattr(e, 'status', None) == 429:
                # The HTTP request was explicitly rejected, not accepted with
                # an unknown fill. Do not poll for an order that never existed.
                return self._rate_limit_rejected(getattr(e, 'headers', None))
            confirmed = await self._confirm_taker(coi, qty)
            if confirmed is not None:
                return confirmed
            msg = f"{type(e).__name__}: {e}"
            if getattr(e, "status", None) == 429 or "(429)" in str(e):
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": True}
        if err is not None or (getattr(resp, "code", 200) or 200) != 200:
            if fut is not None:
                self.orders_feed.unwatch(coi)
            msg = str(err) if err is not None else \
                f"tx rejected code={resp.code} msg={getattr(resp, 'message', None)}"
            if self._is_rate_limit_rejection(resp, err):
                return self._rate_limit_rejected()
            # Only an explicit API rejection is safe to re-sign. An accepted
            # transaction or a transport exception goes through same-ID lookup.
            if _retry_nonce and 'invalid nonce' in msg.lower():
                if await self._refresh_nonce():
                    log.warning('[%s] API rejected nonce before acceptance; refreshed nonce, retry once', self.name)
                    return await self.send_taker(is_buy=is_buy, qty=qty, limit_px=limit_px,
                                                 reduce_only=reduce_only, risk=risk, _retry_nonce=False)
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False}
        if fut is None:
            confirmed = await self._confirm_taker(coi, qty)
            if confirmed is not None:
                return confirmed
            return {"status": "sent-unconfirmed", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True}
        try:
            info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
            return {"status": info["status"], "filled_base": info["filled_base"],
                    "avg_px": info.get("avg_px"), "err": None, "unresolved": False}
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(coi)
            log.warning("[%s] WS settlement delayed for coi %d after %.1fs; checking the same order via REST/WS",
                        self.name, coi, self.settle_timeout)
            confirmed = await self._confirm_taker(coi, qty)
            if confirmed is not None:
                return confirmed
            return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True}

    @staticmethod
    def _is_rate_limit_rejection(response, error):
        code = response.get('code') if isinstance(response, dict) else getattr(response, 'code', None)
        # Accepted transactions can include JSON quota metadata such as
        # {"ratelimit": "didn't use volume quota"} in message. That is not a
        # rejection and must not freeze every worker for 60 seconds.
        if error is None and code == 200:
            return False
        detail = response.get('message', '') if isinstance(response, dict) else getattr(response, 'message', '')
        message = str(error if error is not None else detail).lower()
        return (code in (429, 23000)
                or any(text in message for text in ('too many requests', 'ratelimit', 'rate limit')))

    def _rate_limit_result(self):
        delay = max(0, math.ceil(self.rate_limit_until - time.time()))
        return dict(status='rate-limited', filled_base=0.0, avg_px=None,
                    err=f'RATE_LIMITED: request not accepted; cooldown {delay}s', unresolved=False)

    def _rate_limit_rejected(self, headers=None):
        try:
            retry_after = float((headers or {}).get('Retry-After', 0))
            if not math.isfinite(retry_after):
                retry_after = 0
        except (TypeError, ValueError):
            retry_after = 0
        delay = max(RATE_LIMIT_COOLDOWN_SEC, retry_after)
        self.rate_limit_until = max(self.rate_limit_until, time.time() + delay)
        self._nonce_refresh_required = True
        log.warning('[%s] rate-limit rejection confirmed, zero fill; cooldown %.0fs, then resync nonce; '
                    'L1-address limits are shared across tasks', self.name, delay)
        return self._rate_limit_result()

    async def _refresh_nonce(self):
        manager = getattr(self.signer, 'nonce_manager', None)
        refresh = getattr(manager, 'async_hard_refresh_nonce', None)
        synchronous = getattr(manager, 'hard_refresh_nonce', None)
        try:
            index = self.conf.lighter_creds.api_key_index
            if refresh is not None:
                if getattr(manager, '_entropy_nonce_guarded', False):
                    await asyncio.wait_for(refresh(index), REST_TIMEOUT)
                else:
                    await asyncio.wait_for(self._sdk_request('/api/v1/nextNonce', lambda: refresh(index)), REST_TIMEOUT)
            elif synchronous is not None:
                await asyncio.wait_for(self._sdk_request('/api/v1/nextNonce',
                                       lambda: asyncio.to_thread(synchronous, index)), REST_TIMEOUT)
            else:
                log.warning('[%s] SDK nonce refresh unavailable', self.name)
                return False
            return True
        except Exception as error:
            log.warning('[%s] nonce refresh failed (%s)', self.name, type(error).__name__)
            return False

    async def _confirm_taker(self, coi, qty):
        """Look up the original IOC after a transport/WS delay; never resubmit."""
        order = MakerOrder(coi, 0, time.time())
        deadline = time.monotonic() + TAKER_RECHECK_SEC
        while time.monotonic() < deadline:
            try:
                state = await asyncio.wait_for(self.maker_state(order),
                                               min(REST_TIMEOUT, deadline - time.monotonic()))
                filled, price = state['filled_base'], state.get('avg_px')
                if (state['terminal'] and math.isfinite(filled) and 0 <= filled <= qty + 1e-12
                        and (filled == 0 or (price is not None and math.isfinite(price) and price > 0))):
                    log.info('[%s] IOC RECHECK confirmed coi=%d status=%s filled=%.8g/%.8g',
                             self.name, coi, state['status'], filled, qty)
                    return dict(status=state['status'], filled_base=filled, avg_px=price,
                                err=None, unresolved=False)
            except Exception as error:
                log.warning('[%s] IOC RECHECK coi=%d not yet confirmed (%s)',
                            self.name, coi, type(error).__name__)
            await asyncio.sleep(min(.25, max(0, deadline - time.monotonic())))
        return None

    # -------------------------------------------------------------- accounts

    def _account_params(self):
        return {'by': 'index', 'value': str(self.conf.lighter_creds.account_index)}

    async def _account(self, *, equity_only=False, background=False, priority=None) -> Optional[dict]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params=self._account_params(),
                               cache_ttl=60 if equity_only else (30 if background else .2),
                               priority=priority if priority is not None else (BACKGROUND if equity_only or background else CRITICAL),
                               not_before=self.last_traded_ts if not equity_only else 0)
        for acct in data.get("accounts") or []:
            return acct
        return None

    async def fetch_equity(self):
        acct = await self._account(equity_only=True)
        if acct is None:
            return None
        return (float(acct.get("total_asset_value") or 0.0),
                float(acct.get("available_balance") or 0.0))

    async def fetch_liquidation_risk(self):
        from .liquidation import margin_snapshot, number
        from .rest_reads import NORMAL
        data = await self._get('/api/v1/account', params=self._account_params(),
                               cache_ttl=5, priority=NORMAL)
        rows = data.get('accounts')
        if not isinstance(rows, list) or len(rows) != 1:
            raise ValueError('risk account unavailable')
        account = rows[0]
        if int(account['account_index']) != self.conf.lighter_creds.account_index:
            raise ValueError('risk account identity mismatch')
        positions = account['positions']
        if not isinstance(positions, list):
            raise ValueError('risk positions unavailable')
        sizes = [number(p['position']) for p in positions]
        if any(size < 0 for size in sizes):
            raise ValueError('negative Lighter risk position magnitude')
        # Do not mix isolated collateral into the cross-margin denominator.
        # The endpoint's isolated allocation is not a complete isolated equity/MMR snapshot.
        if any(number(p['position']) > 0 and int(p['margin_mode']) != 0 for p in positions):
            raise ValueError('isolated margin risk not supported; no safe cross fallback')
        return margin_snapshot(account['cross_asset_value'], account['cross_maintenance_margin_requirement'],
                               exposed=any(size > 0 for size in sizes),
                               source='Lighter cross account')

    async def has_open_orders(self, *, priority=CRITICAL) -> bool:
        from lighter import OrderApi
        auth, err = self.signer.create_auth_token_with_expiry()
        if err or not auth:
            raise RuntimeError(f"[{self.name}] open orders authentication unavailable")
        method = OrderApi(self.signer.api_client).account_active_orders
        params = inspect.signature(method).parameters
        kwargs = {
            "account_index": self.conf.lighter_creds.account_index,
            "market_id": self.market_id,
            "authorization" if "authorization" in params else "auth": auth,
        }
        response = await self._sdk_request('/api/v1/accountActiveOrders', lambda: method(**kwargs),
                                          priority=priority,
                                          params={'account': self.conf.lighter_creds.account_index,
                                                  'market': self.market_id})
        data = response.to_dict() if hasattr(response, "to_dict") else response
        if (not isinstance(data, dict) or data.get("code") != 200
                or not isinstance(data.get("orders"), list)):
            raise RuntimeError(f"[{self.name}] invalid open orders response")
        return bool(data["orders"])

    async def fetch_position(self, *, background=False, priority=None) -> float:
        acct = await self._account(background=background, priority=priority)
        if acct is None:
            raise RuntimeError(f"[{self.name}] account not found")
        if not isinstance(acct.get("positions"), list):
            raise RuntimeError(f"[{self.name}] invalid positions response")
        for p in acct["positions"]:
            if int(p["market_id"]) == self.market_id:
                size, sign = float(p["position"]), float(p["sign"])
                if (not math.isfinite(size) or size < 0
                        or sign not in (-1, 0, 1) or (size > 0 and sign == 0)):
                    raise RuntimeError(f"[{self.name}] invalid position size/sign")
                return sign * size
        return 0.0

    async def fetch_position_background(self):
        return await self.fetch_position(background=True)

    async def close(self) -> None:
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
