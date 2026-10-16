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

log = logging.getLogger("lighter")

AUTH_REFRESH_SEC = 8 * 60
REST_TIMEOUT = 10.0


def terminal_status(status: str) -> bool:
    return status in {"filled", "expired", "rejected"} or status.startswith("canceled")


class AccountOrdersFeed:
    """Authenticated stream of our own order updates (settlement channel)."""

    def __init__(self, name: str, ws_url: str, market_id: int,
                 account_index: int, signer) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.account_index = account_index
        self.signer = signer
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
                log.warning("[%s] account ws error: %s — retry in %.0fs",
                            self.name, e, backoff)
                self.ready.clear()
                if stop.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            self.ready.clear()


class LighterVenue:
    kind = "lighter"

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

    # ------------------------------------------------------------------ REST

    async def _get(self, path: str, params: Optional[dict] = None):
        async with self.session.get(
                self.profile.api_url + path, params=params,
                timeout=aiohttp.ClientTimeout(total=REST_TIMEOUT)) as r:
            r.raise_for_status()
            return await r.json()

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
        err = signer.check_client()
        if err is not None:
            raise RuntimeError(
                f"[{self.name}] API key check failed "
                f"(account={c.account_index}, api_key_index={c.api_key_index}, "
                f"credentials={c.env_prefix}LIGHTER_*, url={self.profile.api_url}): {err}")
        self.signer = signer
        log.info("[%s] signer ready (account %d, api_key_index %d, credentials %sLIGHTER_*)",
                 self.name, c.account_index, c.api_key_index, c.env_prefix)

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        tasks = [asyncio.create_task(
            LighterBookFeed(self.name, self.profile.ws_url, self.market_id,
                            self.book, notify).run(stop),
            name=f"book-{self.key}")]
        if live:
            self.orders_feed = AccountOrdersFeed(
                self.name, self.profile.ws_url, self.market_id,
                self.conf.lighter_creds.account_index, self.signer)
            tasks.append(asyncio.create_task(self.orders_feed.run(stop),
                                             name=f"acct-{self.key}"))
        return tasks

    def ready_to_trade(self) -> bool:
        return self.orders_feed is not None and self.orders_feed.ready.is_set()

    async def warm_http(self) -> None:
        """Keep the order-path HTTPS connections warm (a cold TLS handshake
        adds 10-15ms to the first order after an idle spell)."""
        try:
            await self._get("/api/v1/status")
        except Exception as e:
            log.debug("[%s] keepalive ping failed: %r", self.name, e)
        try:
            data = await self._get("/api/v1/orderBookDetails",
                                   params={"market_id": str(self.market_id)})
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
            async with sess.get(self.profile.api_url + "/api/v1/status",
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
                await r.read()
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
        try:
            _tx, resp, err = await asyncio.wait_for(self.signer.create_order(
                market_index=self.market_id, client_order_index=order.client_id,
                base_amount=int(round(qty * 10 ** self.size_decimals)),
                price=int(round(limit_px * 10 ** self.price_decimals)), is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_LIMIT,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=False, order_expiry=SignerClient.DEFAULT_28_DAY_ORDER_EXPIRY),
                timeout=self.settle_timeout)
            if err is not None or (getattr(resp, "code", 200) or 200) != 200:
                order.rejected, order.err = True, "Lighter maker transaction rejected"
        except Exception:
            pass  # ambiguous submission: resolve by client index, never resend
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
            response = await method(**kwargs)
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
        _tx, resp, err = await self.signer.cancel_order(
            market_index=self.market_id, order_index=int(order.order_id))
        if err or (getattr(resp, "code", 200) or 200) != 200:
            raise RuntimeError("Lighter cancellation rejected")

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False) -> dict:
        """Market order with avg-price protection; settle via account ws."""
        assert self.signer is not None
        from lighter import SignerClient
        coi = self._next_coi()
        fut = self.orders_feed.watch(coi) if self.orders_feed else None
        base_amount = int(round(qty * 10 ** self.size_decimals))
        price = int(round(limit_px * 10 ** self.price_decimals))
        try:
            _tx, resp, err = await self.signer.create_order(
                market_index=self.market_id,
                client_order_index=coi,
                base_amount=base_amount,
                price=price,
                is_ask=not is_buy,
                order_type=SignerClient.ORDER_TYPE_MARKET,
                time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                reduce_only=reduce_only,
                order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            )
        except Exception as e:
            if fut is not None:
                self.orders_feed.unwatch(coi)
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
            if "rate limit" in msg.lower():
                msg = "RATE_LIMITED: " + msg
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": msg, "unresolved": False}
        if fut is None:
            return {"status": "sent-unconfirmed", "filled_base": 0.0,
                    "avg_px": None, "err": None, "unresolved": True}
        try:
            info = await asyncio.wait_for(fut, timeout=self.settle_timeout)
            return {"status": info["status"], "filled_base": info["filled_base"],
                    "avg_px": info.get("avg_px"), "err": None, "unresolved": False}
        except asyncio.TimeoutError:
            self.orders_feed.unwatch(coi)
            log.warning("[%s] no settle confirmation for coi %d in %.1fs",
                        self.name, coi, self.settle_timeout)
            return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": True}

    # -------------------------------------------------------------- accounts

    async def _account(self) -> Optional[dict]:
        c = self.conf.lighter_creds
        if c is None or c.account_index is None:
            return None
        data = await self._get("/api/v1/account",
                               params={"by": "index",
                                       "value": str(c.account_index)})
        for acct in data.get("accounts") or []:
            return acct
        return None

    async def fetch_equity(self):
        acct = await self._account()
        if acct is None:
            return None
        return (float(acct.get("total_asset_value") or 0.0),
                float(acct.get("available_balance") or 0.0))

    async def has_open_orders(self) -> bool:
        from lighter import OrderApi
        auth, err = self.signer.create_auth_token_with_expiry()
        if err or not auth:
            raise RuntimeError(f"[{self.name}] open orders authentication unavailable")
        method = OrderApi(self.signer.api_client).account_active_orders
        params = inspect.signature(method).parameters
        response = await method(**{
            "account_index": self.conf.lighter_creds.account_index,
            "market_id": self.market_id,
            "authorization" if "authorization" in params else "auth": auth,
        })
        data = response.to_dict() if hasattr(response, "to_dict") else response
        if (not isinstance(data, dict) or data.get("code") != 200
                or not isinstance(data.get("orders"), list)):
            raise RuntimeError(f"[{self.name}] invalid open orders response")
        return bool(data["orders"])

    async def fetch_position(self) -> float:
        acct = await self._account()
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

    async def close(self) -> None:
        if self.signer is not None:
            try:
                await self.signer.api_client.close()
            except Exception:
                pass
