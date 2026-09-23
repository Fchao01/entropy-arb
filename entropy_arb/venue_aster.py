"""Aster USD1 perpetual adapter using Pro API V3 EIP-712 signing."""
from __future__ import annotations
import asyncio, logging, math, time
from urllib.parse import urlencode
import aiohttp
try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets import connect as ws_connect
from .book import OrderBook

log = logging.getLogger("aster")

class AsterVenue:
    kind = "aster"
    def __init__(self, conf, session, settle_timeout_sec):
        self.conf, self.session = conf, session
        self.key, self.name = conf.key, conf.label
        self.api_url = conf.aster_api_url.rstrip("/")
        self.market_api_url = self.api_url
        self.market_api_version = "v3"
        self.ws_url = "wss://fstream.asterdex.com"
        self.book = OrderBook(); self.position = 0.0; self.cash = 0.0
        self.stream_position = None
        self.volume_usd = 0.0; self.equity = self.free = self.start_equity = None
        self.fee_bps = conf.fee_bps; self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min; self.last_traded_ts = 0.0
        self.symbol = conf.symbol.upper(); self.size_decimals = 6
        self.price_decimals = 6; self.min_base = 1e-6; self.min_quote = 5.0
        self.settle_timeout = settle_timeout_sec; self._order_id = 0

    def _next_nonce(self):
        now = time.time_ns() // 1000
        self._nonce = max(getattr(self, "_nonce", 0) + 1, now)
        return self._nonce

    def _sign_params(self, params):
        try:
            from eth_account import Account
            try:
                from eth_account.messages import encode_typed_data
                _encode = lambda payload: encode_typed_data(full_message=payload)
            except ImportError:
                from eth_account.messages import encode_structured_data
                _encode = encode_structured_data
        except ImportError as e:
            raise RuntimeError("Aster V3 trading needs eth-account; pip install -r requirements-live.txt") from e
        p = dict(params)
        p.setdefault("user", self.conf.aster_user)
        p["signer"] = self.conf.aster_signer
        p["nonce"] = str(self._next_nonce())
        encoded = urlencode(p)
        typed = {"types": {"EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}],
                "Message": [{"name": "msg", "type": "string"}]},
                "primaryType": "Message",
                "domain": {"name": "AsterSignTransaction", "version": "1",
                           "chainId": 1666,
                           "verifyingContract": "0x0000000000000000000000000000000000000000"},
                "message": {"msg": encoded}}
        sig = Account.sign_message(_encode(typed),
                                   private_key=self.conf.aster_private_key).signature.hex()
        p["signature"] = sig
        return p

    async def _request(self, method, path, params=None, signed=False):
        p = self._sign_params(params or {}) if signed else dict(params or {})
        kwargs = {"timeout": aiohttp.ClientTimeout(total=10)}
        kwargs["headers"] = {"User-Agent": "entropy-arb/1.0"}
        if method == "GET":
            kwargs["params"] = p
        else:
            kwargs["data"] = p
            kwargs["headers"]["Content-Type"] = "application/x-www-form-urlencoded"
        async with self.session.request(method, self.api_url + path, **kwargs) as r:
            body = await r.text()
            if r.status >= 400:
                # V3 documents HTTP 503 as an accepted-but-unknown outcome.
                suffix = " (execution status unknown)" if r.status == 503 else ""
                raise RuntimeError(f"Aster HTTP {r.status}{suffix}: {body[:300]}")
            try:
                return __import__("json").loads(body) if body else {}
            except ValueError:
                raise RuntimeError(f"Aster invalid JSON response: {body[:300]}")

    async def _get(self, path, params=None, signed=False):
        return await self._request("GET", path, params, signed)

    def _select_market(self, info):
        symbols = info.get("symbols") or []
        requested = self.symbol.upper()
        exact = next((s for s in symbols
                      if str(s.get("symbol", "")).upper() == requested), None)
        if exact is not None:
            return exact

        # The CLI uses the base asset (for example SNDK), while Aster's
        # USD1 contract is usually named SNDKUSD1. Keep the mapping local to
        # this venue so the light-rh primary still receives the CLI symbol.
        base = requested.removesuffix("USD1")
        matches = [s for s in symbols
                   if str(s.get("baseAsset", "")).upper() == base
                   and str(s.get("quoteAsset", "")).upper() == "USD1"
                   and str(s.get("marginAsset", "")).upper() == "USD1"
                   and str(s.get("status", "TRADING")).upper() == "TRADING"]
        if len(matches) == 1:
            self.symbol = str(matches[0]["symbol"])
            return matches[0]
        if len(matches) > 1:
            names = ", ".join(str(s.get("symbol")) for s in matches)
            raise RuntimeError(f"[ASTER] ambiguous USD1 markets for {requested}: {names}")
        return None

    async def load_market(self):
        try:
            info = await self._get("/fapi/v3/exchangeInfo")
        except Exception as primary_error:
            fallback = self.api_url.replace("fapi3.", "fapi.")
            if fallback == self.api_url:
                raise
            log.warning("[ASTER] V3 market endpoint unavailable (%s); "
                        "trying public V1 market data at %s",
                        primary_error, fallback)
            try:
                async with self.session.get(
                        fallback + "/fapi/v1/exchangeInfo",
                        headers={"User-Agent": "entropy-arb/1.0"},
                        timeout=aiohttp.ClientTimeout(total=10)) as r:
                    body = await r.text()
                    if r.status >= 400:
                        raise RuntimeError(
                            f"Aster fallback HTTP {r.status}: {body[:300]}")
                    info = __import__("json").loads(body) if body else {}
                self.market_api_url = fallback
                self.market_api_version = "v1"
            except Exception as fallback_error:
                raise RuntimeError(
                    f"Aster market data unavailable on V3 ({primary_error}) "
                    f"and V1 fallback ({fallback_error}); this is usually "
                    "an IP/region/WAF restriction") from fallback_error
        requested = self.symbol
        item = self._select_market(info)
        if not item:
            raise RuntimeError(f"[ASTER] {requested} not found as a USD1 market")
        quote = item.get("quoteAsset")
        margin = item.get("marginAsset")
        if quote != "USD1" or margin != "USD1":
            raise RuntimeError(f"[ASTER] {self.symbol} is {quote}/{margin}-margined, expected USD1/USD1")
        for f in item.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                self.min_base = float(f.get("minQty", self.min_base)); self.size_decimals = max(0, int(round(-math.log10(float(f.get("stepSize", self.min_base))))))
            if f.get("filterType") == "PRICE_FILTER":
                self.price_decimals = max(0, int(round(-math.log10(float(f.get("tickSize", 1e-6))))))
        log.info("[ASTER] %s USD1 perpetual loaded", self.symbol)

    def init_signer(self):
        if not (self.conf.aster_user and self.conf.aster_signer and self.conf.aster_private_key):
            raise RuntimeError("live Aster V3 trading needs ASTER_USER_ADDRESS, ASTER_SIGNER_ADDRESS, and ASTER_SIGNER_PRIVATE_KEY")
        try:
            from eth_account import Account
            derived = Account.from_key(self.conf.aster_private_key).address.lower()
        except Exception as e:
            raise RuntimeError(f"invalid Aster signer private key: {e}") from e
        if derived != self.conf.aster_signer.lower():
            raise RuntimeError(f"Aster signer private key derives {derived}, not ASTER_SIGNER_ADDRESS")

    async def validate_position_mode(self):
        mode = await self._get("/fapi/v3/positionSide/dual", signed=True)
        if str(mode.get("dualSidePosition", "false")).lower() == "true":
            raise RuntimeError(
                "Aster account is in Hedge Mode; this bot requires One-way "
                "Mode because it uses reduceOnly without positionSide")

    def start_tasks(self, stop, notify, live):
        tasks = [asyncio.create_task(self._book_loop(stop, notify), name=f"book-{self.key}")]
        if live:
            tasks.append(asyncio.create_task(self._user_loop(stop), name=f"user-{self.key}"))
        return tasks

    async def _book_loop(self, stop, notify):
        """Maintain a Binance-compatible Aster diff book with REST snapshot."""
        while not stop.is_set():
            try:
                if self.market_api_version == "v3":
                    snap = await self._get(
                        "/fapi/v3/depth",
                        {"symbol": self.symbol, "limit": 1000})
                else:
                    async with self.session.get(
                            self.market_api_url + "/fapi/v1/depth",
                            params={"symbol": self.symbol, "limit": 1000},
                            headers={"User-Agent": "entropy-arb/1.0"},
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
                        r.raise_for_status()
                        snap = await r.json()
                self.book.apply_hl([[{"px":p,"sz":q} for p,q in snap.get("bids",[])], [{"px":p,"sz":q} for p,q in snap.get("asks",[])]])
                last = int(snap.get("lastUpdateId", 0)); notify()
                async with ws_connect(f"{self.ws_url}/ws/{self.symbol.lower()}@depth@100ms", ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                    async for raw in ws:
                        e = __import__('json').loads(raw)
                        if int(e.get("u", 0)) <= last: continue
                        if int(e.get("U", 0)) > last + 1 or (e.get("pu") is not None and int(e["pu"]) != last):
                            break
                        for p,q in e.get("b",[]): self._apply_level(self.book.bids, p, q)
                        for p,q in e.get("a",[]): self._apply_level(self.book.asks, p, q)
                        self.book.ready = True; self.book.touch(); self.book.last_update_ts = time.time(); last = int(e["u"]); notify()
            except asyncio.CancelledError: raise
            except Exception as e:
                self.book.ready = False; log.warning("[ASTER] depth poll failed: %s", e); await asyncio.sleep(1)

    @staticmethod
    def _apply_level(side, price, size):
        p, q = float(price), float(size)
        if q <= 0: side.pop(p, None)
        else: side[p] = q

    async def _user_loop(self, stop):
        """Consume Aster Futures USER_DATA order/account events; REST remains fallback."""
        while not stop.is_set():
            try:
                key = (await self._request("POST", "/fapi/v3/listenKey", signed=True)).get("listenKey")
                if not key:
                    raise RuntimeError("Aster listenKey response was empty")
                keepalive = asyncio.create_task(
                    self._keepalive_loop(stop, key),
                    name=f"keepalive-{self.key}")
                try:
                    async with ws_connect(f"{self.ws_url}/ws/{key}", ping_interval=20, ping_timeout=20) as ws:
                        async for raw in ws:
                            e = __import__('json').loads(raw); typ = e.get("e")
                            if typ == "ACCOUNT_UPDATE":
                                for p in (e.get("a", {}).get("P", []) or []):
                                    if p.get("s") == self.symbol:
                                        self.stream_position = float(p.get("pa", 0))
                            if typ == "listenKeyExpired" or stop.is_set():
                                break
                finally:
                    keepalive.cancel()
                    await asyncio.gather(keepalive, return_exceptions=True)
            except asyncio.CancelledError: raise
            except Exception as e:
                log.warning("[ASTER] user stream failed: %s", e); await asyncio.sleep(2)

    async def _keepalive_loop(self, stop, key):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=30 * 60)
                return
            except asyncio.TimeoutError:
                try:
                    await self._request("PUT", "/fapi/v3/listenKey",
                                        {"listenKey": key}, signed=True)
                except Exception as e:
                    log.warning("[ASTER] listenKey keepalive failed: %s", e)

    def ready_to_trade(self): return bool(self.conf.aster_user and self.conf.aster_signer and self.conf.aster_private_key)
    def px_round(self, px, round_up):
        f = 10 ** self.price_decimals
        return (math.ceil(px*f) if round_up else math.floor(px*f)) / f
    async def warm_http(self): return None

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only=False):
        side = "BUY" if is_buy else "SELL"
        p = {"symbol": self.symbol, "side": side, "type": "LIMIT", "timeInForce": "IOC",
             "quantity": f"{qty:.{self.size_decimals}f}", "price": f"{limit_px:.{self.price_decimals}f}",
             "reduceOnly": "true" if reduce_only else "false"}
        try:
            o = await self._request("POST", "/fapi/v3/order", p, signed=True)
            if o.get("status") in ("NEW", "PARTIALLY_FILLED") and o.get("orderId"):
                deadline = time.time() + self.settle_timeout
                while time.time() < deadline and o.get("status") in ("NEW", "PARTIALLY_FILLED"):
                    await asyncio.sleep(0.05)
                    o = await self._get("/fapi/v3/order", {"symbol": self.symbol, "orderId": o["orderId"]}, signed=True)
            status = o.get("status", "UNKNOWN"); filled = float(o.get("executedQty", 0) or 0)
            quote = float(o.get("cumQuote", 0) or 0)
            return {"status": status, "filled_base": filled, "avg_px": quote/filled if filled else None, "err": None, "unresolved": False}
        except Exception as e:
            unknown = ("Aster HTTP 5" in str(e)
                       or isinstance(e, (aiohttp.ClientError,
                                          asyncio.TimeoutError, OSError)))
            return {"status":"send-failed", "filled_base":0.0, "avg_px":None, "err":repr(e), "unresolved":unknown}

    async def fetch_position(self):
        rows = await self._get("/fapi/v3/positionRisk", {"symbol": self.symbol}, signed=True)
        return sum(float(row.get("positionAmt", 0) or 0) for row in rows)
    async def fetch_equity(self):
        rows = await self._get("/fapi/v3/balance", signed=True)
        a = next((x for x in rows if x.get("asset") == "USD1"), None)
        return ((float(a.get("balance", 0)), float(a.get("availableBalance", 0))) if a else None)
    async def close(self): pass
