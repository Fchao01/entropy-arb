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
        self.ws_url = "wss://fstream.asterdex.com"
        self.book = OrderBook(); self.position = 0.0; self.cash = 0.0
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
        if method == "GET":
            kwargs["params"] = p
        else:
            kwargs["data"] = p
            kwargs["headers"] = {"Content-Type": "application/x-www-form-urlencoded"}
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

    async def load_market(self):
        info = await self._get("/fapi/v3/exchangeInfo")
        item = next((s for s in info.get("symbols", []) if s.get("symbol") == self.symbol), None)
        if not item: raise RuntimeError(f"[ASTER] {self.symbol} not found")
        quote = item.get("quoteAsset") or item.get("marginAsset")
        if quote != "USD1": raise RuntimeError(f"[ASTER] {self.symbol} is {quote}-margined, expected USD1")
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

    def start_tasks(self, stop, notify, live):
        tasks = [asyncio.create_task(self._book_loop(stop, notify), name=f"book-{self.key}")]
        if live:
            tasks.append(asyncio.create_task(self._user_loop(stop), name=f"user-{self.key}"))
        return tasks

    async def _book_loop(self, stop, notify):
        """Maintain a Binance-compatible Aster diff book with REST snapshot."""
        while not stop.is_set():
            try:
                snap = await self._get("/fapi/v3/depth", {"symbol": self.symbol, "limit": 1000})
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
                async with ws_connect(f"{self.ws_url}/ws/{key}", ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        e = __import__('json').loads(raw); typ = e.get("e")
                        if typ == "ACCOUNT_UPDATE":
                            for p in (e.get("a", {}).get("P", []) or []):
                                if p.get("s") == self.symbol: self.position = float(p.get("pa", 0))
                        if stop.is_set(): break
                await self._request("PUT", "/fapi/v3/listenKey", {"listenKey": key}, signed=True)
            except asyncio.CancelledError: raise
            except Exception as e:
                log.warning("[ASTER] user stream failed: %s", e); await asyncio.sleep(2)

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
            unknown = "HTTP 503" in str(e)
            return {"status":"send-failed", "filled_base":0.0, "avg_px":None, "err":repr(e), "unresolved":unknown}

    async def fetch_position(self):
        rows = await self._get("/fapi/v3/positionRisk", {"symbol": self.symbol}, signed=True)
        return float(rows[0].get("positionAmt", 0)) if rows else 0.0
    async def fetch_equity(self):
        rows = await self._get("/fapi/v3/balance", signed=True)
        a = next((x for x in rows if x.get("asset") == "USD1"), None)
        return ((float(a.get("balance", 0)), float(a.get("availableBalance", 0))) if a else None)
    async def close(self): pass
