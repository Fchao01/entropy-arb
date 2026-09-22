"""Aster USD1-margined perpetuals adapter (Binance-compatible Futures V3 API)."""
from __future__ import annotations
import asyncio, hashlib, hmac, logging, math, time
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
        self.api_url = "https://fapi.asterdex.com"
        self.book = OrderBook(); self.position = 0.0; self.cash = 0.0
        self.volume_usd = 0.0; self.equity = self.free = self.start_equity = None
        self.fee_bps = conf.fee_bps; self.cap_usd = conf.cap_usd
        self.orders_per_min = conf.orders_per_min; self.last_traded_ts = 0.0
        self.symbol = conf.symbol.upper(); self.size_decimals = 6
        self.price_decimals = 6; self.min_base = 1e-6; self.min_quote = 5.0
        self.settle_timeout = settle_timeout_sec; self._order_id = 0

    async def _get(self, path, params=None, signed=False):
        p = dict(params or {})
        if signed:
            p["timestamp"] = int(time.time()*1000); p["recvWindow"] = 5000
            q = urlencode(p); p["signature"] = hmac.new((self.conf.aster_api_secret or '').encode(), q.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": self.conf.aster_api_key} if signed else {}
        async with self.session.get(self.api_url + path, params=p, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status(); return await r.json()

    async def load_market(self):
        info = await self._get("/fapi/v1/exchangeInfo")
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
        if not (self.conf.aster_api_key and self.conf.aster_api_secret):
            raise RuntimeError("live Aster trading needs ASTER_API_KEY and ASTER_API_SECRET")

    def start_tasks(self, stop, notify, live):
        tasks = [asyncio.create_task(self._book_loop(stop, notify), name=f"book-{self.key}")]
        if live:
            tasks.append(asyncio.create_task(self._user_loop(stop), name=f"user-{self.key}"))
        return tasks

    async def _book_loop(self, stop, notify):
        """Maintain a Binance-compatible Aster diff book with REST snapshot."""
        while not stop.is_set():
            try:
                snap = await self._get("/fapi/v1/depth", {"symbol": self.symbol, "limit": 1000})
                self.book.apply_hl([[{"px":p,"sz":q} for p,q in snap.get("bids",[])], [{"px":p,"sz":q} for p,q in snap.get("asks",[])]])
                last = int(snap.get("lastUpdateId", 0)); notify()
                async with ws_connect(f"wss://fstream.asterdex.com/ws/{self.symbol.lower()}@depth@100ms", ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
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
                async with self.session.post(self.api_url + "/fapi/v1/listenKey", headers={"X-MBX-APIKEY": self.conf.aster_api_key}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    r.raise_for_status(); key = (await r.json()).get("listenKey")
                async with ws_connect(f"wss://fstream.asterdex.com/ws/{key}", ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        e = __import__('json').loads(raw); typ = e.get("e")
                        if typ == "ACCOUNT_UPDATE":
                            for p in (e.get("a", {}).get("P", []) or []):
                                if p.get("s") == self.symbol: self.position = float(p.get("pa", 0))
                        if stop.is_set(): break
                async with self.session.put(self.api_url + "/fapi/v1/listenKey", params={"listenKey": key}, headers={"X-MBX-APIKEY": self.conf.aster_api_key}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    await r.read()
            except asyncio.CancelledError: raise
            except Exception as e:
                log.warning("[ASTER] user stream failed: %s", e); await asyncio.sleep(2)

    def ready_to_trade(self): return bool(self.conf.aster_api_key and self.conf.aster_api_secret)
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
            p["timestamp"] = int(time.time()*1000); p["recvWindow"] = 5000
            q = urlencode(p)
            p["signature"] = hmac.new((self.conf.aster_api_secret or '').encode(), q.encode(), hashlib.sha256).hexdigest()
            async with self.session.post(self.api_url + "/fapi/v1/order", params=p,
                                         headers={"X-MBX-APIKEY": self.conf.aster_api_key},
                                         timeout=aiohttp.ClientTimeout(total=10)) as r:
                r.raise_for_status(); o = await r.json()
            if o.get("status") in ("NEW", "PARTIALLY_FILLED") and o.get("orderId"):
                deadline = time.time() + self.settle_timeout
                while time.time() < deadline and o.get("status") in ("NEW", "PARTIALLY_FILLED"):
                    await asyncio.sleep(0.05)
                    o = await self._get("/fapi/v1/order", {"symbol": self.symbol, "orderId": o["orderId"]}, signed=True)
            status = o.get("status", "UNKNOWN"); filled = float(o.get("executedQty", 0) or 0)
            quote = float(o.get("cumQuote", 0) or 0)
            return {"status": status, "filled_base": filled, "avg_px": quote/filled if filled else None, "err": None, "unresolved": False}
        except Exception as e:
            return {"status":"send-failed", "filled_base":0.0, "avg_px":None, "err":repr(e), "unresolved":False}

    async def fetch_position(self):
        rows = await self._get("/fapi/v2/positionRisk", {"symbol": self.symbol}, signed=True)
        return float(rows[0].get("positionAmt", 0)) if rows else 0.0
    async def fetch_equity(self):
        rows = await self._get("/fapi/v2/account", signed=True)
        a = next((x for x in rows.get("assets", []) if x.get("asset") == "USD1"), None)
        return ((float(a.get("walletBalance", 0)), float(a.get("availableBalance", 0))) if a else None)
    async def close(self): pass
