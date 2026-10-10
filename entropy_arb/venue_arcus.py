"""Arcus perpetuals adapter, following https://docs.arcus.xyz/.

Public REST bootstraps market grids and reconciles account state. A persistent
WebSocket carries full L2 snapshots, signed IOC LIMIT orders and the orders
channel. A 202/ACK is never a fill; ambiguous sends are queried, never resent.
See authentication, websocket, market-data/l2orderbook and exchange/place-order
in the official API reference. No spot router or EVM wallet signing is used.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import uuid
from collections import OrderedDict
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from urllib.parse import quote

import aiohttp

from .book import OrderBook
from .config import ARCUS_ENDPOINTS, VenueConf
from .feeds import ws_connect

log = logging.getLogger("arcus")
REQUEST_TIMEOUT_SEC = 10.0
TERMINAL_STATUSES = {"FILLED", "CANCELED", "MARGIN_CANCELED", "REJECTED"}


def canonical(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def grid_int(value: str, unit: Decimal) -> int:
    """Exact engine ticks/quantums; never sign a rounded or fractional integer."""
    n = Decimal(value) / unit
    if not n.is_finite() or n <= 0 or n != n.to_integral_value():
        raise ValueError(f"{value} is not a positive multiple of {unit}")
    return int(n)


class ArcusVenue:
    kind = "arcus"

    def __init__(self, conf: VenueConf, session: aiohttp.ClientSession,
                 settle_timeout_sec: float) -> None:
        self.conf, self.session = conf, session
        self.key, self.name = conf.key, conf.label
        self.api_url, self.ws_url = ARCUS_ENDPOINTS[conf.arcus_network]
        self.settle_timeout = settle_timeout_sec
        self.book = OrderBook()
        self.mark_price = None
        self.position = self.cash = self.volume_usd = 0.0
        self.equity = self.free = self.start_equity = None
        self.fee_bps, self.cap_usd = conf.fee_bps, conf.cap_usd
        self.orders_per_min = conf.orders_per_min
        self.last_traded_ts = 0.0
        self.market_id = -1
        self.market_name = conf.symbol
        self.size_decimals = 0
        self.min_base = self.min_quote = 0.0
        self.max_base = math.inf
        self.tick_size = self.step_size = Decimal("1")
        self.tick_tiers = []
        self._signer = None
        self._api_key = None
        self._ws = None
        self._rpc_id = 0
        self._rpc_pending = {}
        self._send_lock = None
        self._orders_ready = False
        self._book_sequence = None
        self._orders = OrderedDict()
        self._clients = {}
        self._unresolved_orders = {}
        self._rate_limited_until = 0.0

    @property
    def address(self) -> str:
        creds = self.conf.arcus_creds
        if not creds or not creds.account_address:
            raise RuntimeError("ARCUS_ACCOUNT_ADDRESS is required")
        return creds.account_address.lower()

    @property
    def account_index(self) -> int:
        return self.conf.arcus_creds.account_index

    async def _get(self, path: str, **params) -> dict:
        async with self.session.get(
                self.api_url + path, params=params,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC)) as r:
            if r.status == 429:
                raise RuntimeError("RATE_LIMITED: Arcus HTTP 429")
            r.raise_for_status()
            return await r.json()

    async def load_market(self) -> None:
        data = await self._get("/v1/markets")
        markets = data["markets"]
        # Exact official display name first; allow a bare base symbol to pick
        # the standard USD perpetual. No aliases between unrelated assets.
        want = self.conf.symbol.upper()
        matches = [m for m in markets if m["marketDisplayName"].upper() == want]
        if not matches:
            matches = [m for m in markets
                       if m["marketDisplayName"].upper() == want + "-USD"]
        if len(matches) != 1:
            names = ", ".join(m["marketDisplayName"] for m in markets)
            section = "primary" if self.key == "entropy" else "hedge"
            raise RuntimeError(f"[ARCUS] {self.conf.symbol} not found; "
                               f"set {section}.symbol to a market name: {names}")
        market = matches[0]
        if market["status"] != "ONLINE" or market["type"] != "PERPETUAL":
            raise RuntimeError(f"[ARCUS] market unavailable: {market['marketDisplayName']}")
        if market["quoteAsset"] != "USD":
            raise RuntimeError("[ARCUS] only USD perpetuals are supported")
        self.market_id = int(market["marketId"])
        self.market_name = market["marketDisplayName"]
        self.tick_size = Decimal(market["tickSize"])
        self.step_size = Decimal(market["stepSize"])
        if not (self.tick_size.is_finite() and self.tick_size > 0
                and self.step_size.is_finite() and self.step_size > 0):
            raise RuntimeError("[ARCUS] invalid market tick/step sizes")
        # The shared engine rounds base quantities to 10^-size_decimals.
        # Reject any non-decimal-power grid rather than hedge unequal sizes.
        if (self.step_size > 1
                or self.step_size.normalize().as_tuple().digits != (1,)):
            raise RuntimeError("[ARCUS] this stepSize is unsupported by the shared engine")
        self.size_decimals = max(0, -self.step_size.normalize().as_tuple().exponent)
        self.tick_tiers = market["tickTiers"]
        self.min_base = float(market["minOrderSize"])
        self.min_quote = float(market["minOrderNotional"])
        self.max_base = float(market["maxOrderSize"])
        if not (all(math.isfinite(n) for n in
                    (self.min_base, self.max_base, self.min_quote))
                and 0 < self.min_base <= self.max_base and self.min_quote >= 0):
            raise RuntimeError("[ARCUS] invalid market order limits")
        log.info("[ARCUS] %s market_id=%d tick=%s step=%s min_base=%g "
                 "max_base=%g min_quote=%g network=%s",
                 self.market_name, self.market_id, self.tick_size, self.step_size,
                 self.min_base, self.max_base, self.min_quote, self.conf.arcus_network)

    def init_signer(self) -> None:
        creds = self.conf.arcus_creds
        if not creds or not creds.complete:
            raise RuntimeError("[ARCUS] configure ARCUS_ACCOUNT_ADDRESS and "
                               "ARCUS_API_SIGNING_KEY in .env")
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", creds.account_address):
            raise RuntimeError("[ARCUS] invalid ARCUS_ACCOUNT_ADDRESS")
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            from cryptography.hazmat.primitives import serialization
        except ImportError as e:
            raise RuntimeError("[ARCUS] live signing needs cryptography; "
                               "install requirements-live.txt") from e
        seed = creds.signing_key.removeprefix("0x")
        # No error text may echo the private key.
        if not re.fullmatch(r"[0-9a-fA-F]{64}", seed):
            raise RuntimeError("[ARCUS] API Signing Key must be a 32-byte "
                               "Ed25519 seed (64 hex characters)")
        self._signer = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed))
        self._api_key = self._signer.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()

    def _tick_at(self, price: Decimal) -> Decimal:
        for tier in self.tick_tiers:
            upper = tier.get("upToPrice")
            if upper is None or price < Decimal(upper):
                return Decimal(tier["tick"])
        return self.tick_size

    def px_round(self, px: float, round_up: bool) -> float:
        price = Decimal(str(px))
        if not price.is_finite() or price <= 0:
            raise ValueError("[ARCUS] price must be finite and positive")
        rounding = ROUND_CEILING if round_up else ROUND_FLOOR
        # Rounding may cross a tier boundary. Recheck against the final band.
        for _ in range(len(self.tick_tiers) + 2):
            tick = self._tick_at(price)
            rounded = (price / tick).to_integral_value(rounding=rounding) * tick
            if rounded % self._tick_at(rounded) == 0:
                return float(rounded)
            price = rounded
        raise ValueError("[ARCUS] unable to round price to tick tier")

    def start_tasks(self, stop: asyncio.Event, notify, live: bool) -> list:
        self._send_lock = asyncio.Lock()
        return [asyncio.create_task(self._stream(stop, notify, live),
                                    name=f"arcus-{self.key}")]

    def ready_to_trade(self) -> bool:
        return (self._signer is not None and self._ws is not None
                and self._orders_ready and not self._unresolved_orders
                and time.monotonic() >= self._rate_limited_until)

    def _disconnect(self) -> None:
        self._ws = None
        self._orders_ready = False
        self._book_sequence = None
        self.book.clear()
        for fut in self._rpc_pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("Arcus WebSocket disconnected"))
        self._rpc_pending.clear()

    async def _stream(self, stop: asyncio.Event, notify, live: bool) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    self._ws = ws
                    self._rpc_id = 0
                    await ws.send(json.dumps({"type": "subscribe", "channel": "l2Orderbook",
                                              "id": self.market_name, "nLevels": 100}))
                    if live:
                        await ws.send(json.dumps({"type": "subscribe", "channel": "orders",
                                                  "id": self.address, "accountIndex": self.account_index,
                                                  "market": self.market_name}))
                    async for raw in ws:
                        self._handle_message(json.loads(raw), notify)
                        backoff = 1.0
                        if stop.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[ARCUS] stream unavailable: %s; retry in %.0fs", e, backoff)
            finally:
                self._disconnect()
                notify()
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2.0, 30.0)

    def _handle_message(self, msg: dict, notify) -> None:
        self.book.touch()  # transport liveness does not refresh book age
        rpc_id = msg.get("id")
        if isinstance(rpc_id, int) and rpc_id in self._rpc_pending:
            fut = self._rpc_pending[rpc_id]
            if not fut.done():
                fut.set_result(msg)
            return
        if msg.get("type") == "error":
            # Some protocol errors omit RPC id: reconnect and reconcile rather
            # than leave trading enabled after a refused account subscription.
            raise RuntimeError("Arcus protocol error: " + str(msg.get("message", "unknown")))
        channel, typ = msg.get("channel"), msg.get("type")
        if channel == "l2Orderbook" and msg.get("id") == self.market_name:
            if typ == "degraded":
                self.book.clear()
                self._book_sequence = None
                notify()
                return
            if typ not in ("subscribed", "channel_data"):
                return
            ob = msg.get("contents")
            if not isinstance(ob, dict) or "bids" not in ob or "asks" not in ob:
                raise ValueError("[ARCUS] book snapshot missing bids/asks")
            sequence = int(ob["lastSequenceId"])
            if self._book_sequence is not None and sequence < self._book_sequence:
                return  # an older snapshot cannot roll the book backwards
            # Full snapshots replace every level. Equal sequences are valid
            # refreshed snapshots of a quiet market, not missing deltas.
            levels = [[{"px": px, "sz": sz} for px, sz in ob[side]]
                      for side in ("bids", "asks")]
            self.book.apply_hl(levels)
            self._book_sequence = sequence
            notify()
        elif channel == "orders":
            if (str(msg.get("id", "")).lower() != self.address
                    or msg.get("accountIndex") != self.account_index
                    or msg.get("market") != self.market_name):
                return
            if typ == "degraded":
                self._orders_ready = False
                notify()
                return
            if typ not in ("subscribed", "channel_data"):
                return
            contents = msg.get("contents")
            if not isinstance(contents, dict):
                raise ValueError("[ARCUS] orders channel missing contents")
            if typ == "subscribed":
                if contents.get("isSnapshot") is not True or "lastSequenceId" not in contents:
                    raise ValueError("[ARCUS] incomplete orders snapshot")
                for key in ("openOrders", "recentClosedOrders"):
                    rows = contents.get(key) or []
                    for row in rows.values() if isinstance(rows, dict) else rows:
                        self._remember_order(row)
                self._orders_ready = True
            else:
                self._remember_order(contents)
            notify()

    def _remember_order(self, order: dict) -> None:
        if int(order.get("marketId", -1)) != self.market_id:
            return
        oid = order.get("orderId")
        if not oid:
            return
        old = self._orders.get(oid)
        if old:
            # Sequence numbers are per account, with legitimate gaps between
            # different markets. Check ordering per order; never require +1.
            if (order.get("sequenceNumber", 0) and old.get("sequenceNumber", 0)
                    and order["sequenceNumber"] < old["sequenceNumber"]):
                return
            if (order.get("updatedAt", 0) and old.get("updatedAt", 0)
                    and order["updatedAt"] < old["updatedAt"]):
                return
            if self._terminal_result(old) is not None:
                return  # our unique IOC orders can never reopen
            order = {**old, **order}
        self._orders[oid] = order
        self._orders.move_to_end(oid)
        if order.get("clientId"):
            self._clients[order["clientId"]] = oid
        while len(self._orders) > 1000:
            _, removed = self._orders.popitem(last=False)
            self._clients.pop(removed.get("clientId"), None)

    @staticmethod
    def _terminal_result(order: dict):
        status, state = order.get("status"), order.get("state")
        terminal = (status in TERMINAL_STATUSES or state in TERMINAL_STATUSES
                    or (state == "PARTIALLY_FILLED" and order.get("timeInForce") == "IOC"))
        if not terminal or order.get("cancelReason") == "MODIFY_CANCELED":
            return None
        if "filledSize" in order:
            filled = float(order["filledSize"])
        elif "originalSize" in order and "remainingSize" in order:
            filled = float(Decimal(order["originalSize"]) - Decimal(order["remainingSize"]))
        else:
            return None  # a terminal label alone cannot tell us how much filled
        if not math.isfinite(filled) or filled < 0:
            return None
        avg = float(order["avgFillPrice"]) if order.get("avgFillPrice") else None
        if filled > 0 and (avg is None or not math.isfinite(avg) or avg <= 0):
            return None  # query authoritative state instead of fabricating a fill price
        reason = order.get("rejectionReason")
        # Zero-fill IOC is an ordinary outcome, including REJECTED / IOC_CANCELED.
        err = reason if reason and reason not in {"IOC_CANCELED", "COULD_NOT_FILL"} else None
        return {"status": status or state, "filled_base": filled, "avg_px": avg,
                "err": err, "unresolved": False}

    def _place_request(self, *, is_buy: bool, qty: float, limit_px: float,
                       reduce_only: bool, client_id: str) -> dict:
        if self._signer is None or self.market_id < 0:
            raise ValueError("[ARCUS] signer/market not initialized")
        quantity = format(qty, f".{self.size_decimals}f")
        if not math.isclose(float(quantity), qty, rel_tol=1e-12, abs_tol=1e-15):
            raise ValueError("[ARCUS] quantity not on stepSize")
        price = str(limit_px)
        q, p = grid_int(quantity, self.step_size), grid_int(price, self.tick_size)
        if Decimal(price) % self._tick_at(Decimal(price)) != 0:
            raise ValueError("[ARCUS] price not on tick tier")
        if qty < self.min_base or qty > self.max_base:
            raise ValueError("[ARCUS] quantity outside market limits")
        if not reduce_only and qty * limit_px < self.min_quote:
            raise ValueError("[ARCUS] order below minOrderNotional")
        ts = time.time_ns()
        good_til_us = ts // 1000 + 40 * 86400 * 1_000_000
        payload = {"ad": self.address, "ai": self.account_index, "c": client_id,
                   "ct": ts, "g": good_til_us * 1000, "m": self.market_id,
                   "op": 1, "p": p, "q": q, "r": int(reduce_only),
                   "s": 0 if is_buy else 1, "t": 2, "v": 1}
        body = {"address": self.address, "accountIndex": self.account_index,
                "marketId": self.market_id, "orderSide": "BUY" if is_buy else "SELL",
                "orderType": "LIMIT", "timeInForce": "IOC",
                "quantity": quantity, "price": price, "reduceOnly": reduce_only,
                "goodTilTime": str(good_til_us), "clientId": client_id, "timestamp": ts}
        return {"type": "placeOrder", "payload": body, "apiKey": self._api_key,
                "timestamp": str(ts), "signature": self._signer.sign(canonical(payload)).hex()}

    async def _rpc(self, request: dict) -> dict:
        # Allocate IDs inside the send lock: wire order must be strictly increasing.
        async with self._send_lock:
            ws = self._ws
            if ws is None:
                raise ConnectionError("[ARCUS] WebSocket unavailable")
            self._rpc_id += 1
            rpc_id = self._rpc_id
            fut = asyncio.get_running_loop().create_future()
            self._rpc_pending[rpc_id] = fut
            try:
                await ws.send(json.dumps({"type": "post", "id": rpc_id, "request": request}))
            except BaseException:
                self._rpc_pending.pop(rpc_id, None)
                fut.cancel()
                raise
        try:
            return await asyncio.wait_for(fut, timeout=min(REQUEST_TIMEOUT_SEC, self.settle_timeout))
        finally:
            self._rpc_pending.pop(rpc_id, None)

    async def _lookup_order(self, client_id: str, order_id, sent_ns: int):
        params = {"address": self.address, "accountIndex": self.account_index}
        if order_id:
            row = await self._get("/v1/order/" + quote(order_id, safe=""), **params)
            self._remember_order(row)
        else:
            # No ACK may mean the request still reached the engine. Find our
            # unique client ID in the documented history; an empty page is NOT
            # proof of zero fills. Never submit the same order a second time.
            history = await self._get("/v1/orders", **params, market=self.market_name,
                                      limit=1000, **{"from": sent_ns // 1000 - 1_000_000})
            for row in history["orders"]:
                if row.get("clientId") == client_id:
                    self._remember_order(row)
                    break

    async def send_taker(self, *, is_buy: bool, qty: float, limit_px: float,
                         reduce_only: bool = False) -> dict:
        if not self.ready_to_trade():
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": "Arcus not ready (orders subscription/signing required)",
                    "unresolved": False}
        cid = uuid.uuid4().hex
        try:
            request = self._place_request(is_buy=is_buy, qty=qty, limit_px=limit_px,
                                          reduce_only=reduce_only, client_id=cid)
        except Exception:
            # Do not include request or credentials in logs/errors.
            log.exception("[ARCUS] order grid/signing validation failed")
            return {"status": "send-failed", "filled_base": 0.0, "avg_px": None,
                    "err": "Arcus local signing/grid validation failed", "unresolved": False}
        deadline = time.monotonic() + self.settle_timeout
        oid = None
        try:
            ack = await self._rpc(request)
            code = int(ack.get("status", 0))
            result = ack.get("result") or {}
            error = ack.get("error") or {}  # WS errors are outside result
            # Do not trust even a 200 enrichment as final; consume the order
            # lifecycle channel or query the documented authoritative order.
            oid = result.get("orderId")
            invalid_request = (code == 400 and error.get("errorType")
                               in {"InvalidRequest", "Tick", "OracleDeviation", "ReduceOnly",
                                   "OrderSizeTooLarge", "MarketPriceSlippageToleranceTooHigh"})
            transmission_failed = code == 502 and error.get("errorType") == "Transmission"
            if not oid and (code in (401, 403, 429, 503) or invalid_request or transmission_failed):
                # These documented gateway rejections did not accept the order.
                if code == 429:
                    retry_sec = max(0, int(error.get("retryAfterMs", 0))) / 1000
                    self._rate_limited_until = time.monotonic() + retry_sec
                return {"status": "rejected", "filled_base": 0.0, "avg_px": None,
                        "err": ("RATE_LIMITED: " if code == 429 else "")
                               + f"Arcus gateway rejected order ({code})",
                        "unresolved": False}
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # potentially submitted: resolve by client ID / order ID
        while True:
            oid = oid or self._clients.get(cid)
            order = self._orders.get(oid, {})
            settled = self._terminal_result(order)
            if settled is not None:
                return settled
            if time.monotonic() >= deadline:
                break
            try:
                await asyncio.wait_for(
                    self._lookup_order(cid, oid, int(request["timestamp"])),
                    timeout=max(0.001, deadline - time.monotonic()))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # failed reads / 404 / empty history do not prove rejection
            oid = oid or self._clients.get(cid)
            settled = self._terminal_result(self._orders.get(oid, {}))
            if settled is not None:
                return settled
            await asyncio.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
        self._unresolved_orders[cid] = (oid, int(request["timestamp"]))
        log.warning("[ARCUS] order confirmation timed out clientId=%s orderId=%s; "
                    "trading paused until final order state and position reconcile", cid, oid)
        return {"status": "timeout", "filled_base": 0.0, "avg_px": None,
                "err": None, "unresolved": True}

    async def fetch_position(self) -> float:
        # A position snapshot alone cannot rule out an accepted order that is
        # still in flight. Keep trading disabled until its lifecycle is final.
        for cid, (oid, sent_ns) in list(self._unresolved_orders.items()):
            oid = oid or self._clients.get(cid)
            if self._terminal_result(self._orders.get(oid, {})) is None:
                await self._lookup_order(cid, oid, sent_ns)
                oid = oid or self._clients.get(cid)
            if self._terminal_result(self._orders.get(oid, {})) is None:
                raise RuntimeError("[ARCUS] previous order outcome unresolved; trading paused")
        data = await self._get("/v1/positions", address=self.address,
                               accountIndex=self.account_index, market=self.market_name)
        row = data["positions"].get(str(self.market_id))
        size = float(row["size"]) if row else 0.0  # signed base size, not abs(side)
        if not math.isfinite(size):
            raise ValueError("[ARCUS] invalid position size")
        self._unresolved_orders.clear()
        return size

    async def fetch_equity(self):
        data = await self._get("/v1/account", address=self.address,
                               accountIndex=self.account_index)
        return float(data["equity"]), float(data["freeCollateral"])

    async def warm_http(self) -> None:
        data = await self._get("/v1/markets", market=self.market_name)
        for market in data.get("markets") or []:
            if market.get("marketDisplayName") != self.market_name:
                continue
            value = float(market.get("markPrice"))
            if math.isfinite(value) and value > 0:
                self.mark_price = value
            break

    async def close(self) -> None:
        ws = self._ws
        self._disconnect()
        if ws is not None:
            await ws.close()
