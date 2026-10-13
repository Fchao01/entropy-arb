"""Read-only three-venue market discovery and spread monitoring.

The trading engine intentionally remains a two-leg executor.  This module is
the observability side of the application: it discovers symbols shared by
Entropy (Hyperliquid ``io``), Lighter Robinhood and Arcus, subscribes to their
public books, and evaluates the same executable-price inequalities used by
the engine.  It never creates an account signer or submits an order.
"""
from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

from .config import ARCUS_ENDPOINTS, HLCreds, LighterCreds, LighterProfile, VenueConf
from .book import OrderBook
from .venue_arcus import ArcusVenue
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue
from .telegram import TelegramNotifier


HL_INFO = "https://api.hyperliquid.xyz/info"
RH_API = "https://api.rh.lighter.xyz"
RH_WS = "wss://api.rh.lighter.xyz/stream"
RH_PROFILE = LighterProfile("robinhood", RH_API, RH_WS, 466324)


def normalize_symbol(value: str) -> str:
    """Normalize display names without inventing aliases between assets."""
    value = str(value or "").upper().strip()
    if ":" in value:
        value = value.rsplit(":", 1)[1]
    for suffix in ("-USD", "-USDC", "-USDG"):
        if value.endswith(suffix):
            value = value[:-len(suffix)]
            break
    return value


def _decimal_step(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


async def _json(session: aiohttp.ClientSession, method: str, url: str,
                **kwargs) -> Any:
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.request(method, url, timeout=timeout, **kwargs) as response:
        response.raise_for_status()
        return await response.json()


async def discover_candidates(session: aiohttp.ClientSession) -> dict:
    """Discover symbols shared by any two public market lists."""
    async def lighter():
        data = await _json(session, "GET", RH_API + "/api/v1/orderBooks")
        result = {}
        for row in data.get("order_books") or []:
            if row.get("status") != "active":
                continue
            symbol = normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            try:
                decimals = int(row.get("supported_size_decimals"))
                step = _decimal_step(10 ** -decimals) if decimals >= 0 else None
            except (TypeError, ValueError, OverflowError):
                step = None
            result[symbol] = {
                "symbol": row.get("symbol"), "status": row.get("status"),
                "quote": "USDG", "step": step,
                "min_base": _decimal_step(row.get("min_base_amount")) or 0,
                "min_notional": _decimal_step(row.get("min_quote_amount")) or 0,
            }
        return result

    async def arcus():
        data = await _json(session, "GET", ARCUS_ENDPOINTS["mainnet"][0] + "/v1/markets")
        result = {}
        for row in data.get("markets") or []:
            if row.get("status") != "ONLINE" or row.get("type") != "PERPETUAL":
                continue
            if str(row.get("quoteAsset", "")).upper() != "USD":
                continue
            symbol = normalize_symbol(row.get("marketDisplayName"))
            if not symbol:
                continue
            result[symbol] = {
                "symbol": row.get("marketDisplayName"), "status": row.get("status"),
                "quote": "USD", "step": _decimal_step(row.get("stepSize")),
                "min_base": _decimal_step(row.get("minOrderSize")) or 0,
                "min_notional": _decimal_step(row.get("minOrderNotional")) or 0,
            }
        return result

    async def entropy():
        data = await _json(session, "POST", HL_INFO, json={"type": "meta", "dex": "io"})
        result = {}
        for row in data.get("universe") or []:
            if row.get("isDelisted"):
                continue
            raw = row.get("name") or ""
            symbol = normalize_symbol(raw)
            if not symbol:
                continue
            try:
                decimals = int(row.get("szDecimals"))
                step = _decimal_step(10 ** -decimals) if decimals >= 0 else None
            except (TypeError, ValueError, OverflowError):
                step = None
            result[symbol] = {
                "symbol": raw, "status": "ACTIVE", "quote": "USDC",
                "step": step, "min_base": step or 0, "min_notional": 0,
            }
        return result

    results = await asyncio.gather(lighter(), arcus(), entropy(), return_exceptions=True)
    names = ("lighter_rh", "arcus", "entropy")
    def error_text(error):
        status = getattr(error, "status", None)
        message = str(error) or repr(error)
        return f"HTTP {status}: {message}" if status else message
    errors = {name: error_text(value) for name, value in zip(names, results)
              if isinstance(value, Exception)}
    maps = {name: value for name, value in zip(names, results)
            if not isinstance(value, Exception)}
    common = {symbol for market in maps.values() for symbol in market
              if sum(symbol in value for value in maps.values()) >= 2}
    symbols = []
    for symbol in sorted(common):
        venues = {name: maps[name][symbol] for name in names
                  if name in maps and symbol in maps[name]}
        checks = {
            "all_active": True,
            "perpetual": True,
            "base_units_known": all(v["step"] is not None for v in venues.values()),
            "minimums_known": all(v["min_base"] > 0 for v in venues.values()),
            "rh_primary": "lighter_rh" in venues,
            "arcus_hedge": "arcus" in venues,
            "entropy_hedge": "entropy" in venues,
        }
        warnings = []
        if not checks["base_units_known"]:
            warnings.append("至少一个市场没有可验证的数量步长")
        if not checks["minimums_known"]:
            warnings.append("至少一个市场没有有效的最小下单数量")
        # Market APIs do not expose a universal contract multiplier.  We only
        # call this a candidate when all observable checks pass; the UI keeps
        # this limitation visible instead of claiming a mathematically perfect hedge.
        compatible = all(checks.values())
        symbols.append({
            "symbol": symbol, "compatible": compatible, "monitorable": True,
            "label": "三市场价差" if len(venues) == 3 else "双市场价差",
            "checks": checks, "warnings": warnings,
            "venues": {name: {k: v for k, v in value.items() if k != "status"}
                       for name, value in venues.items()},
        })
    return {
        "updated_at": time.time(),
        "symbols": symbols,
        "compatible_symbols": [row["symbol"] for row in symbols if row["compatible"]],
        "monitorable_symbols": [row["symbol"] for row in symbols],
        "market_counts": {name: len(value) for name, value in maps.items()},
        "errors": errors,
        "compatibility_basis": (
            "任意两个市场都有的币种即可监控，不按数量步长、最小下单数量或策略阈值筛选。"
            "行情价差不代表可直接交易，实盘仍需核对合约规格和费用。"
        ),
    }


@dataclass
class MonitorRules:
    entropy_fee_bps: float = 0.0
    rh_fee_bps: float = 0.0
    arcus_fee_bps: float = 0.0
    source: str = "脚本适配检查"
    primary_key: str = "rh"


def _net_edge(sell_bid: Optional[float], buy_ask: Optional[float],
              sell_fee_bps: float, buy_fee_bps: float) -> Optional[float]:
    if None in (sell_bid, buy_ask) or not buy_ask or buy_ask <= 0:
        return None
    # Same inequality as book.crossable_base, expressed as a displayed bps edge.
    value = (sell_bid * (1 - sell_fee_bps / 1e4)
             / (buy_ask * (1 + buy_fee_bps / 1e4)) - 1) * 1e4
    return value if math.isfinite(value) else None


class TripleVenueMonitor:
    """One read-only symbol monitor; lifecycle is owned by TaskManager."""

    def __init__(self, notifier=None):
        self.notifier = notifier
        self.session: Optional[aiohttp.ClientSession] = None
        self.stop_event: Optional[asyncio.Event] = None
        self.feed_tasks = []
        self.sample_task = None
        self.venues = {}
        self.market_specs = {}
        self.errors = {}
        self.symbol = None
        self.rules = MonitorRules()
        self.running = False
        self.updated_at = 0.0
        self.alert_state = {}
        self.last_alert = {}
        self.notify_tasks = set()


    @staticmethod
    def _conf(key, kind, label, symbol, *, fee=0.0, dex="io") -> VenueConf:
        if kind == "hl":
            return VenueConf(key=key, kind=kind, label=label, symbol=symbol,
                             fee_bps=fee, cap_usd=0, orders_per_min=0,
                             hl_dex=dex, hl_creds=HLCreds(None, None))
        if kind == "lighter":
            return VenueConf(key=key, kind=kind, label=label, symbol=symbol,
                             fee_bps=fee, cap_usd=0, orders_per_min=0,
                             lighter_profile=RH_PROFILE,
                             lighter_creds=LighterCreds(None, None, None))
        return VenueConf(key=key, kind=kind, label=label, symbol=symbol,
                         fee_bps=fee, cap_usd=0, orders_per_min=0,
                         arcus_network="mainnet", arcus_creds=None)

    async def start(self, candidate: dict, rules: MonitorRules) -> dict:
        await self.stop()
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ttl_dns_cache=300))
        self.stop_event = asyncio.Event()
        self.symbol, self.rules = candidate["symbol"], rules
        sources = candidate["venues"]
        source_keys = {"entropy": "entropy", "rh": "lighter_rh", "arcus": "arcus"}
        self.market_specs = {key: sources[source_key] for key, source_key in source_keys.items()
                             if source_key in sources}
        self.errors = {}
        try:
            if "entropy" in sources:
                self.venues["entropy"] = HLVenue(
                    self._conf("entropy", "hl", "ENTROPY", sources["entropy"]["symbol"],
                               fee=rules.entropy_fee_bps),
                    "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws", self.session, 5.0)
            if "lighter_rh" in sources:
                self.venues["rh"] = LighterVenue(
                    self._conf("rh", "lighter", "RH", sources["lighter_rh"]["symbol"],
                               fee=rules.rh_fee_bps), self.session, 5.0)
            if "arcus" in sources:
                self.venues["arcus"] = ArcusVenue(
                    self._conf("arcus", "arcus", "ARCUS", sources["arcus"]["symbol"],
                               fee=rules.arcus_fee_bps), self.session, 5.0)
            results = await asyncio.gather(*(venue.load_market() for venue in self.venues.values()),
                                           return_exceptions=True)
            for key, result in zip(list(self.venues), results):
                if isinstance(result, Exception):
                    self.errors[key] = str(result) or repr(result)
                    await self.venues.pop(key).close()
            if len(self.venues) < 2:
                raise RuntimeError("至少需要两个有效市场才能监控价差：" + str(self.errors))
            for venue in self.venues.values():
                self.feed_tasks.extend(venue.start_tasks(self.stop_event, self._touch, False))
            self.sample_task = asyncio.create_task(self._sample_loop(), name="triple-monitor-sampler")
            self.running = True
            self.updated_at = time.time()
            return self.snapshot()
        except Exception:
            await self.stop()
            raise

    def _touch(self):
        self.updated_at = time.time()

    async def _sample_loop(self):
        while self.stop_event and not self.stop_event.is_set():
            self._touch()
            self._check_alerts()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    def _pair(self, left_key: str, right_key: str, script_compatible: bool) -> dict:
        primary = self.rules.primary_key
        if right_key == primary:
            left_key, right_key = right_key, left_key
        left, right = self.venues[left_key], self.venues[right_key]
        fresh = left.book.is_fresh(10) and right.book.is_fresh(10)
        left_mid, right_mid = (left.book.mid(), right.book.mid()) if fresh else (None, None)
        mid = ((left_mid / right_mid) - 1) * 1e4 if left_mid and right_mid else None
        sell = _net_edge(left.book.best_bid(), right.book.best_ask(),
                          left.fee_bps, right.fee_bps) if fresh else None
        buy = _net_edge(right.book.best_bid(), left.book.best_ask(),
                        right.fee_bps, left.fee_bps) if fresh else None
        return {
            "key": f"{left_key}-{right_key}", "left": left.name, "right": right.name,
            "mid_spread_bps": mid, "sell_left_buy_right_bps": sell,
            "buy_left_sell_right_bps": buy, "script_compatible": script_compatible,
        }

    def snapshot(self) -> dict:
        pairs = []
        if self.venues:
            for left_key, right_key in (("entropy", "rh"), ("entropy", "arcus"), ("rh", "arcus")):
                if left_key not in self.venues or right_key not in self.venues:
                    continue
                specs_known = not self.market_specs or all(
                    self.market_specs[key].get("step") is not None
                    and self.market_specs[key].get("min_base", 0) > 0
                    for key in (left_key, right_key))
                compatible = specs_known and self.rules.primary_key in (left_key, right_key)
                pairs.append(self._pair(left_key, right_key, compatible))
        venues = []
        for key, venue in self.venues.items():
            book = venue.book
            venues.append({"key": key, "name": venue.name, "symbol": venue.conf.symbol,
                           "bid": book.best_bid(), "ask": book.best_ask(), "mid": book.mid(),
                           "fresh": book.is_fresh(10),
                           "age_sec": time.time() - book.last_update_ts if book.ready else None})
        return {"running": self.running, "symbol": self.symbol, "updated_at": self.updated_at,
                "rules": self.rules.__dict__, "venues": venues, "pairs": pairs,
                "errors": self.errors}

    def _check_alerts(self):
        if not self.notifier or not self.notifier.enabled:
            return
        now = time.time()
        for pair in self.snapshot()["pairs"]:
            for direction, edge_key in (("卖左买右", "sell_left_buy_right_bps"),
                                        ("买左卖右", "buy_left_sell_right_bps")):
                edge = pair[edge_key]
                # This is only a notification condition. Candidate
                # compatibility above never depends on a threshold.
                active = pair["script_compatible"] and edge is not None and edge > 0
                key = pair["key"] + ":" + direction
                if not active:
                    self.alert_state[key] = False
                    continue
                if self.alert_state.get(key) and now - self.last_alert.get(key, 0) < 60:
                    continue
                self.alert_state[key] = True
                self.last_alert[key] = now
                text = (f"📊 价差信号 · {self.symbol}\n"
                        f"路径：{pair['left']} ⇄ {pair['right']} · {direction}\n"
                        f"可成交净价差：{edge:+.2f} bps（扣除当前配置费率后为正）\n"
                        "只读行情提示，不代表实盘利润；下单前仍需核对合约规格")
                task = asyncio.create_task(self.notifier.send(text), name="telegram-spread-alert")
                self.notify_tasks.add(task)
                task.add_done_callback(self.notify_tasks.discard)

    async def stop(self):
        if self.stop_event:
            self.stop_event.set()
        tasks = list(self.feed_tasks)
        if self.sample_task:
            tasks.append(self.sample_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.notify_tasks:
            await asyncio.gather(*self.notify_tasks, return_exceptions=True)
        for venue in self.venues.values():
            try:
                await venue.close()
            except Exception:
                pass
        if self.session:
            await self.session.close()
        if self.notifier is not None:
            await self.notifier.close()
        self.session = None
        self.feed_tasks = []
        self.sample_task = None
        self.stop_event = None
        self.venues = {}
        self.running = False


class MultiVenueMonitor:
    """Run isolated read-only three-venue monitors for an overview table."""

    def __init__(self, notifier=None):
        self.notifier = notifier
        self.monitors: dict[str, TripleVenueMonitor] = {}
        self.errors: dict[str, str] = {}
        self.rules = MonitorRules()
        self.running = False
        self.selected_symbol = None

    async def start(self, candidate: dict, rules: MonitorRules):
        return await self.start_all([candidate], rules)

    async def start_all(self, candidates: list[dict], rules: MonitorRules):
        await self.stop()
        self.rules = rules
        self.errors = {}
        selected = [row for row in candidates if len(row.get("venues", {})) >= 2]
        semaphore = asyncio.Semaphore(4)

        async def launch(candidate):
            symbol = candidate["symbol"]
            token = getattr(self.notifier, "token", "") if self.notifier else ""
            chat_id = getattr(self.notifier, "chat_id", "") if self.notifier else ""
            child = TripleVenueMonitor(TelegramNotifier(token=token, chat_id=chat_id)
                                       if self.notifier else None)
            try:
                async with semaphore:
                    await child.start(candidate, rules)
                self.monitors[symbol] = child
                for key, message in child.errors.items():
                    self.errors[f"{symbol}/{key}"] = message
            except Exception as error:
                self.errors[symbol] = str(error) or repr(error)
                await child.stop()

        await asyncio.gather(*(launch(candidate) for candidate in selected))
        self.running = bool(self.monitors)
        self.selected_symbol = next(iter(self.monitors), None)
        return self.snapshot()

    def snapshot(self):
        rows = {symbol: monitor.snapshot() for symbol, monitor in self.monitors.items()}
        selected = rows.get(self.selected_symbol)
        return {"running": self.running, "updated_at": time.time(),
                "rules": self.rules.__dict__, "symbols": rows,
                "errors": self.errors, "selected_symbol": self.selected_symbol,
                "selected": selected, "count": len(rows)}

    async def stop(self):
        if self.monitors:
            await asyncio.gather(*(monitor.stop() for monitor in self.monitors.values()),
                                 return_exceptions=True)
        self.monitors = {}
        self.running = False
        self.selected_symbol = None
