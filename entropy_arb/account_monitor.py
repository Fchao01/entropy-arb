"""Public RH/Core account observation. No credentials, signers, or order APIs.

The database contains observed fills, not an exchange-wide account directory or
an audited PnL ledger. Quotes are attached strictly as-of the fill timestamp.
SQLite work runs in a thread so aggregation cannot block the trading console.
"""
from __future__ import annotations

import asyncio
import json
import math
import random
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from contextlib import closing
from pathlib import Path

import aiohttp

from .config import LIGHTER_PROFILES

VENUES = ("lighter-rh", "lighter")
LABELS = {"lighter-rh": "RH", "lighter": "Lighter"}
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def finite(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def timestamp(value):
    value = finite(value)
    if value is None or value <= 0:
        return None
    if value >= 1e17:
        value /= 1e9
    elif value >= 1e14:
        value /= 1e6
    elif value >= 1e11:
        value /= 1e3
    return value


def account_id(value):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]{1,16}", str(value)):
        raise ValueError("账户编号必须是非负整数")
    result = int(value)
    if result > 2**48 - 1:
        raise ValueError("账户编号超出范围")
    return result


def symbols_config(value):
    if not isinstance(value, str) or len(value) > 4000:
        raise ValueError("监控币种格式无效")
    value = value.strip().upper()
    if value == "*":
        return []
    values = list(dict.fromkeys(re.split(r"[,，\s]+", value)))
    if not values or any(not re.fullmatch(r"[A-Z0-9][A-Z0-9_.:-]{0,39}", s) for s in values):
        raise ValueError("填写币种，例如 BTC,ETH；或 * 监控两边全部活跃永续市场")
    return values


def _symbol(value):
    value = str(value).upper()
    for suffix in ("-USDC", "-USDG", "-USD"):
        if value.endswith(suffix):
            return value[:-len(suffix)]
    return value


class AccountStore:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=5000;
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS accounts (
                venue TEXT NOT NULL, id INTEGER NOT NULL, address TEXT,
                watched INTEGER NOT NULL DEFAULT 0, fetched REAL NOT NULL DEFAULT 0,
                attempted REAL NOT NULL DEFAULT 0, error TEXT,
                snapshot TEXT, PRIMARY KEY(venue,id));
            CREATE INDEX IF NOT EXISTS account_address ON accounts(address);
            CREATE TABLE IF NOT EXISTS trades (
                venue TEXT NOT NULL, market INTEGER NOT NULL, id TEXT NOT NULL,
                symbol TEXT NOT NULL, ts REAL NOT NULL, received REAL NOT NULL,
                price REAL NOT NULL, size REAL NOT NULL, usd REAL NOT NULL,
                ask INTEGER NOT NULL, bid INTEGER NOT NULL, maker_ask INTEGER NOT NULL,
                kind TEXT NOT NULL, bps REAL, ask_before REAL, bid_before REAL,
                PRIMARY KEY(venue,market,id));
            CREATE INDEX IF NOT EXISTS trade_time ON trades(ts);
            CREATE INDEX IF NOT EXISTS trade_ask ON trades(venue,ask,ts);
            CREATE INDEX IF NOT EXISTS trade_bid ON trades(venue,bid,ts);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY, ts REAL NOT NULL, venue TEXT, kind TEXT NOT NULL,
                message TEXT NOT NULL);
            CREATE VIEW IF NOT EXISTS fills AS
              SELECT venue,market,id,symbol,ts,price,size,usd,ask AS account,
                     -1 AS side,maker_ask AS maker,kind,bps,ask_before AS before_position
              FROM trades
              UNION ALL
              SELECT venue,market,id,symbol,ts,price,size,usd,bid AS account,
                     1 AS side,1-maker_ask AS maker,kind,bps,bid_before AS before_position
              FROM trades;
        """)
        self.db.commit()
        path.chmod(0o600)

    def settings(self):
        with self.lock:
            return {r["key"]: json.loads(r["value"]) for r in self.db.execute("SELECT * FROM settings")}

    def setting(self, key, value):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, json.dumps(value)))

    def event(self, venue, kind, message):
        with self.lock, self.db:
            self.db.execute("INSERT INTO events(ts,venue,kind,message) VALUES (?,?,?,?)",
                            (time.time(), venue, kind, str(message)[:500]))

    def ingest(self, rows):
        with self.lock, self.db:
            before = self.db.total_changes
            self.db.executemany("INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            added = self.db.total_changes - before
            self.db.executemany("INSERT OR IGNORE INTO accounts(venue,id) VALUES (?,?)",
                                ((r[0], a) for r in rows for a in (r[9], r[10])))
            return added

    def save_account(self, venue, aid, data=None, error=None):
        now = time.time()
        address = str((data or {}).get("l1_address", "")).lower()
        address = address if ADDRESS.fullmatch(address) and int(address, 16) else None
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO accounts(venue,id) VALUES (?,?)", (venue, aid))
            if data is not None:
                self.db.execute("UPDATE accounts SET address=?,snapshot=?,fetched=?,attempted=?,error=NULL "
                                "WHERE venue=? AND id=?",
                                (address, json.dumps(data, allow_nan=False), now, now, venue, aid))
            else:
                self.db.execute("UPDATE accounts SET attempted=?,error=? WHERE venue=? AND id=?",
                                (now, str(error)[:300], venue, aid))

    def watch(self, venue, aid, watched):
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO accounts(venue,id) VALUES (?,?)", (venue, aid))
            self.db.execute("UPDATE accounts SET watched=? WHERE venue=? AND id=?", (int(watched), venue, aid))

    def link_account(self, venue, aid, address):
        # An address-directory response is not a fresh balance/position snapshot.
        with self.lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO accounts(venue,id) VALUES (?,?)", (venue, aid))
            self.db.execute("UPDATE accounts SET address=? WHERE venue=? AND id=?", (address, venue, aid))

    def reader(self):
        db = sqlite3.connect(str(self.path), timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        return db

    def next_account(self, venue):
        with self.lock:
            row = self.db.execute("""SELECT id FROM accounts WHERE venue=? AND attempted<?
                AND (watched=1 OR fetched=0) ORDER BY watched DESC,attempted,id LIMIT 1""",
                                  (venue, time.time() - 120)).fetchone()
            return row[0] if row else None

    def overview(self, since, venue="all", search="", page=1):
        where, args = "ts>=?", [since]
        if venue != "all":
            where += " AND venue=?"
            args.append(venue)
        with closing(self.reader()) as db:
            summary = dict(db.execute(
                "SELECT COUNT(*) trades,COALESCE(SUM(usd),0) market_volume,MIN(ts) first_trade,MAX(ts) last_trade "
                "FROM trades WHERE " + where, args).fetchone())
            query = """SELECT f.venue,f.account AS id,COUNT(*) trade_count,SUM(f.usd) volume,
                SUM(CASE WHEN f.side=1 THEN f.usd ELSE 0 END) buy_volume,
                SUM(CASE WHEN f.maker=1 THEN f.usd ELSE 0 END)/SUM(f.usd)*100 maker_pct,
                MIN(f.ts) first_trade,MAX(f.ts) last_trade,COUNT(DISTINCT f.symbol) symbols,
                a.address,a.watched,a.fetched,a.error
                FROM fills f JOIN accounts a ON a.venue=f.venue AND a.id=f.account WHERE """
            condition = "f.ts>=?"
            params = [since]
            if venue != "all":
                condition += " AND f.venue=?"
                params.append(venue)
            if search:
                condition += " AND (CAST(f.account AS TEXT) LIKE ? ESCAPE '\\' OR a.address LIKE ? ESCAPE '\\')"
                escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                params.extend(["%" + escaped + "%"] * 2)
            query += condition + " GROUP BY f.venue,f.account"
            total = db.execute("SELECT COUNT(*) FROM (" + query + ")", params).fetchone()[0]
            rows = [dict(r) for r in db.execute(query + " ORDER BY volume DESC,f.venue,f.account LIMIT 50 OFFSET ?",
                                                    params + [(page - 1) * 50])]
            summary["accounts"] = db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            events = [dict(r) for r in db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 20")]
            watches = [dict(r) for r in db.execute("SELECT venue,id FROM accounts WHERE watched=1 ORDER BY venue,id")]
            summary["gaps"] = db.execute("SELECT COUNT(*) FROM events WHERE kind IN ('gap','overflow','invalid')").fetchone()[0]
            return dict(summary=summary, accounts=rows, total=total, page=page, events=events, watches=watches)

    def detail(self, venue, aid, since):
        with closing(self.reader()) as db:
            account = db.execute("SELECT * FROM accounts WHERE venue=? AND id=?", (venue, aid)).fetchone()
            if not account:
                return None
            account = dict(account)
            account["snapshot"] = json.loads(account["snapshot"]) if account["snapshot"] else None
            fills = [dict(r) for r in db.execute(
                "SELECT * FROM fills WHERE venue=? AND account=? AND ts>=? "
                "ORDER BY ts DESC,market,CAST(id AS INTEGER) DESC LIMIT 2001", (venue, aid, since))]
            truncated = len(fills) > 2000
            fills = fills[:2000]
            peers = []
            if account["address"]:
                peers = [dict(r) for r in db.execute(
                    "SELECT venue,id,address,fetched FROM accounts WHERE address=? AND venue!=?",
                    (account["address"], venue))]
            peer_fills = []
            if account["address"]:
                peer_fills = [dict(r) for r in db.execute(
                    "SELECT f.* FROM fills f JOIN accounts a ON a.venue=f.venue AND a.id=f.account "
                    "WHERE a.address=? AND f.venue!=? AND f.ts>=? ORDER BY f.ts DESC LIMIT 2001",
                    (account["address"], venue, since))]
            boundaries = [r[0] for r in db.execute(
                "SELECT ts FROM events WHERE ts>=? AND (venue=? OR venue IS NULL) "
                "AND kind IN ('gap','overflow','invalid','start','stop')", (since, venue))]
            return dict(account=account, fills=fills[:100], analysis_fills=len(fills), truncated=truncated,
                        peers=peers, patterns=patterns(fills), cycles=observed_cycles(fills, boundaries),
                        matches=hedge_matches(fills, peer_fills[:2000]), peer_truncated=len(peer_fills) > 2000)

    def close(self):
        with self.lock:
            self.db.close()


def patterns(fills):
    """Observable tendencies only, never definitive strategy classifications."""
    if not fills:
        return []
    volume = sum(r["usd"] for r in fills)
    maker = sum(r["usd"] for r in fills if r["maker"]) / volume
    buys = sum(r["usd"] for r in fills if r["side"] == 1) / volume
    result = []
    if len(fills) >= 20 and maker >= .8 and .3 <= buys <= .7:
        result.append(dict(label="疑似双向挂单交易", evidence=f"样本 {len(fills)} 笔；Maker 金额占比 {maker:.1%}，买入占比 {buys:.1%}"))
    if len(fills) >= 10 and max(buys, 1 - buys) >= .9:
        result.append(dict(label="样本内偏单向成交", evidence=f"买入金额占比 {buys:.1%}；可能是建仓、平仓或在其他平台对冲"))
    return result


def observed_cycles(fills, boundaries=()):
    """Only start a round trip when the exchange reports a zero pre-fill position.

    A missing/inconsistent pre-fill position invalidates the round trip. These
    are single-venue cash differences, not net cross-venue strategy profits.
    """
    active, complete = {}, []
    for row in sorted(fills, key=lambda r: (r["ts"], r["market"], int(r["id"]))):
        key = row["symbol"]
        before = row["before_position"]
        tolerance = max(1e-10, row["size"] * 1e-7)
        if before is None:
            active.pop(key, None)
            continue
        cycle = active.get(key)
        if cycle and abs(cycle["position"] - before) > tolerance:
            cycle = None
            active.pop(key, None)
        if cycle is None and abs(before) <= tolerance:
            cycle = dict(symbol=key, started_at=row["ts"], cash=0.0, position=0.0,
                         count=0, entry_bps=row["bps"], exit_bps=None)
            active[key] = cycle
        if cycle is None:
            continue
        after = before + row["side"] * row["size"]
        if before * after < -tolerance * tolerance:
            active.pop(key, None)  # Reversal is not an observed flat boundary.
            continue
        cycle["cash"] -= row["side"] * row["price"] * row["size"]
        cycle["count"] += 1
        cycle["position"] = after
        if abs(after) <= tolerance:
            cycle.update(ended_at=row["ts"], hold_sec=row["ts"] - cycle["started_at"], exit_bps=row["bps"])
            if not any(cycle["started_at"] <= boundary <= row["ts"] for boundary in boundaries):
                complete.append(cycle)
            active.pop(key, None)
    return sorted(complete, key=lambda r: r["ended_at"], reverse=True)[:50]


def hedge_matches(fills, peer_fills, seconds=3.0):
    """Greedy, one-to-one candidates among same-address opposite-venue fills.

    Excludes liquidation and matches only comparable base quantities; split
    fills and different-wallet hedges may remain unmatched rather than guessed.
    """
    candidates, used = [], set()
    for row in sorted(fills, key=lambda r: r["ts"], reverse=True):
        if row["kind"] != "trade":
            continue
        options = [p for p in peer_fills
                   if (p["venue"], p["market"], p["id"], p["account"]) not in used
                   and p["symbol"] == row["symbol"] and p["side"] == -row["side"]
                   and p["kind"] == "trade" and abs(p["ts"] - row["ts"]) <= seconds
                   and abs(p["size"] - row["size"]) / max(p["size"], row["size"]) <= .05]
        if not options:
            continue
        peer = min(options, key=lambda p: abs(p["ts"] - row["ts"]))
        used.add((peer["venue"], peer["market"], peer["id"], peer["account"]))
        rh, core = (row, peer) if row["venue"] == "lighter-rh" else (peer, row)
        candidates.append(dict(symbol=row["symbol"], ts=row["ts"], account=row["account"],
                               peer_venue=peer["venue"], peer_account=peer["account"],
                               side=row["side"], size=row["size"], peer_size=peer["size"],
                               delay_ms=(peer["ts"] - row["ts"]) * 1000,
                               quote_bps=row["bps"], fill_bps=(rh["price"] / core["price"] - 1) * 1e4))
    return candidates[:50]


class AccountMonitor:
    def __init__(self, path: Path):
        self.store = AccountStore(path)
        self.running = False
        self.session = None
        self.tasks = []
        self.guard = asyncio.Lock()
        self.http_locks = {v: asyncio.Lock() for v in VENUES}
        self.http_next = defaultdict(float)
        self.send_locks = {v: asyncio.Lock() for v in VENUES}
        self.send_next = defaultdict(float)
        self.markets = {}
        self.quotes = defaultdict(lambda: deque(maxlen=600))
        self.connections = {}
        self.errors = {}
        self.queue = asyncio.Queue(maxsize=20000)
        self.dropped = 0
        self.skipped = 0
        self.inserted = 0
        self.started_at = None
        self.config = {"symbols": "BTC,ETH", "recent": False}
        self.backfill = {"done": 0, "total": 0}

    async def request(self, venue, path, **params):
        if path not in ("orderBooks", "account", "accountsByL1Address", "recentTrades"):
            raise ValueError("不是允许的公开查询接口")
        async with self.http_locks[venue]:
            await asyncio.sleep(max(0, self.http_next[venue] - time.monotonic()))
            self.http_next[venue] = time.monotonic() + 4
            url = LIGHTER_PROFILES[venue].api_url + "/api/v1/" + path
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as response:
                if response.status in (429, 405):
                    self.http_next[venue] = time.monotonic() + 60
                response.raise_for_status()
                data = await response.json()
                if data.get("code", 200) != 200:
                    if data.get("code") == 23000:
                        self.http_next[venue] = time.monotonic() + 60
                    raise ValueError(f"公开接口返回 {data.get('code')}：{str(data.get('message', ''))[:160]}")
                return data

    async def start(self, config):
        wanted = symbols_config(config.get("symbols", "BTC,ETH"))
        recent = config.get("recent", False)
        if not isinstance(recent, bool):
            raise ValueError("近期成交补充选项必须是布尔值")
        async with self.guard:
            await asyncio.to_thread(self.store.setting, "enabled", False)
            await self._stop()
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=8, ttl_dns_cache=300), trust_env=True,
                timeout=aiohttp.ClientTimeout(total=15, sock_connect=8))
            self.config = {"symbols": ",".join(wanted) if wanted else "*", "recent": recent}
            self.errors, self.connections, self.markets = {}, {}, {}
            self.quotes.clear()
            results = await asyncio.gather(*(self.request(v, "orderBooks") for v in VENUES), return_exceptions=True)
            for venue, result in zip(VENUES, results):
                if isinstance(result, Exception):
                    self.errors[venue] = str(result) or type(result).__name__
                    continue
                self.markets[venue] = {}
                for row in result.get("order_books") or []:
                    try:
                        symbol, mid = _symbol(row["symbol"]), int(row["market_id"])
                    except (KeyError, TypeError, ValueError):
                        self.errors["markets/" + venue] = "忽略了无法解析的市场元数据"
                        continue
                    if (row.get("status") == "active" and row.get("market_type") == "perp"
                            and finite(row.get("multiplier", 1)) == 1 and (not wanted or symbol in wanted)):
                        self.markets[venue][mid] = symbol
            missing = set(wanted) - {s for markets in self.markets.values() for s in markets.values()}
            if missing:
                self.errors["symbols"] = "未找到活跃单位乘数永续市场：" + ",".join(sorted(missing))
            if not any(self.markets.values()):
                await self._stop()
                raise ValueError("没有找到所选活跃永续市场；" + "；".join(self.errors.values()))
            self.running = True
            self.started_at = time.time()
            self.backfill = {"done": 0, "total": sum(len(m) for m in self.markets.values()) if recent else 0}
            self.tasks = [asyncio.create_task(self._writer()), asyncio.create_task(self._enrich())]
            for venue, markets in self.markets.items():
                ids = list(markets)
                for offset in range(0, len(ids), 80):
                    self.tasks.append(asyncio.create_task(self._stream(venue, ids[offset:offset + 80], offset // 80)))
                if recent:
                    self.tasks.append(asyncio.create_task(self._backfill(venue)))
            await asyncio.to_thread(self.store.setting, "config", self.config)
            await asyncio.to_thread(self.store.setting, "enabled", True)
            await asyncio.to_thread(self.store.event, None, "start", "开始公开监控：" + self.config["symbols"])
        return self.status()

    async def resume(self):
        settings = await asyncio.to_thread(self.store.settings)
        self.config = settings.get("config", self.config)
        if settings.get("enabled"):
            await asyncio.to_thread(self.store.event, None, "gap", "恢复上次启用的监控；控制台离线期间未采集")
            try:
                await self.start(self.config)
            except Exception as error:
                self.errors["startup"] = str(error)

    async def _stop(self):
        self.running = False
        tasks, self.tasks = self.tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        pending = []
        while not self.queue.empty():
            pending.append(self.queue.get_nowait())
        if pending:
            await asyncio.to_thread(self.store.ingest, pending)
        await self._record_drops()
        if self.session is not None:
            await self.session.close()
            self.session = None
        self.connections.clear()

    async def stop(self):
        async with self.guard:
            await self._stop()
            await asyncio.to_thread(self.store.setting, "enabled", False)
            await asyncio.to_thread(self.store.event, None, "stop", "用户停止监控；历史数据保留")
        return self.status()

    async def close(self):
        async with self.guard:
            was_running = self.running
            await self._stop()
            if was_running:
                await asyncio.to_thread(self.store.event, None, "gap", "控制台退出；下次启动前的数据未采集")
            await asyncio.to_thread(self.store.close)

    def status(self):
        return dict(running=self.running, started_at=self.started_at, config=self.config,
                    markets={v: len(m) for v, m in self.markets.items()},
                    connections=dict(self.connections), errors=dict(self.errors), dropped=self.dropped,
                    inserted=self.inserted, queue_size=self.queue.qsize(), backfill=dict(self.backfill), skipped=self.skipped)

    async def _record_drops(self):
        if self.dropped:
            count, self.dropped = self.dropped, 0
            self.skipped += count
            await asyncio.to_thread(self.store.event, None, "overflow", f"无效数据或队列满，跳过 {count} 条；统计不完整")

    async def _backfill(self, venue):
        for market in self.markets.get(venue, {}):
            try:
                data = await self.request(venue, "recentTrades", market_id=market, limit=100)
                self.consume(venue, {"channel": f"trade:{market}", "trades": data.get("trades", [])})
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.errors["recent/" + venue] = str(error)[:300]
                await asyncio.to_thread(self.store.event, venue, "gap", f"市场 {market} 近期成交补充失败：{error}")
            finally:
                self.backfill["done"] += 1

    async def _stream(self, venue, markets, group):
        key = f"{venue}/{group}"
        delay = 1
        while self.running:
            self.connections[key] = "连接中"
            try:
                async with self.session.ws_connect(LIGHTER_PROFILES[venue].ws_url + "?readonly=true",
                                                   heartbeat=25, receive_timeout=75) as ws:
                    async def subscribe():
                        for market in markets:
                            for channel in ("ticker", "trade"):
                                async with self.send_locks[venue]:
                                    await asyncio.sleep(max(0, self.send_next[venue] - time.monotonic()))
                                    self.send_next[venue] = time.monotonic() + .5
                                    await ws.send_json({"type": "subscribe", "channel": f"{channel}/{market}"})
                        self.connections[key] = "已订阅"

                    sender = asyncio.create_task(subscribe())
                    try:
                        self.connections[key] = "订阅中"
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(message.data)
                                if data.get("type") == "ping":
                                    await ws.send_json({"type": "pong"})
                                elif data.get("type") == "error" or data.get("error"):
                                    raise ValueError(str(data)[:300])
                                else:
                                    self.consume(venue, data)
                                self.errors.pop(key, None)
                                delay = 1
                            elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                        if sender.done():
                            sender.result()
                    finally:
                        sender.cancel()
                        await asyncio.gather(sender, return_exceptions=True)
                raise ConnectionError("公开行情流已断开")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.errors[key] = str(error)[:300]
                self.connections[key] = "重连等待"
                await asyncio.to_thread(self.store.event, venue, "gap", f"{key}: {error}")
                await asyncio.sleep(delay + random.random())
                delay = min(60, delay * 2)

    def premium_at(self, symbol, ts):
        values = []
        for venue in VENUES:
            quote = next((q for q in reversed(self.quotes[(venue, symbol)]) if q[0] <= ts), None)
            if quote is None or ts - quote[0] > 3 or time.time() - quote[2] > 10:
                return None
            values.append(quote[1])
        return (values[0] / values[1] - 1) * 1e4

    def consume(self, venue, data):
        channel = str(data.get("channel", "")).replace("/", ":").split(":")
        if len(channel) != 2 or not channel[1].isdigit():
            return
        market = int(channel[1])
        symbol = self.markets.get(venue, {}).get(market)
        if not symbol:
            return
        now = time.time()
        if channel[0] == "ticker":
            ticker = data.get("ticker") or {}
            bid, ask = finite((ticker.get("b") or {}).get("price")), finite((ticker.get("a") or {}).get("price"))
            ts = timestamp(ticker.get("last_updated_at") or data.get("last_updated_at") or data.get("timestamp"))
            if bid and ask and 0 < bid <= ask and ts and ts <= now + 5:
                self.quotes[(venue, symbol)].append((ts, (bid + ask) / 2, now))
            return
        if channel[0] != "trade":
            return
        for row in list(data.get("trades") or []) + list(data.get("liquidation_trades") or []):
            try:
                if int(row.get("market_id", market)) != market:
                    raise ValueError("成交市场与频道不一致")
                ts = timestamp(row.get("timestamp"))
                price, size = finite(row.get("price")), finite(row.get("size"))
                usd = finite(row.get("usd_amount"))
                tid = str(row.get("trade_id_str") or row.get("trade_id", ""))
                ask, bid = account_id(row.get("ask_account_id")), account_id(row.get("bid_account_id"))
                if not ts or ts > now + 60 or not price or price <= 0 or not size or size <= 0 or not tid.isdigit():
                    raise ValueError("无效成交字段")
                if usd is None or usd <= 0 or not isinstance(row.get("is_maker_ask"), bool):
                    raise ValueError("成交金额或 Maker 标记无效")
                maker_ask = row["is_maker_ask"]
                maker_before = finite(row.get("maker_position_size_before"))
                taker_before = finite(row.get("taker_position_size_before"))
                item = (venue, market, tid, symbol, ts, now, price, size, usd, ask, bid, int(maker_ask),
                        str(row.get("type", "trade")), self.premium_at(symbol, ts),
                        maker_before if maker_ask else taker_before,
                        taker_before if maker_ask else maker_before)
                self.queue.put_nowait(item)
            except (ValueError, TypeError, asyncio.QueueFull):
                self.dropped += 1

    async def _writer(self):
        while True:
            batch = []
            try:
                try:
                    batch.append(await asyncio.wait_for(self.queue.get(), 1))
                except asyncio.TimeoutError:
                    await self._record_drops()
                    continue
                await asyncio.sleep(.25)
                while len(batch) < 2000 and not self.queue.empty():
                    batch.append(self.queue.get_nowait())
                self.inserted += await asyncio.to_thread(self.store.ingest, batch)
                self.errors.pop("storage", None)
                batch = []
                await self._record_drops()
            except asyncio.CancelledError:
                if batch:
                    await asyncio.to_thread(self.store.ingest, batch)
                raise
            except Exception as error:
                self.errors["storage"] = str(error)[:300]
                # Retain the failed batch; never silently discard a database error.
                for row in batch:
                    try:
                        self.queue.put_nowait(row)
                    except asyncio.QueueFull:
                        self.dropped += 1
                await asyncio.sleep(5)

    async def fetch_account(self, venue, aid, link=True):
        data = await self.request(venue, "account", by="index", value=aid)
        row = next((r for r in data.get("accounts", []) if int(r.get("index", r.get("account_index", -1))) == aid), None)
        if row is None:
            raise ValueError("公开接口未返回该账户")
        # Store only relevant public fields; no API credentials are read.
        allowed = ("l1_address", "account_type", "positions", "assets", "collateral", "available_balance", "total_asset_value")
        await asyncio.to_thread(self.store.save_account, venue, aid, {k: row[k] for k in allowed if k in row})
        address = str(row.get("l1_address", "")).lower()
        if link and ADDRESS.fullmatch(address) and int(address, 16):
            peer_venue = VENUES[1] if venue == VENUES[0] else VENUES[0]
            peers = await self.request(peer_venue, "accountsByL1Address", l1_address=address)
            for peer in peers.get("sub_accounts") or []:
                try:
                    peer_id = account_id(peer.get("index", peer.get("account_index")))
                except ValueError:
                    continue
                await asyncio.to_thread(self.store.link_account, peer_venue, peer_id, address)

    async def _enrich(self):
        while self.running:
            for venue in VENUES:
                aid = await asyncio.to_thread(self.store.next_account, venue)
                if aid is None:
                    continue
                try:
                    await self.fetch_account(venue, aid, link=False)
                    self.errors.pop("account/" + venue, None)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await asyncio.to_thread(self.store.save_account, venue, aid, None, str(error))
                    self.errors["account/" + venue] = str(error)[:300]
            await asyncio.sleep(4)
