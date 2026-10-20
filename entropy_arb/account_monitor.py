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
from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from contextlib import closing
from functools import cached_property
from pathlib import Path

import aiohttp

from .config import LIGHTER_PROFILES
from .rest_reads import RestReads, RestReadCooldown, BACKGROUND, CRITICAL, http_status, response_headers
from .account_analysis import analyse_strategy, pair_phase, reconstruct_cycles

VENUES = ("lighter-rh", "lighter")
LABELS = {"lighter-rh": "RH", "lighter": "Lighter"}
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def wallet_address(value):
    if not isinstance(value, str) or not ADDRESS.fullmatch(value.strip()) or not int(value.strip(), 16):
        raise ValueError("请输入非零的 0x 开头、40 位十六进制钱包地址")
    return value.strip().lower()


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
        self.evidence_cache = {}
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
            CREATE TABLE IF NOT EXISTS address_checks (
                venue TEXT NOT NULL,address TEXT NOT NULL,checked REAL NOT NULL,
                complete INTEGER NOT NULL,error TEXT,PRIMARY KEY(venue,address));
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

    def directory_check(self, venue, address, *, complete=False, error=None):
        with self.lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO address_checks VALUES (?,?,?,?,?)",
                            (venue, address, time.time(), int(complete), str(error)[:300] if error else None))
            self.evidence_cache.clear()

    def checked_address(self, venue, address):
        with closing(self.reader()) as db:
            row = db.execute("SELECT * FROM address_checks WHERE venue=? AND address=?", (venue, address)).fetchone()
            return dict(row) if row else None

    def reader(self):
        db = sqlite3.connect(str(self.path), timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        return db

    def next_account(self, venue):
        with self.lock:
            row = self.db.execute("""SELECT a.id FROM accounts a WHERE a.venue=? AND a.attempted<?
                AND (a.watched=1 OR a.fetched=0 OR (a.address IS NOT NULL AND NOT EXISTS
                    (SELECT 1 FROM address_checks c WHERE c.address=a.address AND c.venue!=a.venue
                     AND c.error IS NULL AND c.checked>?)))
                ORDER BY a.watched DESC,a.attempted,a.id LIMIT 1""",
                                  (venue, time.time() - 120, time.time() - 3600)).fetchone()
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
            for row in rows:
                row["hedge"] = self.account_evidence(db, row, since)
            summary["accounts"] = db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            events = [dict(r) for r in db.execute("SELECT * FROM events ORDER BY id DESC LIMIT 20")]
            watches = [dict(r) for r in db.execute("SELECT venue,id FROM accounts WHERE watched=1 ORDER BY venue,id")]
            summary["gaps"] = db.execute("SELECT COUNT(*) FROM events WHERE kind IN ('gap','overflow','invalid')").fetchone()[0]
            return dict(summary=summary, accounts=rows, total=total, page=page, events=events, watches=watches)

    def account_evidence(self, db, account, since):
        """Short-lived sample summary, never a claim of an owner's true strategy."""
        address, venue, aid = account["address"], account["venue"], account["id"]
        result = dict(code="address_pending", label="待查询地址", reason="公开地址尚未查询，不能判断对冲", matched_groups=0, peers=[], checked_at=time.time())
        if not address:
            if account["fetched"]:
                result.update(code="address_unknown", label="地址不可关联", reason="公开信息未提供可关联地址，无法核验另一平台")
            return result
        key = venue, aid, address, int(since // 60)
        cached = self.evidence_cache.get(key)
        if cached and time.time() - cached["checked_at"] < 15:
            return cached
        peers = [dict(r) for r in db.execute("SELECT venue,id FROM accounts WHERE address=? AND venue!=?", (address, venue))]
        result["peers"] = peers
        if not peers:
            check = db.execute("SELECT * FROM address_checks WHERE address=? AND venue!=?", (address, venue)).fetchone()
            if check and not check["error"] and check["complete"]:
                result.update(code="no_peer", label="未查到同地址对边", reason="已查询公开地址目录；不排除使用不同钱包或其他平台对冲")
            elif check and check["error"]:
                result.update(code="lookup_failed", label="对边查询失败", reason=check["error"])
            else:
                result.update(code="lookup_pending", label="待核验对边", reason="尚未完成另一平台地址查询；打开详情会自动查询")
        else:
            own = [dict(r) for r in db.execute("SELECT * FROM fills WHERE venue=? AND account=? AND ts>=? ORDER BY ts DESC LIMIT 2000", (venue, aid, since))]
            other = [dict(r) for r in db.execute("SELECT f.* FROM fills f JOIN accounts a ON a.venue=f.venue AND a.id=f.account WHERE a.address=? AND f.venue!=? AND f.ts>=? ORDER BY f.ts DESC LIMIT 2000", (address, venue, since))]
            matches = hedge_matches(own, other, limit=None)
            result.update(matched_groups=len(matches), own_sample=len(own), peer_sample=len(other))
            if matches:
                result.update(code="matched", label=f"疑似对冲 {len(matches)} 组", reason="同地址样本中已找到 3 秒内、数量相差不超过 5% 的双边反向成交")
            elif not other:
                result.update(code="peer_no_trades", label="对边尚无成交数据", reason="已关联对边账户，但所选窗口未采集到对边成交；不是没有对冲的证明")
            else:
                result.update(code="unmatched", label="双边有成交，未匹配", reason=f"本账户最近 {len(own)} 笔、对边最近 {len(other)} 笔，未满足同币种反向、3 秒、5% 条件")
        if len(self.evidence_cache) >= 1000:
            self.evidence_cache.clear()
        self.evidence_cache[key] = result
        return result

    def address_detail(self, address, since):
        address = wallet_address(address)
        with closing(self.reader()) as db:
            accounts = [dict(r) for r in db.execute("SELECT venue,id,fetched FROM accounts WHERE address=? ORDER BY venue,id", (address,))]
            checks = [dict(r) for r in db.execute("SELECT * FROM address_checks WHERE address=?", (address,))]
            samples, stats = {}, {}
            for venue in VENUES:
                params = address, venue, since
                where = "FROM fills f JOIN accounts a ON a.venue=f.venue AND a.id=f.account WHERE a.address=? AND f.venue=? AND f.ts>=?"
                stats[venue] = dict(db.execute("SELECT COUNT(*) trades,COALESCE(SUM(f.usd),0) volume,MIN(f.ts) first_trade,MAX(f.ts) last_trade " + where, params).fetchone())
                samples[venue] = [dict(r) for r in db.execute("SELECT f.* " + where + " ORDER BY f.ts DESC LIMIT 2001", params)][:2000]
            matches = hedge_matches(samples[VENUES[0]], samples[VENUES[1]], limit=None)
            boundaries = [r[0] for r in db.execute("SELECT ts FROM events WHERE ts>=? AND kind IN ('gap','overflow','invalid','start','stop')", (since,))]
            fills = samples[VENUES[0]] + samples[VENUES[1]]
            cycles, incomplete = reconstruct_cycles(matches, fills, boundaries)
            paired = {(l["venue"], l["account"], l["market"], l["id"], l["side"]) for p in matches for l in p["legs"]}
            unmatched = [trade_detail(l) for l in sorted(fills, key=lambda l: l["ts"], reverse=True)
                         if (l["venue"], l["account"], l["market"], l["id"], l["side"]) not in paired]
            return dict(address=address, accounts=accounts, checks=checks, stats=stats,
                        matched_groups=len(matches), complete_cycles=len(cycles), cycles=cycles[:50],
                        matches=matches[:50], incomplete=incomplete[:20], unmatched=unmatched[:100],
                        unmatched_count=len(unmatched), strategy=analyse_strategy(cycles, matches),
                        truncated=any(stats[v]["trades"] > 2000 for v in VENUES), checked_at=time.time())

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
                        matches=hedge_matches(fills, peer_fills[:2000]),
                        peer_fills=[trade_detail(r) for r in peer_fills[:100]],
                        peer_analysis_fills=min(len(peer_fills), 2000), peer_truncated=len(peer_fills) > 2000,
                        hedge=self.account_evidence(db, account, since))

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


def trade_detail(row):
    """A confirmed public fill and its position change, without inferred fees."""
    result = {key: row[key] for key in (
        "venue", "account", "market", "id", "symbol", "ts", "side", "price",
        "size", "usd", "maker", "kind", "bps", "before_position")}
    before = row["before_position"]
    action = "开平未知"
    if before is not None:
        after = before + row["side"] * row["size"]
        tolerance = max(1e-10, row["size"] * 1e-7)
        if abs(before) <= tolerance:
            action = "开多" if row["side"] == 1 else "开空"
        elif abs(after) <= tolerance:
            action = "平多" if before > 0 else "平空"
        elif before * after < 0:
            action = "平空转多" if after > 0 else "平多转空"
        elif abs(after) > abs(before):
            action = "加多" if after > 0 else "加空"
        else:
            action = "减多" if before > 0 else "减空"
    result["position_action"] = action
    return result


def hedge_matches(fills, peer_fills, seconds=3.0, limit=50):
    """Greedy, one-to-one candidates among same-address opposite-venue fills.

    Excludes liquidation and matches only comparable base quantities; split
    fills and different-wallet hedges may remain unmatched rather than guessed.
    """
    candidates, used, pools = [], set(), defaultdict(list)
    for index, peer in enumerate(peer_fills):
        if peer["kind"] == "trade":
            pools[(peer["symbol"], peer["side"])].append((peer["ts"], index, peer))
    for pool in pools.values():
        pool.sort(key=lambda value: (value[0], value[1]))
    times = {key: [item[0] for item in pool] for key, pool in pools.items()}
    for row in sorted(fills, key=lambda r: r["ts"], reverse=True):
        if row["kind"] != "trade":
            continue
        key = row["symbol"], -row["side"]
        pool, ticks = pools.get(key, []), times.get(key, [])
        nearby = pool[bisect_left(ticks, row["ts"] - seconds):bisect_right(ticks, row["ts"] + seconds)]
        options = [(index, p) for _, index, p in nearby
                   if (p["venue"], p["market"], p["id"], p["account"]) not in used
                   and p["symbol"] == row["symbol"] and p["side"] == -row["side"]
                   and p["kind"] == "trade" and abs(p["ts"] - row["ts"]) <= seconds
                   and abs(p["size"] - row["size"]) / max(p["size"], row["size"]) <= .05]
        if not options:
            continue
        _, peer = min(options, key=lambda value: (abs(value[1]["ts"] - row["ts"]), value[0]))
        used.add((peer["venue"], peer["market"], peer["id"], peer["account"]))
        rh, core = (row, peer) if row["venue"] == "lighter-rh" else (peer, row)
        candidates.append(dict(symbol=row["symbol"], ts=row["ts"], account=row["account"],
                               peer_venue=peer["venue"], peer_account=peer["account"],
                               side=row["side"], size=row["size"], peer_size=peer["size"],
                               delay_ms=(peer["ts"] - row["ts"]) * 1000,
                               quote_bps=row["bps"], fill_bps=(rh["price"] / core["price"] - 1) * 1e4,
                               total_volume=row["usd"] + peer["usd"],
                               legs=[trade_detail(row), trade_detail(peer)]))
        candidates[-1]["phase"] = pair_phase(candidates[-1])
        if limit is not None and len(candidates) >= limit:
            break
    return candidates


class AccountMonitor:
    # Public discovery/enrichment shares budget with live strategies. Keep this
    # delay outside RestReads.get(): never hold a shared account-cache lock
    # while waiting for the monitor's next polling slot.
    HTTP_INTERVAL_SEC = 10.0
    CACHE_TTL = {"account": 15, "orderBooks": 300,
                 "accountsByL1Address": 300, "recentTrades": 30}

    def __init__(self, path: Path):
        self.store = AccountStore(path)
        self.running = False
        self.session = None
        self.tasks = []
        self.http_next = defaultdict(float)
        self.send_next = defaultdict(float)
        self.markets = {}
        self.quotes = defaultdict(lambda: deque(maxlen=600))
        self.connections = {}
        self.errors = {}
        self.dropped = 0
        self.skipped = 0
        self.inserted = 0
        self.started_at = None
        self.config = {"symbols": "BTC,ETH", "recent": False}
        self.backfill = {"done": 0, "total": 0}

    # TaskManager is also constructed by synchronous tooling. Python 3.9's
    # primitives bind on construction; create them in their running loop.
    @cached_property
    def guard(self):
        return asyncio.Lock()

    @cached_property
    def http_locks(self):
        return {v: asyncio.Lock() for v in VENUES}

    @cached_property
    def send_locks(self):
        return {v: asyncio.Lock() for v in VENUES}

    @cached_property
    def queue(self):
        return asyncio.Queue(maxsize=20000)

    @cached_property
    def requests(self):
        return {v: RestReads(LIGHTER_PROFILES[v].api_url) for v in VENUES}

    async def request(self, venue, path, *, _session=None, **params):
        if path not in ("orderBooks", "account", "accountsByL1Address", "recentTrades"):
            raise ValueError("不是允许的公开查询接口")
        async with self.http_locks[venue]:
            await asyncio.sleep(max(0, self.http_next[venue] - time.monotonic()))
            self.http_next[venue] = time.monotonic() + self.HTTP_INTERVAL_SEC
            url = LIGHTER_PROFILES[venue].api_url + "/api/v1/" + path
            async def fetch():
                async with (_session or self.session).get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as response:
                    if response.status in (429, 405):
                        self.http_next[venue] = time.monotonic() + 60
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, ValueError):
                        response.raise_for_status()
                        raise
                    # A missing public address is a confirmed empty directory result.
                    if path == 'accountsByL1Address' and response.status == 400 and data.get('code') == 21100:
                        return {'code': 200, 'sub_accounts': []}
                    response.raise_for_status()
                    if data.get('code', 200) != 200:
                        if data.get('code') == 23000:
                            raise RestReadCooldown(await self.requests[venue].cooldown(response.headers))
                        raise ValueError(f"公开接口返回 {data.get('code')}")
                    return data
            ttl = self.CACHE_TTL[path]
            return await self.requests[venue].get('/api/v1/' + path, params, fetch, ttl, priority=BACKGROUND)

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
                    inserted=self.inserted, queue_size=self.__dict__['queue'].qsize() if 'queue' in self.__dict__ else 0,
                    backfill=dict(self.backfill), skipped=self.skipped)

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
                await self.requests[venue].acquire(CRITICAL, bucket='ws')
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
                if http_status(error) in (429, 405) and not isinstance(error, RestReadCooldown):
                    until = await self.requests[venue].cooldown(response_headers(error))
                    delay = max(delay, until - time.time())
                elif isinstance(error, RestReadCooldown):
                    delay = max(delay, error.until - time.time())
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
            await self.resolve_address(peer_venue, address)

    async def resolve_address(self, venue, address, *, session=None, force=False):
        address = wallet_address(address)
        previous = await asyncio.to_thread(self.store.checked_address, venue, address)
        if previous and not force and time.time() - previous["checked"] < (60 if previous["error"] else 600):
            return
        try:
            peers = await self.request(venue, "accountsByL1Address", l1_address=address, _session=session)
            if "sub_accounts" not in peers or not isinstance(peers["sub_accounts"], list):
                raise ValueError("公开账户目录响应格式异常，无法判断是否存在账户")
            for peer in peers.get("sub_accounts") or []:
                try:
                    peer_id = account_id(peer.get("index", peer.get("account_index")))
                except ValueError:
                    continue
                await asyncio.to_thread(self.store.link_account, venue, peer_id, address)
            complete = not bool(peers.get("next_cursor"))
            await asyncio.to_thread(self.store.directory_check, venue, address, complete=complete)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await asyncio.to_thread(self.store.directory_check, venue, address, error=str(error) or type(error).__name__)
            raise

    async def lookup_address(self, address):
        address = wallet_address(address)
        # Address analysis is available even while continuous collection is stopped.
        async with aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=15, sock_connect=8)) as session:
            await asyncio.gather(*(self.resolve_address(v, address, session=session, force=True) for v in VENUES), return_exceptions=True)
        return address

    async def _enrich(self):
        while self.running:
            for venue in VENUES:
                aid = await asyncio.to_thread(self.store.next_account, venue)
                if aid is None:
                    continue
                try:
                    await self.fetch_account(venue, aid, link=True)
                    self.errors.pop("account/" + venue, None)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    await asyncio.to_thread(self.store.save_account, venue, aid, None, str(error))
                    self.errors["account/" + venue] = str(error)[:300]
            await asyncio.sleep(self.HTTP_INTERVAL_SEC)
