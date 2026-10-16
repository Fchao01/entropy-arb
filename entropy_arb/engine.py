"""Two-venue arbitrage engine: configurable primary vs hedge venue.

The signal is a fixed band around a configured midline (config.yaml):

    SELL entropy / BUY hedge  when executable premium >= midline + upper (+fees)
    BUY entropy / SELL hedge  when executable premium <= midline - lower (+fees)

Around the signal: per-direction persistence arming,
per-venue inventory ladder + position caps, per-venue order budgets and
reactive rate-limit exclusion, net-delta hedging, venue-outage pausing with
probing, and periodic on-chain reconciliation. There is no paper mode: the
bot either trades live or runs --record-only (data collection, no strategy).
Both venues' books are recorded to 1-minute CSV bars throughout.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import time
from collections import deque
from typing import Dict, List, Optional

import aiohttp

from .book import ArbPlan, OrderBook, floor_step, plan_arb
from .bps_strategy import BpsStrategy, recorder_source
from .maker import execute_primary_maker
from .config import Config
from .recorder import MinuteRecorder
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue
from .venue_arcus import ArcusVenue
from .telegram import TelegramNotifier, hedge_message, trade_message
from .execution_stats import ExecutionStats

log = logging.getLogger("engine")

CSV_HEADER = ["ts", "direction", "buy_venue", "sell_venue", "qty",
              "buy_limit", "sell_limit", "buy_notional", "sell_notional",
              "exp_edge_usd", "gross_edge_usd", "marginal_premium_bps",
              "midline_bps", "inv_add_bps", "ok", "buy_fill", "sell_fill",
              "buy_status", "sell_status", "fill_edge_usd",
              "buy_fee_usd", "sell_fee_usd", "fees_usd",
              "book_age_ms", "book_skew_ms", "send_total_ms",
              "buy_send_ms", "sell_send_ms", "buy_avg_px", "sell_avg_px",
              "realized_edge_bps", "edge_shortfall_bps"]
BALANCE_POLL_SEC = 30.0


class Engine:
    def __init__(self, cfg: Config, record_only: bool = False,
                 control_file: str = None, config_path: str = None) -> None:
        self.cfg = cfg
        self.record_only = record_only
        self.control_file = control_file
        self.config_path = config_path
        self.session: Optional[aiohttp.ClientSession] = None
        self.entropy = None
        self.hedge = None
        self.venues: Dict[str, object] = {}
        self.recorder: Optional[MinuteRecorder] = None
        self.markets_ready = False
        # asyncio primitives are created lazily on the running loop.  This
        # keeps Engine constructible from synchronous tooling and Python 3.9,
        # where asyncio.Event() binds to the current loop at construction.
        self.stop = None
        self._update_evt = None
        self._reconcile_evt = None
        self._stop_requested = False
        # per-venue locks: an execution holds both; a reconcile holds one, so
        # a chain read can never race an in-flight order on that venue
        self._venue_locks: Dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self.halted = False
        self.manual_paused = False
        self.active_maker = None
        self._feed_stop = None
        self._control_state = None
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.total_fees_usd = 0.0
        self.execution_stats = ExecutionStats()
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: Optional[float] = None
        # per-direction persistence arming: direction key -> first-seen ts
        self._armed: Dict[str, Optional[float]] = {self.direction_key(True): None,
                                                   self.direction_key(False): None}
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: Optional[float] = None
        # proactive per-venue send budget: timestamps of recent order sends
        self._sends: Dict[str, deque] = {}
        # reactive per-venue throttle: venue key -> excluded until
        self._venue_limited_until: Dict[str, float] = {}
        # venue outage tracking: key -> down-since ts; a down venue pauses
        # trading and is probed every venue_probe_sec until it answers
        self._venue_down: Dict[str, float] = {}
        self._venue_probe_at: Dict[str, float] = {}
        self._venue_fetch_fails: Dict[str, int] = {}
        self._position_uncertain: set = set()
        # per-execution records for the dashboard (newest last)
        self.recent_trades: deque = deque(maxlen=50)
        self.telegram = TelegramNotifier()
        self.telegram_profile = os.environ.get("TELEGRAM_PROFILE", "default")
        self._telegram_tasks: set = set()
        self._telegram_disabled_logged = False
        self.bps_strategy = BpsStrategy(self) if cfg.strategy_mode == "bps" else None
        log.info("[TELEGRAM] profile=%s; trade notifications %s",
                 self.telegram_profile,
                 "enabled" if self.telegram.enabled else
                 "disabled: missing bot token or chat id")

    def ensure_async_state(self) -> None:
        """Create loop-bound events once the application loop is running."""
        if self.stop is None:
            self.stop = asyncio.Event()
            self._update_evt = asyncio.Event()
            self._reconcile_evt = asyncio.Event()
            self._feed_stop = asyncio.Event()
            if self._stop_requested:
                self.stop.set()

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v) -> bool:
        """True while the venue is under its max_orders_per_min (sliding 60s)."""
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _record_send(self, v) -> None:
        self._sends.setdefault(v.key, deque()).append(time.time())

    def request_stop(self) -> None:
        self._stop_requested = True
        if self.stop is not None:
            self.stop.set()
            self._update_evt.set()
            self._reconcile_evt.set()

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        self.ensure_async_state()
        # Long keepalive so order-path connections survive quiet spells; the
        # keepalive loop pings inside this window to hold them open.
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            keepalive_timeout=75.0, ttl_dns_cache=300))
        self.telegram.session = self.session
        try:
            await self._run_inner()
        finally:
            await self.session.close()

    def _make_venue(self, vc):
        if vc.kind == "arcus":
            return ArcusVenue(vc, self.session, self.cfg.settle_timeout_sec)
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec)

    async def _run_inner(self) -> None:
        cfg = self.cfg
        tasks: List[asyncio.Task] = []
        try:
            await self._run_markets(tasks)
        finally:
            self.stop.set()
            if self._exec_tasks:  # let in-flight executions settle, never cancel
                log.info("waiting for %d in-flight execution(s) to settle",
                         len(self._exec_tasks))
                await asyncio.wait(self._exec_tasks,
                                   timeout=cfg.shutdown_grace_sec)
                if self._exec_tasks:
                    self.halted = True
                    log.critical("shutdown execution still unresolved; check exchange open orders and positions")
            self._feed_stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            results = await asyncio.gather(*(v.close() for v in self.venues.values()),
                                           return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    log.error("venue cleanup failed: %s", result)
            if self._telegram_tasks:
                await asyncio.gather(*self._telegram_tasks, return_exceptions=True)
            try:
                await self.telegram.close()
            except Exception:
                log.exception("telegram cleanup failed")
            log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                     "fill edge $%.4f", self.trades, self.hedges,
                     self.total_exp_edge, self.total_fill_edge)
        if cfg.primary_order_type == "maker" and (self.active_maker is not None or self._exec_tasks):
            raise RuntimeError("maker shutdown unconfirmed; check exchange orders and positions")

    async def _run_markets(self, tasks: List[asyncio.Task]) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.venues = {"entropy": self.entropy}
        self.hedge = self._make_venue(cfg.hedge)
        self.venues["hedge"] = self.hedge
        loads = [asyncio.create_task(v.load_market(), name="load-market")
                 for v in self.venues.values()]
        tasks.extend(loads)
        await asyncio.gather(*loads)
        self.markets_ready = True

        live = not self.record_only
        if live:
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials for both venues in .env "
                    "(see .env.example); use --record-only to run without "
                    "them / 实盘需要在 .env 中配置两个交易所的密钥，仅采集数据"
                    "请用 --record-only")
            self.entropy.init_signer()
            self.hedge.init_signer()
            for v in self.venues.values():
                if v.kind == "arcus":
                    await v.verify_live_credentials()
            if self.entropy.kind == self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
        if (self.entropy.kind == self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False  # shared account: count once

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair %s(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.name, self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, cfg.primary_fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)
        log.info("strategy config=%s thresholds={midline_bps: %+.3f, upper_bps: %.3f, lower_bps: %.3f}",
                 self.config_path or "<in-memory>", cfg.midline_bps,
                 cfg.upper_bps, cfg.lower_bps)
        if cfg.primary_order_type == 'maker' and cfg.maker_cancel_policy == 'best_price':
            log.info('primary execution=maker maker_policy=best_price reprice_delay=%.3fs '
                     'primary_fee=%.3fbps (no total timeout)',
                     cfg.maker_reprice_delay_sec, cfg.primary_fee_bps)
        else:
            log.info("primary execution=%s maker_timeout=%.2fs primary_fee=%.3fbps",
                     cfg.primary_order_type, cfg.maker_timeout_sec, cfg.primary_fee_bps)
        if cfg.primary_order_type == 'maker':
            log.info('[MAKER POLICY] policy=%s reprice_delay=%.3fs; %s',
                     cfg.maker_cancel_policy, cfg.maker_reprice_delay_sec,
                     '信号有效且在最优价时持续保留，不设总等待上限'
                     if cfg.maker_cancel_policy == 'best_price' else '按最长等待时间撤单')
        if self.bps_strategy:
            log.info('[BPS PRICE] configured_basis=%s entry_percentile=%.2f exit_percentile=%.2f; '
                     'executable compares sell bid / buy ask, both triggers >= target; existing cycles retain their saved basis',
                     cfg.bps_price_basis, cfg.bps_entry_percentile, cfg.bps_exit_percentile)
        if cfg.strategy_mode == "spread":
            log.info("[SPREAD] executable price spread > %.3f bps; both directions; "
                     "no history, batch limit or profit target; fees reported separately", cfg.spread_bps)

        recovering_positions = False
        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")
            await self._reconcile_positions(hedge=False, strict=True)
            log.info("starting positions: %s (net %+.6g)",
                     " ".join(f"{v.name}={v.position:+.6g}"
                              for v in self.venues.values()),
                     sum(v.position for v in self.venues.values()))
            if self.bps_strategy:
                recovering_positions = await self.bps_strategy.recover_pending()
            if not recovering_positions:
                self._check_starting_positions()
            if self.bps_strategy and not recovering_positions:
                await self.bps_strategy.verify_existing_pair()
                self.bps_strategy.restore()

        for v in self.venues.values():
            # Account/order streams must stay alive while a stop is canceling
            # a resting maker order and hedging its last fills.
            tasks += v.start_tasks(self._feed_stop, self._update_evt.set, live)
        if recovering_positions:
            await self.bps_strategy.finish_startup_recovery()
            self._check_starting_positions()
            self.bps_strategy.restore()
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(cfg.recorder_csv, self.entropy.book,
                                           self.hedge.book, cfg.staleness_sec,
                                           source=recorder_source(cfg))
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
        if not self.record_only:
            if self.bps_strategy:
                tasks.append(asyncio.create_task(self.bps_strategy.refresh_loop(), name="bps-levels"))
            tasks.append(asyncio.create_task(self._strategy_loop(),
                                             name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                             name="keepalive"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if self.control_file and not self.record_only:
            tasks.append(asyncio.create_task(self._control_loop(), name="control"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(),
                                             name="reconcile"))

        await self.stop.wait()

    # --------------------------------------------------------------- signals

    def _inv_add_bps(self, buy, sell) -> float:
        """Inventory ladder: a surcharge that grows once a venue's position
        passes floor_frac of its cap in the direction the trade would add to
        (buying adds when that venue is >= flat long; selling adds when the
        venue is <= flat short). Max of the two venues' ramps."""
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell) -> float:
        """Net hurdle (bps, on top of fees) for the direction buy->sell.

        selling entropy: executable premium must clear midline + upper;
        buying entropy: the reverse premium must clear lower - midline."""
        if self.cfg.strategy_mode == "spread":
            return self.cfg.spread_bps
        if sell.key == "entropy":
            base = self.cfg.midline_bps + self.cfg.upper_bps
        else:
            base = self.cfg.lower_bps - self.cfg.midline_bps
        return base + self._inv_add_bps(buy, sell)

    def _headroom(self, buy, sell, ref_px: float) -> float:
        hb = buy.cap_usd - buy.position * ref_px
        hs = sell.cap_usd + sell.position * ref_px
        return min(hb, hs)

    def _plan(self, buy, sell, cap_notional: float):
        if self.cfg.primary_order_type == "maker":
            primary_buy = buy is self.entropy
            book = self.entropy.book
            price = book.best_bid() if primary_buy else book.best_ask()
            if price is None:
                return None, "empty_book"
            price = self.entropy.px_round(price, round_up=not primary_buy)
            return self._maker_plan_at(buy, sell, cap_notional, price)
        return plan_arb(
            buy.book, sell.book,
            threshold_bps=self._eff_threshold(buy, sell),
            buy_fee_bps=buy.fee_bps, sell_fee_bps=sell.fee_bps,
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
            gross_threshold_bps=(self.cfg.spread_bps
                                 if self.cfg.strategy_mode == "spread" else None),
        )

    def _maker_plan_at(self, buy, sell, cap_notional, price):
        if price <= 0:
            return None, "empty_book"
        # The maker quote provides a fixed price, not marketable primary depth.
        # Hedge book depth, inventory caps and order size still limit the quote.
        quote = OrderBook()
        primary_buy = buy is self.entropy
        buy_price = price if primary_buy else buy.book.best_ask()
        if buy_price is None:
            return None, "empty_book"
        # cap_notional limits the BUY leg. A fixed primary SELL quote must
        # not be shrunk a second time using the different primary price.
        available = max(cap_notional / buy_price, self._step) / self.cfg.take_fraction
        quote.asks = {price: available}
        quote.bids = {price: available}
        return plan_arb(
            quote if primary_buy else buy.book,
            sell.book if primary_buy else quote,
            threshold_bps=self._eff_threshold(buy, sell),
            buy_fee_bps=self.cfg.primary_fee_bps if primary_buy else buy.fee_bps,
            sell_fee_bps=sell.fee_bps if primary_buy else self.cfg.primary_fee_bps,
            take_fraction=self.cfg.take_fraction, cap_notional=cap_notional,
            min_base=self._min_base, min_notional=self._min_notional, size_step=self._step)

    def _maker_at_best_price(self, primary_buy, price):
        best = self.entropy.book.best_bid() if primary_buy else self.entropy.book.best_ask()
        if best is None or not math.isfinite(best) or best <= 0:
            return False
        # A quote improving on the last public BBO also retains priority;
        # our small own order may not yet appear in the public snapshot.
        return ((price >= best if primary_buy else price <= best)
                or math.isclose(price, best, rel_tol=1e-12, abs_tol=1e-12))

    def _maker_quote_valid(self, buy, sell, plan, price):
        if (not self.entropy.book.is_fresh(self.cfg.staleness_sec)
                or not self.hedge.book.is_fresh(self.cfg.staleness_sec)
                or not self.entropy.ready_to_trade() or not self.hedge.ready_to_trade()
                or self._venue_limited(self.hedge) or not self._venue_rate_ok(self.hedge)
                or self._venue_down):
            return False
        if self.bps_strategy and plan.action == "bps_open":
            strategy = self.bps_strategy
            entry = strategy.locked_entry
            if (entry is None or strategy.awaiting_levels
                    or (self.cfg.bps_levels == 'rolling' and (strategy.window_end is None
                        or time.time() - strategy.window_end > 120))):
                return False
            premium = self.premium_bps()
            if premium is None or (premium > entry if strategy.long else premium < entry):
                return False
            current = strategy.plan(buy, sell, quote=price)
            return (current is not None and current.qty >= plan.qty - self._step / 2
                    and strategy.entry_check(current, buy, sell, strategy.locked_entry, strategy.locked_exit))
        current, _ = self._maker_plan_at(buy, sell, plan.buy_notional, price)
        return current is not None and current.qty >= plan.qty - self._step / 2

    # -------------------------------------------------------------- strategy

    def direction_key(self, sell_primary: bool) -> str:
        # Preserve old direction values for existing Entropy CSV consumers.
        suffix = "entropy" if self.cfg.primary_venue == "entropy" else "primary"
        return ("sell_" if sell_primary else "buy_") + suffix

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    async def _control_loop(self) -> None:
        while not self.stop.is_set():
            paused = False
            try:
                with open(self.control_file, encoding="utf-8") as handle:
                    value = json.load(handle)
                paused = bool(value.get("paused", False)) if isinstance(value, dict) else False
            except FileNotFoundError:
                pass
            except (OSError, ValueError, TypeError):
                log.warning("invalid pause control file; keeping current state")
                paused = self.manual_paused
            if paused != self.manual_paused:
                self.manual_paused = paused
                log.warning("manual strategy pause %s", "enabled" if paused else "disabled")
                self._update_evt.set()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted or self.manual_paused:
            return
        now = time.time()
        exiting = self.bps_strategy and self.bps_strategy.entries and (
            self.bps_strategy.exit_ready(now))
        if not exiting and now - self.last_trade_ts < cfg.cooldown_sec:
            self._schedule_poke(cfg.cooldown_sec - (now - self.last_trade_ts))
            return
        best = self._scan(now)
        if best is None:
            return
        buy, sell, plan = best
        # _scan verified both locks free and nothing ran since (no awaits),
        # so these acquires take the no-suspension fast path
        await self._vlock(buy.key).acquire()
        await self._vlock(sell.key).acquire()
        # run as a task so a shutdown cancels the strategy loop's await, never
        # the in-flight execution itself (both legs must settle)
        t = asyncio.create_task(self._execute_locked(buy, sell, plan))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(self, buy, sell, plan: ArbPlan) -> None:
        """Run one execution while holding both venue locks (acquired by the
        caller), then release them and settle the aftermath: unresolved
        outcomes escalate to reconcile, everything else gets a net-delta
        check."""
        unresolved = False
        before_qty = abs(self.entropy.position)
        try:
            if self.bps_strategy:
                self.bps_strategy.before_execution(plan)
            unresolved = await self._execute(buy, sell, plan)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
            if self.bps_strategy:
                unresolved = True
                self.halted = True
        finally:
            self._vlock(buy.key).release()
            self._vlock(sell.key).release()
        if unresolved:
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        if self.bps_strategy:
            self.bps_strategy.after_execution(plan, before_qty, unresolved)
        self._update_evt.set()  # freed venues may have a queued opportunity

    def _scan(self, now: float):
        """Evaluate both directions; returns the best executable
        (buy, sell, plan), or None."""
        cfg = self.cfg
        if self.bps_strategy:
            return self.bps_strategy.scan(now)
        best = None
        for buy, sell, dkey in ((self.hedge, self.entropy, self.direction_key(True)),
                                (self.entropy, self.hedge, self.direction_key(False))):
            if not (buy.book.is_fresh(cfg.staleness_sec)
                    and sell.book.is_fresh(cfg.staleness_sec)):
                continue
            if not (buy.ready_to_trade() and sell.ready_to_trade()):
                continue
            if self._venue_down:
                continue  # a venue in outage pauses the (only) pair
            if self._vlock(buy.key).locked() or self._vlock(sell.key).locked():
                continue  # mid-execution or mid-reconcile
            if self._venue_limited(buy) or self._venue_limited(sell):
                continue  # reactive 429 exclusion
            if not (self._venue_rate_ok(buy) and self._venue_rate_ok(sell)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                continue
            # never refire into books that predate the venue's own last trade
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                continue
            plan, reason = self._plan(buy, sell, cfg.max_order_notional)
            edge_present = reason not in ("no_edge", "empty_book")
            if not edge_present:
                self._armed[dkey] = None
                continue
            armed = self._armed.get(dkey)
            persistence = 0.0 if cfg.strategy_mode == "spread" else cfg.premium_persist_sec
            if armed is None:
                # premium persistence: only fire if the edge survives
                # premium_persist_sec (filters one-tick phantoms)
                self._armed[dkey] = now
                armed = now
                if persistence > 0 or cfg.strategy_mode != "spread":
                    self._schedule_poke(persistence)
                    continue
            if now - armed < persistence:
                self._schedule_poke(persistence - (now - armed))
                continue
            if plan is None:
                continue
            headroom = self._headroom(buy, sell, plan.buy_limit)
            if headroom < plan.buy_notional:
                plan, _ = self._plan(buy, sell,
                                     min(cfg.max_order_notional, headroom))
                if plan is None:
                    self._skiplog("%s blocked by position caps (headroom $%.0f)",
                                  dkey, max(headroom, 0.0))
                    continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan)
        return best

    # ------------------------------------------------------------- execution

    async def _timed_send(self, venue, *, is_buy: bool, qty: float,
                          limit_px: float, reduce_only: bool = False) -> tuple:
        """Send one leg and return (result, elapsed_ms).

        The elapsed value is local event-loop time, so it is suitable for
        latency distributions even when wall-clock time is adjusted.
        """
        started = time.perf_counter()
        try:
            result = await venue.send_taker(is_buy=is_buy, qty=qty,
                                            limit_px=limit_px,
                                            **({"reduce_only": True} if reduce_only else {}))
        except Exception as e:
            result = {"status": "send-failed", "filled_base": 0.0,
                      "avg_px": None, "err": repr(e), "unresolved": True}
        return result, (time.perf_counter() - started) * 1000.0

    async def _execute(self, buy, sell, plan: ArbPlan) -> bool:
        """Send both legs and settle the fills. Both venue locks are held by
        the caller. Returns True when an outcome is unresolved and the caller
        must escalate to reconcile."""
        if self.halted:
            return False
        cfg = self.cfg
        if cfg.strategy_mode == "spread":
            ask = buy.book.best_ask()
            if not ask:
                return False
            cap = min(cfg.max_order_notional, plan.buy_notional, plan.qty * ask,
                      self._headroom(buy, sell, ask))
            plan, _ = self._plan(buy, sell, cap)
            if plan is None:
                return False
        inv_bps = (0.0 if self.bps_strategy or cfg.strategy_mode == "spread"
                   else self._inv_add_bps(buy, sell))
        direction = self.direction_key(sell.key == self.entropy.key)
        closing = plan.action == "bps_close"
        if plan.action != "arb":
            direction = plan.action + ":" + direction
        signal_ts = time.time()
        book_age_ms = max(0.0, signal_ts - min(buy.book.last_update_ts,
                                               sell.book.last_update_ts)) * 1000.0
        book_skew_ms = abs(buy.book.last_update_ts - sell.book.last_update_ts) * 1000.0
        self.last_trade_ts = time.time()
        log.info("[ARB] %s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, buy.name, plan.qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)
        slip = (cfg.leg_slippage_bps if plan.slippage_bps is None else plan.slippage_bps) / 1e4
        buy_bound = buy.px_round(plan.buy_limit * (1 + slip), round_up=False)
        sell_bound = sell.px_round(plan.sell_limit * (1 - slip), round_up=True)
        send_started = time.perf_counter()
        if cfg.primary_order_type == "maker" and not closing and not (self.bps_strategy and self.bps_strategy.executable):
            (binfo, buy_send_ms), (sinfo, sell_send_ms) = await execute_primary_maker(self, buy, sell, plan)
        else:
            self._record_send(buy)
            self._record_send(sell)
            (binfo, buy_send_ms), (sinfo, sell_send_ms) = await asyncio.gather(
                self._timed_send(buy, is_buy=True, qty=plan.qty,
                                 limit_px=buy_bound, **({"reduce_only": True} if closing else {})),
                self._timed_send(sell, is_buy=False, qty=plan.qty,
                                 limit_px=sell_bound, **({"reduce_only": True} if closing else {})))
        send_total_ms = (time.perf_counter() - send_started) * 1000.0
        for v, info, side in ((buy, binfo, "buy"), (sell, sinfo, "sell")):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
        bfill = binfo["filled_base"]
        sfill = sinfo["filled_base"]
        primary_before = self.entropy.position
        self.execution_stats.record(buy.key, True, bfill, binfo.get("avg_px"), buy.position, self._step)
        self.execution_stats.record(sell.key, False, sfill, sinfo.get("avg_px"), sell.position, self._step)
        if self.bps_strategy:
            self.bps_strategy.record_fill(buy, True, bfill, binfo.get("avg_px"), plan.buy_fee,
                                          opening=plan.action == "bps_open")
            self.bps_strategy.record_fill(sell, False, sfill, sinfo.get("avg_px"), plan.sell_fee,
                                          opening=plan.action == "bps_open")
        if binfo.get("unresolved") or sinfo.get("unresolved"):
            self.execution_stats.unresolved = True
        primary_fill = bfill if buy is self.entropy else sfill
        primary_sign = 1 if buy is self.entropy else -1
        added = primary_fill if primary_before * primary_sign >= 0 else max(0, primary_fill - abs(primary_before))
        if not closing and min(added, bfill, sfill) >= self._step / 2:
            self.execution_stats.openings += 1
        buy.position += bfill
        sell.position -= sfill
        bpx = binfo.get("avg_px") or plan.buy_limit
        spx = sinfo.get("avg_px") or plan.sell_limit
        buy_fee_usd = bfill * bpx * plan.buy_fee
        sell_fee_usd = sfill * spx * plan.sell_fee
        fees_usd = buy_fee_usd + sell_fee_usd
        self.total_fees_usd += fees_usd
        if bfill:
            buy.cash -= bfill * bpx * (1 + plan.buy_fee)
            buy.volume_usd += bfill * bpx
        if sfill:
            sell.cash += sfill * spx * (1 - plan.sell_fee)
            sell.volume_usd += sfill * spx

        matched = min(bfill, sfill)
        fill_edge = 0.0
        realized_edge_bps = None
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            fill_edge = matched * (sinfo["avg_px"] * (1 - plan.sell_fee)
                                   - binfo["avg_px"] * (1 + plan.buy_fee))
            realized_edge_bps = (
                sinfo["avg_px"] * (1 - plan.sell_fee)
                / (binfo["avg_px"] * (1 + plan.buy_fee)) - 1.0) * 1e4
            self.total_fill_edge += fill_edge
        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f", direction,
                 buy.name, binfo["status"], bfill, plan.qty,
                 sell.name, sinfo["status"], sfill, plan.qty, matched, fill_edge)
        shortfall_bps = (realized_edge_bps - plan.marginal_premium_bps
                         if realized_edge_bps is not None else None)
        log.info("[TIMING] %s: book_age=%.1fms skew=%.1fms send=%.1fms "
                 "buy=%.1fms sell=%.1fms realized_edge=%s shortfall=%s",
                 direction, book_age_ms, book_skew_ms, send_total_ms,
                 buy_send_ms, sell_send_ms,
                 f"{realized_edge_bps:.3f}bps" if realized_edge_bps is not None else "—",
                 f"{shortfall_bps:+.3f}bps" if shortfall_bps is not None else "—")
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        unresolved = binfo.get("unresolved") or sinfo.get("unresolved")
        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        sent_ok = not hard_err and not unresolved
        if sent_ok:
            self.consec_errors = 0
        elif not rate_limited:
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self.halted = True
                log.critical("HALTED after %d consecutive execution problems "
                             "— flatten manually and restart / 连续执行异常，"
                             "引擎已停止，请手动平仓后重启", self.consec_errors)
        if sent_ok and (cfg.primary_order_type == "taker" or bfill or sfill):
            self.trades += 1
            self.total_exp_edge += plan.exp_edge_usd * (matched / plan.qty if cfg.primary_order_type == "maker" else 1)
        self._record_trade(direction, plan,
                           None if unresolved else fill_edge,
                           f"{binfo['status']}/{sinfo['status']}", sent_ok)
        self._log_csv(direction, buy, sell, plan, sent_ok, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps,
                      book_age_ms=book_age_ms, book_skew_ms=book_skew_ms,
                      send_total_ms=send_total_ms, buy_send_ms=buy_send_ms,
                      sell_send_ms=sell_send_ms,
                      buy_avg_px=binfo.get("avg_px"),
                      sell_avg_px=sinfo.get("avg_px"),
                      buy_fee_usd=buy_fee_usd, sell_fee_usd=sell_fee_usd,
                      fees_usd=fees_usd,
                      realized_edge_bps=realized_edge_bps,
                      edge_shortfall_bps=shortfall_bps)
        trade_volume = bfill * (binfo.get("avg_px") or plan.buy_limit) \
            + sfill * (sinfo.get("avg_px") or plan.sell_limit)
        self._notify_trade(direction, buy, sell, plan, sent_ok, bfill, sfill,
                           binfo["status"], sinfo["status"],
                           None if unresolved else fill_edge, trade_volume)
        self.last_trade_ts = time.time()
        return bool(unresolved)

    def _notify_trade(self, direction, buy, sell, plan, ok, bfill, sfill,
                      buy_status, sell_status, fill_edge, trade_volume) -> None:
        if not self.telegram.enabled:
            if not self._telegram_disabled_logged:
                log.warning("[TELEGRAM] trade notification skipped: bot token or chat id is not available in this task process")
                self._telegram_disabled_logged = True
            return
        message = trade_message(
            symbol=self.cfg.symbol, direction=direction,
            buy_venue=buy.name, sell_venue=sell.name, qty=plan.qty,
            buy_status=buy_status, sell_status=sell_status,
            buy_fill=bfill, sell_fill=sfill, fill_edge=fill_edge, ok=ok,
            trade_volume=trade_volume, total_volume=self._total_volume(),
            total_profit=self.total_fill_edge, hedges=self.hedges,
            balances=self._notification_balances(),
            account_delta=self.account_delta())
        task = asyncio.create_task(self.telegram.send(message), name="telegram-trade")
        self._telegram_tasks.add(task)
        task.add_done_callback(self._telegram_finished)

    def _telegram_finished(self, task) -> None:
        self._telegram_tasks.discard(task)
        if task.cancelled():
            return
        try:
            if not task.result():
                log.warning("[TELEGRAM] trade notification was not delivered")
        except Exception:
            log.exception("[TELEGRAM] trade notification task failed")

    def _total_volume(self) -> float:
        return sum(float(getattr(v, "volume_usd", 0.0) or 0.0)
                   for v in self.venues.values())

    def _notification_balances(self) -> list[dict]:
        return [{"name": v.name, "free": v.free, "equity": v.equity}
                for v in self.venues.values()]

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool) -> None:
        self.recent_trades.append({
            "ts": time.time(), "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": plan.exp_edge_usd, "fill": fill_edge, "status": status,
            "ok": ok})

    def _check_starting_positions(self) -> None:
        net = sum(v.position for v in self.venues.values())
        if not math.isfinite(net) or abs(net) > self.cfg.net_tolerance_base:
            raise RuntimeError(
                f"startup positions are unbalanced (net {net:+.8g}); no orders sent / "
                "启动时已有仓位不平衡，已拒绝启动，不会自动平仓；请核对两边账户和旧仓位")

    async def _maybe_hedge(self) -> None:
        if self.bps_strategy and self.halted:
            return
        if self._position_uncertain:
            return
        if self.active_maker is not None:
            return  # unknown live order/hedge: position reads alone cannot resolve it
        net = sum(v.position for v in self.venues.values())
        tolerance = self.bps_strategy.tolerance if self.bps_strategy else self.cfg.net_tolerance_base
        for _ in range(3 if self.bps_strategy else 1):
            if abs(net) <= tolerance:
                break
            confirmed = await self._hedge(net)
            remaining = sum(v.position for v in self.venues.values())
            # Only retry a confirmed partial reduction. Unknown outcomes must
            # never be sent twice, nor can an untradeable dust tail be enlarged.
            if not confirmed or abs(remaining) >= abs(net) - 1e-12:
                break
            net = remaining
        remaining = sum(v.position for v in self.venues.values())
        if self.cfg.primary_order_type == "maker" and abs(remaining) > tolerance:
            self.halted = True
            log.critical("[MAKER] residual base %.8g exceeds tolerance; stopped new quotes", remaining)

    async def _hedge(self, net: float) -> bool:
        """Reduce the venue that carries the imbalance back toward net zero
        (reduce-only taker with hedge_slippage_bps price protection)."""
        cfg = self.cfg
        is_sell = net > 0
        sgn = 1.0 if net > 0 else -1.0
        slip = cfg.hedge_slippage_bps / 1e4
        for v in sorted(self.venues.values(),
                        key=lambda x: (self._venue_limited(x), -x.position * sgn)):
            if v.position * sgn <= 0:
                continue
            if v.key in self._venue_down \
                    or not v.book.is_fresh(cfg.staleness_sec):
                continue  # unreachable or blind: cannot hedge here
            lk = self._vlock(v.key)
            if lk.locked():
                continue
            qty = floor_step(min(abs(net), abs(v.position)), self._step)
            if qty < v.min_base:
                continue
            ref = v.book.best_bid() if is_sell else v.book.best_ask()
            if ref is None:
                continue
            limit = v.px_round(ref * (1 - slip), False) if is_sell \
                else v.px_round(ref * (1 + slip), True)
            if qty * limit < max(cfg.min_order_notional, v.min_quote):
                continue
            await lk.acquire()  # verified free, no awaits since: fast path
            try:
                log.warning("[HEDGE] net %+.6g — %s %.6g on %s @%.6g",
                            net, "SELL" if is_sell else "BUY", qty, v.name, limit)
                self.hedges += 1
                self._record_send(v)  # counts toward the budget, never blocked
                info, _ = await self._timed_send(v, is_buy=not is_sell, qty=qty,
                                                limit_px=limit, reduce_only=True)
                fill = info.get('filled_base')
                if (not isinstance(fill, (int, float)) or not math.isfinite(fill)
                        or not 0 <= fill <= qty + self._step / 2):
                    info = dict(status='invalid-hedge-fill', filled_base=0, avg_px=None,
                                err='invalid residual hedge quantity', unresolved=True)
                hedge_ok = not info.get("err") and not info.get("unresolved")
                if info.get("err") or info.get("unresolved"):
                    if info.get("unresolved"):
                        self.execution_stats.unresolved = True
                    log.error("[HEDGE] %s: %s", v.name,
                              info.get("err") or "unresolved")
                    if str(info.get("err", "")).startswith("RATE_LIMITED"):
                        self._mark_limited(v)
                    self._reconcile_evt.set()
                    if self.bps_strategy:
                        self.halted = self.bps_strategy.pending = True
                        self.bps_strategy.reason = '残余仓位对冲失败或未确认，暂停新订单'
                        self.bps_strategy.save()
                else:
                    fill = info["filled_base"]
                    self.execution_stats.record(v.key, not is_sell, fill, info.get("avg_px"),
                                                v.position, self._step)
                    if self.bps_strategy:
                        self.bps_strategy.record_fill(v, not is_sell, fill, info.get("avg_px"),
                                                      v.fee_bps / 1e4, opening=False)
                    v.position += -fill if is_sell else fill
                    if fill:
                        px = info.get("avg_px") or limit
                        fee = v.fee_bps / 1e4
                        self.total_fees_usd += fill * px * fee
                        v.cash += fill * px * (1 - fee) if is_sell \
                            else -fill * px * (1 + fee)
                        v.volume_usd += fill * px
                    log.info("[HEDGE SETTLED] %s %s %.6g/%.6g",
                             v.name, info["status"], fill, qty)
                self._notify_hedge(
                    v, "SELL" if is_sell else "BUY", qty,
                    info.get("status", "unknown"), info.get("filled_base", 0.0),
                    hedge_ok, net,
                    (info.get("filled_base", 0.0) or 0.0)
                    * (info.get("avg_px") or limit))
                v.last_traded_ts = time.time()
            finally:
                lk.release()
            return hedge_ok
        log.warning("[HEDGE] net %+.6g below hedgeable minimum — carrying "
                    "(next reconcile retries)", net)
        return False

    def _notify_hedge(self, venue, side, qty, status, fill, ok, net,
                      trade_volume) -> None:
        if not self.telegram.enabled:
            if not self._telegram_disabled_logged:
                log.warning("[TELEGRAM] hedge notification skipped: bot token or chat id is not available in this task process")
                self._telegram_disabled_logged = True
            return
        message = hedge_message(symbol=self.cfg.symbol, venue=venue.name,
                                side=side, qty=qty, status=status, fill=fill,
                                ok=ok, net=net, trade_volume=trade_volume,
                                total_volume=self._total_volume(),
                                total_profit=self.total_fill_edge,
                                hedges=self.hedges,
                                balances=self._notification_balances(),
                                account_delta=self.account_delta())
        task = asyncio.create_task(self.telegram.send(message), name="telegram-hedge")
        self._telegram_tasks.add(task)
        task.add_done_callback(self._telegram_finished)

    # --------------------------------------------------- reconcile / status

    # Lighter's REST account state lags its ws settlements; overwriting a
    # venue that traded seconds ago "restores" stale positions and triggers
    # phantom hedge oscillations. Grace-guard + venue lock prevent that.
    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool,
                                   strict: bool = False) -> None:
        now = time.time()
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue  # just traded: chain read would be stale
            if v.key in self._venue_down \
                    and now < self._venue_probe_at.get(v.key, 0.0):
                continue  # down venue: probe only every venue_probe_sec
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs),
            return_exceptions=True)
        for r in got:
            if isinstance(r, BaseException):
                raise r  # strict startup: fail loudly
        if hedge:
            await self._maybe_hedge()

    async def _reconcile_venue(self, v, strict: bool) -> None:
        async with self._vlock(v.key):
            now = time.time()
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return  # traded while waiting for the lock
            try:
                r = await v.fetch_position()
                if not math.isfinite(r):
                    raise ValueError('non-finite position')
                if self.bps_strategy and not strict and abs(r - v.position) > self.bps_strategy.tolerance:
                    self._position_uncertain.add(v.key)
                    self.bps_strategy.reason = '等待两边实时仓位复核'
                    log.warning('[BPS] %s position discrepancy local=%+.8g REST=%+.8g; verifying before adoption',
                                v.name, v.position, r)
                    # REST account snapshots can lag account-order fills.
                    # Keep the venue lock and local fill ledger while verifying.
                    await asyncio.sleep(0.5)
                    verified = await v.fetch_position()
                    if not math.isfinite(verified):
                        raise ValueError('non-finite verification position')
                    if abs(verified - v.position) <= self.bps_strategy.tolerance:
                        log.info('[BPS] %s delayed position snapshot resolved; confirmed fill ledger retained', v.name)
                        r = v.position
                    elif abs(verified - r) > self.bps_strategy.tolerance:
                        log.warning('[BPS] %s position snapshots still changing; new orders blocked until rechecked', v.name)
                        self._reconcile_evt.set()
                        return
                    else:
                        r = verified
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}")
                if self.bps_strategy:
                    self._position_uncertain.add(v.key)
                    self.bps_strategy.reason = '等待两边实时仓位复核'
                # exchange unreachable (e.g. scheduled maintenance): pause
                # trading and keep probing until it answers again
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical("[%s] API unreachable (%d attempts) — "
                                 "trading PAUSED; probing every %.0fs until "
                                 "it recovers", v.name, n,
                                 self.cfg.venue_probe_sec)
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r",
                                v.name, n, e)
                return
            if v.key in self._venue_down:
                log.warning("[%s] API recovered after %.0fs outage; %s", v.name,
                            now - self._venue_down.pop(v.key),
                            'engine remains HALTED' if self.halted else 'other trading checks still apply')
                self._update_evt.set()
            self._position_uncertain.discard(v.key)
            self._venue_fetch_fails[v.key] = 0
            delta = r - v.position
            if abs(delta) > 1e-12:
                if self.bps_strategy and not strict and abs(delta) > self.bps_strategy.tolerance:
                    self.halted = True
                    self.bps_strategy.pending = True
                    self.bps_strategy.reason = "链上仓位与已确认成交不一致，请核对账户"
                    self.bps_strategy.save()
                    log.critical("[BPS] unexpected position change on %s; no automatic flatten", v.name)
                if abs(delta) > self.cfg.net_tolerance_base:
                    log.warning("[%s] reconcile: chain %+.6g vs local %+.6g "
                                "— adopting chain", v.name, r, v.position)
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(),
                                       timeout=self.cfg.reconcile_sec)
                self._reconcile_evt.clear()
                await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            for v in self.venues.values():
                try:
                    got = await v.fetch_equity()
                    if got is not None:
                        v.equity, v.free = got
                        if v.start_equity is None:
                            v.start_equity = v.equity
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.debug("[%s] equity poll failed: %r", v.name, e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except asyncio.TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    def account_delta(self) -> Optional[float]:
        """Change in real account equity since start (both venues)."""
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> Optional[float]:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec) else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            if self.bps_strategy:
                state = self.bps_strategy.snapshot()
                log.info("[status] %s | mid_prem %s bps | BPS basis=%s open_signal=%s close_signal=%s entry=%s exit=%s filled_batches=%d %s "
                         "| pos %s net %+.6g | trades %d hedges %d | MTM %s%s%s",
                         books, prem_s, state['price_basis'], state['open_signal_bps'], state['close_signal_bps'],
                         state["entry_bps"], state["exit_bps"], state["entries"],
                         state["reason"], pos, net, self.trades, self.hedges,
                         f"${pnl:+.4f}" if pnl is not None else "—", rec,
                         " *** HALTED ***" if self.halted else "")
                continue
            log.info("[status] %s | prem %s bps (band %+.2f..%+.2f) | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s",
                     books, prem_s, cfg.midline_bps - cfg.lower_bps,
                     cfg.midline_bps + cfg.upper_bps, pos, net, self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, rec,
                     " *** HALTED ***" if self.halted else "")

    def _log_csv(self, direction, buy, sell, plan: ArbPlan, ok: bool, bfill,
                 sfill, bstatus, sstatus, fill_edge, inv_bps, *,
                 book_age_ms=None, book_skew_ms=None, send_total_ms=None,
                 buy_send_ms=None, sell_send_ms=None, buy_avg_px=None,
                 sell_avg_px=None, buy_fee_usd=None, sell_fee_usd=None,
                 fees_usd=None, realized_edge_bps=None,
                 edge_shortfall_bps=None) -> None:
        try:
            path = self.cfg.trades_csv
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.replace(path, path + ".old")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow([f"{time.time():.3f}",
                            direction, buy.name, sell.name, f"{plan.qty:.8g}",
                            plan.buy_limit, plan.sell_limit,
                            f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                            f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                            f"{plan.marginal_premium_bps:.3f}",
                            f"{self.cfg.midline_bps:.3f}",
                            f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                            f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}",
                            "" if buy_fee_usd is None else f"{buy_fee_usd:.4f}",
                            "" if sell_fee_usd is None else f"{sell_fee_usd:.4f}",
                            "" if fees_usd is None else f"{fees_usd:.4f}",
                            "" if book_age_ms is None else f"{book_age_ms:.3f}",
                            "" if book_skew_ms is None else f"{book_skew_ms:.3f}",
                            "" if send_total_ms is None else f"{send_total_ms:.3f}",
                            "" if buy_send_ms is None else f"{buy_send_ms:.3f}",
                            "" if sell_send_ms is None else f"{sell_send_ms:.3f}",
                            "" if buy_avg_px is None else f"{buy_avg_px:.10g}",
                            "" if sell_avg_px is None else f"{sell_avg_px:.10g}",
                            "" if realized_edge_bps is None else f"{realized_edge_bps:.3f}",
                            "" if edge_shortfall_bps is None else f"{edge_shortfall_bps:.3f}"])
        except Exception:
            log.exception("csv write failed")
