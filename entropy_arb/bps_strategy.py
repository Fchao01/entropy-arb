"""Level-based paired positions; rolling levels read the YAML recorder CSV."""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import math
import os
import time
from pathlib import Path

from .book import ArbPlan, floor_step, walk_depth
from .recorder import prepare_recorder_source

log = logging.getLogger('bps')


def recorder_source(cfg):
    return dict(primary=cfg.primary_venue, hedge=cfg.hedge_venue,
                primary_symbol=cfg.primary.symbol, hedge_symbol=cfg.hedge.symbol)


def percentile(values, percent):
    values = sorted(values)
    offset = (len(values) - 1) * percent / 100
    lo = int(offset)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (offset - lo)


def rolling_levels(cfg, now):
    """Require one valid, completed bar per minute; never use future bars."""
    end = int(now // 60) * 60
    start = end - cfg.bps_window_minutes * 60
    points = {}
    with open(cfg.recorder_csv, newline='') as handle:
        header = next(csv.reader(handle))
        metadata = prepare_recorder_source(cfg.recorder_csv, recorder_source(cfg))
        since = float(metadata['since'])
        # Read a bounded tail in the background, even for long-running CSVs.
        size = os.fstat(handle.fileno()).st_size
        offset = max(0, size - cfg.bps_window_minutes * 2048 - 8192)
        if offset:
            handle.seek(offset)
            handle.readline()
        for values in csv.reader(handle):
            row = dict(zip(header, values))
            try:
                ts = float(row['minute_ts'])
                value = float(row['premium_close_bps'])
                samples = int(row['samples'])
                if (math.isfinite(ts) and ts % 60 == 0 and start <= ts < end and ts >= since
                        and math.isfinite(value) and samples >= 10):
                    points[int(ts)] = value
            except (KeyError, ValueError, OverflowError):
                continue
    if len(points) != cfg.bps_window_minutes:
        raise ValueError(f'等待连续分钟数据 {len(points)}/{cfg.bps_window_minutes}；读取 {cfg.recorder_csv}')
    values = list(points.values())
    entry = percentile(values, cfg.bps_entry_percentile)
    exit = percentile(values, cfg.bps_exit_percentile)
    if (cfg.bps_direction == 'long_primary' and entry >= exit
            or cfg.bps_direction == 'short_primary' and entry <= exit):
        raise ValueError('窗口内溢价波动不足，开平仓点位重合')
    return entry, exit, end


class BpsStrategy:
    def __init__(self, engine):
        self.engine = engine
        self.cfg = engine.cfg
        self.entry = self.cfg.bps_entry if self.cfg.bps_levels == 'manual' else None
        self.exit = self.cfg.bps_exit if self.cfg.bps_levels == 'manual' else None
        self.window_end = None
        self.entries = 0
        self.locked_entry = self.locked_exit = None
        self.closing = False
        self.pending = False
        self.last_entry_at = 0.0
        self.reason = '等待数据' if self.entry is None else '等待开仓点位'
        self.projected_net_bps = None
        self.path = Path(self.cfg.log_file).with_suffix('.bps-state.json')
        self.identity = [self.cfg.symbol, self.cfg.primary_venue, self.cfg.hedge_venue,
                         self.cfg.primary.symbol, self.cfg.hedge.symbol, self.cfg.bps_direction]
        for venue in (self.cfg.primary, self.cfg.hedge):
            creds = venue.lighter_creds or venue.arcus_creds or venue.hl_creds
            account = [getattr(creds, key, None) for key in
                       ('account_address', 'account_index')]
            # The account-less HL configuration derives its address from the key.
            if venue.kind == 'hl' and not account[0]:
                account.append(getattr(creds, 'private_key', None))
            self.identity.append(hashlib.sha256(repr(account).encode()).hexdigest())

    @property
    def long(self):
        return self.cfg.bps_direction == 'long_primary'

    def flat(self):
        # Net-delta tolerance is not a flat-position threshold (e.g. 0.001
        # BTC may still be a material paired position).
        return all(abs(v.position) < self.engine._step / 2 for v in self.engine.venues.values())

    @property
    def tolerance(self):
        return max(1e-12, min(self.cfg.net_tolerance_base, self.engine._step / 2))

    def restore(self):
        state = None
        if self.path.exists():
            try:
                state = json.loads(self.path.read_text())
                if not isinstance(state, dict):
                    raise ValueError('invalid checkpoint')
                if state.get('pending'):
                    raise RuntimeError('bps 上次订单未完成确认，不能仅凭零仓位重新启动；请核对未成交订单及两边仓位')
            except (OSError, ValueError, TypeError) as error:
                raise RuntimeError('bps 状态文件损坏，请核对订单和仓位') from error
        if self.flat():
            self.path.unlink(missing_ok=True)
            return
        try:
            if not isinstance(state, dict):
                raise ValueError('missing checkpoint')
            positions = state['positions']
            if (state['identity'] != self.identity or state['pending']
                    or any(abs(v.position - positions[v.key]) > self.tolerance
                           for v in self.engine.venues.values())):
                raise ValueError('checkpoint mismatch or unfinished execution')
            for key in ('entries', 'locked_entry', 'locked_exit', 'closing', 'last_entry_at'):
                setattr(self, key, state[key])
            if (self.entries < 1 or not all(math.isfinite(x) for x in (self.locked_entry, self.locked_exit))
                    or self.engine.entropy.position * (1 if self.long else -1) <= 0):
                raise ValueError('invalid cycle')
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise RuntimeError('bps 策略发现无法确认归属的旧仓位或未完成订单，已拒绝启动；请核对订单和仓位后处理') from error
        log.info('[BPS] restored cycle entries=%s frozen_exit=%+.3f closing=%s',
                 self.entries, self.locked_exit, self.closing)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {key: getattr(self, key) for key in
                ('entries', 'locked_entry', 'locked_exit', 'closing', 'pending', 'last_entry_at')}
        data.update(identity=self.identity, positions={v.key: v.position for v in self.engine.venues.values()})
        temp = self.path.with_suffix('.tmp')
        temp.write_text(json.dumps(data, allow_nan=False))
        os.replace(temp, self.path)

    async def refresh_loop(self):
        previous = None
        while not self.engine.stop.is_set():
            minute = int(time.time() // 60)
            if self.cfg.bps_levels == 'rolling' and minute != previous:
                try:
                    entry, exit, end = await asyncio.to_thread(rolling_levels, self.cfg, time.time())
                    self.entry, self.exit, self.window_end = entry, exit, end
                    previous = minute
                    self.reason = '持仓中' if self.entries else '等待开仓点位'
                    log.info('[BPS LEVELS] csv=%s window=%d entry=%+.3f exit=%+.3f bps frozen_exit=%s',
                             self.cfg.recorder_csv, self.cfg.bps_window_minutes, entry, exit, self.locked_exit)
                except (OSError, ValueError, StopIteration, KeyError, TypeError, csv.Error) as error:
                    self.entry = self.exit = None
                    reason = str(error)
                    if reason != self.reason:
                        log.info('[BPS LEVELS] %s', reason)
                    self.reason = reason
                self.engine._update_evt.set()
            try:
                await asyncio.wait_for(self.engine.stop.wait(), 5)
            except asyncio.TimeoutError:
                pass

    def snapshot(self):
        return dict(levels=self.cfg.bps_levels, direction=self.cfg.bps_direction,
                    entry_bps=self.locked_entry if self.entries else self.entry,
                    exit_bps=self.locked_exit if self.entries else self.exit,
                    window_minutes=self.cfg.bps_window_minutes, window_end=self.window_end,
                    entries=self.entries, max_entries=self.cfg.bps_max_entries,
                    closing=self.closing, reason=self.reason,
                    projected_net_bps=self.projected_net_bps, data_path=self.cfg.recorder_csv)

    def plan(self, buy, sell, closing=False, quote=None):
        eng, cfg = self.engine, self.cfg
        maker = cfg.primary_order_type == 'maker' and not closing
        pbuy = buy is eng.entropy
        asks, bids = buy.book.sorted_asks(), sell.book.sorted_bids()
        if not asks or not bids:
            return None
        if maker:
            price = quote or (eng.entropy.book.best_bid() if pbuy else eng.entropy.book.best_ask())
            price = eng.entropy.px_round(price, round_up=not pbuy)
            if pbuy:
                asks = [(price, cfg.max_order_notional / price / cfg.take_fraction)]
            else:
                bids = [(price, cfg.max_order_notional / price / cfg.take_fraction)]
        # Bound depth by the top-price slippage range; never assume all depth is executable.
        slip = cfg.leg_slippage_bps / 1e4
        asks = [(px, sz) for px, sz in asks if px <= asks[0][0] * (1 + slip)]
        bids = [(px, sz) for px, sz in bids if px >= bids[0][0] * (1 - slip)]
        depth = min(sum(sz for _, sz in asks), sum(sz for _, sz in bids))
        max_px = max(asks[0][0], bids[0][0])
        qty = min(depth * (1 if closing else cfg.take_fraction), cfg.max_order_notional / max_px)
        if closing:
            qty = min(qty, max(0, -buy.position), max(0, sell.position))
        else:
            for venue in (buy, sell):
                mid = venue.book.mid()
                cap = min(venue.cap_usd, cfg.bps_max_position_usd)
                qty = min(qty, max(0, cap / mid - abs(venue.position)))
        qty = floor_step(qty, eng._step)
        if closing:
            paired = min(max(0, -buy.position), max(0, sell.position))
            minimum_tail = max(eng._min_base, buy.min_quote / asks[0][0],
                               sell.min_quote / (bids[-1][0] * (1 - slip)))
            minimum_tail = round(math.ceil(minimum_tail / eng._step - 1e-9) * eng._step, 12)
            residual = paired - qty
            if eng._step / 2 <= residual < minimum_tail:
                # A per-order cap or limited depth must not strand an
                # untradeable last slice. Leave at least one valid order.
                qty = floor_step(max(0, paired - minimum_tail), eng._step)
        if qty <= 0 or qty < eng._min_base:
            return None
        bp, bn = walk_depth(asks, qty)
        sp, sn = walk_depth(bids, qty)
        bounded = min(qty, cfg.max_order_notional / max(bp, sp))
        if not closing:
            for venue, price in ((buy, bp), (sell, sp)):
                remaining = min(venue.cap_usd, cfg.bps_max_position_usd) - abs(venue.position) * venue.book.mid()
                bounded = min(bounded, max(0, remaining) / price)
        if bounded < qty:
            qty = floor_step(bounded, eng._step)
            if qty <= 0 or qty < eng._min_base:
                return None
            bp, bn = walk_depth(asks, qty)
            sp, sn = walk_depth(bids, qty)
        if closing and eng._step / 2 <= paired - qty < minimum_tail:
            qty = floor_step(max(0, paired - minimum_tail), eng._step)
            if qty <= 0 or qty < eng._min_base:
                return None
            bp, bn = walk_depth(asks, qty)
            sp, sn = walk_depth(bids, qty)
        minimum = max(buy.min_quote, sell.min_quote) if closing else eng._min_notional
        if min(bn, sn) < minimum:
            return None
        bf = cfg.primary_fee_bps if maker and pbuy else buy.fee_bps
        sf = cfg.primary_fee_bps if maker and not pbuy else sell.fee_bps
        return ArbPlan(qty, bp, sp, bn, sn, depth, depth * max_px,
                       (bids[0][0] / asks[0][0] - 1) * 1e4,
                       (sp / bp - 1) * 1e4, bf / 1e4, sf / 1e4,
                       action='bps_close' if closing else 'bps_open',
                       cycle_entry_bps=self.locked_entry if self.entries else self.entry,
                       cycle_exit_bps=self.locked_exit if self.entries else self.exit)

    def projected_return(self, plan, exit_bps):
        """Cost guard estimate at exit ratio; hedge mid assumed unchanged."""
        eng, cfg = self.engine, self.cfg
        p, h = eng.entropy, eng.hedge
        target_mid = h.book.mid() * (1 + exit_bps / 1e4)
        if target_mid <= 0:
            return -math.inf
        p_half = (p.book.best_ask() - p.book.best_bid()) / (2 * p.book.mid())
        slip = cfg.leg_slippage_bps / 1e4
        # Exit is always reduce-only taker; include entry and exit price bounds.
        if self.long:
            close = plan.qty * (target_mid * (1 - p_half) * (1 - slip) * (1 - p.fee_bps / 1e4)
                                - h.book.best_ask() * (1 + slip) * (1 + h.fee_bps / 1e4))
            notional = plan.buy_notional
            entry_slip = plan.sell_notional * slip * (1 - plan.sell_fee)
            if cfg.primary_order_type != 'maker':
                entry_slip += plan.buy_notional * slip * (1 + plan.buy_fee)
        else:
            close = plan.qty * (h.book.best_bid() * (1 - slip) * (1 - h.fee_bps / 1e4)
                                - target_mid * (1 + p_half) * (1 + slip) * (1 + p.fee_bps / 1e4))
            notional = plan.sell_notional
            entry_slip = plan.buy_notional * slip * (1 + plan.buy_fee)
            if cfg.primary_order_type != 'maker':
                entry_slip += plan.sell_notional * slip * (1 - plan.sell_fee)
        return (plan.exp_edge_usd - entry_slip + close) / notional * 1e4

    def scan(self, now):
        eng, cfg = self.engine, self.cfg
        if any(not v.book.is_fresh(cfg.staleness_sec) or not v.ready_to_trade()
               or eng._vlock(v.key).locked() or eng._venue_limited(v)
               for v in eng.venues.values()) or eng._venue_down:
            self.reason = '等待可交易的双腿行情'
            return None
        if any(not eng._venue_rate_ok(v) or v.book.last_update_ts <= v.last_traded_ts
               for v in eng.venues.values()):
            return None
        premium = eng.premium_bps()
        if premium is None:
            return None
        if self.entries and (self.closing or (premium >= self.locked_exit if self.long else premium <= self.locked_exit)):
            if not self.closing:
                self.closing = True  # latch across partial fills and restarts
                try:
                    self.save()
                except OSError:
                    eng.halted = True
                    self.reason = '平仓状态无法保存，暂停交易；请核对仓位'
                    log.exception('[BPS] checkpoint failed')
                    return None
            buy, sell = (eng.hedge, eng.entropy) if self.long else (eng.entropy, eng.hedge)
            plan = self.plan(buy, sell, closing=True)
            self.reason = '平仓中' if plan else '剩余仓位低于交易所最小规模；请核对仓位'
            if plan is None and not self.flat():
                eng.halted = True
                log.critical('[BPS] %s', self.reason)
            return (buy, sell, plan) if plan else None
        entry = self.locked_entry if self.entries else self.entry
        exit = self.locked_exit if self.entries else self.exit
        if entry is None or (self.cfg.bps_levels == 'rolling'
                             and (self.window_end is None or now - self.window_end > 120)):
            return None
        condition = premium <= entry if self.long else premium >= entry
        key = eng.direction_key(not self.long)
        if not condition:
            eng._armed[key] = None
            self.reason = '持仓中，等待平仓点位' if self.entries else '等待开仓点位'
            return None
        if self.entries >= cfg.bps_max_entries or now - self.last_entry_at < cfg.bps_add_interval_sec:
            return None
        if eng._armed.get(key) is None:
            eng._armed[key] = now
            eng._schedule_poke(cfg.premium_persist_sec)
        if now - eng._armed[key] < cfg.premium_persist_sec:
            return None
        buy, sell = (eng.entropy, eng.hedge) if self.long else (eng.hedge, eng.entropy)
        plan = self.plan(buy, sell)
        if plan is None:
            self.reason = '仓位上限或盘口深度不足'
            return None
        self.projected_net_bps = self.projected_return(plan, exit)
        if self.projected_net_bps <= cfg.bps_cost_buffer_bps:
            self.reason = '预计回升空间不足以覆盖开平仓成本'
            eng._skiplog('[BPS] entry blocked: projected net %.3fbps <= buffer %.3fbps', self.projected_net_bps, cfg.bps_cost_buffer_bps)
            return None
        return buy, sell, plan

    def before_execution(self, plan):
        if not self.entries and plan.action == 'bps_open':
            self.locked_entry, self.locked_exit = plan.cycle_entry_bps, plan.cycle_exit_bps
        self.pending = True
        self.save()  # a crash after submission cannot silently re-adopt fills
        log.info('[BPS %s] premium=%s entry=%s frozen_exit=%s entries=%d',
                 plan.action, self.engine.premium_bps(), self.locked_entry, self.locked_exit, self.entries)

    def after_execution(self, plan, before_qty, unresolved):
        eng = self.engine
        if unresolved or abs(sum(v.position for v in eng.venues.values())) > self.tolerance:
            eng.halted = True
            self.reason = '成交或对冲未确认，暂停交易'
            self.save()
            return
        if self.flat():
            self.entries = 0
            self.locked_entry = self.locked_exit = None
            self.closing = False
            self.last_entry_at = time.time()
            for key in eng._armed:
                eng._armed[key] = None
        elif plan.action == 'bps_open' and abs(eng.entropy.position) > before_qty + eng._step / 2:
            self.entries += 1
            self.last_entry_at = time.time()
        self.pending = False
        self.save()
        self.reason = '平仓中' if self.closing else '持仓中' if self.entries else '等待开仓点位'
