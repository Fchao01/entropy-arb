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
from .bps_pnl import CycleCosts

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


def rolling_levels(cfg, now, *, include_coverage=False):
    """Use valid completed bars inside the time window, allowing data gaps."""
    end = int(now // 60) * 60
    start = end - cfg.bps_window_minutes * 60
    points = {}
    present, low_samples, invalid_premium, before_source = set(), set(), set(), set()
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
                if not (math.isfinite(ts) and ts % 60 == 0 and start <= ts < end):
                    continue
                present.add(int(ts))
                if ts < since:
                    before_source.add(int(ts))
                    continue
                try:
                    samples = int(row['samples'])
                except (KeyError, ValueError, TypeError):
                    low_samples.add(int(ts))
                    continue
                try:
                    value = float(row['premium_close_bps'])
                except (KeyError, ValueError, TypeError):
                    invalid_premium.add(int(ts))
                    continue
                if samples < 10:
                    low_samples.add(int(ts))
                elif not math.isfinite(value):
                    invalid_premium.add(int(ts))
                else:
                    points[int(ts)] = value
            except (KeyError, ValueError, TypeError, OverflowError):
                continue
    # min_coverage_pct is accepted for old YAMLs, but no longer gates signals.
    required = min(cfg.bps_window_minutes, cfg.bps_min_valid_minutes)
    coverage = dict(valid_minutes=len(points), required_minutes=required,
                    coverage_pct=len(points) / cfg.bps_window_minutes * 100,
                    missing_minutes=cfg.bps_window_minutes - len(present),
                    low_sample_minutes=len(low_samples - points.keys()),
                    invalid_premium_minutes=len(invalid_premium - points.keys()),
                    source_filtered_minutes=len(before_source - points.keys()))
    if len(points) < required:
        raise ValueError(f'等待有效分钟数据 {len(points)}/{cfg.bps_window_minutes}；'
                         f'至少 {required} 条；缺行 {coverage["missing_minutes"]} 分钟，'
                         f'采样不足 {coverage["low_sample_minutes"]} 分钟，'
                         f'无效溢价 {coverage["invalid_premium_minutes"]} 分钟，'
                         f'来源过滤 {coverage["source_filtered_minutes"]} 分钟；'
                         f'允许缺口，读取 {cfg.recorder_csv}')
    values = list(points.values())
    entry = percentile(values, cfg.bps_entry_percentile)
    exit = percentile(values, cfg.bps_exit_percentile)
    if (cfg.bps_direction == 'long_primary' and entry >= exit
            or cfg.bps_direction == 'short_primary' and entry <= exit):
        raise ValueError('窗口内溢价波动不足，开平仓点位重合')
    result = entry, exit, end
    if include_coverage:
        return (*result, coverage)
    return result


class BpsStrategy:
    def __init__(self, engine):
        self.engine = engine
        self.cfg = engine.cfg
        self.entry = self.cfg.bps_entry if self.cfg.bps_levels == 'manual' else None
        self.exit = self.cfg.bps_exit if self.cfg.bps_levels == 'manual' else None
        self.window_end = None
        self.coverage = None
        self.entries = 0
        self.locked_entry = self.locked_exit = None
        self.closing = False
        self.pending = False
        self.adopted_position = False
        self.awaiting_levels = False
        self.last_entry_at = 0.0
        self.costs = CycleCosts()
        self.last_closed_at = 0.0
        self.last_add_premium = None
        self.close_reason = None
        self.last_cycle_net_usd = None
        self.reason = '等待数据' if self.entry is None else '等待开仓点位'
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

    def exit_reached(self, premium):
        return (premium is not None and self.locked_exit is not None
                and (premium >= self.locked_exit if self.long else premium <= self.locked_exit))

    @property
    def profit_mode(self):
        return self.cfg.bps_exit_mode == 'profit'

    @property
    def execution_slippage(self):
        return min(self.cfg.leg_slippage_bps, self.cfg.bps_execution_slippage_bps) if self.profit_mode else self.cfg.leg_slippage_bps

    def record_fill(self, venue, is_buy, qty, price, fee, *, opening):
        self.costs.record(venue.key, is_buy, qty, price, fee, venue.position,
                          self.engine._step, opening and venue is self.engine.entropy, time.time())

    def exit_quote(self):
        """Conservative full-cycle net at current depth; never a realised fill."""
        if self.pending or not self.entries or not self.costs.known or self.costs.open_notional <= 0:
            return None
        cash = self.costs.cash
        slip = self.execution_slippage / 1e4
        for venue in self.engine.venues.values():
            if not venue.book.is_fresh(self.cfg.staleness_sec):
                return None
            buying = venue.position < 0
            levels = venue.book.sorted_asks() if buying else venue.book.sorted_bids()
            if not levels:
                return None
            top = levels[0][0]
            levels = [(px, qty) for px, qty in levels
                      if (px <= top * (1 + slip) if buying else px >= top * (1 - slip))]
            qty = abs(venue.position)
            if sum(size for _, size in levels) + 1e-12 < qty:
                return None  # A thin book cannot price the entire remaining exit.
            marginal, _ = walk_depth(levels, qty)
            bound = venue.px_round(marginal * (1 + slip if buying else 1 - slip), round_up=not buying)
            fee = venue.fee_bps / 1e4  # Both exit legs are taker, including maker openings.
            cash += qty * bound * (-1 - fee if buying else 1 - fee)
        return dict(net_usd=cash, net_bps=cash / self.costs.open_notional * 1e4,
                    basis_usd=self.costs.open_notional, slippage_reserve_bps=self.execution_slippage)

    def exit_decision(self, now, quote=None):
        if not self.entries:
            return None
        if self.closing:
            return self.close_reason or '继续减仓'
        if not self.profit_mode:
            return 'bps 平仓点位' if self.exit_reached(self.engine.premium_bps()) else None
        if (self.cfg.bps_max_hold_sec > 0 and self.costs.started_at
                and now - self.costs.started_at >= self.cfg.bps_max_hold_sec):
            return '最长持仓到期'
        if not self.costs.known:
            return '旧仓成本未知，按原 bps 点位退出' if self.exit_reached(self.engine.premium_bps()) else None
        quote = self.exit_quote() if quote is None else quote
        if quote is None:
            return None
        if self.cfg.bps_stop_loss_bps > 0 and quote['net_bps'] <= -self.cfg.bps_stop_loss_bps:
            return '净收益止损'
        if quote['net_bps'] >= self.cfg.bps_take_profit_bps:
            return '净收益止盈'
        return None

    def exit_ready(self, now):
        return self.exit_decision(now) is not None

    def entry_check(self, plan, buy, sell, entry, exit):
        if not self.profit_mode:
            return True
        width = abs(exit - entry)
        if width < self.cfg.bps_min_gap_bps:
            self.reason = f'开仓空间不足 {width:.2f}/{self.cfg.bps_min_gap_bps:g} bps'
            return False
        # Uses current order depth, not a prediction of future profit.
        bp, sp = plan.buy_notional / plan.qty, plan.sell_notional / plan.qty
        executable = ((bp / sp - 1) if self.long else (sp / bp - 1)) * 1e4
        if (executable > entry if self.long else executable < entry):
            self.reason = f'可成交开仓报价未达点位 {executable:+.2f} / {entry:+.2f} bps'
            return False
        # Current round-trip spread + all four configured fees + slippage reserve.
        reverse_cash = 0.0
        for venue, opening_buy in ((buy, True), (sell, False)):
            levels = venue.book.sorted_bids() if opening_buy else venue.book.sorted_asks()
            if sum(size for _, size in levels) + 1e-12 < plan.qty:
                self.reason = '完整往返盘口深度不足'
                return False
            _, notional = walk_depth(levels, plan.qty)
            fee = venue.fee_bps / 1e4
            reverse_cash += notional * (1 - fee if opening_buy else -1 - fee)
        basis = plan.buy_notional if buy is self.engine.entropy else plan.sell_notional
        current_cost = max(0, -(plan.exp_edge_usd + reverse_cash) / basis * 1e4)
        required = current_cost + 4 * self.execution_slippage + self.cfg.bps_cost_buffer_bps
        if width < required:
            self.reason = f'开仓空间未覆盖盘口成本 {width:.2f}/{required:.2f} bps'
            return False
        return True

    def _check_paired_positions(self):
        primary, hedge = self.engine.entropy.position, self.engine.hedge.position
        sign = 1 if self.long else -1
        if (not all(math.isfinite(value) for value in (primary, hedge))
                or primary * sign <= 0 or hedge * sign >= 0
                or abs(primary + hedge) > self.tolerance):
            raise RuntimeError('bps 旧仓位未配对或方向与 bps.direction 不一致，请核对两腿数量及配置方向')

    def _adopt_positions(self):
        self._check_paired_positions()
        self.entries = 1  # Historical batch count is unknown; represent the existing pair once.
        self.adopted_position = True
        self.last_entry_at = time.time()
        self.costs.inherit({v.key: v.position for v in self.engine.venues.values()}, time.time())
        if self.cfg.bps_levels == 'manual':
            self.locked_entry, self.locked_exit = self.entry, self.exit
        else:
            self.awaiting_levels = True
        self.reason = '旧仓已接管，等待滚动点位' if self.awaiting_levels else '旧仓已接管，等待平仓点位'
        try:
            self.save()
        except OSError as error:
            raise RuntimeError('bps 接管旧仓的状态无法保存，已停止启动；请检查日志目录写入权限') from error
        log.warning('[BPS] 已接管配对旧仓 %s=%+.8g %s=%+.8g；历史批次未知，计为 1 批；'
                    '点位来源=%s，平仓点位=%s；历史成本不计作本次已知开仓成本',
                    self.engine.entropy.name, self.engine.entropy.position,
                    self.engine.hedge.name, self.engine.hedge.position,
                    self.cfg.bps_levels, self.locked_exit)

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
            if self.profit_mode and state and state.get('identity') == self.identity:
                closed_at = state.get('last_closed_at', 0.0)
                if not isinstance(closed_at, (int, float)) or not math.isfinite(closed_at):
                    raise RuntimeError('bps 冷却状态损坏，请核对状态文件')
                self.last_closed_at = closed_at
                self.last_cycle_net_usd = state.get('last_cycle_net_usd')
                return
            self.path.unlink(missing_ok=True)
            return
        self._check_paired_positions()
        if state is None:
            self._adopt_positions()
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
            self.adopted_position = state.get('adopted_position', False)
            self.awaiting_levels = state.get('awaiting_levels', False)
            self.last_closed_at = state.get('last_closed_at', 0.0)
            self.last_add_premium = state.get('last_add_premium')
            self.close_reason = state.get('close_reason')
            self.last_cycle_net_usd = state.get('last_cycle_net_usd')
            positions_now = {v.key: v.position for v in self.engine.venues.values()}
            if state.get('cycle_costs') is not None:
                self.costs = CycleCosts.restore(state['cycle_costs'], positions_now, self.tolerance)
                if self.costs.known:
                    # Retain known inventory basis; new-session turnover/fees still start at zero.
                    from collections import deque
                    self.engine.execution_stats.lots = {key: deque([list(lot) for lot in lots])
                                                        for key, lots in self.costs.stats.lots.items()}
            else:
                self.costs.inherit(positions_now, time.time())
            waiting = (self.adopted_position and self.awaiting_levels and not self.closing
                       and self.locked_entry is None and self.locked_exit is None)
            if (self.entries < 1 or (not waiting and not all(
                    math.isfinite(x) for x in (self.locked_entry, self.locked_exit)))
                    or self.engine.entropy.position * (1 if self.long else -1) <= 0):
                raise ValueError('invalid cycle')
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
            raise RuntimeError('bps 策略发现无法确认归属的旧仓位或未完成订单，已拒绝启动；请核对订单和仓位后处理') from error
        if waiting and self.cfg.bps_levels == 'manual':
            self.locked_entry, self.locked_exit = self.entry, self.exit
            self.awaiting_levels = False
            self.save()
        log.info('[BPS] restored cycle entries=%s frozen_exit=%s closing=%s awaiting_levels=%s',
                 self.entries, self.locked_exit, self.closing, self.awaiting_levels)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {key: getattr(self, key) for key in
                ('entries', 'locked_entry', 'locked_exit', 'closing', 'pending', 'last_entry_at',
                 'adopted_position', 'awaiting_levels')}
        data.update(identity=self.identity, positions={v.key: v.position for v in self.engine.venues.values()})
        data.update(cycle_costs=self.costs.dump(), last_closed_at=self.last_closed_at,
                    last_add_premium=self.last_add_premium, close_reason=self.close_reason,
                    last_cycle_net_usd=self.last_cycle_net_usd)
        temp = self.path.with_suffix('.tmp')
        temp.write_text(json.dumps(data, allow_nan=False))
        os.replace(temp, self.path)

    async def refresh_loop(self):
        previous = None
        last_error = None
        while not self.engine.stop.is_set():
            minute = int(time.time() // 60)
            if self.cfg.bps_levels == 'rolling' and minute != previous:
                try:
                    entry, exit, end, coverage = await asyncio.to_thread(
                        rolling_levels, self.cfg, time.time(), include_coverage=True)
                    self.entry, self.exit, self.window_end = entry, exit, end
                    self.coverage = coverage
                    if self.awaiting_levels:
                        self.locked_entry, self.locked_exit = entry, exit
                        self.awaiting_levels = False
                        try:
                            self.save()
                        except OSError:
                            self.engine.halted = True
                            self.reason = '接管旧仓点位无法保存，暂停交易；请核对仓位'
                            raise
                        log.info('[BPS] 旧仓点位已锁定 entry=%+.3f exit=%+.3f', entry, exit)
                    previous = minute
                    last_error = None
                    if not self.engine.halted and not self.closing:
                        self.reason = '持仓中' if self.entries else '等待开仓点位'
                    log.info('[BPS LEVELS] csv=%s window=%d valid=%d/%d coverage=%.1f%% '
                             'required=%d missing=%d low_samples=%d invalid_premium=%d source_filtered=%d '
                             'entry=%+.3f exit=%+.3f bps frozen_exit=%s',
                             self.cfg.recorder_csv, self.cfg.bps_window_minutes,
                             coverage['valid_minutes'], self.cfg.bps_window_minutes,
                             coverage['coverage_pct'], coverage['required_minutes'],
                             coverage['missing_minutes'], coverage['low_sample_minutes'],
                             coverage['invalid_premium_minutes'], coverage['source_filtered_minutes'],
                             entry, exit, self.locked_exit)
                except (OSError, ValueError, StopIteration, KeyError, TypeError, csv.Error) as error:
                    self.entry = self.exit = None
                    self.coverage = None
                    reason = str(error)
                    if reason != last_error:
                        log.info('[BPS LEVELS] %s', reason)
                    last_error = reason
                    if not self.engine.halted and not self.closing:
                        self.reason = reason
                self.engine._update_evt.set()
            try:
                await asyncio.wait_for(self.engine.stop.wait(), 5)
            except asyncio.TimeoutError:
                pass
            if self.entries and self.profit_mode and not self.engine.halted:
                self.engine._update_evt.set()  # Hold deadlines are checked even during quiet books.

    def snapshot(self):
        return dict(levels=self.cfg.bps_levels, direction=self.cfg.bps_direction,
                    exit_mode=self.cfg.bps_exit_mode, exit_quote=self.exit_quote() if self.profit_mode else None,
                    take_profit_bps=self.cfg.bps_take_profit_bps, stop_loss_bps=self.cfg.bps_stop_loss_bps,
                    max_hold_sec=self.cfg.bps_max_hold_sec,
                    cost_known=not self.pending and self.costs.known and self.costs.open_notional > 0,
                    cycle_started_at=self.costs.started_at, close_reason=self.close_reason,
                    last_cycle_net_usd=self.last_cycle_net_usd,
                    entry_bps=self.locked_entry if self.entries else self.entry,
                    exit_bps=self.locked_exit if self.entries else self.exit,
                    window_minutes=self.cfg.bps_window_minutes, window_end=self.window_end,
                    min_valid_minutes=self.cfg.bps_min_valid_minutes, coverage=self.coverage,
                    entries=self.entries, max_entries=self.cfg.bps_max_entries,
                    adopted_position=self.adopted_position, awaiting_levels=self.awaiting_levels,
                    closing=self.closing, reason=self.reason,
                    data_path=self.cfg.recorder_csv)

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
        slip = self.execution_slippage / 1e4
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
                       cycle_exit_bps=self.locked_exit if self.entries else self.exit,
                       slippage_bps=self.execution_slippage if self.profit_mode else None)

    def scan(self, now):
        eng, cfg = self.engine, self.cfg
        if eng.halted or self.pending:
            return None
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
        exit_reason = self.exit_decision(now)
        if exit_reason:
            if not self.closing:
                self.closing = True  # latch across partial fills and restarts
                self.close_reason = exit_reason
                try:
                    self.save()
                except OSError:
                    eng.halted = True
                    self.reason = '平仓状态无法保存，暂停交易；请核对仓位'
                    log.exception('[BPS] checkpoint failed')
                    return None
            buy, sell = (eng.hedge, eng.entropy) if self.long else (eng.entropy, eng.hedge)
            plan = self.plan(buy, sell, closing=True)
            self.reason = f'平仓中 · {self.close_reason}' if plan else '等待足够的减仓深度或最小下单规模'
            # Thin depth is transient. Halt only when the actual remainder is dust.
            dust = any(abs(v.position) < max(v.min_base, v.min_quote / v.book.mid())
                       for v in eng.venues.values())
            if plan is None and not self.flat() and dust:
                eng.halted = True
                log.critical('[BPS] %s', self.reason)
            return (buy, sell, plan) if plan else None
        if self.profit_mode and self.entries and not self.costs.known:
            self.reason = '旧仓成本未知，等待原 bps 平仓点位；不继续加仓'
            return None
        if not eng._entry_data_allowed(eng.entropy, eng.hedge):
            eng._armed[eng.direction_key(not self.long)] = None
            self.reason = '等待 100ms 内的双腿行情' if cfg.max_ws_age_ms == 100 else '等待新鲜的双腿行情'
            return None
        if self.profit_mode and not self.entries and now - self.last_closed_at < cfg.bps_cycle_cooldown_sec:
            self.reason = '平仓后冷却中'
            eng._schedule_poke(cfg.bps_cycle_cooldown_sec - (now - self.last_closed_at))
            return None
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
        if self.profit_mode and self.entries and self.last_add_premium is not None:
            improved = self.last_add_premium - premium if self.long else premium - self.last_add_premium
            if improved < cfg.bps_add_step_bps:
                self.reason = '等待价差进一步改善后加仓'
                return None
        buy, sell = (eng.entropy, eng.hedge) if self.long else (eng.hedge, eng.entropy)
        plan = self.plan(buy, sell)
        if self.profit_mode and (plan is None or not self.entry_check(plan, buy, sell, entry, exit)):
            eng._armed[key] = None
            if plan is None:
                self.reason = '仓位上限或盘口深度不足'
            return None
        if eng._armed.get(key) is None:
            eng._armed[key] = now
            eng._schedule_poke(max(cfg.premium_persist_sec, cfg.bps_entry_confirm_sec if self.profit_mode else 0))
        if now - eng._armed[key] < max(cfg.premium_persist_sec, cfg.bps_entry_confirm_sec if self.profit_mode else 0):
            self.reason = '等待开仓信号持续确认'
            return None
        if plan is None:
            self.reason = '仓位上限或盘口深度不足'
            return None
        self.reason = '等待开仓成交'
        return buy, sell, plan

    def before_execution(self, plan):
        if not self.entries and plan.action == 'bps_open':
            self.locked_entry, self.locked_exit = plan.cycle_entry_bps, plan.cycle_exit_bps
            self.costs = CycleCosts()
            self.close_reason = None
        self.pending = True
        self.save()  # a crash after submission cannot silently re-adopt fills
        log.info('[BPS %s] premium=%s entry=%s frozen_exit=%s entries=%d',
                 plan.action, self.engine.premium_bps(), self.locked_entry, self.locked_exit, self.entries)

    def abort_before_send(self, plan):
        self.pending = False
        if not self.entries and plan.action == 'bps_open':
            self.locked_entry = self.locked_exit = None
        self.reason = '行情超时，未发单；等待新鲜行情'
        self.save()

    def after_execution(self, plan, before_qty, unresolved):
        eng = self.engine
        if unresolved or abs(sum(v.position for v in eng.venues.values())) > self.tolerance:
            eng.halted = True
            self.reason = '成交或对冲未确认，暂停交易'
            self.save()
            return
        if self.flat():
            self.last_cycle_net_usd = self.costs.cash if self.costs.known else None
            log.info('[BPS CYCLE] complete net=%s basis=$%.4f exit_reason=%s',
                     self.last_cycle_net_usd, self.costs.open_notional, self.close_reason)
            self.last_closed_at = time.time()
            self.last_add_premium = None
            self.costs = CycleCosts()
            self.entries = 0
            self.locked_entry = self.locked_exit = None
            self.adopted_position = self.awaiting_levels = False
            self.closing = False
            self.last_entry_at = time.time()
            for key in eng._armed:
                eng._armed[key] = None
        elif plan.action == 'bps_open' and abs(eng.entropy.position) > before_qty + eng._step / 2:
            self.entries += 1
            self.last_entry_at = time.time()
            self.last_add_premium = eng.premium_bps()
        self.pending = False
        self.save()
        self.reason = '平仓中' if self.closing else '持仓中' if self.entries else '等待开仓点位'
