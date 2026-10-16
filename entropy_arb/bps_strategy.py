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

from .book import ArbPlan, crossable_base, floor_step, walk_depth
from .recorder import prepare_recorder_source
from .bps_pnl import CycleCosts
from .reduction import reduction_rules
from .maker import rh_hedge_depth

log = logging.getLogger('bps')
PENDING_RECOVERY_RECHECK_SEC = 5.0


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
    """Reuse completed history; by default select the latest N valid minute bars."""
    end = int(now // 60) * 60
    records = cfg.bps_history_mode == 'records'
    start = 0 if records else end - cfg.bps_window_minutes * 60
    with open(cfg.recorder_csv, newline='') as handle:
        header = next(csv.reader(handle))
        metadata = prepare_recorder_source(cfg.recorder_csv, recorder_source(cfg))
        since = float(metadata['since'])
        # Recorder CSVs append chronologically. Expand the bounded tail when
        # invalid/duplicate/source-filtered rows hide the required valid bars.
        size = os.fstat(handle.fileno()).st_size
        tail_bytes = cfg.bps_window_minutes * 2048 + 8192
        while True:
            offset = max(0, size - tail_bytes)
            points = {}
            present, low_samples, invalid_premium, before_source = set(), set(), set(), set()
            handle.seek(offset)
            if offset:
                handle.readline()
            else:
                next(csv.reader(handle))
            for values in csv.reader(handle):
                row = dict(zip(header, values))
                try:
                    ts = float(row['minute_ts'])
                    if not (math.isfinite(ts) and ts % 60 == 0 and start <= ts < end):
                        continue
                    ts = int(ts)
                    present.add(ts)
                    if ts < since:
                        before_source.add(ts)
                        continue
                    try:
                        samples = int(row['samples'])
                    except (KeyError, ValueError, TypeError):
                        low_samples.add(ts)
                        continue
                    try:
                        if cfg.bps_price_basis == 'executable':
                            pb, pa, hb, ha = (float(row[k]) for k in
                                              ('entropy_bid', 'entropy_ask', 'hedge_bid', 'hedge_ask'))
                            if not all(math.isfinite(x) and x > 0 for x in (pb, pa, hb, ha)) or pb > pa or hb > ha:
                                raise ValueError('invalid quotes')
                            buy_primary = (hb / pa - 1) * 1e4
                            sell_primary = (pb / ha - 1) * 1e4
                            value = ((buy_primary, sell_primary) if cfg.bps_direction == 'long_primary'
                                     else (sell_primary, buy_primary))
                        else:
                            value = float(row['premium_close_bps'])
                    except (KeyError, ValueError, TypeError):
                        invalid_premium.add(ts)
                        continue
                    if samples < 10:
                        low_samples.add(ts)
                    elif not all(math.isfinite(x) for x in (value if isinstance(value, tuple) else (value,))):
                        invalid_premium.add(ts)
                    else:
                        points[ts] = value
                except (KeyError, ValueError, TypeError, OverflowError):
                    continue
            if not records or offset == 0 or len(points) >= cfg.bps_window_minutes:
                break
            tail_bytes *= 2
    if records:
        points = {ts: points[ts] for ts in sorted(points)[-cfg.bps_window_minutes:]}
    required = min(cfg.bps_window_minutes, cfg.bps_min_valid_minutes)
    first = min(points) if points else None
    last = max(points) if points else None
    if records:
        missing = int((last - first) // 60 + 1) - len(points) if points else 0
    else:
        missing = cfg.bps_window_minutes - len(present)
    coverage = dict(valid_minutes=len(points), required_minutes=required,
                    coverage_pct=len(points) / cfg.bps_window_minutes * 100,
                    missing_minutes=missing,
                    low_sample_minutes=len(low_samples - points.keys()),
                    invalid_premium_minutes=len(invalid_premium - points.keys()),
                    source_filtered_minutes=len(before_source - points.keys()),
                    history_mode=cfg.bps_history_mode,
                    history_start_ts=first, history_end_ts=last + 60 if last is not None else None,
                    latest_data_age_sec=max(0, now - last - 60) if last is not None else None)
    if len(points) < required:
        source_note = '历史有效记录，不要求连续' if records else '最近时间窗口，允许缺口'
        raise ValueError(f'等待有效分钟数据 {len(points)}/{cfg.bps_window_minutes}；'
                         f'至少 {required} 条；缺行 {coverage["missing_minutes"]} 分钟，'
                         f'采样不足 {coverage["low_sample_minutes"]} 分钟，'
                         f'无效溢价 {coverage["invalid_premium_minutes"]} 分钟，'
                         f'来源过滤 {coverage["source_filtered_minutes"]} 分钟；'
                         f'{source_note}，读取 {cfg.recorder_csv}')
    values = list(points.values())
    entry = percentile([v[0] for v in values] if cfg.bps_price_basis == 'executable' else values, cfg.bps_entry_percentile)
    exit = percentile([v[1] for v in values] if cfg.bps_price_basis == 'executable' else values, cfg.bps_exit_percentile)
    if cfg.bps_price_basis == 'mid' and (cfg.bps_direction == 'long_primary' and entry >= exit
            or cfg.bps_direction == 'short_primary' and entry <= exit):
        raise ValueError('历史溢价波动不足，开平仓点位重合')
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
        self.locked_price_basis = None
        self.closing = False
        self.pending = False
        self.startup_recovery = None
        self.adopted_position = False
        self.awaiting_levels = False
        self.last_entry_at = 0.0
        self.costs = CycleCosts()
        self.last_closed_at = 0.0
        self.last_add_premium = None
        self.close_reason = None
        self.last_cycle_net_usd = None
        self._dust_log = None
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

    @property
    def price_basis(self):
        return self.locked_price_basis if self.entries and self.locked_price_basis else self.cfg.bps_price_basis

    @property
    def executable(self):
        return self.price_basis == 'executable'

    def signal(self, closing=False, plan=None):
        if not self.executable:
            return self.engine.premium_bps()
        if plan is not None:
            return plan.marginal_premium_bps
        primary_buy = self.long != closing
        buy, sell = ((self.engine.entropy, self.engine.hedge) if primary_buy
                     else (self.engine.hedge, self.engine.entropy))
        ask, bid = buy.book.best_ask(), sell.book.best_bid()
        return (bid / ask - 1) * 1e4 if ask and bid else None

    def flat(self):
        # Net-delta tolerance is not a flat-position threshold (e.g. 0.001
        # BTC may still be a material paired position).
        return all(abs(v.position) <= 1e-12 for v in self.engine.venues.values())

    @property
    def tolerance(self):
        return max(1e-12, min(self.cfg.net_tolerance_base, self.engine._step / 2))

    def _dust_prices(self):
        venues = list(self.engine.venues.values())
        if not all(v.book.is_fresh(self.cfg.staleness_sec) for v in venues):
            return None
        prices = [v.book.mid() for v in venues]
        return prices if all(p is not None and math.isfinite(p) and p > 0 for p in prices) else None

    def precision_dust(self, positions=None):
        """Classify quantity/price only; order certainty is checked separately."""
        eng = self.engine
        positions = positions if positions is not None else [v.position for v in eng.venues.values()]
        net = sum(positions)
        cap = self.cfg.bps_max_dust_notional_usd
        if not math.isfinite(net) or abs(net) <= 1e-12 or abs(net) > eng._step + 1e-12 or cap <= 0:
            return False
        prices = self._dust_prices()
        if prices is None or abs(net) * max(prices) > cap + 1e-12:
            return False
        # An outage never turns a normally tradeable imbalance into dust.
        for venue, price, position in zip(eng.venues.values(), prices, positions):
            if position * net <= 0:
                continue
            step, minimum_base, minimum_quote = reduction_rules(venue, eng._step, self.cfg.min_order_notional)
            qty = floor_step(min(abs(net), abs(position)), step)
            if qty > 0 and qty >= minimum_base and qty * price >= minimum_quote:
                return False
        return True

    def net_balanced(self):
        net = sum(v.position for v in self.engine.venues.values())
        return math.isfinite(net) and (abs(net) <= 1e-12 or self.precision_dust())

    def confirmed_residual_reason(self):
        net = sum(v.position for v in self.engine.venues.values())
        prices = self._dust_prices()
        value = f'（名义敞口约 ${abs(net) * max(prices):.4f}）' if prices else ''
        return (f'成交已确认，剩余净仓位 {net:+.8g} {self.cfg.symbol}{value}未消除；'
                f'{self.engine.residual_block_reason(net)}；暂停新订单')

    def only_dust_positions(self, positions=None):
        positions = positions if positions is not None else [v.position for v in self.engine.venues.values()]
        if all(abs(position) <= 1e-12 for position in positions):
            return False
        prices = self._dust_prices()
        return (prices is not None and (abs(sum(positions)) <= 1e-12 or self.precision_dust(positions))
                and all(abs(position) <= self.engine._step + 1e-12
                        and abs(position) * price <= self.cfg.bps_max_dust_notional_usd + 1e-12
                        for position, price in zip(positions, prices)))

    def residual_status(self):
        net = sum(v.position for v in self.engine.venues.values())
        prices = self._dust_prices()
        accepted = (not self.pending and not self.engine.halted and self.engine.active_maker is None
                    and not self.engine._position_uncertain and self.precision_dust())
        return dict(net_base=net, notional_usd=abs(net) * max(prices) if prices else None,
                    accepted_dust=accepted, max_dust_notional_usd=self.cfg.bps_max_dust_notional_usd)

    def note_dust(self):
        if not self.precision_dust():
            self._dust_log = None
            return
        net = sum(v.position for v in self.engine.venues.values())
        previous = self._dust_log
        now = time.monotonic()
        if previous is None or abs(net - previous[0]) > 1e-12 or now - previous[1] >= 60:
            log.warning('[BPS DUST] confirmed precision residual net=%+.8g value=$%.6f cap=$%.2f; '
                        'below order minimum, continue monitoring without rounding positions to zero',
                        net, self.residual_status()['notional_usd'], self.cfg.bps_max_dust_notional_usd)
            self._dust_log = (net, now)

    def exit_reached(self, premium):
        return (premium is not None and self.locked_exit is not None
                and (premium >= self.locked_exit if self.executable or self.long else premium <= self.locked_exit))

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
            fee = (self.cfg.primary_fee_bps if self.cfg.primary_maker_only
                   and venue is self.engine.entropy else venue.fee_bps) / 1e4
            cash += qty * bound * (-1 - fee if buying else 1 - fee)
        return dict(net_usd=cash, net_bps=cash / self.costs.open_notional * 1e4,
                    basis_usd=self.costs.open_notional, slippage_reserve_bps=self.execution_slippage)

    def exit_decision(self, now, quote=None):
        if not self.entries:
            return None
        if not self.profit_mode:
            return 'bps 平仓点位' if self.exit_reached(self.signal(closing=True)) else None
        if self.closing:
            return self.close_reason or '继续减仓'
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
            fee = (self.cfg.primary_fee_bps if self.cfg.primary_maker_only
                   and venue is self.engine.entropy else venue.fee_bps) / 1e4
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
                or ((primary * sign <= 0 or hedge * sign >= 0) and not self.only_dust_positions())
                or not self.net_balanced()):
            raise RuntimeError('bps 旧仓位未配对或方向与 bps.direction 不一致，请核对两腿数量及配置方向')

    def _adopt_positions(self):
        self._check_paired_positions()
        self.entries = 1  # Historical batch count is unknown; represent the existing pair once.
        self.locked_price_basis = self.cfg.bps_price_basis
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

    def _use_current_basis(self):
        if self.price_basis == self.cfg.bps_price_basis or self.profit_mode:
            return False
        previous = self.price_basis
        self.locked_price_basis = self.cfg.bps_price_basis
        self.closing = False
        self.close_reason = None
        self.last_add_premium = None
        self.adopted_position = True
        self.awaiting_levels = self.cfg.bps_levels == 'rolling'
        self.locked_entry, self.locked_exit = ((None, None) if self.awaiting_levels else (self.entry, self.exit))
        self.reason = '配对旧仓已切换当前口径，等待历史点位' if self.awaiting_levels else '配对旧仓已切换当前点位'
        for key in self.engine._armed:
            self.engine._armed[key] = None
        log.warning('[BPS ADOPT] paired positions retained; basis %s -> %s; '
                    'recompute current levels before trading, no flatten required', previous, self.locked_price_basis)
        return True

    async def verify_existing_pair(self):
        """Validate a fresh adoption/rebase before enabling strategy loops."""
        if self.flat():
            return
        self._check_paired_positions()
        original = self.path.read_text() if self.path.exists() else None
        state = json.loads(original) if original is not None else None
        if state is not None:
            if not isinstance(state, dict) or state.get('identity') != self.identity or state.get('pending'):
                return  # restore()/recover_pending() retain their strict checks.
            if state.get('active_maker'):
                raise RuntimeError('bps 旧主腿订单仍需确认，不能仅凭配对仓位接管')
            mismatch = any(abs(v.position - state.get('positions', {}).get(v.key, float('inf'))) > self.tolerance
                           for v in self.engine.venues.values())
            switching = (state.get('locked_price_basis') or 'mid') != self.cfg.bps_price_basis
            if not mismatch and not switching:
                return
        else:
            mismatch = False
        for attempt in range(2):
            if self.engine.stop.is_set():
                raise RuntimeError('bps 旧仓核验已停止')
            for venue in self.engine.venues.values():
                timeout = max(10, self.cfg.settle_timeout_sec)
                try:
                    has_orders = await asyncio.wait_for(venue.has_open_orders(), timeout)
                    live = await asyncio.wait_for(venue.fetch_position(), timeout)
                except Exception as error:
                    raise RuntimeError(f'bps 旧仓查询失败 ({type(error).__name__})，未接管') from error
                if has_orders is not False:
                    raise RuntimeError('bps 接管旧仓前存在未成交订单，等待确认')
                if (not isinstance(live, (int, float)) or isinstance(live, bool)
                        or not math.isfinite(live) or abs(live - venue.position) > self.tolerance):
                    raise RuntimeError('bps 接管旧仓时实时仓位变化，等待稳定后重试')
            if attempt == 0:
                try:
                    await asyncio.wait_for(self.engine.stop.wait(), PENDING_RECOVERY_RECHECK_SEC)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise RuntimeError('bps 旧仓核验已停止')
        current = self.path.read_text() if self.path.exists() else None
        if current != original:
            raise RuntimeError('bps 旧仓核验期间状态文件变化，未接管')
        if mismatch:
            archive = self.path.with_name(f'{self.path.name}.adopted-{time.time_ns()}.bak')
            with archive.open('x', encoding='utf-8') as output:
                output.write(original)
            self._adopt_positions()
            log.warning('[BPS ADOPT] verified paired positions differ from checkpoint; '
                        'adopted without flattening; historical costs unknown; archive=%s', archive)

    async def recover_pending(self):
        """Verify old orders and stable positions before enabling recovery.

        Read authoritative orders and then positions twice, allowing account
        REST state to catch up. Never clear uncertainty on a failed/partial
        response, or reuse costs from ambiguous/manual fills. Nonflat positions
        defer reduce-only repair until account and market feeds are ready.
        """
        if not self.path.exists():
            return
        try:
            original = self.path.read_text()
            state = json.loads(original)
            if not isinstance(state, dict):
                raise ValueError('invalid checkpoint')
        except (OSError, ValueError, TypeError) as error:
            raise RuntimeError('bps 状态文件损坏，请核对订单和仓位') from error
        net = sum(v.position for v in self.engine.venues.values())
        dust_candidate = math.isfinite(net) and 1e-12 < abs(net) <= self.engine._step + 1e-12
        if not state.get('pending') and not dust_candidate:
            return
        if state.get('identity') != self.identity:
            raise RuntimeError('bps 未确认状态的账户或交易对与当前配置不一致，不能自动解除')
        log.warning('[BPS RECOVERY] 检测到旧 pending 或精度尾差，开始查询两边未成交订单与实时持仓')
        query_timeout = max(10.0, self.cfg.settle_timeout_sec)

        async def check(venue):
            try:
                has_orders = await asyncio.wait_for(venue.has_open_orders(), query_timeout)
                position = await asyncio.wait_for(venue.fetch_position(), query_timeout)
            except Exception as error:
                # Do not include transport exceptions that may contain an
                # authenticated URL/header or other account secrets.
                raise RuntimeError(f'bps 自动核验未通过 [{venue.name}]：'
                                   f'查询失败 ({type(error).__name__})；'
                                   '保留 pending，未启动交易') from error
            if has_orders is not False:
                raise RuntimeError(f'bps 自动核验未通过 [{venue.name}]：'
                                   '存在未成交订单或未能确认订单为空；保留 pending')
            if (not isinstance(position, (int, float)) or isinstance(position, bool)
                    or not math.isfinite(position)):
                raise RuntimeError(f'bps 自动核验未通过 [{venue.name}]：'
                                   '实时持仓数据无效；保留 pending')
            return venue, position

        await self._resolve_saved_maker(state.get('active_maker'))
        verified = []
        for attempt in range(2):
            if self.engine.stop.is_set():
                raise RuntimeError('bps 自动核验已停止，保留 pending')
            results = await asyncio.gather(*(check(v) for v in self.engine.venues.values()),
                                           return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            if verified and any(abs(position - old_position) > 1e-12
                                for (_, position), (_, old_position) in zip(results, verified)):
                raise RuntimeError('bps 两次实时持仓不同，等待旧订单及仓位稳定；保留 pending')
            verified = results
            log.info('[BPS RECOVERY] 第 %d/2 次核验通过：%s；均无未成交订单',
                     attempt + 1, ' '.join(f'{v.name}={p:+.8g}' for v, p in verified))
            if attempt == 0:
                try:
                    await asyncio.wait_for(self.engine.stop.wait(), PENDING_RECOVERY_RECHECK_SEC)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise RuntimeError('bps 自动核验已停止，保留 pending')
        if self.engine.stop.is_set():
            raise RuntimeError('bps 自动核验已停止，保留 pending')
        if any(abs(position) > 1e-12 for _, position in verified):
            sign = 1 if self.long else -1
            positions = {v.key: position for v, position in verified}
            if positions[self.engine.entropy.key] * sign < -1e-12 or positions[self.engine.hedge.key] * sign > 1e-12:
                raise RuntimeError('bps 旧仓方向与配置不同，不能自动恢复；保留 pending')
            self.startup_recovery = dict(original=original, state=state, positions=positions)
            self.pending = True
            for venue, position in verified:
                venue.position = position
            log.warning('[BPS RECOVERY] 旧订单已核验，两次仓位稳定；等待行情后仅减少多余仓位 net=%+.8g',
                        sum(positions.values()))
            return True
        try:
            if self.path.read_text() != original:
                raise RuntimeError('bps 核验期间状态文件变化，保留 pending')
            archive = self.path.with_name(f'{self.path.name}.recovered-{time.time_ns()}.bak')
            # Keep the original pending checkpoint in place until save()
            # atomically replaces it. A disk/write failure must not erase it.
            with archive.open('x', encoding='utf-8') as output:
                output.write(original)
            for venue, position in verified:
                venue.position = position
            self.__init__(self.engine)
            self.last_closed_at = time.time()
            self.save()
        except OSError as error:
            raise RuntimeError('bps 核验通过但状态归档/保存失败，已停止启动') from error
        log.warning('[BPS RECOVERY] 两次核验均为零持仓、无未成交订单，已自动解除旧 pending；'
                    '原状态归档至 %s；旧轮盈亏未重建', archive)

    async def _resolve_saved_maker(self, saved):
        """Cancel only a durably recorded own maker; never all account orders."""
        if not saved:
            return  # Legacy checkpoints rely on the two no-open-order checks.
        from .maker import MakerOrder
        primary = self.engine.entropy
        if saved.get('venue') != primary.name or not saved.get('client_id'):
            raise RuntimeError('bps 旧挂单标识无效；保留 pending')
        client_id = saved['client_id']
        if primary.kind == 'lighter':
            client_id = int(client_id)
        elif primary.kind == 'hl':
            from hyperliquid.utils.types import Cloid
            client_id = Cloid.from_str(client_id)
        order = MakerOrder(client_id, saved['price'], time.time())
        deadline = time.monotonic() + max(30, 2 * self.cfg.settle_timeout_sec)
        cancel_sent = False
        while time.monotonic() < deadline and not self.engine.stop.is_set():
            try:
                result = await asyncio.wait_for(primary.maker_state(order), self.cfg.settle_timeout_sec)
                if result['terminal']:
                    log.warning('[BPS RECOVERY] 旧主腿挂单已确认终态 client_id=%s status=%s filled=%s',
                                saved['client_id'], result['status'], result['filled_base'])
                    return
                if not cancel_sent and result['status'] != 'pending-confirmation':
                    order.cancel_requested_at = time.monotonic()
                    await asyncio.wait_for(primary.cancel_maker(order), self.cfg.settle_timeout_sec)
                    cancel_sent = True
            except Exception as error:
                log.warning('[BPS RECOVERY] 旧挂单查询/撤单尚未确认 (%s)，稍后重试', type(error).__name__)
            try:
                await asyncio.wait_for(self.engine.stop.wait(), 1)
            except asyncio.TimeoutError:
                pass
        raise RuntimeError('bps 旧挂单最终状态仍未确认；保留 pending，未发送仓位修正单')

    async def finish_startup_recovery(self):
        """Only run after feeds start, and before any strategy/reconcile loop."""
        recovery, eng = self.startup_recovery, self.engine
        if not recovery:
            return
        original, state, positions = recovery['original'], recovery['state'], recovery['positions']
        if self.path.read_text() != original:
            raise RuntimeError('bps 核验期间状态文件变化；保留 pending')
        archive = self.path.with_name(f'{self.path.name}.recovered-{time.time_ns()}.bak')
        with archive.open('x', encoding='utf-8') as output:
            output.write(original)
        self.entries = max(1, int(state.get('entries', 1)))
        self.locked_price_basis = state.get('locked_price_basis') or 'mid'
        last_entry = state.get('last_entry_at', 0.0)
        if not isinstance(last_entry, (float, int)) or not math.isfinite(last_entry):
            raise RuntimeError('bps 旧加仓时间无效；保留 pending')
        self.last_entry_at = last_entry
        self.locked_entry, self.locked_exit = state.get('locked_entry'), state.get('locked_exit')
        valid_levels = all(isinstance(x, (float, int)) and math.isfinite(x)
                           for x in (self.locked_entry, self.locked_exit))
        self.awaiting_levels = not valid_levels and self.cfg.bps_levels == 'rolling'
        if not valid_levels:
            self.locked_entry, self.locked_exit = ((self.entry, self.exit)
                                                  if not self.awaiting_levels else (None, None))
        self.adopted_position = True
        self.closing = False
        self.close_reason = None
        if not state.get('pending'):
            # A clean restart with dust retains the established cycle metadata,
            # especially a latched profit-mode exit and its add-spacing basis.
            for key in ('closing', 'close_reason', 'last_add_premium', 'last_closed_at', 'last_cycle_net_usd'):
                if key in state:
                    setattr(self, key, state[key])
        if (not state.get('pending') and state.get('cycle_costs') is not None
                and all(abs(positions[v.key] - state['positions'][v.key]) <= self.tolerance
                        for v in eng.venues.values())):
            self.costs = CycleCosts.restore(state['cycle_costs'], positions, self.tolerance)
        else:
            self.costs.inherit(positions, time.time())
        self._use_current_basis()
        # Old ambiguous fills have unknown prices. Do not manufacture P&L.
        self.save()  # durable pending before the first reduce-only repair
        initial_net = sum(positions.values())
        deadline = time.monotonic() + max(30, 2 * self.cfg.settle_timeout_sec)
        for _ in range(3):
            net = sum(v.position for v in eng.venues.values())
            if abs(net) <= 1e-12:
                break
            while not eng.stop.is_set() and time.monotonic() < deadline:
                if all(v.ready_to_trade() and v.book.is_fresh(self.cfg.staleness_sec)
                       for v in eng.venues.values()):
                    break
                try:
                    await asyncio.wait_for(eng.stop.wait(), .1)
                except asyncio.TimeoutError:
                    pass
            if eng.stop.is_set() or time.monotonic() >= deadline:
                raise RuntimeError('bps 自动修正等待行情超时或停止；保留 pending')
            if self.net_balanced():
                break  # Fresh prices now verify the size + USD dust limits.
            # Recheck that no external order/position changed during feed setup.
            for venue in eng.venues.values():
                if await asyncio.wait_for(venue.has_open_orders(), self.cfg.settle_timeout_sec) is not False:
                    raise RuntimeError('bps 修正前发现未成交订单；保留 pending')
                live = await asyncio.wait_for(venue.fetch_position(), self.cfg.settle_timeout_sec)
                if not math.isfinite(live) or abs(live - venue.position) > self.tolerance:
                    raise RuntimeError('bps 修正前实时仓位变化；保留 pending')
            if eng.stop.is_set() or not all(v.ready_to_trade() and v.book.is_fresh(self.cfg.staleness_sec)
                                           for v in eng.venues.values()):
                raise RuntimeError('bps 修正前行情失效或已停止；保留 pending')
            excess = next(v for v in eng.venues.values() if v.position * net > 0)
            slip = self.cfg.hedge_slippage_bps / 1e4
            limit = (excess.px_round(excess.book.best_bid() * (1 - slip), False) if net > 0
                     else excess.px_round(excess.book.best_ask() * (1 + slip), True))
            cap_price = max(excess.book.best_ask(), limit)
            step, min_base, min_quote = reduction_rules(excess, eng._step, self.cfg.min_order_notional)
            qty = floor_step(min(abs(net), self.cfg.max_order_notional / cap_price), step)
            minimum = max(min_base, min_quote / limit)
            minimum = round(math.ceil(minimum / step - 1e-9) * step, 12)
            capped_qty = qty
            if self.tolerance < abs(net) - qty < minimum:
                qty = floor_step(abs(net) - minimum, step)
            if qty < minimum:
                reason = (f'bps 自动修正被拦截 [{excess.name}]：净差额 {net:+.8g}，'
                          f'按单笔上限可下 {capped_qty:.8g}，避免小尾仓后拟下 {qty:.8g}；'
                          f'保护价 {limit:.8g}，减仓最小数量 {min_base:.8g}，'
                          f'减仓最小金额 ${min_quote:.4f}，单笔上限 ${self.cfg.max_order_notional:.4f}；'
                          '差额低于最小下单规模或单笔上限不足以拆分；保留 pending')
                log.error('[BPS RECOVERY BLOCKED] %s', reason)
                raise RuntimeError(reason)
            confirmed = await eng._hedge(math.copysign(qty, net))
            self.save()
            if not confirmed or eng.halted:
                raise RuntimeError('bps 自动修正成交未确认或失败；保留 pending，禁止重复补单')
            remaining = sum(v.position for v in eng.venues.values())
            if abs(remaining) >= abs(net) - 1e-12:
                raise RuntimeError('bps 自动修正未减少差额或低于最小下单金额；保留 pending')
            # Let account REST catch up before another reduction or verification.
            try:
                await asyncio.wait_for(eng.stop.wait(), PENDING_RECOVERY_RECHECK_SEC)
            except asyncio.TimeoutError:
                pass
            else:
                raise RuntimeError('bps 自动核验已停止，保留 pending')
        if not self.net_balanced():
            raise RuntimeError('bps 自动修正后仍有差额；保留 pending，未开启交易')
        for attempt in range(2):
            for venue in eng.venues.values():
                if await asyncio.wait_for(venue.has_open_orders(), self.cfg.settle_timeout_sec) is not False:
                    raise RuntimeError('bps 修正后发现未成交订单；保留 pending')
                live = await asyncio.wait_for(venue.fetch_position(), self.cfg.settle_timeout_sec)
                if not math.isfinite(live) or abs(live - venue.position) > self.tolerance:
                    raise RuntimeError('bps 修正后仓位尚未确认；保留 pending')
            if attempt == 0:
                try:
                    await asyncio.wait_for(eng.stop.wait(), PENDING_RECOVERY_RECHECK_SEC)
                except asyncio.TimeoutError:
                    pass
                else:
                    raise RuntimeError('bps 自动核验已停止，保留 pending')
        if self.flat():
            self.__init__(eng)
            self.last_closed_at = time.time()
        self.pending = False
        self.startup_recovery = None
        self.save()
        self.note_dust()
        log.warning('[BPS RECOVERY] 核验/修正完成 net=%+.8g -> %+.8g precision_dust=%s；'
                    '旧状态归档=%s；历史成本=%s',
                    initial_net, sum(v.position for v in eng.venues.values()), self.precision_dust(),
                    archive, '已知' if self.costs.known else '未知，不补算盈利')

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
            self.locked_price_basis = state.get('locked_price_basis') or 'mid'
            if self.locked_price_basis not in ('mid', 'executable'):
                raise ValueError('invalid cycle price basis')
            self.adopted_position = state.get('adopted_position', False)
            self.awaiting_levels = state.get('awaiting_levels', False)
            self.last_closed_at = state.get('last_closed_at', 0.0)
            self.last_add_premium = state.get('last_add_premium')
            self.close_reason = state.get('close_reason')
            self.last_cycle_net_usd = state.get('last_cycle_net_usd')
            positions_now = {v.key: v.position for v in self.engine.venues.values()}
            if state.get('cycle_costs') is not None:
                self.costs = CycleCosts.restore(state['cycle_costs'], positions_now, self.tolerance)
                # Retain even unknown-price dust lots: dropping a tiny inherited
                # lot would wrongly make its later realized P&L look known.
                # New-session turnover and fees still start at zero.
                from collections import deque
                self.engine.execution_stats.lots = {key: deque([list(lot) for lot in lots])
                                                    for key, lots in self.costs.stats.lots.items()}
            else:
                self.costs.inherit(positions_now, time.time())
            waiting = (self.adopted_position and self.awaiting_levels and not self.closing
                       and self.locked_entry is None and self.locked_exit is None)
            if ((self.entries < 1 and not self.only_dust_positions()) or (not waiting and not all(
                    math.isfinite(x) for x in (self.locked_entry, self.locked_exit)))
                    or (self.engine.entropy.position * (1 if self.long else -1) <= 0 and not self.only_dust_positions())):
                raise ValueError('invalid cycle')
        except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
            raise RuntimeError('bps 策略发现无法确认归属的旧仓位或未完成订单，已拒绝启动；请核对订单和仓位后处理') from error
        if waiting and self.cfg.bps_levels == 'manual':
            self.locked_entry, self.locked_exit = self.entry, self.exit
            self.awaiting_levels = False
            self.save()
        if self._use_current_basis():
            original = self.path.read_text()
            archive = self.path.with_name(f'{self.path.name}.basis-{time.time_ns()}.bak')
            with archive.open('x', encoding='utf-8') as output:
                output.write(original)
            self.save()
        log.info('[BPS] restored cycle entries=%s frozen_exit=%s closing=%s awaiting_levels=%s',
                 self.entries, self.locked_exit, self.closing, self.awaiting_levels)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {key: getattr(self, key) for key in
                ('entries', 'locked_entry', 'locked_exit', 'closing', 'pending', 'last_entry_at',
                 'adopted_position', 'awaiting_levels')}
        data.update(identity=self.identity, positions={v.key: v.position for v in self.engine.venues.values()})
        data['locked_price_basis'] = self.locked_price_basis
        data['active_maker'] = self.engine.active_maker
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
                        if self.price_basis != self.cfg.bps_price_basis:
                            from dataclasses import replace
                            old_cfg = replace(self.cfg, bps_price_basis=self.price_basis,
                                              bps_entry_percentile=100 - self.cfg.bps_entry_percentile if self.long else self.cfg.bps_entry_percentile,
                                              bps_exit_percentile=self.cfg.bps_exit_percentile if self.long else 100 - self.cfg.bps_exit_percentile)
                            old_entry, old_exit, _ = await asyncio.to_thread(rolling_levels, old_cfg, time.time())
                        else:
                            old_entry, old_exit = entry, exit
                        self.locked_entry, self.locked_exit = old_entry, old_exit
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
                    if not self.engine.halted and not self.closing and not self.pending:
                        self.reason = '持仓中' if self.entries else '等待开仓点位'
                    log.info('[BPS LEVELS] csv=%s window=%d valid=%d/%d coverage=%.1f%% '
                             'required=%d missing=%d low_samples=%d invalid_premium=%d source_filtered=%d '
                             'entry=%+.3f exit=%+.3f bps frozen_exit=%s history_mode=%s history_start=%s history_end=%s',
                             self.cfg.recorder_csv, self.cfg.bps_window_minutes,
                             coverage['valid_minutes'], self.cfg.bps_window_minutes,
                             coverage['coverage_pct'], coverage['required_minutes'],
                             coverage['missing_minutes'], coverage['low_sample_minutes'],
                             coverage['invalid_premium_minutes'], coverage['source_filtered_minutes'],
                             entry, exit, self.locked_exit, self.cfg.bps_history_mode,
                             coverage.get('history_start_ts'), coverage.get('history_end_ts'))
                except (OSError, ValueError, StopIteration, KeyError, TypeError, csv.Error) as error:
                    self.entry = self.exit = None
                    self.coverage = None
                    reason = str(error)
                    if reason != last_error:
                        log.info('[BPS LEVELS] %s', reason)
                    last_error = reason
                    if not self.engine.halted and not self.closing and not self.pending:
                        self.reason = reason
                self.engine._update_evt.set()
            try:
                await asyncio.wait_for(self.engine.stop.wait(), 5)
            except asyncio.TimeoutError:
                pass
            if self.entries and self.profit_mode and not self.engine.halted:
                self.engine._update_evt.set()  # Hold deadlines are checked even during quiet books.

    def snapshot(self):
        return dict(levels=self.cfg.bps_levels, history_mode=self.cfg.bps_history_mode, direction=self.cfg.bps_direction,
                    price_basis=self.price_basis, configured_price_basis=self.cfg.bps_price_basis,
                    open_signal_bps=self.signal(), close_signal_bps=self.signal(closing=True),
                    position_residual=self.residual_status(),
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
                    entries=self.entries,
                    adopted_position=self.adopted_position, awaiting_levels=self.awaiting_levels,
                    closing=self.closing, reason=self.reason,
                    data_path=self.cfg.recorder_csv)

    def plan(self, buy, sell, closing=False, quote=None):
        eng, cfg = self.engine, self.cfg
        # Closing plans retain the venue minimums; the configured opening
        # notional floor must not strand an otherwise valid close.
        configured_minimum = 0 if closing else cfg.min_order_notional
        _, buy_min_base, buy_min_quote = reduction_rules(buy, eng._step, configured_minimum)
        _, sell_min_base, sell_min_quote = reduction_rules(sell, eng._step, configured_minimum)
        minimum_base = max(buy_min_base, sell_min_base) if closing else eng._min_base
        maker = cfg.primary_order_type == 'maker' and (not closing or cfg.primary_maker_only)
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
        target = ((self.locked_exit if closing else self.locked_entry) if self.entries
                  else (self.exit if closing else self.entry))
        if self.executable:
            if target is None:
                return None
            qualified, _ = crossable_base(asks, bids, target / 1e4)
            depth = min(depth, qualified)
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
        hedge_depth = rh_hedge_depth(eng, pbuy) if maker else None
        if hedge_depth is not None:
            qty = min(qty, hedge_depth['capacity_base'])
        def bounded_tail(quantity):
            # Only tolerate the unavoidable fractional remainder after closing
            # every orderable step, never deliberately strand a small slice
            # because a cap/depth restriction shortened this batch.
            return (quantity >= floor_step(paired, eng._step) - 1e-12
                    and self.only_dust_positions([v.position + (quantity if v is buy else -quantity)
                                                 for v in eng.venues.values()]))
        if closing:
            paired = min(max(0, -buy.position), max(0, sell.position))
            minimum_tail = max(minimum_base, buy_min_quote / asks[0][0],
                               sell_min_quote / (bids[-1][0] * (1 - slip)))
            minimum_tail = round(math.ceil(minimum_tail / eng._step - 1e-9) * eng._step, 12)
            residual = paired - qty
            if eng._step / 2 <= residual < minimum_tail and not bounded_tail(qty):
                # A per-order cap or limited depth must not strand an
                # untradeable last slice. Leave at least one valid order.
                qty = floor_step(max(0, paired - minimum_tail), eng._step)
        if qty <= 0 or qty < minimum_base:
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
            if qty <= 0 or qty < minimum_base:
                return None
            bp, bn = walk_depth(asks, qty)
            sp, sn = walk_depth(bids, qty)
        if closing and eng._step / 2 <= paired - qty < minimum_tail and not bounded_tail(qty):
            qty = floor_step(max(0, paired - minimum_tail), eng._step)
            if qty <= 0 or qty < minimum_base:
                return None
            bp, bn = walk_depth(asks, qty)
            sp, sn = walk_depth(bids, qty)
        minimum = max(buy_min_quote, sell_min_quote) if closing else eng._min_notional
        if min(bn, sn) < minimum:
            return None
        bf = cfg.primary_fee_bps if maker and pbuy else buy.fee_bps
        sf = cfg.primary_fee_bps if maker and not pbuy else sell.fee_bps
        safe_slip = None
        if self.executable:
            ratio = 1 + target / 1e4
            safe_slip = min(slip, max(0, (sp - ratio * bp) / (sp + ratio * bp))) * 1e4
        return ArbPlan(qty, bp, sp, bn, sn, depth, depth * max_px,
                       (bids[0][0] / asks[0][0] - 1) * 1e4,
                       (sp / bp - 1) * 1e4, bf / 1e4, sf / 1e4,
                       action='bps_close' if closing else 'bps_open',
                       cycle_entry_bps=self.locked_entry if self.entries else self.entry,
                       cycle_exit_bps=self.locked_exit if self.entries else self.exit,
                       slippage_bps=safe_slip if self.executable else self.execution_slippage if self.profit_mode else None)

    def scan(self, now):
        eng, cfg = self.engine, self.cfg
        if eng.halted or self.pending:
            return None
        if eng._position_uncertain:
            self.reason = '等待两边实时仓位复核'
            return None
        limited = [v for v in eng.venues.values() if eng._venue_limited(v)]
        if limited:
            self.reason = f'{" / ".join(v.name for v in limited)} 限流冷却中，冷却后自动重评 BPS 点位'
            until = min(max(eng._venue_limited_until.get(v.key, 0), getattr(v, 'rate_limit_until', 0)) for v in limited)
            eng._schedule_poke(max(.01, until - time.time()))
            return None
        if any(not v.book.is_fresh(cfg.staleness_sec) or not v.ready_to_trade()
               or eng._vlock(v.key).locked() or eng._venue_limited(v)
               for v in eng.venues.values()) or eng._venue_down:
            self.reason = '等待可交易的双腿行情'
            return None
        if not self.net_balanced():
            eng.halted = self.pending = True
            self.reason = self.confirmed_residual_reason()
            self.save()
            log.critical('[BPS PAUSED] %s', self.reason)
            return None
        if any(not eng._venue_rate_ok(v) or v.book.last_update_ts <= v.last_traded_ts
               for v in eng.venues.values()):
            return None
        premium = self.signal()
        if premium is None:
            return None
        exit_reason = self.exit_decision(now)
        if exit_reason:
            if not self.closing:
                self.closing = True
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
            if plan is None and self.only_dust_positions():
                self.reason = '仅剩小额精度尾仓，继续监控；未将真实仓位归零'
                self.note_dust()
            elif plan is None and not self.flat() and dust:
                eng.halted = True
                log.critical('[BPS] %s', self.reason)
            if plan:
                return buy, sell, plan
            if not self.only_dust_positions() or self.profit_mode:
                return None
            # An untradeable tiny remainder must not prevent a fresh entry
            # when its own entry signal and position limits allow it.
        if self.closing and not self.profit_mode:
            # Ordinary level exits are reconsidered for every slice, including
            # restored cycles. Keep the frozen levels and remaining cost basis;
            # an entry signal may add again within the existing position caps.
            self.closing = False
            self.close_reason = None
            try:
                self.save()
            except OSError:
                eng.halted = True
                self.reason = '交易状态无法保存，暂停交易'
                log.exception('[BPS] checkpoint failed')
                return None
        if self.profit_mode and self.entries and not self.costs.known:
            self.reason = '旧仓成本未知，等待原 bps 平仓点位；不继续加仓'
            return None
        if self.entries and self.price_basis != cfg.bps_price_basis:
            self.reason = '旧仓按原口径退出，新一轮使用配置口径；不继续加旧仓'
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
        condition = premium >= entry if self.executable or not self.long else premium <= entry
        key = eng.direction_key(not self.long)
        if not condition:
            eng._armed[key] = None
            self.reason = '持仓中，等待平仓点位' if self.entries else '等待开仓点位'
            return None
        if now - self.last_entry_at < cfg.bps_add_interval_sec:
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
            self.locked_price_basis = self.cfg.bps_price_basis
            if not self.only_dust_positions():
                self.costs = CycleCosts()
            self.close_reason = None
        self.pending = True
        self.save()  # a crash after submission cannot silently re-adopt fills
        log.info('[BPS %s] basis=%s signal=%s entry=%s frozen_exit=%s entries=%d',
                 plan.action, self.price_basis, self.signal(closing=plan.action == 'bps_close', plan=plan),
                 self.locked_entry, self.locked_exit, self.entries)

    def after_execution(self, plan, before_qty, unresolved):
        eng = self.engine
        already_halted = eng.halted
        if (unresolved or eng.halted or eng.active_maker is not None
                or eng._position_uncertain or not self.net_balanced()):
            eng.halted = self.pending = True
            if unresolved or eng.active_maker is not None or eng._position_uncertain:
                self.reason = ('两边实时仓位尚未核实，暂停新订单' if eng._position_uncertain else
                               '订单或对冲结果未确认，暂停新订单')
            elif not already_halted:
                self.reason = self.confirmed_residual_reason()
            self.save()
            log.critical('[BPS PAUSED] %s', self.reason)
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
            self.locked_price_basis = None
            self.adopted_position = self.awaiting_levels = False
            self.closing = False
            self.last_entry_at = time.time()
            for key in eng._armed:
                eng._armed[key] = None
        elif plan.action == 'bps_open' and abs(eng.entropy.position) > before_qty + eng._step / 2:
            self.close_reason = None
            self.entries += 1
            self.last_entry_at = time.time()
            self.last_add_premium = eng.premium_bps()
        self.pending = False
        self.save()
        self.note_dust()
        self.reason = '平仓中' if self.closing else '持仓中' if self.entries else '等待开仓点位'
