"""Web-managed account liquidation guard. Never interprets free balance as health.

The shared latch is independent of task pause/config files and survives restart.
Emergency orders are reduce-only, journaled before sending, never blindly retried.
"""
from __future__ import annotations

from contextlib import contextmanager
import asyncio
import fcntl
import json
import logging
import math
import os
from pathlib import Path
import tempfile
import time

from .book import floor_step
from .reduction import reduction_rules

log = logging.getLogger('liquidation')
DEFAULTS = dict(enabled=False, warn_pct=60., block_pct=70., close_pct=80.,
                poll_sec=5., stale_sec=20., slippage_bps=50.)


def number(value):
    if value is None or isinstance(value, bool):
        raise ValueError('missing numeric risk field')
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('non-finite risk field')
    return value


def margin_snapshot(equity, maintenance, *, exposed=True, source='exchange'):
    equity, maintenance = number(equity), number(maintenance)
    if maintenance < 0:
        raise ValueError('negative maintenance requirement')
    if maintenance == 0 and exposed and equity > 0:
        raise ValueError('zero maintenance for an exposed account is not a verified risk snapshot')
    # Insolvent exposed accounts must trip even if maintenance rounds to zero.
    ratio = 100. if equity <= 0 and (exposed or maintenance > 0) else (min(10000., maintenance / equity * 100) if equity > 0 else 0.)
    return dict(equity=equity, maintenance=maintenance, risk_pct=ratio, source=source)


class LiquidationGuard:
    def __init__(self, path):
        self.path = Path(path)

    def read(self):
        try:
            value = json.loads(self.path.read_text())
            if not isinstance(value, dict) or not isinstance(value.get('settings'), dict):
                raise ValueError('invalid liquidation guard')
            return value
        except FileNotFoundError:
            # Only the manager creates this file; a missing file in a worker is unsafe.
            raise ValueError('liquidation guard unavailable') from None

    @contextmanager
    def edit(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(str(self.path) + '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            value = self.read() if self.path.exists() else dict(settings=dict(DEFAULTS), latched=None, tasks={})
            yield value
            out, temporary = tempfile.mkstemp(dir=self.path.parent)
            try:
                with os.fdopen(out, 'w') as handle:
                    json.dump(value, handle, ensure_ascii=False, allow_nan=False)
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        finally:
            os.close(fd)

    def initialize(self):
        with self.edit():
            pass

    def configure(self, settings):
        if not isinstance(settings, dict) or set(settings) != set(DEFAULTS):
            raise ValueError('风控配置字段不完整')
        if not isinstance(settings['enabled'], bool):
            raise ValueError('enabled 必须是布尔值')
        settings = {k: v if k == 'enabled' else number(v) for k, v in settings.items()}
        if not 0 < settings['warn_pct'] < settings['block_pct'] < settings['close_pct'] < 100:
            raise ValueError('阈值须满足 0 < 预警 < 禁止开仓 < 紧急平仓 < 100%')
        if not 5 <= settings['poll_sec'] <= 60 or not settings['poll_sec'] * 2 <= settings['stale_sec'] <= 180:
            raise ValueError('查询间隔须为 5–60 秒，过期时间至少为间隔两倍且不超过 180 秒')
        if not 0 < settings['slippage_bps'] <= 500:
            raise ValueError('紧急平仓滑点须大于 0 且不超过 500 bps')
        with self.edit() as state:
            if state['latched']:
                raise ValueError('紧急风控已锁定，不能通过修改配置解除')
            state['settings'] = settings
        return self.view()

    def enroll(self, ids):
        with self.edit() as state:
            for task_id in ids:
                state['tasks'].setdefault(task_id, dict(updated_at=0, metrics=[], error='等待账户风险数据'))
            # Retain emergency targets until verified flat/reset; do not let delete bypass the latch.
            if not state['latched']:
                state['tasks'] = {k: v for k, v in state['tasks'].items() if k in ids}

    def report(self, task_id, metrics, error=None):
        with self.edit() as state:
            previous = state['tasks'].get(task_id, {})
            state['tasks'][task_id] = {**previous, 'metrics': metrics, 'error': error,
                                       'updated_at': time.time()}
            if state['settings']['enabled'] and not state['latched']:
                for metric in metrics:
                    if metric['risk_pct'] >= state['settings']['close_pct']:
                        state['latched'] = dict(at=time.time(), task_id=task_id,
                            reason=f"{metric['venue']} 保证金风险率 {metric['risk_pct']:.2f}% 达到紧急平仓阈值")
                        log.critical('[LIQUIDATION TRIGGER] %s', state['latched']['reason'])
                        break

    def progress(self, task_id, **fields):
        with self.edit() as state:
            row = state['tasks'].setdefault(task_id, {})
            row.update(fields, progress_at=time.time())

    def reset(self):
        with self.edit() as state:
            if not state['latched']:
                raise ValueError('没有待解除的风控锁定')
            if not state['tasks'] or any(not row.get('flat') or any(v for k,v in row.items() if k.startswith('pending')) or
                    time.time() - row.get('progress_at', 0) > state['settings']['stale_sec']
                    for row in state['tasks'].values()):
                raise ValueError('全部任务须有最新的零仓位及无挂单确认，才能解除锁定')
            state['latched'] = None
            # Restart normal tasks explicitly after reset; watcher workers never trade.
            state['settings']['enabled'] = False
        return self.view()

    def acknowledge_orders(self, confirmed):
        if confirmed is not True:
            raise ValueError('请先到交易所核对全部未知订单均已终结，并明确确认')
        with self.edit() as state:
            if not state['latched']:
                raise ValueError('仅紧急锁定期间可以请求重新核验')
            for row in state['tasks'].values():
                for key in ('entropy', 'hedge'):
                    pending = row.get('pending_' + key)
                    if pending:
                        row['acknowledged_' + key] = pending
                        row['pending_' + key] = None
                row['flat'] = False
                row['message'] = '已人工确认订单终结；等待重新查询仓位，风控锁定继续保留'
        return self.view()

    def view(self):
        state = self.read()
        settings, now = state['settings'], time.time()
        blocked = False
        for row in state['tasks'].values():
            row['stale'] = now - row.get('updated_at', 0) > settings['stale_sec']
            row['warning'] = any(m['risk_pct'] >= settings['warn_pct'] for m in row.get('metrics', []))
            blocked |= bool(row.get('error') or row['stale'] or
                           any(m['risk_pct'] >= settings['block_pct'] for m in row.get('metrics', [])))
        state['blocked'] = bool(state['latched'] or settings['enabled'] and blocked)
        return state


class LiquidationProtection:
    """One engine's participant in the global guard; CLI engines have no guard."""
    def __init__(self, engine, path, task_id, watch_only=False):
        self.engine, self.guard, self.task_id = engine, LiquidationGuard(path), task_id
        self.watch_only = watch_only
        self.state = None
        self.error = None
        self.next_poll = 0
        self.next_flatten = 0
        self.tripped = False

    def refresh(self):
        try:
            self.state = self.guard.view()
            self.tripped |= bool(self.state.get('latched'))
            self.error = None
        except (OSError, ValueError, KeyError, TypeError):
            self.error = '无法读取全局风控状态；禁止新增交易'
        return self.state

    @property
    def emergency(self):
        return bool(self.state and self.state.get('latched'))

    @property
    def blocked(self):
        return bool(self.watch_only or self.tripped or self.error or self.state is None or self.state.get('blocked'))

    def snapshot(self):
        return dict(watch_only=self.watch_only, blocked=self.blocked, error=self.error,
                    latched=self.state.get('latched') if self.state else None,
                    task=self.state.get('tasks', {}).get(self.task_id) if self.state else None)

    async def poll(self):
        engine = self.engine
        self.refresh()
        if not self.state or not self.state['settings']['enabled'] or time.monotonic() < self.next_poll:
            return
        self.next_poll = time.monotonic() + self.state['settings']['poll_sec']
        metrics, errors = [], []
        async def fetch(venue):
            try:
                metric = await asyncio.wait_for(venue.fetch_liquidation_risk(),
                                                timeout=self.state['settings']['poll_sec'])
                metric['venue'] = venue.name
                return metric, None
            except asyncio.CancelledError:
                raise
            except Exception as error:
                detail = type(error).__name__
                if isinstance(error, KeyError) and error.args and str(error.args[0]).replace('_', '').isalnum():
                    detail = '缺少风险字段 ' + str(error.args[0])[:60]
                elif isinstance(error, ValueError) and str(error).startswith('isolated margin risk'):
                    detail = '存在逐仓仓位，暂不支持自动爆仓风险判断'
                return None, venue.name + ': ' + detail
        queries = [asyncio.create_task(fetch(v)) for v in engine.venues.values()]
        try:
            for done in asyncio.as_completed(queries):
                metric, error = await done
                if metric is not None:
                    metrics.append(metric)
                if error:
                    errors.append(error)
                self.guard.report(self.task_id, metrics, '; '.join(errors) or
                                  ('等待另一腿数据' if len(metrics) < len(queries) else None))
                self.refresh()
                if self.emergency:
                    break
        finally:
            for query in queries:
                if not query.done():
                    query.cancel()
            await asyncio.gather(*queries, return_exceptions=True)

    async def flatten(self):
        e, guard = self.engine, self.guard
        self.refresh()
        if not self.emergency:
            return
        row = self.state['tasks'].get(self.task_id, {})
        if row.get('flat') and time.time() - row.get('progress_at', 0) < self.state['settings']['stale_sec'] * .75:
            return
        if row.get('pending_legacy'):
            # A crash or ambiguous send cannot be inferred safe from a position snapshot.
            guard.progress(self.task_id, flat=False, message='订单结果未确认；禁止重复提交，请核对订单')
            return
        if e.active_maker is not None or e._exec_tasks:
            guard.progress(self.task_id, flat=False, message='等待原订单撤销及成交结算')
            return
        results = await asyncio.gather(*(self._reduce(v) for v in e.venues.values()), return_exceptions=True)
        flat = all(r is True for r in results)
        details = [str(r) if isinstance(r, str) else type(r).__name__ for r in results if r is not True]
        guard.progress(self.task_id, flat=flat,
                       message='双腿已确认零仓位且无挂单；等待手动解除' if flat else '；'.join(details))
        if flat and not row.get('flat'):
            log.warning('[EMERGENCY FLAT VERIFIED] task=%s; trading remains locked', self.task_id)

    async def _reduce(self, venue):
        e, guard = self.engine, self.guard
        key = 'pending_' + venue.key
        row = guard.read()['tasks'].get(self.task_id, {})
        if row.get(key):
            return venue.name + ' 上次平仓订单未确认，禁止重复提交'
        if e._vlock(venue.key).locked():
            return venue.name + ' 等待成交结算'
        async with e._vlock(venue.key):
            from .venue_lighter import LighterVenue
            from .rest_reads import NORMAL
            read_options = {'priority': NORMAL} if isinstance(venue, LighterVenue) else {}
            if await venue.has_open_orders(**read_options):
                return venue.name + ' 存在未结订单，等待确认或人工撤单'
            position = number(await venue.fetch_position(**read_options))
            expected = getattr(e, '_risk_expected_positions', {}).get(venue.key)
            acknowledged = row.get('acknowledged_' + venue.key)
            if acknowledged:
                bound = number(acknowledged['position'])
                if position * bound < 0 or abs(position) > abs(bound) + 1e-10:
                    return venue.name + ' 人工核验后仓位超出原任务数量，拒绝接管'
                venue.position = position
                e._position_uncertain.discard(venue.key)
                e.execution_stats.unresolved = True
                e.bps_strategy = None
                guard.progress(self.task_id, **{'acknowledged_' + venue.key: None})
            if expected is not None and (position * expected < 0 or abs(position) > abs(expected) + 1e-10):
                return venue.name + ' 仓位超过本任务保存记录，拒绝处理归属不明仓位'
            if abs(position) <= 1e-12:
                venue.position = position
                if expected is not None:
                    e._risk_expected_positions[venue.key] = 0.
                return True
            if venue.key in e._position_uncertain:
                return venue.name + ' 原订单结果不确定，禁止重复下单'
            # Task owns its market exclusively. Unexpected external deltas are not adopted here.
            if not math.isclose(position, venue.position, abs_tol=1e-10, rel_tol=1e-9):
                return venue.name + ' 仓位与任务记录不一致，请核对外部操作'
            if not venue.book.is_fresh(e.cfg.staleness_sec) or not venue.ready_to_trade():
                return venue.name + ' 等待新鲜盘口及交易连接'
            if e._venue_limited(venue) or not e._venue_rate_ok(venue):
                return venue.name + ' 等待限流预算'
            is_buy = position < 0
            reference = venue.book.best_ask() if is_buy else venue.book.best_bid()
            step, minimum, min_quote = reduction_rules(venue, e._step, e.cfg.min_order_notional)
            slip = self.state['settings']['slippage_bps'] / 10000
            limit = venue.px_round(reference * (1 + slip if is_buy else 1 - slip), round_up=not is_buy)
            levels = venue.book.sorted_asks() if is_buy else venue.book.sorted_bids()
            depth = sum(size for price, size in levels if (price <= limit if is_buy else price >= limit))
            qty = floor_step(min(abs(position), depth, e.cfg.max_order_notional / reference,
                                 getattr(venue, 'max_base', math.inf)), step)
            if qty < minimum or qty * reference < min_quote or qty <= 0:
                return venue.name + ' 剩余仓位/盘口不足交易所最小数量，保留尾仓并告警'
            # Per-leg journals prevent concurrent legs overwriting each other's pending flag.
            guard.progress(self.task_id, **{key: dict(position=position, qty=qty, at=time.time()), 'flat': False})
            e._record_send(venue)
            e.hedges += 1
            info, _ = await e._timed_send(venue, is_buy=is_buy, qty=qty, limit_px=limit,
                                        reduce_only=True, risk=True, emergency=True)
            if info.get('unresolved'):
                e._position_uncertain.add(venue.key)
                e.execution_stats.unresolved = True
                guard.progress(self.task_id, **{key: dict(position=position, qty=qty, uncertain=True, at=time.time())})
                return venue.name + ' 平仓订单结果未知，停止重复提交'
            fill = number(info.get('filled_base', 0))
            if not 0 <= fill <= qty + 1e-10:
                e._position_uncertain.add(venue.key)
                e.execution_stats.unresolved = True
                return venue.name + ' 平仓成交量异常'
            px = info.get('avg_px')
            if fill:
                # Preserve unknown cost rather than fabricating realized PnL.
                e.execution_stats.record(venue.key, is_buy, fill, px, venue.position, step)
                if e.bps_strategy:
                    e.bps_strategy.record_fill(venue, is_buy, fill, px, venue.fee_bps / 10000, opening=False)
                venue.position += fill if is_buy else -fill
                if expected is not None:
                    e._risk_expected_positions[venue.key] = venue.position
                if px is not None:
                    fee = fill * px * venue.fee_bps / 10000
                    venue.cash += (-fill * px - fee) if is_buy else (fill * px - fee)
                    venue.volume_usd += fill * px
                    e.total_fees_usd += fee
                venue.last_traded_ts = time.time()
            if e.bps_strategy:
                e.bps_strategy.save()
            guard.progress(self.task_id, **{key: None,
                'owned_positions': dict(e._risk_expected_positions) if e._risk_expected_positions else None})
            log.warning('[EMERGENCY CLOSE] %s %s qty=%.10g filled=%.10g status=%s',
                        venue.name, 'BUY' if is_buy else 'SELL', qty, fill, info.get('status'))
            return venue.name + ' 正在减仓，等待下轮仓位确认'

    async def run(self):
        e = self.engine
        while not e.stop.is_set():
            try:
                self.refresh()
                if self.emergency:
                    if time.monotonic() >= self.next_flatten:
                        self.next_flatten = time.monotonic() + self.state['settings']['poll_sec']
                        await self.flatten()
                else:
                    await self.poll()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = type(error).__name__
                log.warning('[LIQUIDATION] guard cycle failed (%s)', self.error)
            try:
                await asyncio.wait_for(e.stop.wait(), timeout=1.)
            except asyncio.TimeoutError:
                pass
