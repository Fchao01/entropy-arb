"""Validate an existing order's remaining exposure without sizing a new order."""
from dataclasses import replace
import math
import time

from .book import walk_depth
from .maker import rh_hedge_depth


def check_quote(engine, buy, sell, plan, price):
    primary, hedge, cfg = engine.entropy, engine.hedge, engine.cfg
    primary_buy = buy is primary
    active = engine.active_maker or {}
    filled = float(active.get('filled', 0))
    hedged = float(active.get('hedged', 0))
    remaining = max(0, plan.qty - filled)
    required = max(0, plan.qty - hedged)
    detail = dict(reason=None, remaining_base=remaining, hedge_required_base=required,
                  signal_bps=None, target_bps=None, hedge_capacity_base=None)

    def fail(reason):
        return {**detail, 'reason': reason}

    if engine._position_uncertain:
        return fail('position-unconfirmed')
    for venue in (primary, hedge):
        if not venue.book.is_fresh(cfg.staleness_sec):
            return fail(f'{venue.name}:book-stale')
        if not venue.ready_to_trade() or venue.key in engine._venue_down:
            return fail(f'{venue.name}:not-ready')
    if engine._venue_limited(hedge):
        return fail(f'{hedge.name}:rate-limit-cooldown')
    if not engine._venue_rate_ok(hedge):
        return fail(f'{hedge.name}:order-budget')
    strategy = engine.bps_strategy
    closing = plan.action == 'bps_close'
    target = None
    if strategy and plan.action in ('bps_open', 'bps_close'):
        target = strategy.locked_exit if closing else strategy.locked_entry
        signal = strategy.signal(closing=closing)
        detail.update(signal_bps=signal, target_bps=target)
        if target is None or strategy.awaiting_levels:
            return fail('bps:levels-unavailable')
        if not closing and cfg.bps_levels == 'rolling' and (
                strategy.window_end is None or time.time() - strategy.window_end > 120):
            return fail('bps:history-stale')
        if closing:
            valid = strategy.profit_mode or strategy.exit_reached(signal)
        else:
            valid = signal is not None and (signal >= target if strategy.executable or not strategy.long
                                           else signal <= target)
        if not valid:
            return fail('bps:signal-below-target')
    # The positions are posted to the local ledger after the whole maker
    # lifecycle settles. Reserve both confirmed increments and the open tail.
    if closing:
        if (plan.qty > abs(primary.position) + 1e-12
                or plan.qty > abs(hedge.position) + 1e-12):
            return fail('position:reduction-exceeds-inventory')
    else:
        for venue, sign in ((buy, 1), (sell, -1)):
            cap = min(venue.cap_usd, cfg.bps_max_position_usd) if strategy else venue.cap_usd
            if abs(venue.position + sign * plan.qty) * venue.book.mid() > cap + 1e-8:
                return fail(f'{venue.name}:position-cap')
    # No take_fraction or fresh-order minimum is applied to an existing tail.
    # Confirmed small fills still accumulate/repair through the maker lifecycle.
    levels = hedge.book.sorted_bids() if primary_buy else hedge.book.sorted_asks()
    if not levels or not math.isfinite(price) or price <= 0:
        return fail('hedge:empty-book')
    if any(not math.isfinite(px) or px <= 0 or not math.isfinite(qty) or qty <= 0 for px, qty in levels):
        return fail('hedge:invalid-book')
    slip = (min(cfg.hedge_slippage_bps, strategy.execution_slippage)
            if strategy and strategy.profit_mode else cfg.hedge_slippage_bps) / 1e4
    bound = levels[0][0] * (1 - slip if primary_buy else 1 + slip)
    levels = [(px, qty) for px, qty in levels
              if px >= bound - 1e-12] if primary_buy else [
                  (px, qty) for px, qty in levels if px <= bound + 1e-12]
    if strategy and strategy.executable:
        levels = [(px, qty) for px, qty in levels
                  if ((px / price - 1) if primary_buy else (price / px - 1)) * 1e4 >= target - 1e-9]
    capacity = sum(qty for _, qty in levels)
    reserve = rh_hedge_depth(engine, primary_buy)
    if reserve is not None:
        capacity = min(capacity, reserve['depth_base'])
    detail['hedge_capacity_base'] = capacity
    if capacity + 1e-12 < required:
        return fail('hedge:qualified-depth')
    if required <= 1e-12:
        return detail
    marginal, notional = walk_depth(levels, required)
    bp, sp = (price, marginal) if primary_buy else (marginal, price)
    if strategy:
        if not closing:
            current = replace(plan, qty=required, buy_limit=bp, sell_limit=sp,
                              buy_notional=price * required if primary_buy else notional,
                              sell_notional=notional if primary_buy else price * required)
            if not strategy.entry_check(current, buy, sell, strategy.locked_entry, strategy.locked_exit):
                return fail('bps:entry-cost-check')
    else:
        edge = (sp * (1 - plan.sell_fee) / (bp * (1 + plan.buy_fee)) - 1) * 1e4
        if edge < engine._eff_threshold(buy, sell):
            return fail('threshold:quote-below-target')
    return detail
