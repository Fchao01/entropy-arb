"""Post-only primary lifecycle and hedging of confirmed cumulative fills."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from typing import Optional

from .book import floor_step, walk_depth
from .reduction import reduction_rules

log = logging.getLogger("maker")
RH_HEDGE_RETRY_SEC = 0.1


@dataclass
class MakerOrder:
    client_id: object
    limit_px: float
    sent_at: float
    order_id: object = None
    rejected: bool = False
    err: Optional[str] = None
    last_query: float = 0.0
    cancel_requested_at: Optional[float] = None
    next_query: float = 0.0
    query_failures: int = 0
    query_lock: object = None
    last_state: object = None


def empty_result(status="not-sent", err=None, unresolved=False):
    return {"status": status, "filled_base": 0.0, "avg_px": None,
            "err": err, "unresolved": unresolved}


def rh_hedge_depth(engine, primary_buy):
    cfg, hedge = engine.cfg, engine.hedge
    if not (cfg.primary_maker_only and cfg.hedge_venue == "lighter-rh"):
        return None
    result = dict(capacity_base=0.0, depth_base=0.0, reference_px=None, limit_px=None,
                  take_fraction=cfg.take_fraction)
    if (not hedge.book.is_fresh(cfg.staleness_sec) or not hedge.ready_to_trade()
            or hedge.key in engine._venue_down or engine._venue_limited(hedge)):
        return result
    levels = hedge.book.sorted_bids() if primary_buy else hedge.book.sorted_asks()
    if any(not math.isfinite(price) or price <= 0 or not math.isfinite(size) or size <= 0
           for price, size in levels):
        return result
    reference = levels[0][0]
    slip = cfg.hedge_slippage_bps / 1e4
    limit = hedge.px_round(reference * (1 - slip if primary_buy else 1 + slip), round_up=primary_buy)
    depth = sum(size for price, size in levels
                if (price >= limit - 1e-12 if primary_buy else price <= limit + 1e-12))
    result.update(capacity_base=floor_step(depth * cfg.take_fraction, engine._step),
                  depth_base=depth, reference_px=reference, limit_px=limit)
    return result


async def execute_primary_maker(engine, buy, sell, plan, *, hedge_enabled=True):
    """Required venue locks remain held until cancel/terminal confirmation.

    A cancel ACK is never treated as terminal. Each new cumulative fill is
    hedged once; small slices accumulate until the hedge venue's minimum is
    met. Unknown hedge outcomes stop further hedge sends to prevent duplicates.
    """
    primary, hedge, cfg = engine.entropy, engine.hedge, engine.cfg
    bounded_rh_hedge = cfg.primary_maker_only and cfg.hedge_venue == "lighter-rh"
    reducing = plan.action in ("bps_close", "maker_repair")
    primary_buy = primary is buy
    price = plan.buy_limit if primary_buy else plan.sell_limit
    pinfo, hinfo = empty_result(), empty_result()
    start = time.perf_counter()
    hedge_ms = 0.0
    quote = 0.0
    hedge_prices_known = True
    hedge_failed = False
    hedge_retry_pending = False
    hedge_attempt = 0
    terminal = False
    cancel_at = None
    cancel_sent = False
    cancel_inflight = False
    cancel_reason = None
    last_cancel_attempt = 0.0
    away_since = None
    order = None
    timeout_task = None
    deadline = time.monotonic() + cfg.maker_timeout_sec

    async def request_cancel(reason):
        nonlocal cancel_at, cancel_sent, cancel_inflight, cancel_reason, last_cancel_attempt
        if terminal or order is None:
            return
        if cancel_at is None:
            cancel_at = time.monotonic()
            cancel_reason = reason
            order.cancel_requested_at = cancel_at
            engine.active_maker["state"] = "canceling"
        if cancel_sent or cancel_inflight or (last_cancel_attempt
                and time.monotonic() - last_cancel_attempt < 1):
            return
        cancel_inflight = True
        last_cancel_attempt = time.monotonic()
        engine._record_send(primary)
        # Log when the request starts, rather than after its ACK arrives.
        log.info("[MAKER] cancel requested client_id=%s reason=%s elapsed=%.3fs policy=%s; waiting for final fills",
                 order.client_id, cancel_reason, time.perf_counter() - start, cfg.maker_cancel_policy)
        if cancel_reason.startswith('quote-invalid:'):
            log.info('[MAKER CHECK] %s', getattr(engine, '_maker_validation', {}))
        try:
            await asyncio.wait_for(primary.cancel_maker(order), timeout=cfg.settle_timeout_sec)
            cancel_sent = True
        except Exception as error:
            log.warning("[%s] maker cancel not confirmed: %s", primary.name, type(error).__name__)
        finally:
            cancel_inflight = False

    def cancel_decision():
        nonlocal away_since
        if cancel_reason:
            return cancel_reason
        if hedge_failed:
            return 'hedge-failed'
        if engine.stop.is_set():
            return 'stop-requested'
        if engine.manual_paused:
            return 'manual-pause'
        if engine.halted and hedge_enabled:
            return 'engine-halted'
        # Signal/hedge validity always has priority over queue position.
        if not engine._maker_quote_valid(buy, sell, plan, price):
            detail = getattr(engine, '_maker_validation', {})
            if engine.active_maker is not None:
                engine.active_maker['validation'] = detail
            return 'quote-invalid:' + (detail.get('reason') or 'signal/depth/readiness')
        if cfg.maker_cancel_policy == 'timeout':
            return 'timeout' if time.monotonic() >= deadline else None
        at_best = engine._maker_at_best_price(primary_buy, price)
        engine.active_maker['at_best_price'] = at_best
        if at_best:
            away_since = None
            return None
        if away_since is None:
            away_since = time.monotonic()
        if time.monotonic() - away_since >= cfg.maker_reprice_delay_sec:
            return 'away-from-best-price'
        return None

    async def monitor_quote():
        nonlocal cancel_reason, hedge_failed
        # Independently monitor validity/priority even while an account query
        # or hedge is slow. A best-price order has no total resting deadline.
        try:
            while not terminal:
                reason = cancel_decision()
                if reason:
                    await request_cancel(reason)
                delay = min(0.1, cfg.settle_timeout_sec / 4)
                if cfg.maker_cancel_policy == 'timeout' and cancel_at is None:
                    delay = min(delay, max(0.001, deadline - time.monotonic()))
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            hedge_failed = True
            cancel_reason = 'quote-monitor-error:' + type(error).__name__
            await request_cancel(cancel_reason)

    engine._record_send(primary)
    log.info("[MAKER] %s %s qty=%.8g price=%.10g policy=%s action=%s post_only=true reduce_only=%s %s",
             primary.name, "BUY" if primary_buy else "SELL", plan.qty, price,
             cfg.maker_cancel_policy, plan.action, reducing, f'timeout={cfg.maker_timeout_sec:.2f}s'
             if cfg.maker_cancel_policy == 'timeout' else '最优价持续保留；信号失效立即撤单')
    try:
        order = await primary.place_maker(is_buy=primary_buy, qty=plan.qty, limit_px=price,
                                          **({"reduce_only": True} if reducing else {}))
        client_id = order.client_id.to_raw() if hasattr(order.client_id, 'to_raw') else str(order.client_id)
        engine.active_maker = {"venue": primary.name, "client_id": client_id,
                               "price": price, "qty": plan.qty, "filled": 0.0,
                               "hedged": 0.0, "state": "waiting",
                               "cancel_policy": cfg.maker_cancel_policy,
                               "reduce_only": reducing, "action": plan.action}
        if engine.bps_strategy:
            engine.bps_strategy.save()  # retain the own order id across restarts
        if order.rejected:
            pinfo = empty_result("rejected", order.err)
            terminal = True
        else:
            timeout_task = asyncio.create_task(monitor_quote(), name="maker-quote-monitor")
        while not terminal or hedge_retry_pending:
            try:
                state = (await asyncio.wait_for(primary.maker_state(order), timeout=cfg.settle_timeout_sec)
                         if not terminal else {**pinfo, "terminal": True})
                filled = float(state["filled_base"])
                if (not math.isfinite(filled) or filled < pinfo["filled_base"] - 1e-10
                        or filled > plan.qty + 1e-8):
                    raise ValueError("invalid/non-monotonic cumulative maker fill")
                pinfo = {**state, "unresolved": False}
                terminal = bool(state["terminal"])
                if pinfo.get("err"):
                    hedge_failed = True
                engine.active_maker.update(filled=filled, state=(
                    "canceling" if cancel_at is not None and not terminal else state["status"]))
            except Exception as error:
                detail = type(error).__name__
                if getattr(error, 'status', None) is not None:
                    detail += ' HTTP=' + str(error.status)
                log.warning("[%s] maker state unavailable: %s", primary.name, detail)
                # Uncertain state means cancel, never assume the order vanished.
                deadline = min(deadline, time.monotonic())
                cancel_reason = cancel_reason or ('order-state-unavailable:' + detail)

            if hedge_failed:
                hedge_retry_pending = False
            remainder = floor_step(pinfo["filled_base"] - hinfo["filled_base"], engine._step)
            if hedge_retry_pending and (engine.stop.is_set() or engine.manual_paused):
                hedge_failed = True
                hedge_retry_pending = False
                hinfo["err"] = "RH hedge retry stopped by manual pause/stop; exposure remains"
            if hedge_enabled and remainder > 0 and not hedge_failed:
                levels = hedge.book.sorted_bids() if primary_buy else hedge.book.sorted_asks()
                enough = sum(size for _, size in levels) >= remainder - 1e-12
                if reducing:
                    _, minimum_base, minimum_quote = reduction_rules(hedge, engine._step, 0)
                else:
                    minimum_base = hedge.min_base
                    minimum_quote = max(cfg.min_order_notional, hedge.min_quote)
                if (not hedge.book.is_fresh(cfg.staleness_sec) or not hedge.ready_to_trade()
                        or not enough or not engine._venue_rate_ok(hedge)
                        or (bounded_rh_hedge and (hedge.key in engine._venue_down or engine._venue_limited(hedge)))):
                    hedge_failed = True
                    hedge_retry_pending = False
                    hinfo["err"] = "maker fill cannot be hedged: hedge venue unavailable/depth/budget"
                elif (remainder >= max(minimum_base, engine._step)
                      and remainder * levels[0][0] >= minimum_quote):
                    px, _ = walk_depth(levels, remainder)
                    is_buy = not primary_buy
                    slip = (cfg.hedge_slippage_bps if bounded_rh_hedge or plan.slippage_bps is None
                            else min(cfg.hedge_slippage_bps, plan.slippage_bps)) / 1e4
                    reference = levels[0][0] if bounded_rh_hedge else px
                    bound = hedge.px_round(reference * (1 + slip if is_buy else 1 - slip),
                                           round_up=not is_buy)
                    hedge_attempt += 1
                    engine._record_send(hedge)
                    log.info("[MAKER-HEDGE SEND] %s %s qty=%.8g ref=%.10g limit=%.10g "
                             "slippage_bps=%.6g attempt=%d reduce_only=%s",
                             hedge.name, "BUY" if is_buy else "SELL", remainder, reference, bound,
                             slip * 1e4, hedge_attempt, reducing)
                    try:
                        result, elapsed = await asyncio.wait_for(engine._timed_send(
                            hedge, is_buy=is_buy, qty=remainder, limit_px=bound,
                            risk=True,
                            **({"reduce_only": True} if reducing else {})),
                            timeout=2 * cfg.settle_timeout_sec + 10)
                    except asyncio.TimeoutError:
                        result = empty_result("hedge-timeout", "maker hedge outcome unknown", True)
                        elapsed = (2 * cfg.settle_timeout_sec + 10) * 1000
                    hedge_ms += elapsed
                    log.info("[MAKER-HEDGE RESULT] %s attempt=%d status=%s filled=%s/%s unresolved=%s",
                             hedge.name, hedge_attempt, result.get("status"),
                             result.get("filled_base"), remainder, result.get("unresolved"))
                    hfilled = float(result["filled_base"])
                    if not math.isfinite(hfilled) or not 0 <= hfilled <= remainder + engine._step / 2:
                        result = empty_result("hedge-invalid", "invalid hedge fill quantity", True)
                        hfilled = 0.0
                    hinfo["filled_base"] += hfilled
                    if hfilled:
                        if result.get("avg_px") is None:
                            hedge_prices_known = False
                        quote += hfilled * (result.get("avg_px") or px)
                    hinfo.update(status=result["status"], err=result.get("err"),
                                 unresolved=bool(result.get("unresolved")),
                                 avg_px=(quote / hinfo["filled_base"]
                                         if hinfo["filled_base"] and hedge_prices_known else None))
                    engine.active_maker["hedged"] = hinfo["filled_base"]
                    log.info("[MAKER-HEDGE] cumulative primary=%.8g hedge=%.8g status=%s",
                             pinfo["filled_base"], hinfo["filled_base"], hinfo["status"])
                    hedge_failed = (bool(result.get("err") or result.get("unresolved"))
                                    or hfilled < remainder - engine._step / 2)
                    hedge_retry_pending = (bounded_rh_hedge
                                           and result.get("status") == "canceled-too-much-slippage"
                                           and hfilled == 0 and result.get("unresolved") is False
                                           and not result.get("err"))
                    if hedge_retry_pending:
                        hedge_failed = False
                        if not terminal:
                            cancel_reason = cancel_reason or "hedge-zero-fill-retry"
                        log.warning("[MAKER-HEDGE RETRY] %s confirmed zero fill; next attempt uses latest BBO "
                                    "and configured slippage; no retry-count limit", hedge.name)
                    if hedge_failed and hinfo["err"] is None:
                        hinfo["err"] = "maker hedge incomplete or unresolved"
                elif hedge_retry_pending:
                    hedge_retry_pending = False
                    hedge_failed = True
                    hinfo["err"] = "RH hedge retry below minimum order size/notional"

            if terminal and not hedge_retry_pending:
                break
            reason = cancel_decision()
            if reason is not None:
                await request_cancel(reason)
            if not terminal and cancel_at is not None and time.monotonic() - cancel_at >= cfg.settle_timeout_sec:
                pinfo.update(status="cancel-unconfirmed", unresolved=True,
                             err="maker final state unknown; check exchange open orders before restarting")
                break
            await asyncio.sleep(RH_HEDGE_RETRY_SEC if hedge_retry_pending else min(0.1, cfg.settle_timeout_sec / 4))
    except Exception as error:
        # Placement APIs retain the client id on ambiguous transport errors.
        # An unexpected adapter failure must halt too, not be treated as zero-fill.
        pinfo.update(status="maker-error", unresolved=True, err=type(error).__name__)
        if order is not None:
            await request_cancel('maker-error')
    finally:
        if timeout_task is not None:
            timeout_task.cancel()
            await asyncio.gather(timeout_task, return_exceptions=True)
        if terminal and not hinfo.get("unresolved"):
            engine.active_maker = None
            if (plan.action != 'maker_repair' and pinfo['filled_base'] == 0
                    and not pinfo.get('unresolved')):
                engine._note_maker_zero_fill()
        repairable = (engine.bps_strategy is not None and terminal
                      and not pinfo.get('unresolved') and not hinfo.get('unresolved'))
        if hedge_failed and repairable:
            log.warning('[MAKER] confirmed primary/hedge quantities differ; reduce-only residual repair before new orders')
        if pinfo.get("unresolved") or hinfo.get("unresolved") or (hedge_failed and not repairable):
            engine.halted = True
            if pinfo.get("unresolved") or hinfo.get("unresolved"):
                if engine.active_maker is None:
                    engine.active_maker = {"venue": primary.name, "filled": pinfo["filled_base"],
                                           "hedged": hinfo["filled_base"]}
                engine.active_maker["state"] = "unresolved"
            log.critical("[MAKER] HALTED: unresolved order or incomplete hedge; check open orders and positions")
    if hinfo["status"] == "not-sent" and terminal and pinfo["filled_base"] == 0:
        hinfo["status"] = "no-fill"
    if hedge_enabled and terminal and pinfo["filled_base"] > hinfo["filled_base"] and not hedge_failed:
        log.warning("[MAKER] remaining base %.8g below hedge minimum; reducing residual exposure",
                    pinfo["filled_base"] - hinfo["filled_base"])
    primary_ms = (time.perf_counter() - start) * 1000.0
    if primary_buy:
        return (pinfo, primary_ms), (hinfo, hedge_ms)
    return (hinfo, hedge_ms), (pinfo, primary_ms)
