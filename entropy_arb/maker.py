"""Post-only primary lifecycle and hedging of confirmed cumulative fills."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from typing import Optional

from .book import floor_step, walk_depth

log = logging.getLogger("maker")


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


def empty_result(status="not-sent", err=None, unresolved=False):
    return {"status": status, "filled_base": 0.0, "avg_px": None,
            "err": err, "unresolved": unresolved}


async def execute_primary_maker(engine, buy, sell, plan):
    """Both venue locks remain held until cancel/terminal confirmation.

    A cancel ACK is never treated as terminal. Each new cumulative fill is
    hedged once; small slices accumulate until the hedge venue's minimum is
    met. Unknown hedge outcomes stop further hedge sends to prevent duplicates.
    """
    primary, hedge, cfg = engine.entropy, engine.hedge, engine.cfg
    primary_buy = primary is buy
    price = plan.buy_limit if primary_buy else plan.sell_limit
    pinfo, hinfo = empty_result(), empty_result()
    start = time.perf_counter()
    hedge_ms = 0.0
    quote = 0.0
    hedge_prices_known = True
    hedge_failed = False
    terminal = False
    cancel_at = None
    cancel_sent = False
    cancel_inflight = False
    cancel_reason = None
    last_cancel_attempt = 0.0
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
        log.info("[MAKER] cancel requested client_id=%s reason=%s elapsed=%.3fs timeout=%.2fs; waiting for final fills",
                 order.client_id, cancel_reason, time.perf_counter() - start, cfg.maker_timeout_sec)
        try:
            await asyncio.wait_for(primary.cancel_maker(order), timeout=cfg.settle_timeout_sec)
            cancel_sent = True
        except Exception as error:
            log.warning("[%s] maker cancel not confirmed: %s", primary.name, type(error).__name__)
        finally:
            cancel_inflight = False

    async def cancel_on_timeout():
        # Account queries and hedge confirmations must not postpone cancelling
        # the remaining primary order. Confirmed fills are still hedged below.
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        await request_cancel('timeout')

    engine._record_send(primary)
    log.info("[MAKER] %s %s qty=%.8g price=%.10g timeout=%.2fs",
             primary.name, "BUY" if primary_buy else "SELL", plan.qty, price,
             cfg.maker_timeout_sec)
    try:
        order = await primary.place_maker(is_buy=primary_buy, qty=plan.qty, limit_px=price)
        engine.active_maker = {"venue": primary.name, "client_id": str(order.client_id),
                               "price": price, "qty": plan.qty, "filled": 0.0,
                               "hedged": 0.0, "state": "waiting"}
        if order.rejected:
            pinfo = empty_result("rejected", order.err)
            terminal = True
        else:
            timeout_task = asyncio.create_task(cancel_on_timeout(), name="maker-timeout-cancel")
        while not terminal:
            try:
                state = await asyncio.wait_for(primary.maker_state(order),
                                               timeout=cfg.settle_timeout_sec)
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

            remainder = floor_step(pinfo["filled_base"] - hinfo["filled_base"], engine._step)
            if remainder > 0 and not hedge_failed:
                levels = hedge.book.sorted_bids() if primary_buy else hedge.book.sorted_asks()
                enough = sum(size for _, size in levels) >= remainder - 1e-12
                if (not hedge.book.is_fresh(cfg.staleness_sec) or not hedge.ready_to_trade()
                        or not enough or not engine._venue_rate_ok(hedge)):
                    hedge_failed = True
                    hinfo["err"] = "maker fill cannot be hedged: hedge venue unavailable/depth/budget"
                elif (remainder >= max(hedge.min_base, engine._step)
                      and remainder * levels[0][0] >= max(cfg.min_order_notional, hedge.min_quote)):
                    px, _ = walk_depth(levels, remainder)
                    is_buy = not primary_buy
                    slip = (cfg.hedge_slippage_bps if plan.slippage_bps is None
                            else min(cfg.hedge_slippage_bps, plan.slippage_bps)) / 1e4
                    bound = hedge.px_round(px * (1 + slip if is_buy else 1 - slip),
                                           round_up=not is_buy)
                    engine._record_send(hedge)
                    try:
                        result, elapsed = await asyncio.wait_for(engine._timed_send(
                            hedge, is_buy=is_buy, qty=remainder, limit_px=bound),
                            timeout=2 * cfg.settle_timeout_sec + 10)
                    except asyncio.TimeoutError:
                        result = empty_result("hedge-timeout", "maker hedge outcome unknown", True)
                        elapsed = (2 * cfg.settle_timeout_sec + 10) * 1000
                    hedge_ms += elapsed
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
                    if hedge_failed and hinfo["err"] is None:
                        hinfo["err"] = "maker hedge incomplete or unresolved"

            if terminal:
                break
            reason = cancel_reason
            if reason is None:
                if hedge_failed:
                    reason = 'hedge-failed'
                elif engine.stop.is_set():
                    reason = 'stop-requested'
                elif engine.manual_paused:
                    reason = 'manual-pause'
                elif engine.halted:
                    reason = 'engine-halted'
                elif time.monotonic() >= deadline:
                    reason = 'timeout'
                elif not engine._maker_quote_valid(buy, sell, plan, price):
                    reason = 'quote-invalid:signal/depth/readiness'
            if reason is not None:
                await request_cancel(reason)
            if cancel_at is not None and time.monotonic() - cancel_at >= cfg.settle_timeout_sec:
                pinfo.update(status="cancel-unconfirmed", unresolved=True,
                             err="maker final state unknown; check exchange open orders before restarting")
                break
            await asyncio.sleep(min(0.1, cfg.settle_timeout_sec / 4))
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
        if pinfo.get("unresolved") or hinfo.get("unresolved") or hedge_failed:
            engine.halted = True
            if pinfo.get("unresolved") or hinfo.get("unresolved"):
                if engine.active_maker is None:
                    engine.active_maker = {"venue": primary.name, "filled": pinfo["filled_base"],
                                           "hedged": hinfo["filled_base"]}
                engine.active_maker["state"] = "unresolved"
            log.critical("[MAKER] HALTED: unresolved order or incomplete hedge; check open orders and positions")
    if hinfo["status"] == "not-sent" and terminal and pinfo["filled_base"] == 0:
        hinfo["status"] = "no-fill"
    if terminal and pinfo["filled_base"] > hinfo["filled_base"] and not hedge_failed:
        log.warning("[MAKER] remaining base %.8g below hedge minimum; reducing residual exposure",
                    pinfo["filled_base"] - hinfo["filled_base"])
    primary_ms = (time.perf_counter() - start) * 1000.0
    if primary_buy:
        return (pinfo, primary_ms), (hinfo, hedge_ms)
    return (hinfo, hedge_ms), (pinfo, primary_ms)
