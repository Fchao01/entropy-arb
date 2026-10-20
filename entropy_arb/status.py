"""Atomic, credential-free engine snapshots for the web console."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from pathlib import Path

from .signals import execution_signals

log = logging.getLogger("web-status")


def snapshot(engine) -> dict:
    now = time.time()
    venues = []
    for venue in engine.venues.values():
        book = venue.book
        bid = book.best_bid()
        ask = book.best_ask()
        position = venue.position if not engine.record_only else None
        mark_price = getattr(venue, "mark_price", None)
        balance = dict(engine.balance_status.get(venue.key, {}))
        balance.setdefault("state", "disabled" if engine.record_only else "waiting")
        updated_at = balance.get("updated_at")
        balance["age_sec"] = max(0.0, now - updated_at) if updated_at is not None else None
        if balance["state"] == "ready" and balance["age_sec"] is not None and balance["age_sec"] > 90:
            balance["state"] = "stale"
        conf = venue.conf
        account = None
        if getattr(conf, "kind", None) == "arcus":
            account = {"network": conf.arcus_network,
                       "account_index": conf.arcus_creds.account_index if conf.arcus_creds else None}
        venues.append({
            "key": venue.key, "name": venue.name, "symbol": venue.conf.symbol,
            "bid": bid, "ask": ask,
            "fresh": book.is_fresh(engine.cfg.staleness_sec),
            "age_sec": now - book.last_update_ts if book.ready else None,
            "position": position,
            "mark_price": mark_price,
            "position_usd": (abs(position) * mark_price
                              if position is not None and mark_price is not None
                              else None),
            "equity": venue.equity, "free": venue.free,
            "balance_status": balance, "account": account,
            "margin_block": getattr(engine, '_margin_blocks', {}).get(venue.key),
            "volume_usd": venue.volume_usd,
            "limited": engine._venue_limited(venue),
            "down": venue.key in engine._venue_down,
            "request_budget": (venue._rest_reads.snapshot()
                               if getattr(venue, '_rest_reads', None) is not None else None),
        })
    ready = engine.markets_ready
    live = ready and not engine.record_only
    result = {
        "updated_at": now, "started_at": engine.start_ts,
        "config_path": getattr(engine, "config_path", None),
        "thresholds": {
            "midline_bps": engine.cfg.midline_bps,
            "upper_bps": engine.cfg.upper_bps,
            "lower_bps": engine.cfg.lower_bps,
        },
        "primary_order_type": engine.cfg.primary_order_type,
        "primary_maker_only": engine.cfg.primary_maker_only,
        "maker_timeout_sec": engine.cfg.maker_timeout_sec,
        "maker_cancel_policy": engine.cfg.maker_cancel_policy,
        "maker_reprice_delay_sec": engine.cfg.maker_reprice_delay_sec,
        "maker_requote_delay_sec": engine.cfg.maker_requote_delay_sec,
        "maker_validation": getattr(engine, '_maker_validation', {}),
        "strategy_mode": engine.cfg.strategy_mode,
        "spread_bps": engine.cfg.spread_bps,
        "bps_strategy": engine.bps_strategy.snapshot() if engine.bps_strategy else None,
        "execution_signals": execution_signals(engine),
        "active_maker": getattr(engine, "active_maker", None),
        "liquidation": engine.liquidation.snapshot() if getattr(engine, 'liquidation', None) else None,
        "ready": ready, "halted": engine.halted, "paused": engine.manual_paused,
        "mode": "record" if engine.record_only else "live",
        "telegram_enabled": engine.telegram.enabled,
        "telegram": {
            "enabled": engine.telegram.enabled,
            "profile": engine.telegram_profile,
            "status": engine.telegram.last_status,
            "last_error": engine.telegram.last_error,
            "sent": engine.telegram.sent_count,
            "failed": engine.telegram.failed_count,
            "last_sent_at": engine.telegram.last_sent_at,
        },
        "premium_bps": engine.premium_bps() if ready else None,
        "net_delta": sum(venue.position for venue in engine.venues.values()) if live else None,
        "session_pnl": engine.session_pnl() if live else None,
        "account_delta": engine.account_delta() if live else None,
        "session_volume_usd": engine._total_volume(),
        "session_profit_usd": engine.total_fill_edge,
        "session_fees_usd": getattr(engine, "total_fees_usd", 0.0),
        "execution_stats": engine.execution_stats.snapshot(engine),
        "trades": engine.trades, "hedges": engine.hedges,
        "expected_edge_usd": engine.total_exp_edge,
        "fill_edge_usd": engine.total_fill_edge,
        "errors": engine.consec_errors,
        "minute_rows": engine.recorder.rows_written if engine.recorder else 0,
        "venues": venues,
    }
    return clean_numbers(result)


def clean_numbers(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: clean_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean_numbers(item) for item in value]
    return value


def write_status(engine, path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    try:
        temporary.write_text(json.dumps(snapshot(engine), allow_nan=False), encoding="utf-8")
        os.replace(temporary, target)
    except (OSError, ValueError):
        log.exception("status snapshot write failed")


async def publish_status(engine, path: str) -> None:
    while True:
        write_status(engine, path)
        if engine.stop.is_set():
            return
        try:
            await asyncio.wait_for(engine.stop.wait(), timeout=2)
        except asyncio.TimeoutError:
            pass
