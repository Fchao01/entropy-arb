"""Atomic, credential-free engine snapshots for the web console."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from pathlib import Path

log = logging.getLogger("web-status")


def snapshot(engine) -> dict:
    now = time.time()
    venues = []
    for venue in engine.venues.values():
        book = venue.book
        venues.append({
            "key": venue.key, "name": venue.name, "symbol": venue.conf.symbol,
            "bid": book.best_bid(), "ask": book.best_ask(),
            "fresh": book.is_fresh(engine.cfg.staleness_sec),
            "age_sec": now - book.last_update_ts if book.ready else None,
            "position": venue.position if not engine.record_only else None,
            "equity": venue.equity, "free": venue.free,
            "volume_usd": venue.volume_usd,
            "limited": engine._venue_limited(venue),
            "down": venue.key in engine._venue_down,
        })
    ready = engine.markets_ready
    live = ready and not engine.record_only
    result = {
        "updated_at": now, "started_at": engine.start_ts,
        "ready": ready, "halted": engine.halted, "paused": engine.manual_paused,
        "mode": "record" if engine.record_only else "live",
        "premium_bps": engine.premium_bps() if ready else None,
        "net_delta": sum(venue.position for venue in engine.venues.values()) if live else None,
        "session_pnl": engine.session_pnl() if live else None,
        "account_delta": engine.account_delta() if live else None,
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
