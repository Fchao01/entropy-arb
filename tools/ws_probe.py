#!/usr/bin/env python3
"""Read-only RH/Lighter WS measurements on the machine running this command.

No credentials, order submission, config changes, or CSV writes. Reuse the
production book handler so incremental updates and nonce gaps are respected.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from entropy_arb.book import OrderBook
from entropy_arb.feeds import LighterBookFeed, ws_connect


def distribution(values):
    if not values:
        return None
    values = sorted(values)
    return {key: round(values[math.ceil(p * len(values)) - 1], 3)
            for key, p in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}


class Samples:
    def __init__(self):
        self.message_delays = []
        self.data_ages = []
        self.apply_times = []

    def observe(self, book):
        if not book.ready:
            return
        latency = book.latency
        stamps = [ts for ts in (latency.source_ts, latency.data_ts) if ts is not None]
        if not stamps or latency.received_at is None:
            return
        self.data_ages.append((latency.received_at - min(stamps)) * 1000)
        if latency.source_ts is not None:
            self.message_delays.append((latency.received_at - latency.source_ts) * 1000)
        if latency.apply_ms is not None:
            self.apply_times.append(latency.apply_ms)

    def report(self):
        count = len(self.data_ages)
        return dict(samples=count, message_delay_ms=distribution(self.message_delays),
                    data_age_at_receive_ms=distribution(self.data_ages),
                    parse_and_apply_ms=distribution(self.apply_times),
                    data_over_100ms_pct=(round(100 * sum(round(x, 3) > 100 for x in self.data_ages)
                                               / count, 3) if count else None),
                    clock_warning=any(x < -1 for x in self.message_delays + self.data_ages))


async def probe(name, url, market_id, *, seconds, warmup, compression, max_queue):
    book, samples = OrderBook(), Samples()
    started = None

    def observe():
        if started is not None and time.perf_counter() - started >= warmup:
            samples.observe(book)

    feed = LighterBookFeed(name, url, market_id, book, observe)
    options = dict(max_size=2**23, open_timeout=10, ping_interval=15, ping_timeout=15,
                   compression=compression)
    if max_queue is not None:
        options["max_queue"] = max_queue

    async def consume():
        async with ws_connect(url, **options) as ws:
            nonlocal started
            started = time.perf_counter()

            async def read():
                async for raw in ws:
                    received_at, received_mono = time.time(), time.perf_counter()
                    msg = json.loads(raw)
                    kind = msg.get("type")
                    book.touch()
                    if kind in ("subscribed/order_book", "update/order_book"):
                        await feed._handle_book(ws, msg, kind == "subscribed/order_book",
                                                received_at=received_at, received_mono=received_mono)
                    elif kind == "connected":
                        await feed._subscribe(ws)
                    elif kind == "ping":
                        await ws.send(json.dumps({"type": "pong"}))
                raise ConnectionError("WS closed before the measurement finished")

            try:
                await asyncio.wait_for(read(), timeout=seconds)
            except asyncio.TimeoutError:
                pass  # End of the requested measurement, not a failed connection.

    error = None
    try:
        await consume()
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    return dict(venue=name, market_id=market_id, **samples.report(), error=error)


async def run(args):
    results = await asyncio.gather(*[
        probe(name, url, market_id, seconds=args.seconds, warmup=args.warmup,
              compression=None if args.compression == "none" else "deflate",
              max_queue=args.max_queue)
        for name, url, market_id in (
            ("RH", "wss://api.rh.lighter.xyz/stream", args.rh_market_id),
            ("LIGHTER", "wss://mainnet.zklighter.elliot.ai/stream", args.lighter_market_id))])
    print(json.dumps(dict(seconds=args.seconds, warmup_seconds=args.warmup,
                          compression=args.compression, max_queue=args.max_queue,
                          percentile_method="nearest_rank_all_accepted_books_after_warmup",
                          results=results), ensure_ascii=False, indent=2))
    return int(any(row["error"] or not row["samples"] for row in results))


def main():
    parser = argparse.ArgumentParser(description="只订阅 RH/Lighter 行情，测量本机 WS 接收分位数；不下单。")
    parser.add_argument("--seconds", type=int, default=60, help="每条连接测量秒数，1–600，默认 60")
    parser.add_argument("--warmup", type=float, default=3, help="连接建立后排除的预热秒数，默认 3")
    parser.add_argument("--rh-market-id", type=int, default=1, help="RH 市场 ID，默认 1（BTC）")
    parser.add_argument("--lighter-market-id", type=int, default=1, help="Lighter 市场 ID，默认 1（BTC）")
    parser.add_argument("--compression", choices=("deflate", "none"), default="deflate",
                        help="仅测速连接的压缩方式；不会修改运行中策略")
    parser.add_argument("--max-queue", type=int, default=None,
                        help="仅测速连接的接收队列上限；默认沿用库默认值")
    args = parser.parse_args()
    if not 1 <= args.seconds <= 600 or not math.isfinite(args.warmup) or not 0 <= args.warmup < args.seconds:
        parser.error("需要 1 <= seconds <= 600 且 0 <= warmup < seconds")
    if min(args.rh_market_id, args.lighter_market_id) < 0 or (args.max_queue is not None and args.max_queue < 1):
        parser.error("市场 ID 必须非负，max-queue 必须为正整数")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
