#!/usr/bin/env python3
"""entropy-arb entry point.

    # collect minute data only — no strategy, no credentials needed
    python3 main.py --record-only --symbol SNDK --hedge lighter-rh

    # LIVE trading: real orders, real money (needs .env credentials)
    python3 main.py --symbol SNDK --hedge lighter-rh

--symbol and --hedge are required on every start: the markets you trade are
an explicit decision, not a config default. Add --cn for a Chinese-language
dashboard. There is no paper mode. Collect data with --record-only, set
your thresholds with tools/analyze.py, then go live with small position
caps.

On a terminal the bot shows a live Rich dashboard (books, signal, positions,
PnL, last executions) and writes log lines to logging.file; use
--no-dashboard for plain console logs (nohup/systemd). Strategy lives in
config.yaml, credentials in .env — see the README (English) /
README.zh-CN.md (中文).
"""
import argparse
import asyncio
import contextlib
import fcntl
import logging
import os
import signal
import sys

from entropy_arb.config import (DEFAULT_PRIMARY_VENUE, HEDGE_VENUES,
                                PRIMARY_VENUES, ConfigError, load_config)
from entropy_arb.engine import Engine


def setup_logging(level: str, log_file: str = None,
                  extra_handler: logging.Handler = None) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level, logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")
    if log_file:
        d = os.path.dirname(log_file)
        if d:
            os.makedirs(d, exist_ok=True)
        h = logging.FileHandler(log_file)
    else:
        h = logging.StreamHandler()
    h.setFormatter(fmt)
    root.addHandler(h)
    if extra_handler is not None:
        root.addHandler(extra_handler)
    logging.getLogger("websockets").setLevel(logging.WARNING)


async def amain(cfg, record_only: bool, use_dashboard: bool, force_tty: bool,
                log_buffer, lang: str, status_file: str = None,
                parent_pid: int = None, control_file: str = None,
                config_path: str = None) -> None:
    eng = Engine(cfg, record_only=record_only, control_file=control_file,
                 config_path=config_path)
    eng.ensure_async_state()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, eng.request_stop)
    status_task = None
    dash_task = None
    parent_task = None
    if parent_pid:
        async def monitor_parent():
            while not eng.stop.is_set():
                if os.getppid() != parent_pid:
                    logging.getLogger("engine").warning("web manager disconnected; stopping task (positions remain)")
                    eng.request_stop()
                    return
                await asyncio.sleep(1)
        parent_task = asyncio.create_task(monitor_parent(), name="web-parent")
    if status_file:
        from entropy_arb.status import publish_status
        status_task = asyncio.create_task(publish_status(eng, status_file), name="web-status")
    if use_dashboard:
        from entropy_arb.dashboard import Dashboard
        dash = Dashboard(eng, log_buffer, cfg.log_file, force_terminal=force_tty,
                         lang=lang)
        dash_task = asyncio.create_task(dash.run(), name="dashboard")
    try:
        await eng.run()
    finally:
        eng.request_stop()
        for task in (dash_task, status_task, parent_task):
            if task is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(task, timeout=5)
                if not task.done():
                    task.cancel()
        if status_file:
            from entropy_arb.status import write_status
            write_status(eng, status_file)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Two-venue LIVE arbitrage with a configurable primary and hedge venue. "
                    "Without --record-only, "
                    "real orders are sent.")
    p.add_argument("--symbol", required=True,
                   help="pair name and default symbol on both venues, e.g. SNDK; "
                        "override names with primary.symbol (legacy entropy.symbol) / hedge.symbol in YAML / "
                        "交易组名称及两边默认品种；不同名称可在 YAML 分别指定")
    p.add_argument("--hedge", required=True, choices=HEDGE_VENUES,
                   metavar="VENUE",
                   help=f"hedge venue, one of: {', '.join(HEDGE_VENUES)} / "
                        f"对冲腿，选择一个交易所")
    p.add_argument("--primary", choices=PRIMARY_VENUES, default=None,
                   help=f"primary venue; overrides YAML primary.venue (default: {DEFAULT_PRIMARY_VENUE}) / 主腿交易所")
    p.add_argument("--config", default="config.yaml",
                   help="strategy config (default: config.yaml)")
    p.add_argument("--env-file", default=".env",
                   help="credentials file (default: .env)")
    p.add_argument("--record-only", action="store_true",
                   help="only collect minute data, run no strategy, send no "
                        "orders (needs no credentials)")
    p.add_argument("--cn", action="store_true",
                   help="display the dashboard in Chinese / 仪表盘使用中文")
    p.add_argument("--status-file", default=None,
                   help="write credential-free JSON snapshots for the web console")
    p.add_argument("--parent-pid", type=int, default=None,
                   help=argparse.SUPPRESS)
    p.add_argument("--instance-lock", default=None,
                   help=argparse.SUPPRESS)
    p.add_argument("--control-file", default=None,
                   help=argparse.SUPPRESS)
    disp = p.add_mutually_exclusive_group()
    disp.add_argument("--dashboard", action="store_true",
                      help="force the Rich dashboard even without a tty")
    disp.add_argument("--no-dashboard", action="store_true",
                      help="plain console logs instead of the dashboard")
    args = p.parse_args()
    if args.parent_pid and os.getppid() != args.parent_pid:
        print("startup error: web manager already exited", file=sys.stderr)
        sys.exit(1)
    instance_lock = None
    if args.instance_lock:
        instance_lock = open(args.instance_lock, "a")
        try:
            fcntl.flock(instance_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            print("startup error: task instance already running", file=sys.stderr)
            sys.exit(1)

    try:
        cfg = load_config(args.config, args.env_file,
                          symbol=args.symbol, hedge_venue=args.hedge,
                          primary_venue=args.primary)
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)

    use_dashboard = (cfg.dashboard or args.dashboard) and not args.no_dashboard
    force_tty = args.dashboard
    if use_dashboard and not (sys.stdout.isatty() or force_tty):
        use_dashboard = False

    log_buffer = None
    if use_dashboard:
        try:
            from entropy_arb.dashboard import BufferLogHandler
        except ImportError:
            print("`rich` is not installed — falling back to plain logs "
                  "(pip install -r requirements.txt)", file=sys.stderr)
            use_dashboard = False
    if use_dashboard:
        log_buffer = BufferLogHandler()
        setup_logging(cfg.log_level, log_file=cfg.log_file,
                      extra_handler=log_buffer)
    else:
        setup_logging(cfg.log_level)

    try:
        asyncio.run(amain(cfg, record_only=args.record_only,
                          use_dashboard=use_dashboard, force_tty=force_tty,
                          log_buffer=log_buffer,
                          lang="zh" if args.cn else "en",
                          status_file=args.status_file,
                          parent_pid=args.parent_pid,
                          control_file=args.control_file,
                          config_path=args.config))
    except RuntimeError as e:
        # startup failures (missing credentials, market not found, venue
        # unreachable) — a clean message, not a traceback
        print(f"startup error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
