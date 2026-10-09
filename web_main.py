#!/usr/bin/env python3
"""Start the DEX arbitrage web console; trading tasks start only in the UI."""
import argparse
import os
from pathlib import Path

from aiohttp import web
from dotenv import load_dotenv

from entropy_arb.web import ROOT, ConsoleError, TaskManager, create_app


def main():
    parser = argparse.ArgumentParser(description="DEX 套利与监控控制台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--data-dir", default=str(ROOT / "web-data"))
    parser.add_argument("--max-running", type=int, default=12)
    parser.add_argument("--secure-cookie", action="store_true", help="HTTPS 反向代理部署时启用")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env.web")
    password = os.environ.get("WEB_PASSWORD", "")
    if len(password) < 12 or password == "replace-with-a-long-random-password":
        parser.error("请在 .env.web 设置至少 12 位的 WEB_PASSWORD（参见 .env.web.example）")
    if not 1 <= args.max_running <= 50:
        parser.error("--max-running 必须在 1–50 内")
    try:
        manager = TaskManager(ROOT, Path(args.data_dir), args.max_running)
    except (ConsoleError, ValueError) as error:
        parser.error(str(error))
    print("DEX 控制台已启动；任务默认不自动启动。关闭控制台会停止其管理的任务，持仓不会自动平仓。")
    web.run_app(create_app(manager, password, args.secure_cookie), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
