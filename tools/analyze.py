#!/usr/bin/env python3
"""Analyze recorded minute data and suggest config.yaml thresholds.

Reads the CSV written by the built-in recorder (logs/minutes.csv by default,
or logs/{symbol}/minutes.csv when --symbol is provided)
and prints:

  * the premium distribution (midline candidates),
  * how often each candidate upper/lower band would have fired,
  * a ready-to-paste `thresholds:` snippet.

分析机器人自动采集的分钟级盘口数据，输出溢价分布、各档阈值的触发频率，
以及可直接粘贴进 config.yaml 的 thresholds 建议值。

Usage:
    python3 tools/analyze.py --symbol ETH       # logs/ETH/minutes.csv
    python3 tools/analyze.py --csv path.csv --hours 24 --min-samples 10
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path

# Allow direct execution from tools/ while sharing the web calculation.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from entropy_arb.thresholds import (SHANGHAI, ThresholdDataError, daily_window,
                                   load_window_rows, threshold_values)

CANDIDATES = [1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0]


class ChineseHelpFormatter(argparse.HelpFormatter):
    def add_usage(self, usage, actions, groups, prefix=None):
        super().add_usage(usage, actions, groups,
                          prefix if prefix is not None else "用法：")


class ChineseArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        for source, translated in (
            ("unrecognized arguments:", "无法识别的参数："),
            ("expected one argument", "缺少参数值"),
            ("invalid float value:", "浮点数无效："),
            ("invalid int value:", "整数无效："),
            ("argument ", "参数 "),
        ):
            message = message.replace(source, translated)
        self.print_usage(sys.stderr)
        self.exit(2, f"参数错误：{message}\n")


def pctl(sorted_vals: list, q: float) -> float:
    """Linear-interpolated percentile of a pre-sorted list, q in [0, 100]."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_vals[int(k)]
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def load_rows(path: str, hours: float, min_samples: int) -> list:
    cutoff = time.time() - hours * 3600 if hours > 0 else 0.0
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            try:
                if float(r["minute_ts"]) < cutoff:
                    continue
                if int(r["samples"]) < min_samples:
                    continue
                rows.append({
                    "ts": float(r["minute_ts"]),
                    "prem": float(r["premium_close_bps"]),
                    "sell_max": float(r["sell_edge_max_bps"]),
                    "buy_max": float(r["buy_edge_max_bps"]),
                })
                if not all(math.isfinite(value) for value in rows[-1].values()):
                    rows.pop()
            except (KeyError, TypeError, ValueError):
                continue
    rows.sort(key=lambda row: row["ts"])
    return rows


def main() -> None:
    p = ChineseArgumentParser(description="分析分钟级盘口数据，生成阈值参考值",
                              formatter_class=ChineseHelpFormatter,
                              add_help=False)
    p._positionals.title = "位置参数"
    p._optionals.title = "可选参数"
    p.add_argument("-h", "--help", action="help", help="显示帮助信息并退出")
    p.add_argument("--symbol", metavar="币种",
                   help="读取 logs/{symbol}/minutes.csv 中的币种数据")
    p.add_argument("--csv", default=None, metavar="文件路径",
                   help="CSV 文件路径；优先于 --symbol 指定的路径")
    p.add_argument("--config", metavar="YAML路径",
                   help="从 YAML 的 recorder.csv 读取数据，并使用双腿 taker_fee_bps；需指定 --symbol 和 --hedge")
    p.add_argument("--primary", help="主腿交易所（默认读取 YAML primary.venue）")
    p.add_argument("--hedge", help="对冲交易所，与运行任务的选择一致")
    p.add_argument("--daily", action="store_true",
                   help="与网页一致：分析最近完成的上海时间 08:00–08:00 窗口")
    p.add_argument("--date", metavar="YYYY-MM-DD",
                   help="复算此日期 08:00 结束的窗口，需同时指定 --daily")
    p.add_argument("--json", action="store_true", help="以 JSON 输出数据来源、分析范围、费用及建议阈值")
    p.add_argument("--hours", type=float, default=0.0,
                   metavar="小时数", help="只分析距当前时间最近的 N 小时（0 表示全部数据）")
    p.add_argument("--min-samples", type=int, default=10,
                   metavar="采样数", help="跳过有效采样数少于此值的分钟（默认：10）")
    p.add_argument("--fees-bps", type=float, default=None,
                   metavar="手续费",
                   help="两边吃单手续费之和，单位 bps（默认：0）；一次双腿交易各付"
                        "一边手续费，统计前从盘口价差中扣除。1 bps = 0.01%%")
    args = p.parse_args()
    if args.daily and args.hours != 0:
        p.error("--daily 与 --hours 不能同时使用")
    if args.date and not args.daily:
        p.error("--date 需要同时指定 --daily")
    if args.min_samples < 1 or not math.isfinite(args.hours) or args.hours < 0:
        p.error("采样数必须大于 0，小时数必须是非负有限数字")
    config_path = None
    fees = args.fees_bps if args.fees_bps is not None else 0.0
    if args.config:
        if not args.symbol or not args.hedge:
            p.error("--config 需要 --symbol 和 --hedge，与网页任务一致")
        if args.csv or args.fees_bps is not None:
            p.error("--config 已从 YAML 读取 CSV 路径和手续费，请移除 --csv / --fees-bps")
        from entropy_arb.config import ConfigError, load_config
        config_path = Path(args.config).resolve()
        try:
            cfg = load_config(str(config_path), symbol=args.symbol,
                              primary_venue=args.primary, hedge_venue=args.hedge,
                              credential_env={})
        except ConfigError as error:
            p.error(str(error))
        output = Path(cfg.recorder_csv)
        csv_path = (output if output.is_absolute() else ROOT / output).resolve()
        fees = cfg.entropy.fee_bps + cfg.hedge.fee_bps
    else:
        csv_path = Path(args.csv or (f"logs/{args.symbol}/minutes.csv"
                                    if args.symbol else "logs/minutes.csv")).resolve()
    if not math.isfinite(fees) or fees < 0:
        p.error("手续费必须是非负有限数字")
    window = None
    if args.daily:
        now = None
        if args.date:
            try:
                now = datetime.strptime(args.date, "%Y-%m-%d").replace(hour=8, tzinfo=SHANGHAI)
            except ValueError:
                p.error("日期格式必须是 YYYY-MM-DD")
        window = daily_window(now)

    try:
        rows = (load_window_rows(csv_path, window, args.min_samples) if window
                else load_rows(csv_path, args.hours, args.min_samples))
    except FileNotFoundError:
        print(f"未找到数据文件：{csv_path}。请先运行机器人采集数据"
              "（可使用 --record-only）。",
              file=sys.stderr)
        sys.exit(1)
    if len(rows) < 30:
        print(f"数据不足：{csv_path} 中只有 {len(rows)} 个有效分钟，"
              "建议至少采集数小时后再参考分析结果。", file=sys.stderr)
        if not rows or window:
            sys.exit(1)

    span_h = (rows[-1]["ts"] - rows[0]["ts"]) / 3600.0 + 1 / 60.0
    prem = sorted(r["prem"] for r in rows)
    mean = sum(prem) / len(prem)
    var = sum((x - mean) ** 2 for x in prem) / len(prem)
    median = pctl(prem, 50)

    try:
        midline, sug_upper, sug_lower = threshold_values(rows, fees)
    except ThresholdDataError as error:
        p.error(str(error))
    if args.json:
        print(json.dumps({
            "data_path": str(csv_path), "config_path": str(config_path) if config_path else None,
            "window": window.label if window else None,
            "window_start": window.start_ts if window else None,
            "window_end": window.end_ts if window else None,
            "fees_bps": fees, "min_samples": args.min_samples,
            "rows": len(rows), "span_hours": span_h,
            "suggested": {"midline_bps": midline, "upper_bps": sug_upper, "lower_bps": sug_lower},
        }, ensure_ascii=False))
        return

    print(f"\n=== 数据文件：{csv_path} ===")
    if config_path:
        print(f"YAML 配置：{config_path}（路径及手续费来源）")
    if window:
        print(f"分析范围：{window.start:%Y-%m-%d %H:%M} → {window.end:%Y-%m-%d %H:%M}（上海时间，左闭右开）")
    else:
        print("分析范围：最近指定小时" if args.hours else "分析范围：全部历史；网页使用 --daily 的 08:00–08:00 窗口")
    print(f"有效数据：{len(rows)} 个分钟；首尾时间跨度：{span_h:.1f} 小时\n")
    print("主腿相对对冲腿的溢价分布（分钟结束时的中间价，单位：bps）：")
    print(f"  平均值 {mean:+.2f}   标准差 {math.sqrt(var):.2f}   "
          f"中位数 {median:+.2f}")
    print(f"  5%分位数 {pctl(prem, 5):+.2f}   25%分位数 {pctl(prem, 25):+.2f}")
    print(f"  75%分位数 {pctl(prem, 75):+.2f}   95%分位数 {pctl(prem, 95):+.2f}")

    # room beyond the midline that was actually executable each minute, net
    # of taker fees (config thresholds are net-of-fee: the engine adds fees
    # on top, and recorded edges are pre-fee)
    sell_room = sorted((r["sell_max"] - midline - fees for r in rows),
                       reverse=True)
    buy_room = sorted((r["buy_max"] + midline - fees for r in rows),
                      reverse=True)

    print(f"\n中线 midline_bps = {midline:+.1f}（中位数）；"
          f"每次双腿交易的吃单手续费合计：{fees:.1f} bps。")
    print("各档净阈值达到条件的分钟数：")
    print("  净阈值(bps) |        卖出主腿        |        买入主腿")
    print("              | 达标分钟数  折合每日数 | 达标分钟数  折合每日数")
    per_day = 24.0 / span_h if span_h > 0 else 0.0
    for t in CANDIDATES:
        s_hits = sum(1 for x in sell_room if x >= t)
        b_hits = sum(1 for x in buy_room if x >= t)
        print(f"  {t:>11.1f} | {s_hits:>10} {s_hits * per_day:>10.1f} | "
              f"{b_hits:>10} {b_hits * per_day:>10.1f}")

    # default suggestion: the band that fired in ~10% of minutes (p90 of the
    # fee-adjusted executable room), floored at 1 bps — tune from the table
    print(f"""
阈值参考值（目标为两边各约 10% 的分钟达到条件；四舍五入和最小阈值会影响比例，
已扣除本次分析使用的 {fees:.1f} bps 手续费）：

thresholds:
  midline_bps: {midline}
  upper_bps: {sug_upper}
  lower_bps: {sug_lower}

说明：达标分钟数不等于成交笔数，折合每日数按首尾时间跨度换算，可能包含断档。
参考值根据分钟内最大盘口价差计算，未验证实际成交、滑点或完整开平仓收益。
溢价中枢可能漂移，可使用 --hours 分析距当前时间较近的数据，并定期复查参数。
""")


if __name__ == "__main__":
    main()
