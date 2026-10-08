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
import math
import sys
import time

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
                    "prem_mean": float(r["premium_mean_bps"]),
                    "sell_max": float(r["sell_edge_max_bps"]),
                    "buy_max": float(r["buy_edge_max_bps"]),
                })
            except (KeyError, ValueError):
                continue
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
    p.add_argument("--hours", type=float, default=0.0,
                   metavar="小时数", help="只分析距当前时间最近的 N 小时（0 表示全部数据）")
    p.add_argument("--min-samples", type=int, default=10,
                   metavar="采样数", help="跳过有效采样数少于此值的分钟（默认：10）")
    p.add_argument("--fees-bps", type=float, default=0.0,
                   metavar="手续费",
                   help="两边吃单手续费之和，单位 bps（默认：0）；一次双腿交易各付"
                        "一边手续费，统计前从盘口价差中扣除。1 bps = 0.01%%")
    args = p.parse_args()
    csv_path = args.csv or (f"logs/{args.symbol}/minutes.csv"
                            if args.symbol else "logs/minutes.csv")

    try:
        rows = load_rows(csv_path, args.hours, args.min_samples)
    except FileNotFoundError:
        print(f"未找到数据文件：{csv_path}。请先运行机器人采集数据"
              "（可使用 --record-only）。",
              file=sys.stderr)
        sys.exit(1)
    if len(rows) < 30:
        print(f"数据不足：{csv_path} 中只有 {len(rows)} 个有效分钟，"
              "建议至少采集数小时后再参考分析结果。", file=sys.stderr)
        if not rows:
            sys.exit(1)

    span_h = (rows[-1]["ts"] - rows[0]["ts"]) / 3600.0 + 1 / 60.0
    prem = sorted(r["prem"] for r in rows)
    mean = sum(prem) / len(prem)
    var = sum((x - mean) ** 2 for x in prem) / len(prem)
    median = pctl(prem, 50)

    print(f"\n=== 数据文件：{csv_path} ===")
    print(f"有效数据：{len(rows)} 个分钟；首尾时间跨度：{span_h:.1f} 小时\n")
    print("主腿相对对冲腿的溢价分布（分钟结束时的中间价，单位：bps）：")
    print(f"  平均值 {mean:+.2f}   标准差 {math.sqrt(var):.2f}   "
          f"中位数 {median:+.2f}")
    print(f"  5%分位数 {pctl(prem, 5):+.2f}   25%分位数 {pctl(prem, 25):+.2f}")
    print(f"  75%分位数 {pctl(prem, 75):+.2f}   95%分位数 {pctl(prem, 95):+.2f}")

    midline = round(median, 1) or 0.0   # normalize -0.0
    # room beyond the midline that was actually executable each minute, net
    # of taker fees (config thresholds are net-of-fee: the engine adds fees
    # on top, and recorded edges are pre-fee)
    fees = args.fees_bps
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
    sug_upper = max(round(pctl(sorted(sell_room), 90) * 2) / 2, 1.0)
    sug_lower = max(round(pctl(sorted(buy_room), 90) * 2) / 2, 1.0)
    print(f"""
阈值参考值（目标为两边各约 10% 的分钟达到条件；四舍五入和最小阈值会影响比例，
已扣除 --fees-bps 传入的 {fees:.1f} bps 手续费）：

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
