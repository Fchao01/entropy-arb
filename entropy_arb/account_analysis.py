"""Evidence-based reconstruction of observed RH/Core round trips.

All conclusions refer to the supplied observation window, not the trader's
private strategy. Unknown inventory or missing fills never become a full cycle.
"""
from collections import Counter, defaultdict
from statistics import median


def pair_phase(pair):
    actions = [leg.get("position_action", "开平未知") for leg in pair["legs"]]
    if all(a in ("开多", "开空") for a in actions):
        return "开仓"
    if all(a in ("加多", "加空") for a in actions):
        return "加仓"
    if all(a in ("平多", "平空") for a in actions):
        return "平仓"
    if all(a in ("减多", "减空", "平多", "平空") for a in actions):
        return "减仓"
    return "开平未知" if "开平未知" in actions else "混合仓位变化"


def fill_key(leg):
    return leg["venue"], leg["account"], leg["market"], leg["id"], leg["side"]


def reconstruct_cycles(matches, fills, boundaries=()):
    active, completed, incomplete = {}, [], []
    by_account = defaultdict(list)
    for leg in fills:
        by_account[(leg["venue"], leg["account"], leg["symbol"])].append(leg)
    for pair in sorted(matches, key=lambda p: max(leg["ts"] for leg in p["legs"])):
        legs = {leg["venue"]: leg for leg in pair["legs"]}
        rh, core = legs["lighter-rh"], legs["lighter"]
        key = rh["account"], core["account"], pair["symbol"]
        state = active.get(key)
        reason = None
        for leg in legs.values():
            before = leg["before_position"]
            tolerance = max(1e-10, leg["size"] * 1e-7)
            if before is None:
                reason = "缺少公开成交前仓位，无法确认完整开平仓"
                break
            if state and abs(state["positions"][leg["venue"]] - before) > tolerance:
                reason = "相邻仓位不连续，可能有未匹配成交或缺单"
                break
            after = before + leg["side"] * leg["size"]
            if before * after < -tolerance * tolerance:
                reason = "成交跨过零仓位反向，无法拆分本轮开平仓"
                break
        if reason:
            if state:
                incomplete.append(dict(symbol=key[2], reason=reason, started_at=state["started_at"]))
                active.pop(key, None)
            continue
        if state is None:
            if not all(abs(leg["before_position"]) <= max(1e-10, leg["size"] * 1e-7) for leg in legs.values()):
                continue
            state = dict(symbol=key[2], rh_account=key[0], core_account=key[1],
                         direction="RH 多 / 主网空" if rh["side"] == 1 else "RH 空 / 主网多",
                         rh_open_side=rh["side"], started_at=min(leg["ts"] for leg in legs.values()),
                         positions={}, sequence=[])
            active[key] = state
        state["sequence"].append(pair)
        state["positions"] = {v: leg["before_position"] + leg["side"] * leg["size"] for v, leg in legs.items()}
        if not all(abs(state["positions"][v]) <= max(1e-10, leg["size"] * 1e-7) for v, leg in legs.items()):
            continue
        ended = max(leg["ts"] for leg in legs.values())
        matched_ids = {fill_key(leg) for p in state["sequence"] for leg in p["legs"]}
        missing = False
        for venue, aid in (("lighter-rh", key[0]), ("lighter", key[1])):
            observed = by_account[(venue, aid, key[2])]
            covered = [leg for p in state["sequence"] for leg in p["legs"] if leg["venue"] == venue]
            start, end = min(l["ts"] for l in covered), max(l["ts"] for l in covered)
            if any(fill_key(leg) not in matched_ids for leg in observed if start <= leg["ts"] <= end):
                missing = True
        gap = any(state["started_at"] <= ts <= ended for ts in boundaries)
        if missing or gap:
            incomplete.append(dict(symbol=key[2], started_at=state["started_at"],
                                   reason="本轮跨过采集缺口" if gap else "本轮还有未匹配成交，不能作为完整双腿往返"))
        else:
            state.update(ended_at=ended, hold_sec=ended - state["started_at"],
                         entry_bps=state["sequence"][0]["fill_bps"], exit_bps=pair["fill_bps"],
                         add_groups=sum(pair_phase(p) == "加仓" for p in state["sequence"]))
            state.pop("positions")
            completed.append(state)
        active.pop(key, None)
    for state in active.values():
        incomplete.append(dict(symbol=state["symbol"], started_at=state["started_at"],
                               reason="仅观察到开仓或部分减仓，尚未确认两边都归零"))
    return sorted(completed, key=lambda c: c["ended_at"], reverse=True), incomplete


def analyse_strategy(cycles, matches):
    """Describe observable behavior, avoiding profit or exact-threshold claims."""
    findings = []
    if not matches:
        return {"findings": ["当前样本没有匹配到双边反向成交，不能分析该地址的跨平台策略。"], "metrics": {}}
    maker = {}
    for venue in ("lighter-rh", "lighter"):
        legs = [leg for p in matches for leg in p["legs"] if leg["venue"] == venue]
        amount = sum(leg["usd"] for leg in legs)
        maker[venue] = sum(leg["usd"] for leg in legs if leg["maker"]) / amount * 100 if amount else None
    delays = [next(l["ts"] for l in p["legs"] if l["venue"] == "lighter") -
              next(l["ts"] for l in p["legs"] if l["venue"] == "lighter-rh") for p in matches]
    metrics = dict(rh_maker_pct=maker["lighter-rh"], core_maker_pct=maker["lighter"],
                   median_core_delay_ms=median(delays) * 1000, complete_cycles=len(cycles))
    findings.append(f"匹配样本中，RH 挂单成交金额占比 {maker['lighter-rh']:.1f}%，主网 {maker['lighter']:.1f}%。")
    findings.append(f"主网成交时间减 RH 成交时间的中位数为 {metrics['median_core_delay_ms']:+.0f} 毫秒；时间先后不能证明程序的下单顺序。")
    if not cycles:
        findings.append("未确认完整双腿往返；可以查看开仓或平仓片段，不能据此推断完整策略。")
        return dict(findings=findings, metrics=metrics)
    directions = Counter(c["direction"] for c in cycles)
    converged = sum((c["exit_bps"] - c["entry_bps"]) * c["rh_open_side"] > 0 for c in cycles)
    metrics.update(median_entry_bps=median(c["entry_bps"] for c in cycles),
                   median_exit_bps=median(c["exit_bps"] for c in cycles),
                   median_hold_sec=median(c["hold_sec"] for c in cycles),
                   convergence_cycles=converged,
                   cycles_with_adds=sum(c["add_groups"] > 0 for c in cycles))
    findings.append("完整往返方向：" + "；".join(f"{direction} {count} 轮" for direction, count in directions.items()) + "。")
    findings.append(f"开仓成交价溢价中位数 {metrics['median_entry_bps']:+.3f} bps，平仓 {metrics['median_exit_bps']:+.3f} bps，持有时间中位数 {metrics['median_hold_sec']:.1f} 秒。")
    findings.append(f"{len(cycles)} 轮中有 {converged} 轮的首末双边成交价差朝收敛方向变化，{metrics['cycles_with_adds']} 轮观察到配对加仓。")
    if len(cycles) >= 3 and converged / len(cycles) >= .7:
        findings.append("样本表现符合价差回归交易的特征；仍不能确认其真实策略代码、固定阈值或净盈利。")
    else:
        findings.append("完整往返数量或一致性不足，暂不判定其属于价差回归或趋势策略。")
    return dict(findings=findings, metrics=metrics)
