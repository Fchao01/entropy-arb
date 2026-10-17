import asyncio

import pytest

from entropy_arb.account_analysis import analyse_strategy, reconstruct_cycles
from entropy_arb.account_monitor import AccountMonitor, AccountStore, hedge_matches, wallet_address


def leg(venue, tid, ts, side, before, *, size=1, price=100):
    return dict(venue=venue, market=0, id=str(tid), symbol="BTC", ts=ts, price=price,
                size=size, usd=size * price, account=20 if venue == "lighter-rh" else 30,
                side=side, maker=int(venue == "lighter-rh"), kind="trade", bps=None,
                before_position=before)


def round_trip(offset=0):
    return [leg("lighter-rh", offset + 1, offset + 10, 1, 0, price=99),
            leg("lighter", offset + 2, offset + 10.1, -1, 0),
            leg("lighter-rh", offset + 3, offset + 20, -1, 1),
            leg("lighter", offset + 4, offset + 20.1, 1, -1)]


def matched(fills):
    return hedge_matches([r for r in fills if r["venue"] == "lighter-rh"],
                         [r for r in fills if r["venue"] == "lighter"], limit=None)


def test_wallet_address_validation():
    assert wallet_address("0x" + "AB" * 20) == "0x" + "ab" * 20
    for value in (None, "", "0x" + "0" * 40, "0x1234", "https://localhost"):
        with pytest.raises(ValueError):
            wallet_address(value)


def test_reconstructs_actual_two_leg_entry_exit_and_observed_strategy():
    fills = round_trip()
    matches = matched(fills)
    cycles, incomplete = reconstruct_cycles(matches, fills)
    assert len(cycles) == 1
    assert incomplete == []
    c = cycles[0]
    assert c["direction"] == "RH 多 / 主网空"
    assert c["hold_sec"] == pytest.approx(10.1)
    assert c["entry_bps"] == pytest.approx(-100)
    assert c["exit_bps"] == 0
    assert c["sequence"][0]["phase"] == "开仓"
    assert c["sequence"][-1]["phase"] == "平仓"
    result = analyse_strategy(cycles, matches)
    assert result["metrics"]["rh_maker_pct"] == 100
    assert result["metrics"]["core_maker_pct"] == 0
    assert result["metrics"]["convergence_cycles"] == 1
    assert "不足" in result["findings"][-1]


def test_complete_cycles_need_all_fills_and_no_known_gap():
    fills = round_trip()
    assert reconstruct_cycles(matched(fills), fills, [15])[0] == []
    # An unseen net-zero excursion cannot be silently counted as the observed cycle.
    extras = [leg("lighter-rh", 8, 15, 1, 1), leg("lighter-rh", 9, 16, -1, 2)]
    complete, incomplete = reconstruct_cycles(matched(fills), fills + extras)
    assert complete == []
    assert "未匹配" in incomplete[0]["reason"]
    fills[0]["before_position"] = None
    assert reconstruct_cycles(matched(fills), fills)[0] == []


def test_open_add_reduce_and_close_comparison():
    fills = round_trip()[:2] + [
        leg("lighter-rh", 5, 12, 1, 1, size=.5, price=98),
        leg("lighter", 6, 12.1, -1, -1, size=.5),
        leg("lighter-rh", 7, 17, -1, 1.5, size=.5),
        leg("lighter", 8, 17.1, 1, -1.5, size=.5),
        leg("lighter-rh", 9, 20, -1, 1), leg("lighter", 10, 20.1, 1, -1)]
    complete, incomplete = reconstruct_cycles(matched(fills), fills)
    assert len(complete) == 1
    assert complete[0]["add_groups"] == 1
    assert [p["phase"] for p in complete[0]["sequence"]] == ["开仓", "加仓", "减仓", "平仓"]
    assert incomplete == []
    open_only = fills[:4]
    assert reconstruct_cycles(matched(open_only), open_only)[0] == []
    assert "尚未确认" in reconstruct_cycles(matched(open_only), open_only)[1][0]["reason"]


def test_multiple_accounts_and_directions_do_not_mix_cycles():
    fills = round_trip()
    another = round_trip(100)
    for f in another:
        f["account"] += 10
        f["side"] *= -1
        f["before_position"] *= -1
    cycles, _ = reconstruct_cycles(matched(fills + another), fills + another)
    assert len(cycles) == 2
    assert {c["direction"] for c in cycles} == {"RH 多 / 主网空", "RH 空 / 主网多"}


def test_same_timestamp_unmatched_trade_invalidates_claimed_complete_cycle():
    fills = round_trip()
    extra = leg("lighter-rh", 999, 20, -1, 2)
    assert reconstruct_cycles(matched(fills), fills + [extra])[0] == []


def test_strategy_tag_requires_multiple_consistent_complete_cycles():
    fills = round_trip() + round_trip(100) + round_trip(200)
    matches = matched(fills)
    cycles, _ = reconstruct_cycles(matches, fills)
    assert "符合价差回归" in analyse_strategy(cycles, matches)["findings"][-1]
    assert "不能分析" in analyse_strategy([], [])["findings"][0]


def test_store_address_analysis_and_visible_account_evidence(tmp_path):
    store = AccountStore(tmp_path / "accounts.db")
    address = "0x" + "1" * 40
    try:
        fills = round_trip()
        rows = []
        for f in fills:
            rows.append((f["venue"], f["market"], f["id"], f["symbol"], f["ts"], f["ts"], f["price"],
                         f["size"], f["usd"], f["account"] if f["side"] == -1 else 999,
                         f["account"] if f["side"] == 1 else 999, 1, "trade", None,
                         f["before_position"] if f["side"] == -1 else None,
                         f["before_position"] if f["side"] == 1 else None))
        store.ingest(rows)
        for v, aid in (("lighter-rh", 20), ("lighter", 30)):
            store.link_account(v, aid, address)
        result = store.address_detail(address, 0)
        assert result["matched_groups"] == 2
        assert result["complete_cycles"] == 1
        assert result["stats"]["lighter-rh"]["trades"] == 2
        assert result["stats"]["lighter"]["trades"] == 2
        overview = store.overview(0)
        observed = [r for r in overview["accounts"] if r["id"] == 20][0]
        assert observed["hedge"]["code"] == "matched"
        assert observed["hedge"]["matched_groups"] == 2
        assert store.address_detail(address, 21)["matched_groups"] == 0
    finally:
        store.close()


def test_address_lookup_works_without_continuous_collection_and_records_failure(tmp_path, monkeypatch):
    async def scenario():
        monitor = AccountMonitor(tmp_path / "accounts.db")
        address = "0x" + "2" * 40
        async def request(venue, path, **params):
            assert path == "accountsByL1Address"
            assert params["l1_address"] == address
            if venue == "lighter":
                raise TimeoutError("network unavailable")
            return {"sub_accounts": [{"index": 10}]}
        monkeypatch.setattr(monitor, "request", request)
        try:
            await monitor.lookup_address(address)
            result = monitor.store.address_detail(address, 0)
            assert result["accounts"][0]["id"] == 10
            assert result["complete_cycles"] == 0
            assert next(c for c in result["checks"] if c["venue"] == "lighter")["error"] == "network unavailable"
            assert not monitor.running
        finally:
            await monitor.close()
    asyncio.run(scenario())


def test_directory_not_found_is_empty_only_for_exact_public_error(tmp_path):
    class Response:
        status = 400
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def json(self):
            return {"code": 21100, "message": "account not found"}
        def raise_for_status(self):
            raise RuntimeError("HTTP 400")
    class Session:
        def get(self, *args, **kwargs):
            return Response()
        async def close(self):
            pass
    async def scenario():
        m = AccountMonitor(tmp_path / "accounts.db")
        m.session = Session()
        try:
            result = await m.request("lighter-rh", "accountsByL1Address", l1_address="0x" + "1" * 40)
            assert result["sub_accounts"] == []
            m.http_next["lighter-rh"] = 0
            with pytest.raises(RuntimeError, match="HTTP 400"):
                await m.request("lighter-rh", "account", by="index", value=10)
        finally:
            await m.close()
    asyncio.run(scenario())


def test_address_evidence_does_not_call_missing_data_non_hedging(tmp_path):
    store = AccountStore(tmp_path / "accounts.db")
    address = "0x" + "3" * 40
    try:
        store.watch("lighter-rh", 20, True)
        assert store.detail("lighter-rh", 20, 0)["hedge"]["code"] == "address_pending"
        store.link_account("lighter-rh", 20, address)
        store.directory_check("lighter", address, complete=True)
        assert store.detail("lighter-rh", 20, 0)["hedge"]["code"] == "no_peer"
        store.directory_check("lighter", address, error="temporarily unavailable")
        assert store.detail("lighter-rh", 20, 0)["hedge"]["code"] == "lookup_failed"
        store.link_account("lighter", 30, address)
        store.directory_check("lighter", address, complete=True)
        evidence = store.detail("lighter-rh", 20, 0)["hedge"]
        assert evidence["code"] == "peer_no_trades"
        assert "不是没有对冲" in evidence["reason"]
    finally:
        store.close()
