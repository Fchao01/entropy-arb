"""Synthetic post-only lifecycle tests; never connect to a trading API."""
import asyncio
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from entropy_arb.config import ConfigError, load_config
from entropy_arb.maker import MakerOrder, execute_primary_maker
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue
from entropy_arb.venue_hl import HLVenue
from test_engine import make_engine
from test_arcus import signed_venue


def state(filled, terminal=False, status=None):
    return {"filled_base": filled, "avg_px": 100 if filled else None,
            "terminal": terminal, "status": status or ("filled" if terminal else "open"), "err": None}


def setup_engine(primary_buy=True):
    e = make_engine(midline=0, upper=1, lower=1)
    e.cfg.primary_order_type = "maker"
    e.cfg.maker_timeout_sec, e.cfg.settle_timeout_sec = .005, .02
    e.cfg.staleness_sec = 60
    e.ensure_async_state()
    for v in e.venues.values():
        v.px_round = lambda px, round_up: px
        v.volume_usd = 0
    if primary_buy:
        e.entropy.set_book(100, 101)
        e.hedge.set_book(102, 103)
        buy, sell = e.entropy, e.hedge
    else:
        e.entropy.set_book(99, 100)
        e.hedge.set_book(97, 98)
        buy, sell = e.hedge, e.entropy
    plan, reason = e._plan(buy, sell, 100)
    assert plan is not None, reason
    e._maker_quote_valid = lambda *args: True
    e.entropy.place_maker = AsyncMock(return_value=MakerOrder("test", 100, time.time()))
    e.entropy.cancel_maker = AsyncMock()
    async def hedge(**args):
        return {"status": "filled", "filled_base": args["qty"], "avg_px": 102,
                "err": None, "unresolved": False}
    e.hedge.send_taker = AsyncMock(side_effect=hedge)
    return e, buy, sell, plan


@pytest.mark.parametrize("primary_buy", [True, False])
def test_partial_fills_and_cancel_race_hedge_only_new_quantity(primary_buy):
    async def run():
        e, buy, sell, plan = setup_engine(primary_buy)
        e.entropy.maker_state = AsyncMock(side_effect=[state(.3), state(.3), state(.5, True, "canceled")])
        results = await execute_primary_maker(e, buy, sell, plan)
        pinfo, hinfo = (results[0][0], results[1][0]) if primary_buy else (results[1][0], results[0][0])
        assert pinfo["filled_base"] == .5
        assert hinfo["filled_base"] == pytest.approx(.5)
        calls = e.hedge.send_taker.call_args_list
        assert [c.kwargs["qty"] for c in calls] == pytest.approx([.3, .2])
        assert all(c.kwargs["is_buy"] is (not primary_buy) for c in calls)
        e.entropy.cancel_maker.assert_awaited_once()
        assert e.active_maker is None and not e.halted
    asyncio.run(run())


@pytest.mark.parametrize("trigger", ["pause", "stop", "stale", "edge"])
def test_cancel_triggers_confirm_final_fills(trigger):
    async def run():
        e, buy, sell, plan = setup_engine()
        e.cfg.maker_timeout_sec = 5
        if trigger == "pause":
            e.manual_paused = True
        elif trigger == "stop":
            e.stop.set()
        else:
            e._maker_quote_valid = lambda *args: False
        e.entropy.maker_state = AsyncMock(side_effect=[state(0), state(.2, True, "canceled")])
        results = await execute_primary_maker(e, buy, sell, plan)
        assert results[1][0]["filled_base"] == .2
        e.entropy.cancel_maker.assert_awaited_once()
        assert not e.halted
    asyncio.run(run())


def test_cancel_ack_without_terminal_state_halts_and_blocks_corrective_orders():
    async def run():
        e, buy, sell, plan = setup_engine()
        e.entropy.maker_state = AsyncMock(return_value=state(0))
        results = await execute_primary_maker(e, buy, sell, plan)
        assert results[0][0]["unresolved"] and e.halted and e.active_maker
        e.entropy.position = 1
        e._hedge = AsyncMock()
        await e._maybe_hedge()
        e._hedge.assert_not_awaited()
        e.hedge.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_unknown_hedge_does_not_send_duplicate_after_more_primary_fills():
    async def run():
        e, buy, sell, plan = setup_engine()
        e.entropy.maker_state = AsyncMock(side_effect=[state(.3), state(.4, True, "canceled")])
        e.hedge.send_taker = AsyncMock(return_value={"status": "timeout", "filled_base": 0,
                                                   "avg_px": None, "err": None, "unresolved": True})
        results = await execute_primary_maker(e, buy, sell, plan)
        assert results[0][0]["filled_base"] == .4
        e.hedge.send_taker.assert_awaited_once()
        assert e.halted and e.active_maker is not None
    asyncio.run(run())


def test_subminimum_fill_accumulates_before_one_hedge():
    async def run():
        e, buy, sell, plan = setup_engine()
        e.cfg.maker_timeout_sec = 5
        e.entropy.maker_state = AsyncMock(side_effect=[state(.04), state(.05), state(.2, True)])
        results = await execute_primary_maker(e, buy, sell, plan)
        assert results[1][0]["filled_base"] == .2
        e.hedge.send_taker.assert_awaited_once()
        assert e.hedge.send_taker.call_args.kwargs["qty"] == .2
    asyncio.run(run())


def test_full_execution_accounts_maker_fees_and_partial_expected_edge():
    async def run():
        e, buy, sell, _ = setup_engine()
        e.cfg.primary.maker_fee_bps = 2
        e.hedge.fee_bps = 3
        plan, _ = e._plan(buy, sell, 100)
        e.entropy.maker_state = AsyncMock(return_value=state(.3, True))
        e._notify_trade = lambda *args: None
        await e._execute(buy, sell, plan)
        assert e.entropy.position == pytest.approx(.3)
        assert e.hedge.position == pytest.approx(-.3)
        assert e.total_fees_usd == pytest.approx(.3*100*.0002 + .3*102*.0003)
        assert e.total_exp_edge == pytest.approx(plan.exp_edge_usd*.3/plan.qty)
        assert e.trades == 1
    asyncio.run(run())


def test_maker_prices_use_primary_bid_ask_even_when_taker_edge_fails():
    e, buy, sell, plan = setup_engine()
    e.entropy.set_book(100, 104)
    e.hedge.set_book(102, 103)
    maker, _ = e._plan(buy, sell, 100)
    assert maker.buy_limit == 100
    e.cfg.primary_order_type = "taker"
    taker, _ = e._plan(buy, sell, 100)
    assert taker is None


@pytest.mark.parametrize("mode,timeout", [("bad", 5), ("maker", 0), ("maker", 61)])
def test_maker_config_rejects_invalid_values(tmp_path, mode, timeout):
    p = tmp_path / "config.yaml"
    p.write_text(f"thresholds: {{midline_bps: 0, upper_bps: 1, lower_bps: 1}}\nexecution:\n  primary_order_type: {mode}\n  maker_timeout_sec: {timeout}\n")
    with pytest.raises(ConfigError):
        load_config(str(p), symbol="ETH", hedge_venue="lighter", credential_env={})


def test_maker_fee_defaults_to_configured_taker_fee(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("thresholds: {midline_bps: 0, upper_bps: 1, lower_bps: 1}\nprimary:\n  taker_fee_bps: 2.25\nexecution:\n  primary_order_type: maker\n  maker_timeout_sec: 9\n")
    cfg = load_config(str(p), symbol="ETH", hedge_venue="lighter", credential_env={})
    assert cfg.primary_fee_bps == 2.25 and cfg.maker_timeout_sec == 9


def test_arcus_post_only_and_cancel_signatures():
    from entropy_arb.venue_arcus import canonical
    v = signed_venue()
    req = v._place_request(is_buy=True, qty=.02, limit_px=2500.01,
                           reduce_only=False, client_id="test", post_only=True)
    body = req["payload"]
    assert body["timeInForce"] == "ALO"
    payload = {"ad": v.address, "ai": 0, "c": "test", "ct": body["timestamp"],
               "g": int(body["goodTilTime"])*1000, "m": 2, "op": 1, "p": 250001,
               "q": 200000, "r": 0, "s": 0, "t": 3, "v": 1}
    v._signer.public_key().verify(bytes.fromhex(req["signature"]), canonical(payload))
    cancel = v._cancel_request("test")
    payload = {"ad": v.address, "ai": 0, "c": "test", "ct": int(cancel["timestamp"]),
               "m": 2, "op": 2, "v": 1}
    v._signer.public_key().verify(bytes.fromhex(cancel["signature"]), canonical(payload))
    assert cancel["type"] == "cancelOrder" and cancel["payload"]["clientId"] == "test"


def test_lighter_open_and_partial_feed_and_post_only_wire(monkeypatch):
    async def run():
        feed = AccountOrdersFeed("RH", "", 2, 1, None)
        row = {"client_order_index": 4, "order_index": 99, "status": "open",
               "filled_base_amount": ".2", "filled_quote_amount": "20"}
        feed._handle_orders({"orders": {"2": [row]}})
        assert feed.latest[4]["filled_base_amount"] == ".2" and 4 not in feed._terminal
        feed._handle_orders({"orders": {"2": [{**row, "status": "canceled"}]}})
        assert feed._terminal[4]["filled_base"] == .2
        feed._handle_orders({"orders": {"2": [row]}})
        assert feed.latest[4]["status"] == "canceled"
        constants = SimpleNamespace(ORDER_TYPE_LIMIT=0, ORDER_TIME_IN_FORCE_POST_ONLY=2,
                                    DEFAULT_28_DAY_ORDER_EXPIRY=-1)
        monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=constants))
        e, *_ = setup_engine()
        v = LighterVenue(e.cfg.primary, None, .02)
        v.market_id = 2
        v.signer = SimpleNamespace(create_order=AsyncMock(return_value=(None, SimpleNamespace(code=200), None)),
                                   cancel_order=AsyncMock(return_value=(None, None, None)))
        handle = await v.place_maker(is_buy=True, qty=.2, limit_px=100)
        assert v.signer.create_order.call_args.kwargs["time_in_force"] == 2
        assert v.signer.create_order.call_args.kwargs["order_type"] == 0
        handle.order_id = 99
        await v.cancel_maker(handle)
        assert v.signer.cancel_order.call_args.kwargs["order_index"] == 99
    asyncio.run(run())


def test_hyperliquid_post_only_wire_and_cancel_by_cloid():
    async def run():
        e, *_ = setup_engine()
        v = HLVenue(e.cfg.primary, "", "", None, .02)
        v.coin, v.asset_id = "io:ETH", 110001
        cloid = SimpleNamespace(to_raw=lambda: "0x" + "01"*16)
        v._next_cloid = lambda: cloid
        wires = []
        v._signing = SimpleNamespace(order_request_to_order_wire=lambda req, asset: wires.append(req) or req,
                                    order_wires_to_order_action=lambda wire: {"type": "order", "orders": wire})
        v._signed_action = lambda action: {"action": action}
        v._post_exchange = AsyncMock(return_value=({"status": "ok", "response": {"data": {
            "statuses": [{"resting": {"oid": 9}}]}}}, None, False))
        handle = await v.place_maker(is_buy=False, qty=1, limit_px=100)
        assert wires[0]["order_type"] == {"limit": {"tif": "Alo"}}
        assert handle.order_id == 9
        await v.cancel_maker(handle)
        assert v._post_exchange.call_args.args[0]["action"] == {"type": "cancelByCloid", "cancels": [
            {"asset": 110001, "cloid": cloid.to_raw()}]}
    asyncio.run(run())

@pytest.mark.parametrize("status", ["open", "canceled", "filled", "badAloPxRejected", "marginCanceled", "scheduledCancel"])
def test_hyperliquid_state_terminal_detection_and_unknown_oid(status):
    async def run():
        e, *_ = setup_engine()
        v = HLVenue(e.cfg.primary, "", "", None, .02)
        v.account = SimpleNamespace(query_address="test")
        handle = MakerOrder(SimpleNamespace(to_raw=lambda: "cloid"), 100, time.time())
        response = {"status": "order", "order": {"status": status, "order": {
            "oid": 9, "origSz": "1", "sz": "0" if status == "filled" else "1"}}}
        v._info = AsyncMock(side_effect=[response, [{"oid": 9, "sz": "1", "px": "100"}] if status == "filled" else []])
        result = await v.maker_state(handle)
        assert result["terminal"] is (status != "open")
        assert result["filled_base"] == (1 if status == "filled" else 0)
        v._info = AsyncMock(return_value={"status": "unknownOid"})
        with pytest.raises(RuntimeError, match="unknown"):
            await v.maker_state(handle)
    asyncio.run(run())


def test_arcus_partial_maker_is_not_terminal_and_cancel_is_not_zero_fill():
    from test_arcus import order as arcus_order
    async def run():
        v = signed_venue()
        handle = MakerOrder("client-1", 2500.01, time.time(), order_id="order-1", last_query=time.monotonic())
        v._remember_order(arcus_order(status="OPEN", state="PARTIALLY_FILLED", timeInForce="ALO",
                                     remainingSize="0.01"))
        result = await v.maker_state(handle)
        assert result["filled_base"] == .01 and not result["terminal"]
        v._remember_order(arcus_order(status="CANCELED", state="PARTIALLY_FILLED", timeInForce="ALO",
                                     remainingSize="0.005"))
        result = await v.maker_state(handle)
        assert result["filled_base"] == .015 and result["terminal"]
    asyncio.run(run())


@pytest.mark.parametrize("auth_field", ["authorization", "auth"])
def test_lighter_rest_recovery_uses_selected_auth_signature(monkeypatch, auth_field):
    async def run():
        e, *_ = setup_engine()
        e.cfg.primary.lighter_creds.account_index = 1
        v = LighterVenue(e.cfg.primary, None, .02)
        v.market_id = 2
        v.signer = SimpleNamespace(api_client="test", create_auth_token_with_expiry=lambda: ("token", None))
        seen = []
        async def active_new(authorization, account_index, market_id):
            seen.append((authorization, account_index, market_id))
            return SimpleNamespace(orders=[])
        async def inactive_new(authorization, account_index, market_id, limit):
            seen.append((authorization, account_index, market_id, limit))
            return SimpleNamespace(orders=[{"client_order_index": 4, "order_index": 9, "status": "canceled",
                                           "filled_base_amount": ".2", "filled_quote_amount": "20"}])
        async def active_old(auth, account_index, market_id):
            return await active_new(auth, account_index, market_id)
        async def inactive_old(auth, account_index, market_id, limit):
            return await inactive_new(auth, account_index, market_id, limit)
        api = SimpleNamespace(account_active_orders=active_new if auth_field == "authorization" else active_old,
                              account_inactive_orders=inactive_new if auth_field == "authorization" else inactive_old)
        monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(OrderApi=lambda client: api))
        result = await v.maker_state(MakerOrder(4, 100, time.time()))
        assert result["terminal"] and result["filled_base"] == .2
        assert seen == [("token", 1, 2), ("token", 1, 2, 100)]
    asyncio.run(run())


def test_maker_quote_checks_actual_edge_and_book_age():
    from entropy_arb.engine import Engine
    e, buy, sell, plan = setup_engine()
    check = Engine._maker_quote_valid.__get__(e)
    assert check(buy, sell, plan, plan.buy_limit)
    e.hedge.set_book(99, 100)
    assert not check(buy, sell, plan, plan.buy_limit)
    e.hedge.set_book(102, 103)
    e.hedge.book.alive_ts = 0
    assert not check(buy, sell, plan, plan.buy_limit)


def test_hyperliquid_partial_fills_use_actual_prices_and_do_not_guess_on_lag():
    async def run():
        e, *_ = setup_engine()
        v = HLVenue(e.cfg.primary, "", "", None, .02)
        v.account = SimpleNamespace(query_address="test")
        handle = MakerOrder(SimpleNamespace(to_raw=lambda: "cloid"), 100, time.time())
        response = {"status": "order", "order": {"status": "canceled", "order": {
            "oid": 9, "origSz": "1", "sz": ".6"}}}
        fills = [{"oid": 9, "sz": ".1", "px": "100"}, {"oid": 9, "sz": ".3", "px": "102"},
                 {"oid": 10, "sz": "100", "px": "999"}]
        v._info = AsyncMock(side_effect=[response, fills])
        result = await v.maker_state(handle)
        assert result["terminal"] and result["filled_base"] == pytest.approx(.4)
        assert result["avg_px"] == pytest.approx(101.5)
        v._info = AsyncMock(side_effect=[response, []])
        with pytest.raises(RuntimeError, match="not reconciled"):
            await v.maker_state(handle)
    asyncio.run(run())
