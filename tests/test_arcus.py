"""Arcus official wire contracts; synthetic keys only, no live orders.

Fixtures follow the official OpenAPI/AsyncAPI and signing documentation:
https://docs.arcus.xyz/api-reference/authentication
https://docs.arcus.xyz/api-reference/market-data/l2orderbook
https://docs.arcus.xyz/api-reference/exchange/place-order
"""
import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.config import ArcusCreds, VenueConf
from entropy_arb.venue_arcus import ArcusVenue

ADDRESS = "0x" + "AB" * 20
SEED = "01" * 32  # test fixture only, never an actual account/key


def venue():
    v = ArcusVenue(VenueConf(
        key="hedge", kind="arcus", label="ARCUS", symbol="ETH",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=30,
        arcus_creds=ArcusCreds(ADDRESS, 0, SEED)), None, 0.05)
    v.market_id, v.market_name = 2, "ETH-USD"
    v.tick_size, v.step_size = Decimal("0.01"), Decimal("0.0000001")
    v.tick_tiers = [{"upToPrice": "10000", "tick": "0.01"}, {"tick": "0.02"}]
    v.size_decimals, v.min_base, v.min_quote, v.max_base = 7, 0.001, 5.0, 100000.0
    return v


def signed_venue():
    pytest.importorskip("cryptography")
    v = venue()
    v.init_signer()
    v._orders_ready = True
    v._ws = object()
    return v


def order(**overrides):
    return {"orderId": "order-1", "clientId": "client-1", "marketId": 2,
            "status": "FILLED", "state": "FILLED", "originalSize": "0.02",
            "remainingSize": "0", "avgFillPrice": "2500.01", "timeInForce": "IOC",
            **overrides}


def test_market_definition_and_unsupported_quantity_grids():
    async def run():
        market = {"marketId": 2, "marketDisplayName": "ETH-USD",
                  "status": "ONLINE", "type": "PERPETUAL", "quoteAsset": "USD",
                  "tickSize": "0.01", "stepSize": "0.0000001",
                  "tickTiers": [{"tick": "0.01"}], "minOrderSize": "0.001",
                  "maxOrderSize": "100", "minOrderNotional": "5"}
        v = venue()
        v._get = AsyncMock(return_value={"markets": [market]})
        await v.load_market()
        v._get.assert_awaited_once_with("/v1/markets")
        assert v.market_name == "ETH-USD" and v.size_decimals == 7
        assert v.max_base == 100 and v.min_quote == 5
        v.conf.symbol = "ETH-USD"
        await v.load_market()  # exact name also supported
        for step in ("0.002", "10"):
            v._get = AsyncMock(return_value={"markets": [{**market, "stepSize": step}]})
            with pytest.raises(RuntimeError, match="stepSize"):
                await v.load_market()
        v._get = AsyncMock(return_value={"markets": [{**market, "status": "OFFLINE"}]})
        with pytest.raises(RuntimeError, match="unavailable"):
            await v.load_market()
        v.conf.symbol = "ANTH"
        with pytest.raises(RuntimeError, match="not found"):
            await v.load_market()
    asyncio.run(run())


def test_unresolved_order_blocks_trading_until_final_state_and_position_read():
    async def run():
        v = signed_venue()
        v._unresolved_orders["client-1"] = ("order-1", 1000000000000)
        v._lookup_order = AsyncMock(return_value=None)
        v._get = AsyncMock(return_value={"positions": {}})
        assert not v.ready_to_trade()
        with pytest.raises(RuntimeError, match="unresolved"):
            await v.fetch_position()
        v._get.assert_not_awaited()  # an empty position cannot disprove an in-flight order
        v._remember_order(order())
        v._get = AsyncMock(side_effect=ConnectionError("lost account read"))
        with pytest.raises(ConnectionError):
            await v.fetch_position()
        assert not v.ready_to_trade()
        v._get = AsyncMock(return_value={"positions": {"2": {"size": "0.02"}}})
        assert await v.fetch_position() == 0.02
        assert v.ready_to_trade()
    asyncio.run(run())


def test_orders_readiness_requires_documented_snapshot_markers():
    v = venue()
    msg = {"type": "subscribed", "channel": "orders", "id": ADDRESS,
           "accountIndex": 0, "market": "ETH-USD", "contents": {}}
    with pytest.raises(ValueError, match="snapshot"):
        v._handle_message(msg, lambda: None)
    assert not v._orders_ready
    # The official snapshot payload schema is opaque: rely on its documented
    # markers, not an assumed representation of open/closed order arrays.
    msg["contents"] = {"isSnapshot": True, "lastSequenceId": 42}
    v._handle_message(msg, lambda: None)
    assert v._orders_ready


def test_official_typed_signature_and_body_units():
    v = signed_venue()
    ts = 1791423079000000000
    with patch("entropy_arb.venue_arcus.time.time_ns", return_value=ts):
        req = v._place_request(is_buy=True, qty=0.02, limit_px=2500.01,
                               reduce_only=True, client_id="MixedCase-1")
    body = req["payload"]
    g = (ts // 1000 + 40 * 86400 * 1000000) * 1000
    expected = ('{"ad":"' + ADDRESS.lower() + '","ai":0,"c":"MixedCase-1",'
                f'"ct":{ts},"g":{g},"m":2,"op":1,"p":250001,"q":200000,'
                '"r":1,"s":0,"t":2,"v":1}').encode()
    v._signer.public_key().verify(bytes.fromhex(req["signature"]), expected)
    assert body["price"] == "2500.01" and body["quantity"] == "0.0200000"
    assert body["orderType"] == "LIMIT" and body["timeInForce"] == "IOC"
    assert body["timestamp"] == ts == int(req["timestamp"])
    assert int(body["goodTilTime"]) * 1000 == g
    assert body["clientId"] == "MixedCase-1"
    assert len(req["apiKey"]) == 64 and len(req["signature"]) == 128


def test_tick_tiers_crossing_and_exact_signing_denominator():
    v = signed_venue()
    assert v.px_round(10000.011, True) == 10000.02
    assert v.px_round(10000.011, False) == 10000.0
    with patch("entropy_arb.venue_arcus.time.time_ns", return_value=1791423079000000000):
        req = v._place_request(is_buy=False, qty=0.02, limit_px=10000.02,
                               reduce_only=False, client_id="test-2")
    # Signing still divides by base tickSize (0.01), not the tier tick (0.02).
    body = req["payload"]
    from entropy_arb.venue_arcus import canonical
    payload = {"ad": ADDRESS.lower(), "ai": 0, "c": "test-2", "ct": body["timestamp"],
               "g": int(body["goodTilTime"]) * 1000, "m": 2, "op": 1, "p": 1000002,
               "q": 200000, "r": 0, "s": 1, "t": 2, "v": 1}
    v._signer.public_key().verify(bytes.fromhex(req["signature"]), canonical(payload))
    with pytest.raises(ValueError, match="tick tier"):
        v._place_request(is_buy=True, qty=0.02, limit_px=10000.01,
                         reduce_only=False, client_id="bad-tier")


def test_quantities_are_not_silently_rounded_and_limits_are_enforced():
    v = signed_venue()
    for qty in (0.02000005, 100001.0, float("nan")):
        with pytest.raises(ValueError):
            v._place_request(is_buy=True, qty=qty, limit_px=2500.0,
                             reduce_only=False, client_id="bad-size")
    with pytest.raises(ValueError, match="minOrderNotional"):
        v._place_request(is_buy=True, qty=0.001, limit_px=2500.0,
                         reduce_only=False, client_id="dust")
    # Official spec exempts reduce-only orders from minimum notional.
    req = v._place_request(is_buy=False, qty=0.001, limit_px=2500.0,
                           reduce_only=True, client_id="close")
    assert req["payload"]["reduceOnly"] is True


def test_partial_ioc_is_terminal_and_ack_never_is():
    assert ArcusVenue._terminal_result({"status": "ACK"}) is None
    assert ArcusVenue._terminal_result(order(status="OPEN", state="PARTIALLY_FILLED",
                                           remainingSize="0.01", timeInForce="GTT")) is None
    info = ArcusVenue._terminal_result(order(status="CANCELED", state="PARTIALLY_FILLED",
                                             remainingSize="0.01"))
    assert info["filled_base"] == 0.01 and info["avg_px"] == 2500.01
    assert info["unresolved"] is False
    assert ArcusVenue._terminal_result({"status": "FILLED"}) is None
    assert ArcusVenue._terminal_result(order(avgFillPrice=None)) is None


def test_rejection_keeps_prior_fills_and_reason():
    info = ArcusVenue._terminal_result(order(status="REJECTED", state="REJECTED",
                                             remainingSize="0.01",
                                             rejectionReason="OPEN_INTEREST_CAP_EXCEEDED"))
    assert info["filled_base"] == 0.01
    assert info["err"] == "OPEN_INTEREST_CAP_EXCEEDED"
    zero = ArcusVenue._terminal_result(order(status="REJECTED", state="REJECTED",
                                             remainingSize="0.02", avgFillPrice=None,
                                             rejectionReason="IOC_CANCELED"))
    assert zero["filled_base"] == 0 and zero["err"] is None


def test_full_book_snapshots_replace_and_degraded_invalidates():
    v = venue()
    notify = lambda: None
    def book_msg(seq, bids, typ="channel_data"):
        return {"type": typ, "channel": "l2Orderbook", "id": "ETH-USD",
                "contents": {"lastSequenceId": seq, "bids": bids,
                             "asks": [["2501", "1"]]}}
    v._handle_message(book_msg(100, [["2500", "2"]], "subscribed"), notify)
    v._handle_message(book_msg(105, [["2499", "1"]]), notify)
    assert v.book.bids == {2499.0: 1.0}  # no residual levels, sequence jumps OK
    v._handle_message(book_msg(104, [["2500", "2"]]), notify)
    assert v.book.best_bid() == 2499.0
    v._handle_message({"type": "connected"}, notify)
    before = v.book.last_update_ts
    v._handle_message({"type": "connected"}, notify)
    assert v.book.last_update_ts == before
    v._handle_message({"type": "degraded", "channel": "l2Orderbook", "id": "ETH-USD"}, notify)
    assert not v.book.ready
    v._handle_message(book_msg(1, [["2400", "2"]], "subscribed"), notify)
    assert v.book.best_bid() == 2400.0
    v._disconnect()
    assert not v.book.ready and not v.ready_to_trade()


def test_account_and_market_demux_and_out_of_order_events():
    v = venue()
    def msg(account_index, row):
        return {"type": "channel_data", "channel": "orders", "id": ADDRESS,
                "accountIndex": account_index, "market": "ETH-USD", "contents": row}
    v._handle_message(msg(1, order()), lambda: None)
    assert not v._orders
    v._handle_message(msg(0, order(marketId=1)), lambda: None)
    assert not v._orders
    v._handle_message(msg(0, order(sequenceNumber=7)), lambda: None)
    v._handle_message(msg(0, order(status="ACK", sequenceNumber=6)), lambda: None)
    assert v._orders["order-1"]["status"] == "FILLED"
    v._handle_message({"type": "degraded", "channel": "orders", "id": ADDRESS,
                       "accountIndex": 0, "market": "ETH-USD"}, lambda: None)
    assert not v._orders_ready
    with pytest.raises(RuntimeError, match="whitelist"):
        v._handle_message({"type": "error", "message": "address not on access whitelist"}, lambda: None)


def test_ack_waits_for_real_fill_and_unknown_send_is_never_retried():
    async def run():
        v = signed_venue()
        async def rpc(req):
            cid = req["payload"]["clientId"]
            v._remember_order(order(clientId=cid, status="ACK", state="OPEN"))
            return {"id": 1, "status": 202, "result": {"orderId": "order-1", "status": "ACK"}}
        async def lookup(cid, oid, ts):
            v._remember_order(order(clientId=cid))
        v._rpc, v._lookup_order = AsyncMock(side_effect=rpc), AsyncMock(side_effect=lookup)
        info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
        assert info["filled_base"] == 0.02 and not info["unresolved"]
        assert v._lookup_order.await_count == 1
        assert v._rpc.await_count == 1
        v = signed_venue()
        v._rpc = AsyncMock(side_effect=ConnectionError("lost reply"))
        v._lookup_order = AsyncMock(return_value=None)
        info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
        assert info["unresolved"] and info["filled_base"] == 0
        assert v._rpc.await_count == 1  # never retry potentially delivered orders
        assert v._lookup_order.await_count >= 1
    asyncio.run(run())


def test_disconnected_send_and_gateway_rejection_are_distinct():
    async def run():
        v = venue()
        info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
        assert info["err"] and not info["unresolved"]
        v = signed_venue()
        v._rpc = AsyncMock(return_value={"status": 429, "error": {"message": "rate limited",
                                                                               "retryAfterMs": 30000}})
        v._lookup_order = AsyncMock()
        info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
        assert info["err"].startswith("RATE_LIMITED") and not info["unresolved"]
        v._lookup_order.assert_not_awaited()
        assert not v.ready_to_trade()  # respect Arcus retryAfterMs, including reconnects
        for code, typ in ((400, "Tick"), (502, "Transmission")):
            v = signed_venue()
            v._rpc = AsyncMock(return_value={"status": code, "error": {"errorType": typ}})
            v._lookup_order = AsyncMock()
            info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
            assert info["status"] == "rejected" and not info["unresolved"]
            v._lookup_order.assert_not_awaited()
    asyncio.run(run())


def test_positions_use_signed_size_and_subaccount():
    async def run():
        v = venue()
        v.conf.arcus_creds.account_index = 3
        v._get = AsyncMock(return_value={"positions": {"2": {"size": "-0.125", "side": "SHORT"}}})
        assert await v.fetch_position() == -0.125
        assert v._get.call_args.kwargs["accountIndex"] == 3
        v._get = AsyncMock(return_value={"positions": {}})
        assert await v.fetch_position() == 0
        v._get = AsyncMock(side_effect=ConnectionError("not reachable"))
        with pytest.raises(ConnectionError):
            await v.fetch_position()  # failed account query is never flat
    asyncio.run(run())


def test_local_websocket_routes_ack_and_lifecycle_on_same_connection():
    async def run():
        import aiohttp
        from aiohttp import web
        from aiohttp.test_utils import TestServer
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        seen = []
        v = signed_venue()
        v._ws, v._orders_ready = None, False
        stop = asyncio.Event()

        async def handler(http_req):
            ws = web.WebSocketResponse()
            await ws.prepare(http_req)
            async for raw in ws:
                if raw.type != aiohttp.WSMsgType.TEXT:
                    continue
                message = json.loads(raw.data)
                seen.append(message)
                if message["type"] == "subscribe":
                    c = message["channel"]
                    content = ({"bids": [["2500", "1"]], "asks": [["2501", "1"]],
                                "lastSequenceId": 1} if c == "l2Orderbook" else
                               {"openOrders": [], "recentClosedOrders": [],
                                "isSnapshot": True, "lastSequenceId": 1})
                    await ws.send_json({**message, "type": "subscribed", "contents": content})
                elif message["type"] == "post":
                    assert message["id"] > 0
                    req = message["request"]
                    body = req["payload"]
                    # Verify the signature as the peer, against independently
                    # assembled integer fields from the actual wire body.
                    from entropy_arb.venue_arcus import canonical
                    sig_payload = {"ad": body["address"].lower(), "ai": body["accountIndex"],
                                   "c": body["clientId"], "ct": body["timestamp"],
                                   "g": int(body["goodTilTime"]) * 1000, "m": body["marketId"],
                                   "op": 1, "p": int(Decimal(body["price"]) / Decimal("0.01")),
                                   "q": int(Decimal(body["quantity"]) / Decimal("0.0000001")),
                                   "r": 0, "s": 0, "t": 2, "v": 1}
                    Ed25519PublicKey.from_public_bytes(bytes.fromhex(req["apiKey"])).verify(
                        bytes.fromhex(req["signature"]), canonical(sig_payload))
                    # Deliver fill BEFORE ACK to exercise the important race.
                    await ws.send_json({"type": "channel_data", "channel": "orders", "id": ADDRESS.lower(),
                                        "market": "ETH-USD", "accountIndex": 0,
                                        "contents": order(clientId=body["clientId"])})
                    await ws.send_json({"method": "placeOrder", "id": message["id"], "status": 202,
                                        "result": {"orderId": "order-1", "status": "ACK"}})
            return ws

        app = web.Application()
        app.router.add_get("/v1/ws", handler)
        async with TestServer(app) as server:
            v.ws_url = str(server.make_url("/v1/ws")).replace("http://", "ws://")
            task = v.start_tasks(stop, lambda: None, live=True)[0]
            try:
                async def ready():
                    while not v.ready_to_trade():
                        await asyncio.sleep(0.001)
                await asyncio.wait_for(ready(), 2)
                info = await v.send_taker(is_buy=True, qty=0.02, limit_px=2500.01)
                assert info["filled_base"] == 0.02 and info["avg_px"] == 2500.01
                assert not info["unresolved"]
                assert len([m for m in seen if m["type"] == "post"]) == 1
                assert v.book.ready
            finally:
                stop.set()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await v.close()
    pytest.importorskip("cryptography")
    asyncio.run(run())
