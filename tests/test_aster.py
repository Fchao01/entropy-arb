import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import VenueConf  # noqa: E402
from entropy_arb.venue_aster import (  # noqa: E402
    AsterVenue,
)


def make_venue():
    return AsterVenue(VenueConf(
        key="hedge", kind="aster", label="ASTER", symbol="BTCUSD1",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=120,
        aster_user="0x0000000000000000000000000000000000000001",
        aster_signer="0x19e9c9e2e3a9af6f6f6f98dc4e4f5d5a1b6f0c3e",
        aster_private_key="0x" + "11" * 32,
        aster_api_url="https://fapi3.asterdex.com"), None, 5.0)


def test_aster_v3_signing_fields_and_nonce_are_monotonic():
    venue = make_venue()
    first = venue._sign_params({"symbol": "BTCUSD1", "type": "LIMIT"})
    second = venue._sign_params({"symbol": "BTCUSD1", "type": "LIMIT"})
    assert first["user"].lower().endswith("01")
    assert first["signer"] == venue.conf.aster_signer
    assert len(first["signature"]) == 130
    assert int(second["nonce"]) > int(first["nonce"])


def test_aster_uses_v3_paths():
    venue = make_venue()
    assert venue.api_url == "https://fapi3.asterdex.com"


def test_aster_partial_depth_replaces_top_of_book():
    venue = make_venue()
    venue._apply_partial_depth({
        "b": [["100.0", "2.0"]],
        "a": [["101.0", "3.0"]],
    })
    assert venue.book.best_bid() == 100.0
    assert venue.book.best_ask() == 101.0
    assert venue.book.bids[100.0] == 2.0
    assert venue.book.asks[101.0] == 3.0


def test_aster_fails_over_from_fapi3_on_waf_403():
    class Response:
        def __init__(self, status, body):
            self.status, self.body = status, body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def text(self):
            return self.body

    class Session:
        def __init__(self):
            self.calls = []
            self.responses = [Response(403, "forbidden"), Response(200, "{}")]

        def request(self, method, url, **_kwargs):
            self.calls.append((method, url))
            return self.responses.pop(0)

    venue = make_venue()
    venue.session = Session()
    result = asyncio.run(venue._get("/fapi/v3/positionSide/dual"))
    assert result == {}
    assert venue.api_url == "https://fapi.asterdex.com"
    assert [url for _, url in venue.session.calls] == [
        "https://fapi3.asterdex.com/fapi/v3/positionSide/dual",
        "https://fapi.asterdex.com/fapi/v3/positionSide/dual",
    ]


def test_aster_maps_base_asset_to_usd1_symbol():
    venue = make_venue()
    venue.symbol = "SNDK"
    item = venue._select_market({"symbols": [
        {"symbol": "SNDKUSD1", "baseAsset": "SNDK",
         "quoteAsset": "USD1", "marginAsset": "USD1", "status": "TRADING"},
    ]})
    assert item["symbol"] == "SNDKUSD1"
    assert venue.symbol == "SNDKUSD1"


def test_aster_does_not_map_non_usd1_market():
    venue = make_venue()
    venue.symbol = "SNDK"
    assert venue._select_market({"symbols": [
        {"symbol": "SNDKUSDT", "baseAsset": "SNDK",
         "quoteAsset": "USDT", "marginAsset": "USDT", "status": "TRADING"},
    ]}) is None
