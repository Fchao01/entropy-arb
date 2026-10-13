"""Regression coverage for role swaps and accidental startup flattening.

Synthetic keys and mocked public reads only; no real orders or network calls.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from entropy_arb.config import load_config
from entropy_arb.engine import Engine
from entropy_arb.web import missing_credentials
from test_arcus import ADDRESS, SEED, signed_venue


def config(tmp_path, values, primary="arcus", hedge="lighter-rh"):
    path = tmp_path / "config.yaml"
    path.write_text("""thresholds: {midline_bps: 0, upper_bps: 1, lower_bps: 1}
primary: {taker_fee_bps: 2.25}
hedge: {taker_fee_bps: 2.25}
execution: {primary_order_type: maker}
""")
    return load_config(str(path), symbol="SPY", primary_venue=primary,
                       hedge_venue=hedge, credential_env=values)


def test_primary_only_arcus_group_follows_venue_on_swap(tmp_path):
    values = {"PRIMARY_ARCUS_ACCOUNT_ADDRESS": ADDRESS,
              "PRIMARY_ARCUS_ACCOUNT_INDEX": "3", "PRIMARY_ARCUS_API_SIGNING_KEY": SEED}
    forward = config(tmp_path, values, "lighter-rh", "arcus")
    reverse = config(tmp_path, values)
    assert forward.hedge.arcus_creds == reverse.primary.arcus_creds
    assert forward.hedge.arcus_creds.account_index == 3
    assert forward.hedge.arcus_creds.env_prefix == "PRIMARY_"


@pytest.mark.parametrize("primary", ["arcus", "lighter-rh"])
def test_partial_arcus_group_cannot_borrow_key_from_other_group(tmp_path, primary):
    cfg = config(tmp_path, {"ARCUS_ACCOUNT_ADDRESS": ADDRESS,
                           "PRIMARY_ARCUS_ACCOUNT_ADDRESS": "0x" + "CD" * 20,
                           "PRIMARY_ARCUS_ACCOUNT_INDEX": "3",
                           "PRIMARY_ARCUS_API_SIGNING_KEY": SEED},
                 primary, "lighter-rh" if primary == "arcus" else "arcus")
    v = cfg.primary if primary == "arcus" else cfg.hedge
    assert v.arcus_creds.signing_key is None
    assert not cfg.creds_complete
    assert "ARCUS_API_SIGNING_KEY" in missing_credentials(cfg)


def test_partial_primary_only_arcus_group_reports_selected_prefix(tmp_path):
    cfg = config(tmp_path, {"PRIMARY_ARCUS_API_SIGNING_KEY": SEED})
    assert cfg.primary.arcus_creds.account_address is None
    assert "PRIMARY_ARCUS_ACCOUNT_ADDRESS" in missing_credentials(cfg)


def key_row(v, **overrides):
    return {"apiKey": v._api_key, "address": ADDRESS, "accountIndex": 0,
            "status": "ACTIVE", "validUntil": 0, **overrides}


@pytest.mark.parametrize("overrides", [
    {"accountIndex": 3}, {"address": "0x" + "CD" * 20},
    {"apiKey": "ff" * 32}, {"status": "DELETED"}, {"validUntil": 1},
])
def test_arcus_key_check_rejects_wrong_scope_and_expired_keys(overrides):
    async def run():
        v = signed_venue()
        v._get = AsyncMock(return_value={"apiKeys": [key_row(v, **overrides)]})
        with pytest.raises(RuntimeError, match="no orders sent"):
            await v.verify_live_credentials()
        v._get.assert_awaited_once_with("/v1/apiKeys", address=ADDRESS.lower(), limit=1000)
    asyncio.run(run())


@pytest.mark.parametrize("all_accounts", [False, True])
def test_arcus_key_check_accepts_selected_or_all_subaccounts(all_accounts):
    async def run():
        v = signed_venue()
        row = key_row(v)
        if all_accounts:
            row.pop("accountIndex")
            row["allSubaccounts"] = True
            v.conf.arcus_creds.account_index = 3
        v._get = AsyncMock(return_value={"apiKeys": [row]})
        await v.verify_live_credentials()
    asyncio.run(run())


@pytest.mark.parametrize("response", [{"apiKeys": []}, {}, {"apiKeys": None}])
def test_arcus_key_check_cannot_trust_missing_or_malformed_authorization(response):
    async def run():
        v = signed_venue()
        v._get = AsyncMock(return_value=response)
        with pytest.raises(RuntimeError, match="no orders sent"):
            await v.verify_live_credentials()
    asyncio.run(run())


@pytest.mark.parametrize("maker", [True, False])
def test_gateway_diagnostic_keeps_reason_and_masks_secrets(maker):
    async def run():
        v = signed_venue()
        v._rpc = AsyncMock(return_value={"status": 403, "error": {
            "errorType": "Forbidden", "message": f"accountIndex mismatch {SEED} {ADDRESS}",
            "signature": "should-not-be-included"}})
        if maker:
            result = await v.place_maker(is_buy=True, qty=.02, limit_px=2500.01)
            message = result.err
            assert result.rejected
        else:
            result = await v.send_taker(is_buy=True, qty=.02, limit_px=2500.01)
            message = result["err"]
            assert result["filled_base"] == 0
        assert "Forbidden" in message and "accountIndex mismatch" in message
        assert SEED not in message and ADDRESS not in message
        assert "should-not-be-included" not in message
    asyncio.run(run())


@pytest.mark.parametrize("failure", ["authorization", "unbalanced"])
def test_startup_failure_never_starts_execution_or_corrective_hedging(tmp_path, failure):
    async def run():
        cfg = config(tmp_path, {"ARCUS_ACCOUNT_ADDRESS": ADDRESS,
                               "ARCUS_API_SIGNING_KEY": SEED,
                               "LIGHTER_ACCOUNT_INDEX": "123", "LIGHTER_API_KEY_INDEX": "3",
                               "LIGHTER_API_PRIVATE_KEY": "synthetic-key"})
        e = Engine(cfg)
        e.ensure_async_state()
        venues = {}
        for vc in (cfg.primary, cfg.hedge):
            venues[vc.key] = SimpleNamespace(
                conf=vc, kind=vc.kind, name=vc.label, position=0,
                size_decimals=4, min_base=.001, min_quote=10, fee_bps=vc.fee_bps,
                init_signer=Mock(), load_market=AsyncMock(),
                verify_live_credentials=AsyncMock(), close=AsyncMock(),
                start_tasks=Mock(), send_taker=AsyncMock())
        arcus, rh = venues["entropy"], venues["hedge"]
        e._make_venue = lambda vc: venues[vc.key]
        e._reconcile_positions = AsyncMock()
        e._hedge = AsyncMock()
        if failure == "authorization":
            arcus.verify_live_credentials.side_effect = RuntimeError("wrong key scope")
        else:
            # Reproduce the incident: selected Arcus reads flat while RH
            # still has its old long. Startup must not sell that long.
            rh.position = 8.035
        with pytest.raises(RuntimeError):
            await e._run_inner()
        arcus.start_tasks.assert_not_called()
        rh.start_tasks.assert_not_called()
        arcus.send_taker.assert_not_awaited()
        rh.send_taker.assert_not_awaited()
        arcus.close.assert_awaited_once()
        rh.close.assert_awaited_once()
        e._hedge.assert_not_awaited()
        if failure == "authorization":
            e._reconcile_positions.assert_not_awaited()
        else:
            e._reconcile_positions.assert_awaited_once_with(hedge=False, strict=True)
    asyncio.run(run())


def test_balanced_old_positions_can_resume(tmp_path):
    e = Engine(config(tmp_path, {}))
    e.venues = {"entropy": SimpleNamespace(position=-8.035),
                "hedge": SimpleNamespace(position=8.035)}
    e._check_starting_positions()
