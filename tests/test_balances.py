"""Balance telemetry remains independent of market readiness and order logic."""
import asyncio
import json
import time
from unittest.mock import AsyncMock

import aiohttp
import pytest

from entropy_arb.config import ArcusCreds
from entropy_arb.status import snapshot
from test_dashboard import make_engine


def balance_engine():
    engine = make_engine()
    engine.ensure_async_state()
    engine.entropy.conf = engine.cfg.primary
    engine.hedge.conf = engine.cfg.hedge
    engine.entropy.conf.kind = "arcus"
    engine.entropy.conf.arcus_creds = ArcusCreds("sensitive-address", 3, "sensitive-signing-key")
    engine.entropy.conf.arcus_network = "mainnet"
    engine.entropy.set_book(100, 101)
    engine.hedge.set_book(100, 101)
    return engine


async def poll_once(engine, primary_result):
    engine.stop.clear()
    if isinstance(primary_result, Exception):
        engine.entropy.fetch_equity = AsyncMock(side_effect=primary_result)
    else:
        engine.entropy.fetch_equity = AsyncMock(return_value=primary_result)

    async def hedge_balance():
        engine.stop.set()
        return 100, 80

    engine.hedge.fetch_equity = AsyncMock(side_effect=hedge_balance)
    await asyncio.wait_for(engine._balance_loop(), 1)


def test_balance_failure_keeps_last_good_values_and_reports_recovery(caplog):
    async def run():
        engine = balance_engine()
        await poll_once(engine, ConnectionError("sensitive-address sensitive-signing-key"))
        result = snapshot(engine)["venues"][0]
        assert result["fresh"]
        assert result["equity"] is None and result["free"] is None
        assert result["balance_status"]["state"] == "error"
        assert result["balance_status"]["updated_at"] is None
        assert result["account"] == dict(network="mainnet", account_index=3)
        assert "sensitive-" not in json.dumps(result)
        assert "sensitive-" not in caplog.text
        await poll_once(engine, (1234.5, 1000))
        result = snapshot(engine)["venues"][0]
        checked_at = result["balance_status"]["updated_at"]
        assert result["balance_status"]["state"] == "ready"
        assert result["equity"] == 1234.5 and result["free"] == 1000
        assert engine.entropy.start_equity == 1234.5
        await poll_once(engine, aiohttp.ClientResponseError(None, (), status=429))
        result = snapshot(engine)["venues"][0]
        assert result["equity"] == 1234.5
        assert result["balance_status"]["state"] == "error"
        assert result["balance_status"]["error"] == "HTTP 429"
        assert result["balance_status"]["updated_at"] == checked_at
        await poll_once(engine, (0, 0))
        result = snapshot(engine)["venues"][0]
        assert result["equity"] == 0 and result["free"] == 0
        assert result["balance_status"]["state"] == "ready"
        assert engine.entropy.start_equity == 1234.5
    asyncio.run(run())


@pytest.mark.parametrize("values", [None, (None, 0), (True, 0), (float("nan"), 0), (0, float("inf"))])
def test_balance_invalid_response_is_not_reported_as_zero(values):
    async def run():
        engine = balance_engine()
        await poll_once(engine, values)
        result = snapshot(engine)["venues"][0]
        assert result["equity"] is None
        assert result["balance_status"]["state"] == "error"
    asyncio.run(run())


def test_balance_snapshot_distinguishes_waiting_stale_and_record_only():
    async def run():
        engine = balance_engine()
        assert snapshot(engine)["venues"][0]["balance_status"]["state"] == "waiting"
        await poll_once(engine, (100, 90))
        engine.balance_status[engine.entropy.key]["updated_at"] = time.time() - 100
        assert snapshot(engine)["venues"][0]["balance_status"]["state"] == "stale"
        engine.balance_status.clear()
        engine.record_only = True
        assert snapshot(engine)["venues"][0]["balance_status"]["state"] == "disabled"
    asyncio.run(run())
