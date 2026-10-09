"""Web manager integration tests; child processes never connect to exchanges."""
import asyncio
import fcntl
import json
import os

import aiohttp
import pytest
import yaml
from aiohttp.test_utils import TestClient, TestServer

from entropy_arb.config import ConfigError, load_config
from entropy_arb.engine import Engine
from entropy_arb.status import clean_numbers, snapshot
from entropy_arb.web import ConsoleError, ROOT, TaskManager, create_app, tail_csv

STRATEGY = """thresholds:
  midline_bps: 0
  upper_bps: 4
  lower_bps: 4
primary:
  venue: lighter-rh
hedge:
  taker_fee_bps: 2.25
"""
FAKE_WORKER = """import argparse, signal, time, sys
parser = argparse.ArgumentParser()
parser.add_argument('--symbol')
args, extra = parser.parse_known_args()
print('fake worker ready ' + args.symbol, flush=True)
if args.symbol == 'FAIL':
    sys.exit(7)
signal.signal(signal.SIGTERM, lambda *unused: sys.exit(0))
while True:
    time.sleep(0.05)
"""

PRIVATE_WORKER = """import os, sys, time
secret = os.environ['ARCUS_API_SIGNING_KEY']
sys.stdout.write(secret[:9])
sys.stdout.flush()
time.sleep(0.03)
sys.stdout.write(secret[9:] + '\\n')
print('Authorization: Bearer private-auth-token')
print('record complete')
"""


def manager_at(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    (root / "main.py").write_text(FAKE_WORKER)
    (root / "config.example.yaml").write_text((ROOT / "config.example.yaml").read_text())
    (root / "configs").mkdir()
    (root / "configs/rh-arcus.yaml").write_text((ROOT / "configs/rh-arcus.yaml").read_text())
    return TaskManager(root, tmp_path / "data")


def payload(symbol="ETH"):
    return dict(name=f"{symbol} test", symbol=symbol, primary="lighter-rh",
                hedge="arcus", mode="record", profile="default", config=STRATEGY)


def credentials(manager, profile, lighter_key=1, arcus_key="key-one"):
    directory = manager.root / "credentials"
    directory.mkdir(exist_ok=True)
    (directory / f"{profile}.env").write_text(
        "LIGHTER_ACCOUNT_INDEX=123\n"
        f"LIGHTER_API_KEY_INDEX={lighter_key}\n"
        f"LIGHTER_API_PRIVATE_KEY=fake-lighter-{lighter_key}\n"
        "ARCUS_ACCOUNT_ADDRESS=fake-address\n"
        "ARCUS_ACCOUNT_INDEX=0\n"
        f"ARCUS_API_SIGNING_KEY={arcus_key}\n")


def test_process_lifecycle_and_isolated_paths(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            task = await manager.put(payload())
            task_id = task["id"]
            config = yaml.safe_load((manager.directory(task_id) / "config.yaml").read_text())
            assert config["recorder"]["csv"] == str(manager.directory(task_id) / "minutes.csv")
            result = await manager.start(task_id)
            assert result["state"] == "running" and result["pid"]
            watcher = manager.running[task_id]["watcher"]
            await asyncio.sleep(0.15)
            assert "fake worker ready ETH" in (manager.directory(task_id) / "engine.log").read_text()
            with pytest.raises(ConsoleError, match="重复"):
                await manager.start(task_id)
            with pytest.raises(ConsoleError, match="先停止"):
                await manager.put(payload(), task_id)
            with pytest.raises(ConsoleError, match="不能删除"):
                await manager.delete(task_id)
            await manager.stop(task_id)
            await asyncio.wait_for(watcher, 3)
            assert manager.tasks[task_id]["state"] == "stopped"
            assert manager.tasks[task_id]["pid"] is None
            with pytest.raises(ConsoleError, match="新建任务"):
                await manager.put(payload("BTC"), task_id)
            await manager.put(payload(), task_id)
            await manager.delete(task_id)
            assert (manager.directory(task_id) / "engine.log").exists()
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_multiple_symbols_and_duplicate_combo(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            first = await manager.put(payload("ETH"))
            second = await manager.put(payload("BTC"))
            duplicate = await manager.put(payload("ETH"))
            await manager.start(first["id"])
            await manager.start(second["id"])
            assert len(manager.running) == 2
            with pytest.raises(ConsoleError, match="组合已运行"):
                await manager.start(duplicate["id"])
        finally:
            await manager.close()
        assert not manager.running
    asyncio.run(scenario())


def test_live_confirmation_credentials_and_signer_conflicts(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            first_payload = payload()
            first_payload["mode"] = "live"
            empty = await manager.put(first_payload)
            with pytest.raises(ConsoleError, match="明确确认"):
                await manager.start(empty["id"])
            with pytest.raises(ConsoleError, match="缺少"):
                await manager.start(empty["id"], True)
            credentials(manager, "eth")
            credentials(manager, "btc-same")
            credentials(manager, "btc", lighter_key=2, arcus_key="key-two")
            first_payload["profile"] = "eth"
            first = await manager.put(first_payload)
            second_payload = payload("BTC")
            second_payload.update(mode="live", profile="btc-same")
            conflict = await manager.put(second_payload)
            await manager.start(first["id"], True)
            with pytest.raises(ConsoleError, match="签名密钥"):
                await manager.start(conflict["id"], True)
            second_payload["profile"] = "btc"
            second = await manager.put(second_payload)
            await manager.start(second["id"], True)
            assert len(manager.running) == 2
            overlapping = payload("ETH-PAIR")
            overlapping.update(mode="live", profile="btc",
                               config=STRATEGY.replace("venue: lighter-rh", "venue: lighter-rh\n  symbol: ETH"))
            third = await manager.put(overlapping)
            with pytest.raises(ConsoleError, match="重叠"):
                await manager.start(third["id"], True)
            assert "key-one" not in json.dumps(manager.view(manager.tasks[first["id"]]))
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_failed_exit_restart_and_instance_lock(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        data, root = manager.data, manager.root
        try:
            failed = await manager.put(payload("FAIL"))
            await manager.start(failed["id"])
            await asyncio.wait_for(manager.running[failed["id"]]["watcher"], 3)
            assert manager.tasks[failed["id"]]["state"] == "failed"
            assert manager.tasks[failed["id"]]["exit_code"] == 7
            with pytest.raises(ConsoleError, match="已有一个"):
                TaskManager(root, data)
            with (manager.directory(failed["id"]) / "instance.lock").open("a") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                with pytest.raises(ConsoleError, match="上一进程"):
                    await manager.start(failed["id"])
        finally:
            await manager.close()
        records = json.loads((data / "tasks.json").read_text())
        records[0]["state"] = "running"
        (data / "tasks.json").write_text(json.dumps(records))
        restored = TaskManager(root, data)
        try:
            assert restored.tasks[failed["id"]]["state"] == "interrupted"
            assert not restored.running
        finally:
            await restored.close()
    asyncio.run(scenario())


def test_invalid_config_and_environment_isolation(tmp_path, monkeypatch):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            for bad in ("../../.env", "ETH; echo broken", ""):
                with pytest.raises(ConsoleError, match="币种"):
                    await manager.put(payload(bad))
            for config in (STRATEGY + "unknown: 1\n", STRATEGY.replace("upper_bps: 4", "upper_bps: .nan"),
                           STRATEGY + "sizing:\n  max_order_notional_usd: -1\n", "primary: &loop\n  recurse: *loop\n"):
                invalid = payload()
                invalid["config"] = config
                with pytest.raises((ConsoleError, ConfigError)):
                    await manager.put(invalid)
            credentials(manager, "one", lighter_key=1)
            credentials(manager, "two", lighter_key=2)
            monkeypatch.setenv("LIGHTER_API_KEY_INDEX", "99")
            first = manager.validate({**payload(), "profile": "one"}, "a" * 32)[2]
            second = manager.validate({**payload(), "profile": "two"}, "b" * 32)[2]
            assert first.primary.lighter_creds.api_key_index == 1
            assert second.primary.lighter_creds.api_key_index == 2
            assert os.environ["LIGHTER_API_KEY_INDEX"] == "99"
            cfg = load_config(str(ROOT / "configs/rh-arcus.yaml"), symbol="ETH", hedge_venue="arcus", credential_env={})
            assert cfg.primary.lighter_creds.api_key_index is None
            assert not cfg.creds_complete
            assert snapshot(Engine(cfg, record_only=True))["net_delta"] is None
            assert clean_numbers({"value": float("nan")}) == {"value": None}
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_auth_origin_errors_downloads_and_logout(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        client = TestClient(TestServer(create_app(manager, "test-password-long")), cookie_jar=aiohttp.CookieJar(unsafe=True))
        await client.start_server()
        headers = {"X-Web-Request": "1"}
        try:
            assert (await client.get("/api/tasks")).status == 401
            assert (await client.post("/api/login", json={"password": "test-password-long"})).status == 403
            denied = await client.post("/api/login", json={"password": "bad"}, headers=headers)
            assert denied.status == 401
            login = await client.post("/api/login", json={"password": "test-password-long"}, headers=headers)
            assert login.status == 200
            assert login.cookies["arb_session"]["httponly"]
            assert (await client.get("/api/meta")).status == 200
            cross = await client.post("/api/tasks", json=payload(), headers={**headers, "Origin": "https://attacker.example"})
            assert cross.status == 403
            malformed = await client.post("/api/tasks", data="{", headers={**headers, "Content-Type": "application/json"})
            assert malformed.status == 400
            created = await client.post("/api/tasks", json=payload(), headers=headers)
            task = await created.json()
            assert created.status == 200
            listed = await (await client.get("/api/tasks")).json()
            assert "config" not in listed["tasks"][0]
            directory = manager.directory(task["id"])
            (directory / "minutes.csv").write_text("minute_ts,time_utc,premium_close_bps\n1,2026-01-01,2.5\n")
            (directory / "engine.log").write_text("test log\n")
            detail = await (await client.get(f"/api/tasks/{task['id']}")).json()
            assert detail["minutes"][0]["premium_close_bps"] == "2.5"
            assert (await client.get(f"/api/tasks/{task['id']}/download/minutes")).status == 200
            assert (await client.get(f"/api/tasks/{task['id']}/download/.env")).status == 404
            assert (await client.get("/assets/../.env")).status == 404
            for route in ("/dex-arbitrage", "/monitor", "/assets/app.js", "/assets/style.css"):
                response = await client.get(route)
                assert response.status == 200
                assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
            await client.post("/api/logout", json={}, headers=headers)
            assert (await client.get("/api/tasks")).status == 401
        finally:
            await client.close()
    asyncio.run(scenario())


def test_csv_tail_is_bounded_and_handles_partial_rows(tmp_path):
    path = tmp_path / "minutes.csv"
    path.write_text("minute_ts,premium_close_bps\n" + "".join(f"{index},{index / 10}\n" for index in range(200)) + "partial\n")
    rows = tail_csv(path, 120)
    assert len(rows) == 120 and rows[0]["minute_ts"] == "80"
    assert rows[-1]["minute_ts"] == "199"


def test_configuration_library_privacy_and_retained_values(tmp_path, monkeypatch):
    async def scenario():
        manager = manager_at(tmp_path)
        client = TestClient(TestServer(create_app(manager, "test-password-long")), cookie_jar=aiohttp.CookieJar(unsafe=True))
        await client.start_server()
        headers = {"X-Web-Request": "1"}
        try:
            assert (await client.get("/api/configs/env/default")).status == 401
            assert (await client.put("/api/configs/env/private", json={}, headers=headers)).status == 401
            await client.post("/api/login", json={"password": "test-password-long"}, headers=headers)
            secret = "private-seed-with-'quote-and-\\slash-${NOT_EXPANDED}"
            response = await client.put("/api/configs/env/private", json={"values": {
                "ARCUS_API_SIGNING_KEY": secret, "ARCUS_ACCOUNT_INDEX": "0"}}, headers=headers)
            assert response.status == 200
            assert "private-seed" not in await response.text()
            assert manager.environment("private")["ARCUS_API_SIGNING_KEY"] == secret
            path = manager.root / "credentials/private.env"
            assert path.stat().st_mode & 0o777 == 0o600
            response = await client.get("/api/configs/env/private")
            status = await response.json()
            assert all("value" not in field for field in status["fields"])
            assert next(field for field in status["fields"] if field["name"] == "ARCUS_API_SIGNING_KEY")["configured"]
            assert (await client.put("/api/configs/env/private", json={"values": {}}, headers=headers)).status == 400
            response = await client.put("/api/configs/env/private", json={"overwrite": True,
                "values": {"ARCUS_API_SIGNING_KEY": "", "ARCUS_ACCOUNT_ADDRESS": "fake-account"}}, headers=headers)
            assert response.status == 200
            assert manager.environment("private")["ARCUS_API_SIGNING_KEY"] == secret
            response = await client.put("/api/configs/env/private", json={"overwrite": True,
                "clear": ["ARCUS_API_SIGNING_KEY"]}, headers=headers)
            assert response.status == 200
            assert "ARCUS_API_SIGNING_KEY" not in manager.environment("private")
            for changes in ({"WEB_PASSWORD": "not-allowed"}, {"ARCUS_API_SIGNING_KEY": "evil\nHL_PRIVATE_KEY=injected"},
                            {"LIGHTER_ACCOUNT_INDEX": "-1"}, {"ARCUS_ACCOUNT_INDEX": "10"}):
                response = await client.put("/api/configs/env/private", json={"overwrite": True, "values": changes}, headers=headers)
                assert response.status == 400
                assert "injected" not in await response.text()
            monkeypatch.setenv("HL_PRIVATE_KEY", "ambient-private-seed")
            response = await client.get("/api/configs/env/default")
            assert "ambient-private-seed" not in await response.text()
            assert (await client.put("/api/configs/env/default", json={"clear": ["HL_PRIVATE_KEY"]}, headers=headers)).status == 400
        finally:
            await client.close()
    asyncio.run(scenario())


def test_linked_yaml_uses_latest_server_file_and_blocks_running_edits(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            await manager.save_strategy("eth.yaml", {"config": STRATEGY})
            task = await manager.put({**payload(), "strategy_file": "eth.yaml", "config": "ignored"})
            latest = STRATEGY.replace("midline_bps: 0", "midline_bps: 12")
            await manager.save_strategy("eth.yaml", {"config": latest, "overwrite": True})
            started = await manager.start(task["id"])
            assert "midline_bps: 12" in started["config"]
            saved = yaml.safe_load((manager.directory(task["id"]) / "config.yaml").read_text())
            assert saved["thresholds"]["midline_bps"] == 12
            with pytest.raises(ConsoleError, match="运行中"):
                await manager.save_strategy("eth.yaml", {"config": STRATEGY, "overwrite": True})
            with pytest.raises(ConsoleError, match="运行中"):
                await manager.save_credentials("default", {"values": {"ARCUS_API_SIGNING_KEY": "not-applied"}})
            assert not (manager.root / ".env").exists()
            watcher = manager.running[task["id"]]["watcher"]
            await asyncio.sleep(0.1)
            await manager.stop(task["id"])
            await asyncio.wait_for(watcher, 3)
            changed_market = latest.replace("venue: lighter-rh", "venue: lighter-rh\n  symbol: BTC")
            await manager.save_strategy("eth.yaml", {"config": changed_market, "overwrite": True})
            with pytest.raises(ConsoleError, match="新建任务"):
                await manager.start(task["id"])
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_config_library_rejects_escape_symlinks_and_invalid_yaml(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        try:
            for name in ("../outside.yaml", "/outside.yaml", "bad name.yaml", ".env"):
                with pytest.raises(ConsoleError):
                    await manager.save_strategy(name, {"config": STRATEGY})
            for name in ("../outside", "/outside", ".env.web"):
                with pytest.raises(ConsoleError):
                    await manager.save_credentials(name, {"values": {}})
            outside = tmp_path / "outside.yaml"
            outside.write_text(STRATEGY)
            (manager.root / "configs/link.yaml").symlink_to(outside)
            assert "link.yaml" not in manager.strategies()
            with pytest.raises(ConsoleError, match="符号链接"):
                manager.strategy("link.yaml")
            (manager.root / ".env").symlink_to(outside)
            with pytest.raises(ConsoleError, match="符号链接"):
                manager.environment("default")
            for text in ("thresholds: [", STRATEGY + "private_key: should-not-be-yaml\n",
                         STRATEGY.replace("upper_bps: 4", "upper_bps: .nan"), "primary: {}"):
                with pytest.raises((ConsoleError, ConfigError)):
                    await manager.save_strategy("bad.yaml", {"config": text})
            assert not (manager.root / "configs/bad.yaml").exists()
            (manager.root / "credentials").symlink_to(tmp_path)
            assert manager.profiles() == ["default"]
            with pytest.raises(ConsoleError, match="符号链接"):
                await manager.save_credentials("escape", {"values": {}})
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_details_logs_remain_available_after_stop(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        client = TestClient(TestServer(create_app(manager, "test-password-long")), cookie_jar=aiohttp.CookieJar(unsafe=True))
        await client.start_server()
        try:
            task = await manager.put(payload())
            await manager.start(task["id"])
            watcher = manager.running[task["id"]]["watcher"]
            await asyncio.sleep(0.15)
            await manager.stop(task["id"])
            await asyncio.wait_for(watcher, 3)
            await client.post("/api/login", json={"password": "test-password-long"}, headers={"X-Web-Request": "1"})
            response = await client.get(f"/api/tasks/{task['id']}/logs")
            logs = await response.json()
            assert "fake worker ready ETH" in logs["text"]
            assert "START record" in logs["events"] and "STOP" in logs["events"]
            assert (await client.get(f"/api/tasks/{task['id']}/download/logs")).status == 200
            page = await (await client.get("/dex-arbitrage")).text()
            assert 'id="task-detail-dialog"' in page and 'id="task-detail-log"' in page
        finally:
            await client.close()
    asyncio.run(scenario())


def test_pause_resume_controls_live_task_without_killing_process(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        credentials(manager, "live")
        try:
            task = await manager.put({**payload("ETH"), "mode": "live", "profile": "live"})
            await manager.start(task["id"], True)
            paused = await manager.pause(task["id"], True)
            assert paused["manual_paused"] is True
            assert json.loads((manager.directory(task["id"]) / "control.json").read_text())["paused"] is True
            assert manager.running[task["id"]]["process"].returncode is None
            resumed = await manager.pause(task["id"], False)
            assert resumed["manual_paused"] is False
            assert json.loads((manager.directory(task["id"]) / "control.json").read_text())["paused"] is False
            await manager.stop(task["id"])
            await asyncio.wait_for(manager.running[task["id"]]["watcher"], 3)
            events = (manager.directory(task["id"]) / "events.log").read_text()
            assert "PAUSE requested" in events and "RESUME requested" in events
        finally:
            await manager.close()
    asyncio.run(scenario())


def test_engine_reads_manual_pause_control_and_status(tmp_path):
    async def scenario():
        config = tmp_path / "config.yaml"
        config.write_text(STRATEGY)
        cfg = load_config(str(config), symbol="ETH", hedge_venue="arcus",
                          primary_venue="lighter-rh", credential_env={})
        control = tmp_path / "control.json"
        engine = Engine(cfg, control_file=str(control))
        engine.ensure_async_state()
        worker = asyncio.create_task(engine._control_loop())
        try:
            control.write_text('{"paused": true}')
            await asyncio.sleep(0.3)
            assert engine.manual_paused is True
            assert snapshot(engine)["paused"] is True
            control.write_text('{"paused": false}')
            await asyncio.sleep(0.3)
            assert engine.manual_paused is False
            assert snapshot(engine)["paused"] is False
        finally:
            engine.request_stop()
            await asyncio.wait_for(worker, 2)
    asyncio.run(scenario())


def test_parent_watchdog_and_final_status(tmp_path, monkeypatch):
    from main import amain

    async def fake_run(engine):
        await engine.stop.wait()
        engine.trades = 3

    monkeypatch.setattr(Engine, "run", fake_run)
    cfg = load_config(str(ROOT / "configs/rh-arcus.yaml"), symbol="ETH", hedge_venue="arcus", credential_env={})
    path = tmp_path / "status.json"
    asyncio.run(asyncio.wait_for(amain(cfg, True, False, False, None, "zh",
                                    status_file=str(path), parent_pid=os.getppid() + 100000), 3))
    final = json.loads(path.read_text())
    assert final["trades"] == 3
    assert final["net_delta"] is None
    assert final["mode"] == "record"


def test_private_values_never_reach_saved_logs_or_downloads(tmp_path):
    async def scenario():
        manager = manager_at(tmp_path)
        credentials(manager, "private", arcus_key="fake-sensitive-signing-seed-for-privacy-test")
        (manager.root / "main.py").write_text(PRIVATE_WORKER)
        client = TestClient(TestServer(create_app(manager, "test-password-long")), cookie_jar=aiohttp.CookieJar(unsafe=True))
        await client.start_server()
        try:
            task = await manager.put({**payload(), "profile": "private", "mode": "live"})
            await manager.start(task["id"], True)
            await asyncio.wait_for(manager.running[task["id"]]["watcher"], 3)
            text = (manager.directory(task["id"]) / "engine.log").read_text()
            assert "fake-sensitive" not in text
            assert "private-auth-token" not in text
            assert "[REDACTED]" in text and "record complete" in text
            await client.post("/api/login", json={"password": "test-password-long"}, headers={"X-Web-Request": "1"})
            response = await client.get(f"/api/tasks/{task['id']}/logs")
            assert "fake-sensitive" not in await response.text()
            response = await client.get(f"/api/tasks/{task['id']}/download/logs")
            assert "fake-sensitive" not in await response.text()
        finally:
            await client.close()
    asyncio.run(scenario())


def test_redaction_across_chunks_and_oversized_lines():
    from entropy_arb.privacy import PrivateLogCapture

    secret = "0x" + "a" * 64
    capture = PrivateLogCapture({"HL_PRIVATE_KEY": secret}, max_line_bytes=128)
    chunks = [b"signer=", secret[:13].encode(), secret[13:].upper().encode(),
              b"\n", b"x" * 129, b"private-tail\n", b"status normal\n", b"auth='private-token'"]
    text = b"".join(capture.feed(chunk) for chunk in chunks) + capture.finish()
    assert b"AAAA" not in text and b"private-token" not in text
    assert b"private-tail" not in text
    assert b"status normal" in text and b"LOG LINE OMITTED" in text
    assert b"[REDACTED]" in text
