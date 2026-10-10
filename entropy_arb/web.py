"""Authenticated Linux process manager for the existing trading CLI."""
from __future__ import annotations

import asyncio
import csv
import fcntl
import hashlib
import hmac
import json
import logging
import math
import os
import re
import secrets
import signal
import sys
import tempfile
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import yaml
import aiohttp
from aiohttp import web
from dotenv import dotenv_values

from .config import ConfigError, HEDGE_VENUES, PRIMARY_VENUES, _SCHEMA, _validate, load_config
from .market_monitor import MonitorRules, MultiVenueMonitor, discover_candidates
from .privacy import PrivateLogCapture
from .thresholds import (ThresholdDataError, ThresholdSuggestion, daily_window,
                         load_window_rows, prune_csv, suggest)
from .telegram import TelegramNotifier, test_message

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "web"
CREDENTIAL_BASES = (
    "HL_PRIVATE_KEY", "HL_ACCOUNT_ADDRESS", "HL_PRIVATE_KEY_XYZ", "HL_ACCOUNT_ADDRESS_XYZ",
    "LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX", "LIGHTER_API_PRIVATE_KEY",
    "ARCUS_ACCOUNT_ADDRESS", "ARCUS_ACCOUNT_INDEX", "ARCUS_API_SIGNING_KEY",
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
)
CREDENTIAL_NAMES = set(CREDENTIAL_BASES) | {
    "PRIMARY_" + name for name in CREDENTIAL_BASES if not name.startswith("TELEGRAM_")
}
EDITABLE_CREDENTIALS = tuple(CREDENTIAL_BASES) + tuple(
    "PRIMARY_" + name for name in CREDENTIAL_BASES
    if not name.endswith("_XYZ") and not name.startswith("TELEGRAM_"))
ACTIVE = {"running", "stopping"}
SYMBOL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,39}\Z")
PROFILE_RE = re.compile(r"[A-Za-z0-9_-]{1,40}\Z")
SESSION_SECONDS = 12 * 3600
LOG_LIMIT = 10 * 1024 * 1024
AUTO_THRESHOLD_TZ = ZoneInfo("Asia/Shanghai")
AUTO_THRESHOLD_MIN_ROWS = 30
AUTO_THRESHOLD_MIN_SAMPLES = 10
log = logging.getLogger("web")


class ConsoleError(ValueError):
    pass


def write_json(path: Path, value) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False), encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def finite_config(value, depth=0) -> None:
    if depth > 20:
        raise ConsoleError("配置嵌套过深或包含循环 YAML 引用")
    if isinstance(value, (int, float)):
        try:
            valid_number = math.isfinite(value)
        except OverflowError:
            valid_number = False
        if not valid_number:
            raise ConsoleError("配置数值必须是有限数，不能使用 NaN 或 Infinity")
    if isinstance(value, dict):
        for item in value.values():
            finite_config(item, depth + 1)
    if isinstance(value, list):
        for item in value:
            finite_config(item, depth + 1)


def tail_text(path: Path, limit=192 * 1024) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        handle.seek(max(0, size - limit))
        content = handle.read(limit).decode("utf-8", errors="replace")
    return content.split("\n", 1)[-1] if size > limit else content


def tail_csv(path: Path, limit=120) -> list:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        header = handle.readline().strip().split(",")
    rows = list(csv.reader(tail_text(path, 256 * 1024).splitlines()))
    return [dict(zip(header, row)) for row in rows if len(row) == len(header) and row != header][-limit:]


def paged_csv(path: Path, page=1, page_size=10) -> dict:
    page = max(1, int(page))
    page_size = min(100, max(1, int(page_size)))
    if not path.exists():
        return {"rows": [], "total": 0, "page": 1, "page_size": page_size, "pages": 0}
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    total = len(rows)
    pages = math.ceil(total / page_size) if total else 0
    if pages:
        page = min(page, pages)
        end = total - (page - 1) * page_size
        start = max(0, end - page_size)
        rows = list(reversed(rows[start:end]))
    else:
        page = 1
        rows = []
    return {"rows": rows, "total": total, "page": page,
            "page_size": page_size, "pages": pages}


def market_resource(venue) -> tuple:
    symbol = venue.symbol.upper().split(":")[-1]
    if venue.kind == "arcus" and symbol.endswith("-USD"):
        symbol = symbol[:-4]
    if venue.kind == "hl":
        scope = venue.hl_dex
    elif venue.kind == "lighter":
        scope = venue.lighter_profile.chain_id
    else:
        scope = venue.arcus_network
    return venue.kind, scope, symbol


def signer_resources(venue) -> set:
    if venue.kind == "hl":
        secret = venue.hl_creds.private_key or ""
        scope = "hl"
    elif venue.kind == "lighter":
        creds = venue.lighter_creds
        secret = creds.api_private_key or ""
        scope = str(venue.lighter_profile.chain_id)
        identity = ("lighter-key", scope, creds.account_index, creds.api_key_index)
    else:
        secret = venue.arcus_creds.signing_key or ""
        scope = venue.arcus_network
    fingerprint = hashlib.sha256(secret.removeprefix("0x").lower().encode()).hexdigest()
    resources = {(venue.kind, scope, fingerprint)}
    if venue.kind == "lighter":
        resources.add(identity)
    return resources


def missing_credentials(config) -> list[str]:
    missing = []
    lighter_primary_separate = (
        config.primary_venue in ("lighter", "lighter-rh")
        and config.hedge.kind == "lighter"
    )
    for venue in (config.entropy, config.hedge):
        if venue.kind == "hl":
            if not venue.hl_creds or not venue.hl_creds.private_key:
                missing.append("HL_PRIVATE_KEY")
        elif venue.kind == "lighter":
            prefix = "PRIMARY_" if venue.key == "entropy" and lighter_primary_separate else ""
            credentials = venue.lighter_creds
            if not credentials or credentials.account_index is None:
                missing.append(prefix + "LIGHTER_ACCOUNT_INDEX")
            if not credentials or credentials.api_key_index is None:
                missing.append(prefix + "LIGHTER_API_KEY_INDEX")
            if not credentials or not credentials.api_private_key:
                missing.append(prefix + "LIGHTER_API_PRIVATE_KEY")
        elif venue.kind == "arcus":
            credentials = venue.arcus_creds
            if not credentials or not credentials.account_address:
                missing.append("ARCUS_ACCOUNT_ADDRESS")
            if not credentials or not credentials.signing_key:
                missing.append("ARCUS_API_SIGNING_KEY")
    return list(dict.fromkeys(missing))


class TaskManager:
    def __init__(self, root: Path, data: Path, max_running=12):
        self.root = root.resolve()
        self.data = data.resolve()
        self.data.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.data.chmod(0o700)
        self.lock_file = (self.data / "manager.lock").open("a")
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self.lock_file.close()
            raise ConsoleError("此数据目录已有一个控制台在运行") from error
        self.max_running = max_running
        self.guard = None
        self.running = {}
        self.tasks = {}
        self.closing = False
        self.market_monitor = None
        self.auto_threshold_task = None
        manifest = self.data / "tasks.json"
        if manifest.exists():
            records = json.loads(manifest.read_text(encoding="utf-8"))
            for task in records:
                if not re.fullmatch(r"[a-f0-9]{32}", task["id"]):
                    raise ConsoleError("任务记录包含无效 ID")
                if task["state"] in ACTIVE:
                    task.update(state="interrupted", pid=None, ended_at=time.time())
                self.tasks[task["id"]] = task
        self.save()

    async def monitor_instance(self, profile="default"):
        if self.market_monitor is None:
            values = self.environment(profile) if profile in self.profiles() else {}
            self.market_monitor = MultiVenueMonitor(
                TelegramNotifier(token=values.get("TELEGRAM_BOT_TOKEN"),
                                 chat_id=values.get("TELEGRAM_CHAT_ID")))
        elif profile in self.profiles():
            values = self.environment(profile)
            self.market_monitor.notifier.token = (values.get("TELEGRAM_BOT_TOKEN") or "").strip()
            self.market_monitor.notifier.chat_id = (values.get("TELEGRAM_CHAT_ID") or "").strip()
        return self.market_monitor

    async def monitor_rules(self, strategy_file=None, hedge_venue=None):
        """Build display fees without making market compatibility depend on YAML.

        The three-market page answers a different question from the trading
        engine: which symbols can use the RH-primary hedge path.  A strategy
        file is optional and is only used as a fee source when the caller
        explicitly supplies one; its thresholds are never read here.
        """
        if not strategy_file:
            return MonitorRules(source="脚本适配检查（未加载阈值）", primary_key="rh")
        text = self.strategy(strategy_file)
        raw = self.check_strategy(text)
        primary = raw.get("primary") or raw.get("entropy") or {}
        hedge = raw.get("hedge") or {}
        def number(section, key, default=0.0):
            value = section.get(key, default)
            return float(value)
        primary_venue = (primary.get("venue") or
                         ("entropy" if "entropy" in raw and "primary" not in raw
                          else "lighter-rh"))
        primary_fee = number(primary, "taker_fee_bps")
        hedge_fee = number(hedge, "taker_fee_bps")
        # A strategy describes one pair, while this page displays both
        # possible hedges. The page tells us which configured hedge fee to use.
        hedge_venue = hedge_venue or ("arcus" if "arcus" in str(strategy_file).lower() else "entropy")
        entropy_fee = primary_fee if primary_venue == "entropy" else 0.0
        rh_fee = primary_fee if primary_venue == "lighter-rh" else 0.0
        arcus_fee = primary_fee if primary_venue == "arcus" else 0.0
        if primary_venue == "entropy":
            if hedge_venue == "arcus":
                arcus_fee = hedge_fee
            else:
                rh_fee = hedge_fee
        elif primary_venue == "lighter-rh":
            if hedge_venue == "arcus":
                arcus_fee = hedge_fee
            else:
                entropy_fee = hedge_fee
        elif primary_venue == "arcus":
            if hedge_venue == "lighter-rh":
                rh_fee = hedge_fee
            else:
                entropy_fee = hedge_fee
        primary_key = {"entropy": "entropy", "lighter-rh": "rh", "arcus": "arcus"}.get(primary_venue, "rh")
        return MonitorRules(
            entropy_fee_bps=entropy_fee,
            rh_fee_bps=rh_fee,
            arcus_fee_bps=arcus_fee,
            source=f"费率来源：{strategy_file}（未使用阈值）",
            # The monitoring page follows the current script default even if
            # an old YAML was authored for another primary venue.
            primary_key="rh",
        )

    def save(self):
        write_json(self.data / "tasks.json", list(self.tasks.values()))

    def start_auto_threshold_scheduler(self):
        """Start the daily 08:00 Shanghai threshold job once the app loop runs."""
        if self.auto_threshold_task is None:
            self.auto_threshold_task = asyncio.create_task(
                self._auto_threshold_loop(), name="daily-thresholds")

    def _auto_status_path(self, task_id):
        return self.directory(task_id) / "auto_threshold.json"

    def _read_auto_status(self, task_id):
        path = self._auto_status_path(task_id)
        if not path.exists():
            return {"status": "pending"}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {"status": "pending"}
        except (OSError, ValueError, TypeError):
            return {"status": "failed", "reason": "自动阈值状态文件损坏"}

    def _write_auto_status(self, task_id, value):
        write_json(self._auto_status_path(task_id), value)

    @staticmethod
    def _dated_log_time():
        return datetime.now(AUTO_THRESHOLD_TZ).strftime("%Y-%m-%d %H:%M:%S")

    async def _auto_threshold_loop(self):
        """Run once for each completed Shanghai 08:00 window."""
        while not self.closing:
            now = datetime.now(AUTO_THRESHOLD_TZ)
            target = now.replace(hour=8, minute=0, second=0, microsecond=0)
            if now >= target:
                window = daily_window(now)
                await self.auto_update_all(window)
                target = target + timedelta(days=1)
            delay = max(1.0, (target - now).total_seconds())
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise

    async def auto_update_all(self, window=None):
        """Apply the completed daily window to every live task once."""
        window = window or daily_window()
        for task_id, task in list(self.tasks.items()):
            if task.get("mode") != "live":
                continue
            current = self._read_auto_status(task_id)
            if current.get("window_end") == window.end_ts and current.get("status") in (
                    "applied", "unchanged", "skipped"):
                continue
            try:
                await self.auto_update_task(task_id, window)
            except ThresholdDataError as error:
                self.audit(task_id, "AUTO-THRESHOLD skipped %s" % error)
                self._write_auto_status(task_id, {
                    "status": "skipped", "window": window.label,
                    "window_start": window.start_ts, "window_end": window.end_ts,
                    "checked_at": time.time(), "reason": str(error),
                })
            except Exception as error:
                log.exception("daily threshold update failed for %s", task_id)
                self.audit(task_id, "AUTO-THRESHOLD FAILED %s" % error)
                self._write_auto_status(task_id, {
                    "status": "failed", "window": window.label,
                    "window_start": window.start_ts, "window_end": window.end_ts,
                    "checked_at": time.time(), "reason": str(error),
                })

    def _render_threshold_config(self, text, suggestion):
        raw = self.check_strategy(text)
        thresholds = raw.setdefault("thresholds", {})
        thresholds.update({
            "midline_bps": suggestion.midline_bps,
            "upper_bps": suggestion.upper_bps,
            "lower_bps": suggestion.lower_bps,
        })
        return yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)

    def _threshold_suggestion(self, task_id):
        """Calculate a threshold proposal without changing the task."""
        task = self.get(task_id)
        directory = self.directory(task_id)
        config_path = self.ensure_task_config(task)
        cfg = load_config(str(config_path), symbol=task["symbol"],
                          primary_venue=task["primary"], hedge_venue=task["hedge"],
                          credential_env=self.environment(task["profile"]))
        window = daily_window()
        csv_path = self.configured_path(cfg.recorder_csv)
        if not csv_path.exists():
            raise ConsoleError("还没有分钟行情数据，先运行采集任务")
        rows = load_window_rows(csv_path, window, AUTO_THRESHOLD_MIN_SAMPLES)
        try:
            suggestion = suggest(rows, cfg.entropy.fee_bps + cfg.hedge.fee_bps,
                                 window, AUTO_THRESHOLD_MIN_ROWS)
        except ThresholdDataError as error:
            raise ConsoleError(str(error)) from error
        current = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        return task, window, suggestion, dict(current.get("thresholds", {}))

    async def manual_threshold(self, task_id, apply=False, confirm_live=False,
                               thresholds=None):
        """Preview or apply a threshold proposal requested from the UI."""
        task, window, suggestion, old = self._threshold_suggestion(task_id)
        if not apply:
            return {
                "task_id": task_id, "window": window.label,
                "config_path": str(self.directory(task_id) / "config.yaml"),
                "data_path": str(self.minute_path(task)),
                "window_start": window.start_ts, "window_end": window.end_ts,
                "rows": suggestion.rows, "span_hours": suggestion.span_hours,
                "current": old,
                "suggested": {"midline_bps": suggestion.midline_bps,
                              "upper_bps": suggestion.upper_bps,
                              "lower_bps": suggestion.lower_bps},
            }
        if task["mode"] == "live" and confirm_live is not True:
            raise ConsoleError("修改实盘阈值需要明确确认")
        if thresholds is not None:
            if not isinstance(thresholds, dict):
                raise ConsoleError("阈值必须是 JSON 对象")
            try:
                midline = float(thresholds["midline_bps"])
                upper = float(thresholds["upper_bps"])
                lower = float(thresholds["lower_bps"])
            except (KeyError, TypeError, ValueError) as error:
                raise ConsoleError("阈值必须是数字") from error
            if not all(math.isfinite(value) for value in (midline, upper, lower)):
                raise ConsoleError("阈值必须是有限数字")
            if upper <= 0 or lower <= 0:
                raise ConsoleError("上下入场带宽必须大于 0")
            suggestion = ThresholdSuggestion(midline, upper, lower,
                                              suggestion.rows, suggestion.span_hours,
                                              window)
        await self.auto_update_task(task_id, window, suggestion=suggestion)
        return self._read_auto_status(task_id)

    async def auto_update_task(self, task_id, window, suggestion=None):
        """Update one live task and restart it only after a valid calculation."""
        task = self.get(task_id)
        directory = self.directory(task_id)
        config_path = self.ensure_task_config(task)
        cfg = load_config(str(config_path), symbol=task["symbol"],
                          primary_venue=task["primary"], hedge_venue=task["hedge"],
                          credential_env=self.environment(task["profile"]))
        csv_path = self.configured_path(cfg.recorder_csv)
        if suggestion is None:
            rows = load_window_rows(csv_path, window, AUTO_THRESHOLD_MIN_SAMPLES)
            suggestion = suggest(rows, cfg.entropy.fee_bps + cfg.hedge.fee_bps,
                                 window, AUTO_THRESHOLD_MIN_ROWS)
        current_raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        old_thresholds = dict(current_raw.get("thresholds", {}))
        new_thresholds = {
            "midline_bps": suggestion.midline_bps,
            "upper_bps": suggestion.upper_bps,
            "lower_bps": suggestion.lower_bps,
        }
        changed = any(float(old_thresholds.get(key)) != value
                      for key, value in new_thresholds.items())
        if not changed:
            removed = prune_csv(csv_path, window)
            self._write_auto_status(task_id, {
                "status": "unchanged", "window": window.label,
                "window_start": window.start_ts, "window_end": window.end_ts,
                "data_path": str(csv_path),
                "rows": suggestion.rows, "span_hours": suggestion.span_hours,
                "thresholds": new_thresholds, "removed_rows": removed,
                "checked_at": time.time(),
            })
            self.audit(task_id, "AUTO-THRESHOLD unchanged %s rows=%d removed=%d" %
                       (window.label, suggestion.rows, removed))
            return

        running = task_id in self.running
        runtime = self.running.get(task_id)
        restart = running and task["mode"] == "live"
        if restart:
            await self.pause(task_id, True)
            await self.stop(task_id)
            try:
                await asyncio.wait_for(asyncio.shield(runtime["watcher"]),
                                       timeout=runtime["grace"])
            except asyncio.TimeoutError:
                raise ConsoleError("等待实盘优雅退出超时，未更新阈值；请核对持仓")

        old_text = config_path.read_text(encoding="utf-8")
        new_text = self._render_threshold_config(old_text, suggestion)
        strategy_file = task.get("strategy_file")
        strategy_path = None
        old_strategy_text = None
        source_written = False
        if strategy_file:
            users = [other for other in self.tasks.values()
                     if other.get("strategy_file") == strategy_file and other["id"] != task_id]
            if users:
                raise ConsoleError("自动阈值不能更新被多个任务共用的策略文件")
            strategy_path = self.strategy_path(strategy_file)
            old_strategy_text = strategy_path.read_text(encoding="utf-8")
            new_text = self._render_threshold_config(old_strategy_text, suggestion)
            self.write_config(strategy_path, new_text)
            source_written = True
        try:
            self.write_config(config_path, new_text)
        except Exception:
            if source_written and strategy_path is not None and old_strategy_text is not None:
                self.write_config(strategy_path, old_strategy_text)
            raise
        task["config"] = new_text
        self.save()
        try:
            if restart:
                await self.start(task_id, confirmed=True)
        except Exception:
            # Restore both copies before surfacing the failure.  The stopped
            # task remains recoverable with the previous known-good thresholds.
            self.write_config(config_path, old_text)
            task["config"] = old_text
            if strategy_path is not None and old_strategy_text is not None:
                self.write_config(strategy_path, old_strategy_text)
            self.save()
            raise
        removed = prune_csv(csv_path, window)
        self._write_auto_status(task_id, {
            "status": "applied", "window": window.label,
            "window_start": window.start_ts, "window_end": window.end_ts,
            "data_path": str(csv_path),
            "rows": suggestion.rows, "span_hours": suggestion.span_hours,
            "old_thresholds": old_thresholds, "thresholds": new_thresholds,
            "removed_rows": removed, "applied_at": time.time(),
        })
        self.audit(task_id, "AUTO-THRESHOLD applied %s old=%s new=%s rows=%d removed=%d" %
                   (window.label, old_thresholds, new_thresholds,
                    suggestion.rows, removed))

    def mutex(self):
        if self.guard is None:
            self.guard = asyncio.Lock()
        return self.guard

    def instance_locked(self, task_id):
        path = self.directory(task_id) / "instance.lock"
        if not path.exists():
            return False
        with path.open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
        return False

    def directory(self, task_id):
        return self.data / task_id

    def configured_path(self, value):
        """Resolve a YAML output path relative to the project root."""
        path = Path(value)
        return (path if path.is_absolute() else self.root / path).resolve()

    def _legacy_task_output(self, task_id, symbol, value, filename):
        """Recognize paths injected by old console versions.

        Before output paths became YAML-owned, the web console wrote every
        recorder/log file below ``web-data/<task-id>/``.  Those paths must not
        keep winning over the saved strategy after an upgrade.  Restrict the
        migration to the old task-id/symbol shapes so an explicit user path
        elsewhere remains authoritative.
        """
        if not isinstance(value, str):
            return False
        path = self.configured_path(value)
        roots = (self.directory(task_id), self.data / str(symbol).upper())
        return any(path == root.resolve() / filename for root in roots)

    def task_config(self, task):
        return load_config(str(self.ensure_task_config(task)),
                           symbol=task["symbol"], primary_venue=task["primary"],
                           hedge_venue=task["hedge"], credential_env={})

    def minute_path(self, task):
        return self.configured_path(self.task_config(task).recorder_csv)

    def log_path(self, task):
        return self.configured_path(self.task_config(task).log_file)

    def trades_path(self, task):
        return self.configured_path(self.task_config(task).trades_csv)

    def ensure_task_config(self, task):
        path = self.directory(task["id"]) / "config.yaml"
        # Never replace the snapshot of a running process: it still uses the
        # path loaded at startup. Stopped tasks use their original YAML again,
        # which also removes the old console-imposed recorder path.
        if (task["id"] in self.running or self.instance_locked(task["id"])) and path.is_file() and not path.is_symlink():
            return path
        _, rendered, _, _ = self.validate(task, task["id"])
        self.write_config(path, rendered)
        return path

    def task_snapshot_config(self, task):
        path = self.directory(task["id"]) / "config.yaml"
        if path.is_file() and not path.is_symlink():
            return path
        return self.ensure_task_config(task)

    def get(self, task_id):
        if task_id not in self.tasks:
            raise web.HTTPNotFound(text="任务不存在")
        return self.tasks[task_id]

    def profiles(self):
        directory = self.root / "credentials"
        profiles = ["default"]
        if directory.exists() and not directory.is_symlink() and directory.resolve().parent == self.root:
            profiles += sorted(path.stem for path in directory.glob("*.env")
                               if PROFILE_RE.fullmatch(path.stem) and path.stem != "default"
                               and path.is_file() and not path.is_symlink())
        return profiles

    def confined_path(self, directory, filename):
        path = directory / filename
        if directory.is_symlink() or directory.resolve().parent != self.root or path.is_symlink():
            raise ConsoleError("配置路径不安全，不支持符号链接")
        return path

    def profile_path(self, profile):
        if profile == "default":
            path = self.root / ".env"
            if path.is_symlink():
                raise ConsoleError("凭据文件不能是符号链接")
            return path
        if not isinstance(profile, str) or not PROFILE_RE.fullmatch(profile):
            raise ConsoleError("凭据名称只允许 1–40 位字母、数字、下划线或短横线")
        return self.confined_path(self.root / "credentials", profile + ".env")

    def strategy_path(self, name):
        if name == "@default":
            path = self.root / "config.yaml"
            if path.is_symlink():
                raise ConsoleError("策略文件不能是符号链接")
            return path
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}\.ya?ml", name):
            raise ConsoleError("策略文件名只允许字母、数字、下划线或短横线，并以 .yaml / .yml 结尾")
        return self.confined_path(self.root / "configs", name)

    def strategies(self):
        names = []
        if (self.root / "config.yaml").is_file() and not (self.root / "config.yaml").is_symlink():
            names.append("@default")
        directory = self.root / "configs"
        if directory.exists() and not directory.is_symlink() and directory.resolve().parent == self.root:
            names += sorted(path.name for path in directory.iterdir()
                            if re.fullmatch(r"[A-Za-z0-9_-]{1,40}\.ya?ml", path.name)
                            and path.is_file() and not path.is_symlink())
        return names

    def strategy(self, name):
        path = self.strategy_path(name)
        if not path.is_file():
            raise ConsoleError("策略文件不存在")
        if path.stat().st_size > 128000:
            raise ConsoleError("策略文件过大")
        text = path.read_text(encoding="utf-8")
        self.check_strategy(text)
        return text

    def check_strategy(self, text):
        if not isinstance(text, str) or len(text) > 32000:
            raise ConsoleError("YAML 配置长度不能超过 32000 字符")
        try:
            raw = yaml.safe_load(text)
        except yaml.YAMLError:
            raise ConsoleError("YAML 格式错误，请检查缩进与字段") from None
        finite_config(raw)
        _validate(raw, _SCHEMA)
        thresholds = raw.get("thresholds", {})
        if any(key not in thresholds for key in ("midline_bps", "upper_bps", "lower_bps")):
            raise ConsoleError("YAML 必须包含 midline_bps、upper_bps、lower_bps 阈值")
        return raw

    def credential_status(self, profile):
        values = self.environment(profile) if profile in self.profiles() else {}
        return dict(name=profile, exists=self.profile_path(profile).exists(), fields=[
            dict(name=name, configured=bool(values.get(name)),
                 inherited=profile == "default" and bool(os.environ.get(name)),
                 secret=True) for name in EDITABLE_CREDENTIALS])

    def ensure_unused(self, field, name):
        for task in self.tasks.values():
            if task.get(field) == name and (task["id"] in self.running or self.instance_locked(task["id"])):
                raise ConsoleError("配置正被运行中的任务使用，请先停止相关任务")

    def write_config(self, path, text):
        path.parent.mkdir(exist_ok=True, mode=0o700)
        descriptor, temporary = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(text)
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def write_control(self, task_id, paused):
        path = self.directory(task_id) / "control.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"paused": paused}), encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, path)

    async def save_strategy(self, name, payload):
        async with self.mutex():
            path = self.strategy_path(name)
            if path.exists() and payload.get("overwrite") is not True:
                raise ConsoleError("同名策略已存在，请选择编辑或换一个名称")
            self.ensure_unused("strategy_file", name)
            text = payload.get("config")
            self.check_strategy(text)
            self.write_config(path, text)
            return dict(name=name, config=text)

    async def save_credentials(self, profile, payload):
        async with self.mutex():
            path = self.profile_path(profile)
            if path.exists() and payload.get("overwrite") is not True:
                raise ConsoleError("同名凭据已存在，请选择编辑或换一个名称")
            self.ensure_unused("profile", profile)
            changes, clear = payload.get("values", {}), payload.get("clear", [])
            if not isinstance(changes, dict) or not isinstance(clear, list):
                raise ConsoleError("凭据字段格式不正确")
            if any(name not in EDITABLE_CREDENTIALS for name in changes) or any(
                    not isinstance(name, str) or name not in EDITABLE_CREDENTIALS for name in clear):
                raise ConsoleError("只能保存模板中的交易凭据字段，不能修改控制台密码")
            values = {}
            if path.exists():
                if path.stat().st_size > 128000:
                    raise ConsoleError("凭据文件过大")
                values = dict(dotenv_values(path, interpolate=False))
            for name, value in changes.items():
                if not isinstance(value, str) or len(value) > 2048 or any(
                        ord(character) < 32 for character in value):
                    raise ConsoleError("凭据字段必须是单行文本且不能超过 2048 字符")
                value = value.strip()
                if value and name.endswith("_INDEX"):
                    if not re.fullmatch(r"\d{1,18}", value):
                        raise ConsoleError("账户索引与 API Key 索引必须为非负整数")
                    if "ARCUS_ACCOUNT_INDEX" in name and int(value) > 9:
                        raise ConsoleError("Arcus 账户索引必须在 0–9 之间")
                if value:
                    values[name] = value
            for name in clear:
                if profile == "default" and os.environ.get(name):
                    raise ConsoleError("此字段由服务环境变量覆盖，需在服务器移除该环境变量")
                values.pop(name, None)
            encoded = "\n".join(f"{name}='{str(value).replace(chr(92), chr(92) * 2).replace(chr(39), chr(92) + chr(39))}'"
                                for name, value in values.items() if value is not None) + "\n"
            self.write_config(path, encoded)
            return self.credential_status(profile)

    def environment(self, profile):
        if profile == "default":
            path = self.profile_path(profile)
            values = {name: os.environ[name] for name in CREDENTIAL_NAMES if name in os.environ}
        elif PROFILE_RE.fullmatch(profile or "") and profile in self.profiles():
            path = self.profile_path(profile)
            values = {}
        else:
            raise ConsoleError("凭据配置不存在，请在服务器 credentials 目录添加 .env 文件")
        if path.exists():
            if path.stat().st_size > 128000:
                raise ConsoleError("凭据文件过大")
            file_values = dotenv_values(path, interpolate=False)
            values = {**{key: value for key, value in file_values.items()
                         if key in CREDENTIAL_NAMES and value is not None}, **values}
        return values

    async def telegram_test(self, profile="default"):
        """Send a harmless test message using the selected credential profile."""
        values = self.environment(profile)
        notifier = TelegramNotifier(token=values.get("TELEGRAM_BOT_TOKEN"),
                                    chat_id=values.get("TELEGRAM_CHAT_ID"))
        if not notifier.enabled:
            raise ConsoleError("请先配置 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID")
        try:
            if not await notifier.send(test_message()):
                raise ConsoleError("Telegram 测试消息发送失败，请检查 Bot Token、Chat ID 和网络")
            return {"ok": True}
        finally:
            await notifier.close()

    def validate(self, payload, task_id, strategy_text=None):
        if not isinstance(payload, dict):
            raise ConsoleError("请求必须是 JSON 对象")
        name = str(payload.get("name", "")).strip()
        symbol = str(payload.get("symbol", "")).strip().upper()
        primary = payload.get("primary", "lighter-rh")
        hedge = payload.get("hedge", "arcus")
        mode = payload.get("mode", "record")
        profile = payload.get("profile", "default")
        if not name or len(name) > 60:
            raise ConsoleError("任务名称需为 1–60 个字符")
        if not SYMBOL_RE.fullmatch(symbol):
            raise ConsoleError("请输入有效币种名称，例如 ETH、BTC 或 SNDK")
        if primary not in PRIMARY_VENUES or hedge not in HEDGE_VENUES:
            raise ConsoleError("请选择已支持的交易所")
        if mode not in ("record", "live"):
            raise ConsoleError("模式必须为采集或实盘")
        strategy_file = payload.get("strategy_file") or None
        text = (strategy_text if strategy_text is not None else self.strategy(strategy_file)) if strategy_file else payload.get("config", "")
        if not isinstance(text, str) or len(text) > 32000:
            raise ConsoleError("YAML 配置长度不能超过 32000 字符")
        raw = self.check_strategy(text)
        if not isinstance(raw, dict):
            raise ConsoleError("请输入完整 YAML 策略配置")
        finite_config(raw)
        for section in ("primary", "entropy", "hedge", "recorder", "logging"):
            if section in raw and not isinstance(raw[section], dict):
                raise ConsoleError(f"{section} 配置必须为映射")
        if "primary" in raw and raw["primary"].get("venue", primary) != primary:
            raise ConsoleError("YAML primary.venue 与页面主腿选择不一致")
        directory = self.directory(task_id)
        recorder = raw.get("recorder") or {}
        if self._legacy_task_output(task_id, symbol, recorder.get("csv"), "minutes.csv"):
            # Let load_config use the YAML default logs/{symbol}/minutes.csv.
            # This only removes the old console-injected path.
            recorder.pop("csv", None)
            raw["recorder"] = recorder
        # Keep recorder.csv exactly as authored; load_config expands market
        # placeholders and supplies the same default as the CLI when omitted.
        logging_config = raw.setdefault("logging", {})
        # YAML owns output locations.  The task directory is only the safe
        # fallback for strategies that omit them.
        logging_config.setdefault("trades_csv", str(directory / "trades.csv"))
        logging_config.setdefault("file", str(directory / "engine.log"))
        logging_config["dashboard"] = False
        rendered = yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)
        values = self.environment(profile)
        with tempfile.TemporaryDirectory(dir=self.data) as temporary:
            config_file = Path(temporary) / "config.yaml"
            config_file.write_text(rendered, encoding="utf-8")
            cfg = load_config(str(config_file), symbol=symbol, primary_venue=primary,
                              hedge_venue=hedge, credential_env=values)
        positive = (cfg.entropy.cap_usd, cfg.hedge.cap_usd, cfg.max_order_notional,
                    cfg.min_order_notional, cfg.settle_timeout_sec, cfg.staleness_sec,
                    cfg.reconcile_sec, cfg.status_interval_sec, cfg.venue_probe_sec)
        if any(value <= 0 for value in positive):
            raise ConsoleError("仓位、下单规模与超时时间必须大于 0")
        if cfg.max_order_notional < cfg.min_order_notional:
            raise ConsoleError("最大下单金额不能小于最小下单金额")
        if min(cfg.entropy.orders_per_min, cfg.hedge.orders_per_min, cfg.max_consecutive_errors) <= 0:
            raise ConsoleError("下单频率与连续错误上限必须大于 0")
        if min(cfg.inventory_scale_bps, cfg.cooldown_sec, cfg.premium_persist_sec,
               cfg.net_tolerance_base, cfg.rate_limit_pause_sec, cfg.http_keepalive_sec) < 0:
            raise ConsoleError("库存、冷却、净敞口与暂停参数不能为负数")
        if not 0 <= cfg.inventory_floor_frac < 1:
            raise ConsoleError("inventory.floor_frac 必须在 [0, 1) 内")
        if not math.isfinite(cfg.entropy.cap_usd + cfg.hedge.cap_usd):
            raise ConsoleError("双腿持仓上限合计超出有效数值范围")
        public = dict(name=name, symbol=symbol, primary=primary, hedge=hedge,
                      mode=mode, profile=profile, config=text, strategy_file=strategy_file,
                      cap_usd=cfg.entropy.cap_usd + cfg.hedge.cap_usd,
                      max_order_usd=cfg.max_order_notional)
        return public, rendered, cfg, values

    async def put(self, payload, task_id=None):
        async with self.mutex():
            if task_id:
                old = self.get(task_id)
                if task_id in self.running or self.instance_locked(task_id):
                    raise ConsoleError("请先停止任务再修改配置")
            else:
                if len(self.tasks) >= 100:
                    raise ConsoleError("最多保存 100 个任务，请删除不用的任务")
                task_id = uuid.uuid4().hex
                old = dict(id=task_id, created_at=time.time(), state="stopped",
                           pid=None, started_at=None, ended_at=None, exit_code=None,
                           manual_paused=False)
            strategy_file = payload.get("strategy_file") or None
            save_strategy = bool(strategy_file and payload.get("save_strategy") is True)
            strategy_text = payload.get("config") if save_strategy else None
            public, rendered, cfg, values = self.validate(payload, task_id,
                                                          strategy_text=strategy_text)
            if save_strategy:
                self.ensure_unused("strategy_file", strategy_file)
                self.write_config(self.strategy_path(strategy_file), public["config"])
            if old.get("started_at"):
                old_cfg = load_config(str(self.task_snapshot_config(old)),
                                      symbol=old["symbol"], primary_venue=old["primary"],
                                      hedge_venue=old["hedge"], credential_env={})
                prior_pair = (old["symbol"], old["primary"], old["hedge"],
                              market_resource(old_cfg.primary), market_resource(old_cfg.hedge))
                next_pair = (public["symbol"], public["primary"], public["hedge"],
                             market_resource(cfg.primary), market_resource(cfg.hedge))
                if prior_pair != next_pair:
                    raise ConsoleError("任务已有运行记录；更换币种或交易路径请新建任务，避免混合历史数据")
            directory = self.directory(task_id)
            directory.mkdir(mode=0o700, exist_ok=True)
            (directory / "config.yaml").write_text(rendered, encoding="utf-8")
            (directory / "status.json").unlink(missing_ok=True)
            (directory / "control.json").unlink(missing_ok=True)
            self.tasks[task_id] = {**old, **public, "updated_at": time.time()}
            self.save()
            return self.view(self.tasks[task_id])

    def view(self, task, include_config=True):
        result = dict(task)
        if not include_config:
            result.pop("config", None)
        status_path = self.directory(task["id"]) / "status.json"
        result["status"] = None
        if status_path.exists():
            try:
                result["status"] = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        result["status_stale"] = (result["status"] is None
                                  or time.time() - result["status"].get("updated_at", 0) > 10)
        result["auto_threshold"] = self._read_auto_status(task["id"])
        return result

    async def start(self, task_id, confirmed=False):
        async with self.mutex():
            if self.closing:
                raise ConsoleError("控制台正在关闭")
            task = self.get(task_id)
            if task_id in self.running:
                raise ConsoleError("任务已启动，请勿重复启动")
            if len(self.running) >= self.max_running:
                raise ConsoleError(f"最多同时运行 {self.max_running} 个任务")
            for previous_id in self.tasks:
                if previous_id not in self.running and self.instance_locked(previous_id):
                    raise ConsoleError("上一进程仍在退出，请等待并核对服务器进程与持仓")
            public, rendered, cfg, credentials = self.validate(task, task_id)
            if task.get("started_at"):
                previous = load_config(str(self.task_snapshot_config(task)),
                                       symbol=task["symbol"], primary_venue=task["primary"],
                                       hedge_venue=task["hedge"], credential_env={})
                if (market_resource(previous.primary), market_resource(previous.hedge)) != (
                        market_resource(cfg.primary), market_resource(cfg.hedge)):
                    raise ConsoleError("策略文件改变了市场路径，请新建任务，避免混合历史数据")
            markets = {market_resource(cfg.entropy), market_resource(cfg.hedge)}
            signers = signer_resources(cfg.entropy) | signer_resources(cfg.hedge) if task["mode"] == "live" else set()
            if task["mode"] == "live":
                if confirmed is not True:
                    raise ConsoleError("实盘启动需要明确确认，程序将发送真实订单")
                missing = missing_credentials(cfg)
                if missing:
                    location = ".env" if task["profile"] == "default" else f"credentials/{task['profile']}.env"
                    route = f"{cfg.primary_venue} → {cfg.hedge_venue}"
                    raise ConsoleError(
                        f"当前路径 {route} 需要的凭据字段未完整配置：{', '.join(missing)}；"
                        f"任务使用 {location}，请检查该文件")
            for other_id, runtime in self.running.items():
                other = self.tasks[other_id]
                if other["symbol"] == task["symbol"] and {other["primary"], other["hedge"]} == {task["primary"], task["hedge"]}:
                    raise ConsoleError(f"同币种交易所组合已运行：{other['name']}")
                if self.configured_path(cfg.recorder_csv) == runtime.get("recorder_path"):
                    raise ConsoleError(f"行情 CSV 路径与运行任务「{other['name']}」相同；请在 YAML 中设置独立 recorder.csv")
                if other["mode"] == task["mode"] == "live":
                    if markets & runtime["markets"]:
                        raise ConsoleError(f"交易市场与实盘任务「{other['name']}」重叠，请勿同时管理同一持仓")
                    if signers & runtime["signers"]:
                        raise ConsoleError(f"与「{other['name']}」共用签名密钥；多进程实盘请使用独立 API 密钥或子账户")
            directory = self.directory(task_id)
            (directory / "config.yaml").write_text(rendered, encoding="utf-8")
            (directory / "status.json").unlink(missing_ok=True)
            (directory / "control.json").unlink(missing_ok=True)
            env = {key: value for key, value in os.environ.items()
                   if key not in CREDENTIAL_NAMES and key != "WEB_PASSWORD"}
            if task["mode"] == "live":
                env.update(credentials)
                env["TELEGRAM_PROFILE"] = task["profile"]
            env["PYTHONUNBUFFERED"] = "1"
            command = [sys.executable, "-u", str(self.root / "main.py"),
                       "--symbol", task["symbol"], "--primary", task["primary"],
                       "--hedge", task["hedge"], "--config", str(directory / "config.yaml"),
                       "--env-file", str(directory / "no-credentials.env"),
                       "--no-dashboard", "--status-file", str(directory / "status.json"),
                       "--parent-pid", str(os.getpid()),
                       "--instance-lock", str(directory / "instance.lock"),
                       "--control-file", str(directory / "control.json")]
            if task["mode"] == "record":
                command.append("--record-only")
            process = await asyncio.create_subprocess_exec(
                *command, cwd=self.root, env=env, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT, start_new_session=True)
            task.update(public)
            task.update(state="running", pid=process.pid, started_at=time.time(), ended_at=None,
                        exit_code=None, manual_paused=False)
            runtime = dict(process=process, markets=markets, signers=signers,
                           recorder_path=self.configured_path(cfg.recorder_csv),
                           grace=cfg.settle_timeout_sec + 20,
                           log_capture=PrivateLogCapture(credentials))
            self.running[task_id] = runtime
            runtime["watcher"] = asyncio.create_task(self.watch(task_id, runtime))
            self.audit(task_id, f"START {task['mode']} pid={process.pid}")
            self.save()
            return self.view(task)

    def audit(self, task_id, message):
        path = self.directory(task_id) / "events.log"
        self.rotate(path)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(self._dated_log_time() + " " + message + "\n")

    def rotate(self, path):
        if path.exists() and path.stat().st_size >= LOG_LIMIT:
            for index in (2, 1):
                source = path.with_name(path.name + f".{index}")
                if source.exists():
                    os.replace(source, path.with_name(path.name + f".{index + 1}"))
            os.replace(path, path.with_name(path.name + ".1"))

    async def watch(self, task_id, runtime):
        process = runtime["process"]
        log_path = self.log_path(self.tasks[task_id])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        capture = runtime["log_capture"]
        try:
            while True:
                chunk = await process.stdout.read(8192)
                if not chunk:
                    break
                filtered = capture.feed(chunk)
                if filtered:
                    self.rotate(log_path)
                    with log_path.open("ab") as handle:
                        handle.write(filtered)
            remaining = capture.finish()
            if remaining:
                self.rotate(log_path)
                with log_path.open("ab") as handle:
                    handle.write(remaining)
            await process.wait()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            task = self.tasks[task_id]
            stopped = task["state"] == "stopping"
            task.update(state="stopped" if stopped or process.returncode == 0 else "failed",
                        pid=None, ended_at=time.time(), exit_code=process.returncode)
            self.running.pop(task_id, None)
            self.audit(task_id, f"EXIT code={process.returncode}")
            self.save()

    async def stop(self, task_id, force=False):
        async with self.mutex():
            task = self.get(task_id)
            runtime = self.running.get(task_id)
            if runtime is None:
                return self.view(task)
            if force and task["state"] != "stopping":
                raise ConsoleError("请先请求正常停止，只有停止中的任务可以强制结束")
            task["state"] = "stopping"
            self.audit(task_id, "KILL requested; verify positions" if force else "STOP requested; positions remain")
            try:
                os.killpg(runtime["process"].pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass
            self.save()
            return self.view(task)

    async def pause(self, task_id, paused):
        async with self.mutex():
            task = self.get(task_id)
            if task_id not in self.running or task["state"] != "running":
                raise ConsoleError("只有运行中的任务可以暂停或恢复")
            if task["mode"] != "live":
                raise ConsoleError("只采集任务没有交易策略，不需要暂停")
            self.write_control(task_id, paused)
            task["manual_paused"] = paused
            self.audit(task_id, "PAUSE requested; current execution will settle" if paused
                       else "RESUME requested; risk gates still apply")
            self.save()
            return self.view(task)

    async def delete(self, task_id):
        async with self.mutex():
            self.get(task_id)
            if task_id in self.running:
                raise ConsoleError("运行中的任务不能删除")
            if self.instance_locked(task_id):
                raise ConsoleError("上一进程仍在退出，暂时不能删除")
            del self.tasks[task_id]
            self.save()

    async def close(self):
        self.closing = True
        try:
            if self.auto_threshold_task is not None:
                self.auto_threshold_task.cancel()
                await asyncio.gather(self.auto_threshold_task, return_exceptions=True)
                self.auto_threshold_task = None
            if self.market_monitor is not None:
                await self.market_monitor.stop()
            for task_id in list(self.running):
                await self.stop(task_id)
            runtimes = list(self.running.values())
            for runtime in runtimes:
                try:
                    await asyncio.wait_for(asyncio.shield(runtime["watcher"]), timeout=runtime["grace"])
                except asyncio.TimeoutError:
                    self.audit(next(task_id for task_id, value in self.running.items() if value is runtime),
                               "shutdown timeout; forced termination; verify positions")
                    try:
                        os.killpg(runtime["process"].pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await runtime["watcher"]
        finally:
            self.lock_file.close()


def create_app(manager: TaskManager, password: str, secure_cookie=False):
    sessions = {}
    failures = {}
    password_digest = hashlib.sha256(password.encode()).digest()

    @web.middleware
    async def security(request, handler):
        try:
            if request.path.startswith("/api/"):
                now = time.time()
                if request.method not in ("GET", "HEAD"):
                    origin = request.headers.get("Origin")
                    if request.headers.get("X-Web-Request") != "1" or (origin and urlsplit(origin).netloc != request.host):
                        raise web.HTTPForbidden(text="请求来源校验失败")
                if request.path not in ("/api/login", "/api/session"):
                    token = request.cookies.get("arb_session", "")
                    if sessions.get(token, 0) <= now:
                        sessions.pop(token, None)
                        raise web.HTTPUnauthorized(text="请先登录")
            response = await handler(request)
        except (ConsoleError, ConfigError, yaml.YAMLError, TypeError, AttributeError) as error:
            response = web.json_response({"error": str(error)}, status=400)
        except web.HTTPException as error:
            response = web.json_response({"error": error.text}, status=error.status)
        response.headers.update({
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "same-origin", "X-Frame-Options": "DENY",
            "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'",
        })
        return response

    app = web.Application(middlewares=[security], client_max_size=64 * 1024)

    async def body(request):
        try:
            value = await request.json()
        except (ValueError, UnicodeDecodeError):
            raise ConsoleError("请求不是有效 JSON")
        if not isinstance(value, dict):
            raise ConsoleError("请求必须是 JSON 对象")
        return value

    async def login(request):
        now = time.time()
        remote = request.remote or "local"
        attempts = failures.setdefault(remote, deque())
        while attempts and attempts[0] < now - 300:
            attempts.popleft()
        if len(attempts) >= 10:
            raise web.HTTPTooManyRequests(text="尝试次数过多，请 5 分钟后重试")
        payload = await body(request)
        supplied = payload.get("password", "")
        if not isinstance(supplied, str) or len(supplied) > 1024:
            raise ConsoleError("密码无效")
        if not hmac.compare_digest(hashlib.sha256(supplied.encode()).digest(), password_digest):
            attempts.append(now)
            raise web.HTTPUnauthorized(text="密码不正确")
        failures.pop(remote, None)
        for token in list(sessions):
            if sessions[token] <= now:
                del sessions[token]
        if len(sessions) >= 100:
            sessions.pop(next(iter(sessions)))
        token = secrets.token_urlsafe(32)
        sessions[token] = now + SESSION_SECONDS
        response = web.json_response({"ok": True})
        response.set_cookie("arb_session", token, httponly=True, samesite="Strict",
                            secure=secure_cookie, max_age=SESSION_SECONDS, path="/")
        return response

    async def session(request):
        return web.json_response({"authenticated": sessions.get(request.cookies.get("arb_session", ""), 0) > time.time()})

    async def logout(request):
        sessions.pop(request.cookies.get("arb_session", ""), None)
        response = web.json_response({"ok": True})
        response.del_cookie("arb_session", path="/")
        return response

    async def meta(request):
        templates = {}
        for name in ("rh-arcus", "entropy-rh"):
            path = STATIC / "templates" / f"{name}.yaml"
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            if "entropy" in raw:
                raw["primary"] = {"venue": "entropy", **raw.pop("entropy")}
            templates[name] = yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)
        return web.json_response(dict(primary_venues=PRIMARY_VENUES, hedge_venues=HEDGE_VENUES,
                                      profiles=manager.profiles(), templates=templates,
                                      strategies=manager.strategies(), credential_fields=EDITABLE_CREDENTIALS,
                                      max_running=manager.max_running))

    async def configuration(request):
        kind, name = request.match_info["kind"], request.match_info["name"]
        if kind not in ("yaml", "env"):
            raise web.HTTPNotFound()
        if request.method == "PUT":
            payload = await body(request)
            result = await (manager.save_strategy(name, payload) if kind == "yaml"
                            else manager.save_credentials(name, payload))
        else:
            if kind == "yaml":
                text = manager.strategy(name)
                raw = manager.check_strategy(text)
                if "entropy" in raw and "primary" not in raw:
                    raw["primary"] = {"venue": "entropy", **raw.pop("entropy")}
                result = dict(name=name, config=text, preview=yaml.safe_dump(
                    raw, allow_unicode=True, sort_keys=False))
            else:
                result = manager.credential_status(name)
        return web.json_response(result)

    async def tasks(request):
        return web.json_response({"tasks": [manager.view(task, include_config=False) for task in manager.tasks.values()],
                                  "server_time": time.time(), "max_running": manager.max_running})

    async def put(request):
        result = await manager.put(await body(request), request.match_info.get("id"))
        return web.json_response(result)

    async def start(request):
        payload = await body(request)
        return web.json_response(await manager.start(request.match_info["id"], payload.get("confirm_live", False)))

    async def stop(request):
        payload = await body(request)
        force = payload.get("force", False)
        if not isinstance(force, bool) or (force and payload.get("confirm_force") is not True):
            raise ConsoleError("强制结束必须明确确认")
        return web.json_response(await manager.stop(request.match_info["id"], force))

    async def pause(request):
        return web.json_response(await manager.pause(request.match_info["id"], True))

    async def resume(request):
        return web.json_response(await manager.pause(request.match_info["id"], False))

    async def threshold(request):
        payload = await body(request) if request.method == "POST" else {}
        apply = payload.get("apply", False)
        confirm_live = payload.get("confirm_live", False)
        if not isinstance(apply, bool) or not isinstance(confirm_live, bool):
            raise ConsoleError("阈值操作参数无效")
        return web.json_response(await manager.manual_threshold(
            request.match_info["id"], apply=apply, confirm_live=confirm_live,
            thresholds=payload.get("thresholds") if apply else None))

    async def telegram_test(request):
        payload = await body(request)
        profile = payload.get("profile", "default")
        if not isinstance(profile, str):
            raise ConsoleError("凭据配置名称无效")
        return web.json_response(await manager.telegram_test(profile))

    async def delete(request):
        await manager.delete(request.match_info["id"])
        return web.json_response({"ok": True})

    async def detail(request):
        task = manager.get(request.match_info["id"])
        directory = manager.directory(task["id"])
        try:
            trades_page = int(request.query.get("trades_page", "1"))
            trades_page_size = int(request.query.get("trades_page_size", "10"))
        except ValueError:
            raise web.HTTPBadRequest(text="成交记录分页参数无效")
        trades = paged_csv(manager.trades_path(task), trades_page, trades_page_size)
        return web.json_response(dict(task=manager.view(task),
                                      minutes=tail_csv(manager.minute_path(task)),
                                      trades=trades["rows"],
                                      trades_total=trades["total"],
                                      trades_page=trades["page"],
                                      trades_page_size=trades["page_size"],
                                      trades_pages=trades["pages"]))

    async def logs(request):
        task = manager.get(request.match_info["id"])
        directory = manager.directory(task["id"])
        return web.json_response(dict(text=tail_text(manager.log_path(task)),
                                      events=tail_text(directory / "events.log", 16000)))

    async def market_candidates(request):
        async with aiohttp.ClientSession() as session:
            return web.json_response(await discover_candidates(session))

    async def market_monitor(request):
        monitor = await manager.monitor_instance()
        return web.json_response(monitor.snapshot())

    async def market_monitor_start(request):
        payload = await body(request)
        symbol = str(payload.get("symbol", "")).strip().upper()
        # A strategy YAML is optional here. Compatibility is determined by
        # public market metadata, never by strategy thresholds.
        strategy_file = payload.get("strategy_file")
        hedge_venue = payload.get("hedge_venue")
        profile = payload.get("profile") or "default"
        candidates = None
        async with aiohttp.ClientSession() as session:
            candidates = await discover_candidates(session)
        all_candidates = payload.get("all") is True
        candidate = next((row for row in candidates["symbols"] if row["symbol"] == symbol), None)
        if not all_candidates:
            if candidate is None:
                raise ConsoleError("该币种没有同时出现在 RH、Arcus、Entropy 的活跃市场列表")
            if not candidate["compatible"]:
                raise ConsoleError("该币种的数量步长或最小下单规格无法验证，不能标记为可自动对冲")
        elif not any(row.get("compatible") for row in candidates["symbols"]):
            raise ConsoleError("没有可同时监控的共同币种")
        rules = await manager.monitor_rules(strategy_file, hedge_venue)
        monitor = await manager.monitor_instance(profile)
        if all_candidates:
            return web.json_response(await monitor.start_all(candidates["symbols"], rules))
        return web.json_response(await monitor.start(candidate, rules))

    async def market_monitor_stop(request):
        monitor = await manager.monitor_instance()
        await monitor.stop()
        return web.json_response(monitor.snapshot())

    async def download(request):
        task = manager.get(request.match_info["id"])
        kind = request.match_info["kind"]
        names = {"minutes": "minutes.csv", "trades": "trades.csv", "logs": "engine.log", "events": "events.log"}
        if kind not in names:
            raise web.HTTPNotFound()
        if kind == "minutes":
            path = manager.minute_path(task)
        elif kind == "trades":
            path = manager.trades_path(task)
        elif kind == "logs":
            path = manager.log_path(task)
        else:
            path = manager.directory(request.match_info["id"]) / names[kind]
        if not path.exists():
            raise web.HTTPNotFound(text="还没有生成文件")
        return web.FileResponse(path, headers={"Content-Disposition": f'attachment; filename="{names[kind]}"'})

    async def index(request):
        return web.FileResponse(STATIC / "index.html")

    async def asset(request):
        name = request.match_info["name"]
        if name not in ("app.js", "style.css"):
            raise web.HTTPNotFound()
        return web.FileResponse(STATIC / name)

    async def cleanup(app):
        await manager.close()

    async def startup(app):
        manager.start_auto_threshold_scheduler()

    app.router.add_get("/", index)
    app.router.add_get("/dex-arbitrage", index)
    app.router.add_get("/monitor", index)
    app.router.add_get("/assets/{name}", asset)
    app.router.add_post("/api/login", login)
    app.router.add_get("/api/session", session)
    app.router.add_post("/api/logout", logout)
    app.router.add_get("/api/meta", meta)
    app.router.add_get("/api/configs/{kind}/{name}", configuration)
    app.router.add_put("/api/configs/{kind}/{name}", configuration)
    app.router.add_get("/api/tasks", tasks)
    app.router.add_post("/api/tasks", put)
    app.router.add_put("/api/tasks/{id}", put)
    app.router.add_delete("/api/tasks/{id}", delete)
    app.router.add_post("/api/tasks/{id}/start", start)
    app.router.add_post("/api/tasks/{id}/stop", stop)
    app.router.add_post("/api/tasks/{id}/pause", pause)
    app.router.add_post("/api/tasks/{id}/resume", resume)
    app.router.add_get("/api/tasks/{id}/threshold", threshold)
    app.router.add_post("/api/tasks/{id}/threshold", threshold)
    app.router.add_post("/api/telegram/test", telegram_test)
    app.router.add_get("/api/tasks/{id}", detail)
    app.router.add_get("/api/tasks/{id}/logs", logs)
    app.router.add_get("/api/tasks/{id}/download/{kind}", download)
    app.router.add_get("/api/market-monitor/candidates", market_candidates)
    app.router.add_get("/api/market-monitor", market_monitor)
    app.router.add_post("/api/market-monitor/start", market_monitor_start)
    app.router.add_post("/api/market-monitor/stop", market_monitor_stop)
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)
    return app
