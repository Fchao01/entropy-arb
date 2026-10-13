"""Configuration: strategy from a YAML file, credentials from .env, market
selection (pair symbol + hedge venue) from the command line, with optional
per-venue symbol overrides and configurable primary venue in YAML.

The split is deliberate: config.yaml IS the strategy (thresholds, sizing,
risk) and is safe to share/commit as an example; .env holds only secrets;
the pair is stated explicitly on every start (--symbol, --hedge); venue-specific
names can be set with entropy.symbol / hedge.symbol. Every YAML key is
validated against the schema below, so a typo
is an error rather than a setting that silently does nothing.

Threshold model (fixed numbers the user derives from recorded minute data):

    premium_bps = (primary_price / hedge_price - 1) * 10_000

    SELL primary / BUY hedge  fires when the executable premium
        (entropy bid over hedge ask) >= midline_bps + upper_bps
    BUY primary / SELL hedge  fires when the executable premium
        (entropy ask under hedge bid) <= midline_bps - lower_bps

    Both hurdles are net of both venues' taker fees, so a full round trip
    nets >= (upper_bps + lower_bps) after fees by construction.
"""
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

import yaml
from dotenv import load_dotenv

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz", "arcus", "entropy")
PRIMARY_VENUES = ("entropy", "lighter", "lighter-rh", "tradexyz", "arcus")
DEFAULT_PRIMARY_VENUE = "lighter-rh"

ARCUS_ENDPOINTS = {
    "mainnet": ("https://api.arcus.xyz", "wss://api.arcus.xyz/v1/ws"),
    "testnet": ("https://api.testnet.arcus.xyz", "wss://api.testnet.arcus.xyz/v1/ws"),
}


@dataclass(frozen=True)
class LighterProfile:
    name: str
    api_url: str
    ws_url: str
    chain_id: int


# Endpoint profiles for the two supported zkLighter deployments (these match
# lighter-python's lighter.endpoint_profiles, duplicated here so --record-only
# data collection works without the SDK installed).
LIGHTER_PROFILES: Dict[str, LighterProfile] = {
    "lighter": LighterProfile(
        "mainnet", "https://mainnet.zklighter.elliot.ai",
        "wss://mainnet.zklighter.elliot.ai/stream", 304),
    "lighter-rh": LighterProfile(
        "robinhood", "https://api.rh.lighter.xyz",
        "wss://api.rh.lighter.xyz/stream", 466324),
}


@dataclass
class LighterCreds:
    account_index: Optional[int]
    api_key_index: Optional[int]
    api_private_key: Optional[str]
    env_prefix: str = ""

    @property
    def complete(self) -> bool:
        return (self.account_index is not None and self.api_key_index is not None
                and bool(self.api_private_key))


@dataclass
class HLCreds:
    private_key: Optional[str]
    account_address: Optional[str]

    @property
    def complete(self) -> bool:
        return bool(self.private_key)


@dataclass
class ArcusCreds:
    account_address: Optional[str]
    account_index: int
    signing_key: Optional[str]
    env_prefix: str = ""

    @property
    def complete(self) -> bool:
        return bool(self.account_address and self.signing_key)


@dataclass
class VenueConf:
    key: str                  # "entropy" | "hedge"
    kind: str                 # "hl" | "lighter" | "arcus"
    label: str                # human name for logs, e.g. "ENTROPY", "RH"
    symbol: str
    fee_bps: float
    cap_usd: float
    orders_per_min: int
    # hl
    hl_dex: str = ""
    hl_creds: Optional[HLCreds] = None
    # lighter
    lighter_profile: Optional[LighterProfile] = None
    lighter_creds: Optional[LighterCreds] = None
    # Arcus perpetuals (Ed25519 API Signing Key, not an EVM private key)
    arcus_network: str = "mainnet"
    arcus_creds: Optional[ArcusCreds] = None
    maker_fee_bps: float = 0.0


@dataclass
class Config:
    symbol: str
    hedge_venue: str
    entropy: VenueConf
    hedge: VenueConf
    # thresholds (the whole signal)
    midline_bps: float
    upper_bps: float
    lower_bps: float
    # sizing
    take_fraction: float
    max_order_notional: float
    min_order_notional: float
    # inventory ladder
    inventory_scale_bps: float
    inventory_floor_frac: float
    # execution
    premium_persist_sec: float
    cooldown_sec: float
    settle_timeout_sec: float
    leg_slippage_bps: float
    hedge_slippage_bps: float
    net_tolerance_base: float
    max_consecutive_errors: int
    rate_limit_pause_sec: float
    staleness_sec: float
    reconcile_sec: float
    venue_probe_sec: float
    http_keepalive_sec: float
    # recorder
    recorder_enabled: bool
    recorder_csv: str
    # logging
    log_level: str
    status_interval_sec: float
    trades_csv: str
    dashboard: bool
    log_file: str
    # runtime
    hl_api_url: str = HL_API_URL
    hl_ws_url: str = HL_WS_URL
    primary_venue: str = DEFAULT_PRIMARY_VENUE
    primary_order_type: str = "taker"
    maker_timeout_sec: float = 5.0
    strategy_mode: str = "threshold"
    bps_levels: str = "rolling"
    bps_direction: str = "long_primary"
    bps_entry: float = -9.0
    bps_exit: float = -6.0
    bps_window_minutes: int = 120
    bps_min_coverage_pct: float = 80.0  # Legacy YAML compatibility; no longer gates signals.
    bps_min_valid_minutes: int = 30
    bps_entry_percentile: float = 10.0
    bps_exit_percentile: float = 75.0
    bps_max_entries: int = 1
    bps_add_interval_sec: float = 60.0
    bps_max_position_usd: float = 1000.0
    bps_cost_buffer_bps: float = 0.0

    @property
    def primary(self) -> VenueConf:
        return self.entropy  # legacy field retained for API/CSV compatibility

    @property
    def primary_fee_bps(self) -> float:
        return (self.primary.maker_fee_bps if self.primary_order_type == "maker"
                else self.primary.fee_bps)

    @property
    def shutdown_grace_sec(self) -> float:
        if self.primary_order_type == "maker":
            return self.maker_timeout_sec + 8 * self.settle_timeout_sec + 20
        return self.settle_timeout_sec + 2

    @property
    def creds_complete(self) -> bool:
        for v in (self.entropy, self.hedge):
            if v.kind == "hl" and not (v.hl_creds and v.hl_creds.complete):
                return False
            if v.kind == "lighter" and not (v.lighter_creds
                                            and v.lighter_creds.complete):
                return False
            if v.kind == "arcus" and not (v.arcus_creds and v.arcus_creds.complete):
                return False
        return True


# ----------------------------------------------------------------- YAML layer

# Schema: nested dict of key -> type (or nested dict). Unknown keys are errors.
_SCHEMA: Dict[str, Any] = {
    "strategy": {"mode": str},
    "bps": {
        "levels": str, "direction": str,
        "entry_bps": float, "exit_bps": float,
        "window_minutes": int, "min_coverage_pct": float, "min_valid_minutes": int,
        "entry_percentile": float, "exit_percentile": float,
        "max_entries": int, "add_interval_sec": float,
        "max_position_usd": float, "cost_buffer_bps": float,
    },
    "primary": {
        "venue": str,
        "symbol": str,
        "dex": str,
        "taker_fee_bps": float,
        "maker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "thresholds": {
        "midline_bps": float,
        "upper_bps": float,
        "lower_bps": float,
    },
    "entropy": {
        "symbol": str,
        "dex": str,
        "taker_fee_bps": float,
        "maker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "hedge": {
        "symbol": str,
        "dex": str,
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "arcus": {
        "network": str,
    },
    "sizing": {
        "take_fraction": float,
        "max_order_notional_usd": float,
        "min_order_notional_usd": float,
    },
    "inventory": {
        "scale_bps": float,
        "floor_frac": float,
    },
    "execution": {
        "primary_order_type": str,
        "maker_timeout_sec": float,
        "premium_persist_sec": float,
        "cooldown_sec": float,
        "settle_timeout_sec": float,
        "leg_slippage_bps": float,
        "hedge_slippage_bps": float,
        "net_tolerance_base": float,
        "max_consecutive_errors": int,
        "rate_limit_pause_sec": float,
        "staleness_sec": float,
        "reconcile_sec": float,
        "venue_probe_sec": float,
        "http_keepalive_sec": float,
    },
    "recorder": {
        "enabled": bool,
        "csv": str,
    },
    "logging": {
        "level": str,
        "status_interval_sec": float,
        "trades_csv": str,
        "dashboard": bool,
        "file": str,
    },
}


class ConfigError(ValueError):
    pass


def _validate(node: Any, schema: Dict[str, Any], path: str = "") -> None:
    if not isinstance(node, dict):
        raise ConfigError(f"'{path or '<root>'}' must be a mapping")
    for key, val in node.items():
        here = f"{path}.{key}" if path else str(key)
        if key not in schema:
            raise ConfigError(f"unknown config key '{here}' "
                              f"(valid: {', '.join(sorted(schema))})")
        want = schema[key]
        if isinstance(want, dict):
            _validate(val, want, here)
        elif want is float:
            if not isinstance(val, (int, float)) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be a number, got {val!r}")
        elif want is int:
            if not isinstance(val, int) or isinstance(val, bool):
                raise ConfigError(f"'{here}' must be an integer, got {val!r}")
        elif want is bool:
            if not isinstance(val, bool):
                raise ConfigError(f"'{here}' must be true/false, got {val!r}")
        elif want is str:
            if not isinstance(val, str):
                raise ConfigError(f"'{here}' must be a string, got {val!r}")


def _get(d: dict, section: str, key: str, default):
    return (d.get(section) or {}).get(key, default)


def _path_for_market(template: str, symbol: str, hedge_venue: str,
                     primary_venue: str = DEFAULT_PRIMARY_VENUE) -> str:
    """Expand per-market output paths while allowing fixed custom paths."""
    try:
        return template.format(symbol=symbol, hedge=hedge_venue, primary=primary_venue)
    except (KeyError, ValueError) as e:
        raise ConfigError(
            f"invalid output path template {template!r}: {e}; use only "
            "{symbol}, {primary} and {hedge} / 路径模板只能使用 {symbol}、{primary} 和 {hedge}")


# ------------------------------------------------------------------ env layer

def _env_s(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if v not in (None, "") else None


def _build_venue(raw: dict, section: str, venue: str, key: str, symbol: str,
                 separate_lighter_keys: bool = False,
                 credential_env: Optional[Mapping[str, str]] = None) -> VenueConf:
    params = raw.get(section) or {}
    is_primary = key == "entropy"

    def get_env(name):
        if credential_env is not None:
            return credential_env.get(name) or None
        return _env_s(name)

    lighter_prefix = None
    arcus_prefix = None
    if venue == "arcus":
        # Arcus is a single venue in a pair. Its historical ARCUS_* group
        # stays authoritative when roles swap; a legacy PRIMARY_* group is
        # used only if the base group is entirely absent. Never mix fields.
        names = ("ARCUS_ACCOUNT_ADDRESS", "ARCUS_ACCOUNT_INDEX", "ARCUS_API_SIGNING_KEY")
        arcus_prefix = "" if any(get_env(name) for name in names) else (
            "PRIMARY_" if any(get_env("PRIMARY_" + name) for name in names) else "")
    if venue in LIGHTER_PROFILES:
        if separate_lighter_keys:
            lighter_prefix = "PRIMARY_" if is_primary else ""
        else:
            # A single Lighter/RH venue keeps its account when the trading
            # legs are reversed. Select the entire group, never combine a
            # primary private key with a base account or API key index.
            lighter_names = ("LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX",
                             "LIGHTER_API_PRIVATE_KEY")
            lighter_prefix = "PRIMARY_" if any(
                get_env("PRIMARY_" + name) for name in lighter_names) else ""

    def env_s(name, fallback=None):
        if arcus_prefix is not None:
            return get_env(arcus_prefix + name)
        if lighter_prefix is not None:
            return get_env(lighter_prefix + name)
        if is_primary:
            value = get_env("PRIMARY_" + name)
            if value or separate_lighter_keys:
                return value
        return get_env(fallback or name)

    def env_i(name):
        value = env_s(name)
        try:
            return int(value) if value is not None else None
        except ValueError as e:
            prefix = (arcus_prefix if arcus_prefix is not None else
                      lighter_prefix if lighter_prefix is not None else
                      "PRIMARY_" if is_primary else "")
            raise ConfigError(f"{prefix + name} must be an integer") from e

    default_fee = 1.0 if venue == "tradexyz" else 0.0
    fee = float(params.get("taker_fee_bps", default_fee))
    if not math.isfinite(fee) or fee < 0:
        raise ConfigError(f"{section}.taker_fee_bps must be finite and >= 0")
    maker_fee = float(params.get("maker_fee_bps", fee))
    if not math.isfinite(maker_fee) or maker_fee < 0:
        raise ConfigError(f"{section}.maker_fee_bps must be finite and >= 0")
    default_budget = 120 if venue in ("entropy", "tradexyz") else 30
    common = dict(key=key, symbol=symbol, fee_bps=fee,
                  maker_fee_bps=maker_fee,
                  cap_usd=float(params.get("max_position_usd", 1000.0)),
                  orders_per_min=int(params.get("max_orders_per_min", default_budget)))
    if venue in ("entropy", "tradexyz"):
        dex = params.get("dex", "io" if venue == "entropy" else "xyz")
        if venue == "tradexyz" and dex != "xyz":
            raise ConfigError(f"{section}.dex must be xyz for tradexyz")
        label = "XYZ" if venue == "tradexyz" else ("ENTROPY" if dex == "io" else "HL:" + (dex or "core"))
        private_fallback = ("HL_PRIVATE_KEY_XYZ" if venue == "tradexyz"
                            and get_env("HL_PRIVATE_KEY_XYZ") else "HL_PRIVATE_KEY")
        address_fallback = ("HL_ACCOUNT_ADDRESS_XYZ" if venue == "tradexyz"
                            and get_env("HL_ACCOUNT_ADDRESS_XYZ") else "HL_ACCOUNT_ADDRESS")
        return VenueConf(**common, kind="hl", label=label, hl_dex=dex,
                         hl_creds=HLCreds(env_s("HL_PRIVATE_KEY", private_fallback),
                                          env_s("HL_ACCOUNT_ADDRESS", address_fallback)))
    if "dex" in params:
        raise ConfigError(f"{section}.dex applies only to Hyperliquid venues")
    if venue == "arcus":
        network = _get(raw, "arcus", "network", "mainnet")
        if network not in ARCUS_ENDPOINTS:
            raise ConfigError("arcus.network must be mainnet or testnet")
        if "taker_fee_bps" not in params:
            raise ConfigError(f"{section}.taker_fee_bps is required for Arcus; set the verified account fee")
        index = env_i("ARCUS_ACCOUNT_INDEX")
        index = 0 if index is None else index
        if not 0 <= index <= 9:
            raise ConfigError("ARCUS_ACCOUNT_INDEX must be in [0, 9]")
        return VenueConf(**common, kind="arcus", label="ARCUS", arcus_network=network,
                         arcus_creds=ArcusCreds(env_s("ARCUS_ACCOUNT_ADDRESS"), index,
                                                env_s("ARCUS_API_SIGNING_KEY"), arcus_prefix))
    return VenueConf(**common, kind="lighter", label="LIGHTER" if venue == "lighter" else "RH",
                     lighter_profile=LIGHTER_PROFILES[venue],
                     lighter_creds=LighterCreds(env_i("LIGHTER_ACCOUNT_INDEX"),
                                                env_i("LIGHTER_API_KEY_INDEX"),
                                                env_s("LIGHTER_API_PRIVATE_KEY"),
                                                env_prefix=lighter_prefix))


# -------------------------------------------------------------------- loading

def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str, primary_venue: Optional[str] = None,
                credential_env: Optional[Mapping[str, str]] = None) -> Config:
    if credential_env is None:
        load_dotenv(env_file)
    try:
        with open(config_file) as fh:
            raw = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigError(
            f"config file '{config_file}' not found — copy config.example.yaml "
            f"to config.yaml and edit it / 未找到配置文件，请先复制 "
            f"config.example.yaml 为 config.yaml 并修改")
    _validate(raw, _SCHEMA)

    symbol = (symbol or "").strip()
    if not symbol:
        raise ConfigError("--symbol is required, e.g. --symbol SNDK / "
                          "必须用 --symbol 指定交易品种")
    if hedge_venue not in HEDGE_VENUES:
        raise ConfigError(
            f"--hedge must be one of {list(HEDGE_VENUES)}, got "
            f"{hedge_venue!r} / --hedge 必须是 {list(HEDGE_VENUES)} 之一")

    legacy_entropy_config = "entropy" in raw and "primary" not in raw
    primary_venue = (primary_venue
                     or _get(raw, "primary", "venue",
                             "entropy" if legacy_entropy_config else DEFAULT_PRIMARY_VENUE))
    if primary_venue not in PRIMARY_VENUES:
        raise ConfigError(f"primary.venue / --primary must be one of {list(PRIMARY_VENUES)}")
    if "primary" in raw and "entropy" in raw:
        raise ConfigError("use primary or legacy entropy, not both / 请将 entropy 段替换为 primary，避免配置冲突")
    if primary_venue != "entropy" and "entropy" in raw:
        raise ConfigError("non-Entropy primary requires a primary section / 更换主腿后请将 entropy 段替换为 primary")
    primary_section = "primary" if "primary" in raw else "entropy"
    entropy_symbol = _get(raw, primary_section, "symbol", symbol).strip()
    hedge_symbol = _get(raw, "hedge", "symbol", symbol).strip()
    for section, market_symbol in ((primary_section, entropy_symbol),
                                   ("hedge", hedge_symbol)):
        if not market_symbol:
            raise ConfigError(f"'{section}.symbol' must not be empty / "
                              "交易所币种名称不能为空")

    strategy_mode = _get(raw, "strategy", "mode", "threshold")
    if strategy_mode not in ("threshold", "bps"):
        raise ConfigError("strategy.mode must be threshold or bps")
    bps = dict(raw.get("bps") or {})
    bps_defaults = dict(levels="rolling", direction="long_primary", entry_bps=-9.0,
                        exit_bps=-6.0, window_minutes=120, min_coverage_pct=80.0, min_valid_minutes=30, entry_percentile=10.0,
                        exit_percentile=75.0, max_entries=1, add_interval_sec=60.0,
                        max_position_usd=1000.0, cost_buffer_bps=0.0)
    bps = {**bps_defaults, **bps}
    if bps["levels"] not in ("manual", "rolling") or bps["direction"] not in ("long_primary", "short_primary"):
        raise ConfigError("bps levels must be manual/rolling; direction must be long_primary/short_primary")
    if any(isinstance(v, (int, float)) and not math.isfinite(v) for v in bps.values()):
        raise ConfigError("bps parameters must be finite")
    if not 2 <= bps["window_minutes"] <= 10080 or bps["max_entries"] < 1:
        raise ConfigError("bps.window_minutes must be in [2, 10080]; max_entries must be >= 1")
    if not 0 < bps["min_coverage_pct"] <= 100:
        raise ConfigError("bps.min_coverage_pct must be in (0, 100] / 最低数据覆盖率必须大于 0 且不超过 100%")
    if not 2 <= bps["min_valid_minutes"] <= 10080:
        raise ConfigError("bps.min_valid_minutes must be in [2, 10080] / 最少有效分钟数必须在 2–10080 之间")
    if bps["add_interval_sec"] < 0 or bps["cost_buffer_bps"] < 0 or bps["max_position_usd"] <= 0:
        raise ConfigError("bps interval/buffer must be >= 0; position cap must be > 0")
    for name in ("entry_percentile", "exit_percentile"):
        if not 0 <= bps[name] <= 100:
            raise ConfigError("bps percentiles must be in [0, 100]")
    long = bps["direction"] == "long_primary"
    entry, exit = (bps["entry_bps"], bps["exit_bps"]) if bps["levels"] == "manual" else (bps["entry_percentile"], bps["exit_percentile"])
    if (long and entry >= exit) or (not long and entry <= exit):
        raise ConfigError("bps long entry must be below exit; short entry must be above exit / 开平仓点位顺序不正确")
    thr = raw.get("thresholds") or {}
    if strategy_mode == "bps":
        thr = {"midline_bps": 0, "upper_bps": 4, "lower_bps": 4, **thr}
    for k in ("midline_bps", "upper_bps", "lower_bps"):
        if k not in thr:
            raise ConfigError(f"'thresholds.{k}' is required — derive it from "
                              f"recorded minute data / 必须填写，请用采集的分钟"
                              f"数据计算后填入")
    midline = float(thr["midline_bps"])
    upper, lower = float(thr["upper_bps"]), float(thr["lower_bps"])
    if not all(math.isfinite(value) for value in (midline, upper, lower)):
        raise ConfigError("threshold values must be finite numbers")

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail / 必须在 (0, 1] 之间")

    entropy = _build_venue(raw, primary_section, primary_venue, "entropy", entropy_symbol,
                           separate_lighter_keys=(primary_venue in LIGHTER_PROFILES
                                                  and hedge_venue in LIGHTER_PROFILES),
                           credential_env=credential_env)
    hedge = _build_venue(raw, "hedge", hedge_venue, "hedge", hedge_symbol,
                         separate_lighter_keys=(primary_venue in LIGHTER_PROFILES
                                                and hedge_venue in LIGHTER_PROFILES),
                         credential_env=credential_env)
    if (primary_venue == hedge_venue
            or (entropy.kind == hedge.kind == "hl" and entropy.hl_dex == hedge.hl_dex)):
        raise ConfigError("primary and hedge resolve to the same venue / 主腿与对冲腿不能是同一个交易所")

    leg_slippage_bps = float(_get(raw, "execution", "leg_slippage_bps", 50.0))
    hedge_slippage_bps = float(_get(raw, "execution", "hedge_slippage_bps", 20.0))
    if leg_slippage_bps < 0 or hedge_slippage_bps < 0:
        raise ConfigError("execution slippage limits must be >= 0")
    if strategy_mode == "bps" and not (math.isfinite(leg_slippage_bps + hedge_slippage_bps)
                                      and max(leg_slippage_bps, hedge_slippage_bps) < 10000):
        raise ConfigError("bps strategy slippage budgets must be finite and below 10000 bps")
    primary_order_type = _get(raw, "execution", "primary_order_type", "taker")
    if primary_order_type not in ("taker", "maker"):
        raise ConfigError("execution.primary_order_type must be taker or maker / 主腿成交方式必须为 taker 或 maker")
    maker_timeout_sec = float(_get(raw, "execution", "maker_timeout_sec", 5.0))
    if not math.isfinite(maker_timeout_sec) or not 0 < maker_timeout_sec <= 60:
        raise ConfigError("execution.maker_timeout_sec must be in (0, 60] / 挂单等待时间必须大于 0 且不超过 60 秒")

    return Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        primary_venue=primary_venue,
        primary_order_type=primary_order_type,
        maker_timeout_sec=maker_timeout_sec,
        strategy_mode=strategy_mode,
        bps_levels=bps["levels"], bps_direction=bps["direction"],
        bps_entry=float(bps["entry_bps"]), bps_exit=float(bps["exit_bps"]),
        bps_window_minutes=bps["window_minutes"],
        bps_min_coverage_pct=float(bps["min_coverage_pct"]),
        bps_min_valid_minutes=int(bps["min_valid_minutes"]),
        bps_entry_percentile=float(bps["entry_percentile"]),
        bps_exit_percentile=float(bps["exit_percentile"]),
        bps_max_entries=bps["max_entries"], bps_add_interval_sec=float(bps["add_interval_sec"]),
        bps_max_position_usd=float(bps["max_position_usd"]),
        bps_cost_buffer_bps=float(bps["cost_buffer_bps"]),
        entropy=entropy,
        hedge=hedge,
        midline_bps=midline,
        upper_bps=upper,
        lower_bps=lower,
        take_fraction=take_fraction,
        max_order_notional=float(_get(raw, "sizing", "max_order_notional_usd", 500.0)),
        min_order_notional=float(_get(raw, "sizing", "min_order_notional_usd", 10.0)),
        inventory_scale_bps=float(_get(raw, "inventory", "scale_bps", 10.0)),
        inventory_floor_frac=float(_get(raw, "inventory", "floor_frac", 0.5)),
        premium_persist_sec=float(_get(raw, "execution", "premium_persist_sec", 0.3)),
        cooldown_sec=float(_get(raw, "execution", "cooldown_sec", 0.0)),
        settle_timeout_sec=float(_get(raw, "execution", "settle_timeout_sec", 5.0)),
        leg_slippage_bps=leg_slippage_bps,
        hedge_slippage_bps=hedge_slippage_bps,
        net_tolerance_base=float(_get(raw, "execution", "net_tolerance_base", 0.001)),
        max_consecutive_errors=int(_get(raw, "execution", "max_consecutive_errors", 3)),
        rate_limit_pause_sec=float(_get(raw, "execution", "rate_limit_pause_sec", 10.0)),
        staleness_sec=float(_get(raw, "execution", "staleness_sec", 10.0)),
        reconcile_sec=float(_get(raw, "execution", "reconcile_sec", 15.0)),
        venue_probe_sec=float(_get(raw, "execution", "venue_probe_sec", 30.0)),
        http_keepalive_sec=float(_get(raw, "execution", "http_keepalive_sec", 10.0)),
        recorder_enabled=bool(_get(raw, "recorder", "enabled", True)),
        recorder_csv=_path_for_market(
            _get(raw, "recorder", "csv", "logs/{symbol}/minutes.csv"),
            symbol, hedge_venue, primary_venue),
        log_level=str(_get(raw, "logging", "level", "INFO")).upper(),
        status_interval_sec=float(_get(raw, "logging", "status_interval_sec", 30.0)),
        trades_csv=_path_for_market(
            _get(raw, "logging", "trades_csv", "logs/{symbol}/trades.csv"),
            symbol, hedge_venue, primary_venue),
        dashboard=bool(_get(raw, "logging", "dashboard", True)),
        log_file=_path_for_market(
            _get(raw, "logging", "file", "logs/{symbol}/engine.log"),
            symbol, hedge_venue, primary_venue),
    )
