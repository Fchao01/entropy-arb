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

    premium_bps = (entropy_price / hedge_price - 1) * 10_000

    SELL entropy / BUY hedge  fires when the executable premium
        (entropy bid over hedge ask) >= midline_bps + upper_bps
    BUY entropy / SELL hedge  fires when the executable premium
        (entropy ask under hedge bid) <= midline_bps - lower_bps

    Both hurdles are net of both venues' taker fees, so a full round trip
    nets >= (upper_bps + lower_bps) after fees by construction.
"""
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

HL_API_URL = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"   # official ws — the only HL feed used

HEDGE_VENUES = ("lighter", "lighter-rh", "tradexyz", "arcus", "entropy")
PRIMARY_VENUES = ("entropy", "lighter", "lighter-rh", "tradexyz", "arcus")

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
    close_upper_bps: float
    close_lower_bps: float
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
    latency_buffer_bps: float
    slippage_buffer_bps: float
    max_book_skew_sec: float
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
    primary_venue: str = "entropy"

    @property
    def primary(self) -> VenueConf:
        return self.entropy  # legacy field retained for API/CSV compatibility

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
    "primary": {
        "venue": str,
        "symbol": str,
        "dex": str,
        "taker_fee_bps": float,
        "max_position_usd": float,
        "max_orders_per_min": int,
    },
    "thresholds": {
        "midline_bps": float,
        "upper_bps": float,
        "lower_bps": float,
        "close_upper_bps": float,
        "close_lower_bps": float,
    },
    "entropy": {
        "symbol": str,
        "dex": str,
        "taker_fee_bps": float,
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
        "latency_buffer_bps": float,
        "slippage_buffer_bps": float,
        "max_book_skew_sec": float,
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
                     primary_venue: str = "entropy") -> str:
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
                 separate_lighter_keys: bool = False) -> VenueConf:
    params = raw.get(section) or {}
    is_primary = key == "entropy"

    def env_s(name, fallback=None):
        if is_primary:
            value = _env_s("PRIMARY_" + name)
            if value or separate_lighter_keys:
                return value
        return _env_s(fallback or name)

    def env_i(name):
        value = env_s(name)
        try:
            return int(value) if value is not None else None
        except ValueError as e:
            raise ConfigError(f"{('PRIMARY_' if is_primary else '') + name} must be an integer") from e

    default_fee = 1.0 if venue == "tradexyz" else 0.0
    fee = float(params.get("taker_fee_bps", default_fee))
    if not math.isfinite(fee) or fee < 0:
        raise ConfigError(f"{section}.taker_fee_bps must be finite and >= 0")
    default_budget = 120 if venue in ("entropy", "tradexyz") else 30
    common = dict(key=key, symbol=symbol, fee_bps=fee,
                  cap_usd=float(params.get("max_position_usd", 1000.0)),
                  orders_per_min=int(params.get("max_orders_per_min", default_budget)))
    if venue in ("entropy", "tradexyz"):
        dex = params.get("dex", "io" if venue == "entropy" else "xyz")
        if venue == "tradexyz" and dex != "xyz":
            raise ConfigError(f"{section}.dex must be xyz for tradexyz")
        label = "XYZ" if venue == "tradexyz" else ("ENTROPY" if dex == "io" else "HL:" + (dex or "core"))
        private_fallback = ("HL_PRIVATE_KEY_XYZ" if venue == "tradexyz"
                            and _env_s("HL_PRIVATE_KEY_XYZ") else "HL_PRIVATE_KEY")
        address_fallback = ("HL_ACCOUNT_ADDRESS_XYZ" if venue == "tradexyz"
                            and _env_s("HL_ACCOUNT_ADDRESS_XYZ") else "HL_ACCOUNT_ADDRESS")
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
                                                env_s("ARCUS_API_SIGNING_KEY")))
    return VenueConf(**common, kind="lighter", label="LIGHTER" if venue == "lighter" else "RH",
                     lighter_profile=LIGHTER_PROFILES[venue],
                     lighter_creds=LighterCreds(env_i("LIGHTER_ACCOUNT_INDEX"),
                                                env_i("LIGHTER_API_KEY_INDEX"),
                                                env_s("LIGHTER_API_PRIVATE_KEY")))


# -------------------------------------------------------------------- loading

def load_config(config_file: str = "config.yaml", env_file: str = ".env", *,
                symbol: str, hedge_venue: str, primary_venue: Optional[str] = None) -> Config:
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

    primary_venue = primary_venue or _get(raw, "primary", "venue", "entropy")
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

    thr = raw.get("thresholds") or {}
    for k in ("midline_bps", "upper_bps", "lower_bps"):
        if k not in thr:
            raise ConfigError(f"'thresholds.{k}' is required — derive it from "
                              f"recorded minute data / 必须填写，请用采集的分钟"
                              f"数据计算后填入")
    upper, lower = float(thr["upper_bps"]), float(thr["lower_bps"])
    if upper <= 0 or lower <= 0:
        raise ConfigError("thresholds.upper_bps and lower_bps must be > 0 "
                          "(the round trip nets upper+lower bps after fees)")
    close_upper = float(_get(raw, "thresholds", "close_upper_bps", upper))
    close_lower = float(_get(raw, "thresholds", "close_lower_bps", lower))
    if close_upper <= 0 or close_lower <= 0:
        raise ConfigError("thresholds.close_upper_bps and close_lower_bps "
                          "must be > 0")

    take_fraction = float(_get(raw, "sizing", "take_fraction", 0.5))
    if not 0.0 < take_fraction <= 1.0:
        raise ConfigError("sizing.take_fraction must be in (0, 1] — taking "
                          "more than the profitable depth loses money on the "
                          "tail / 必须在 (0, 1] 之间")

    entropy = _build_venue(raw, primary_section, primary_venue, "entropy", entropy_symbol,
                           separate_lighter_keys=(primary_venue in LIGHTER_PROFILES
                                                  and hedge_venue in LIGHTER_PROFILES))
    hedge = _build_venue(raw, "hedge", hedge_venue, "hedge", hedge_symbol)
    if (primary_venue == hedge_venue
            or (entropy.kind == hedge.kind == "hl" and entropy.hl_dex == hedge.hl_dex)):
        raise ConfigError("primary and hedge resolve to the same venue / 主腿与对冲腿不能是同一个交易所")

    latency_buffer_bps = float(_get(raw, "execution", "latency_buffer_bps", 0.0))
    slippage_buffer_bps = float(_get(raw, "execution", "slippage_buffer_bps", 0.0))
    max_book_skew_sec = float(_get(raw, "execution", "max_book_skew_sec", 0.5))
    leg_slippage_bps = float(_get(raw, "execution", "leg_slippage_bps", 50.0))
    hedge_slippage_bps = float(_get(raw, "execution", "hedge_slippage_bps", 20.0))
    if latency_buffer_bps < 0 or slippage_buffer_bps < 0:
        raise ConfigError("execution latency/slippage buffers must be >= 0")
    if leg_slippage_bps < 0 or hedge_slippage_bps < 0:
        raise ConfigError("execution slippage limits must be >= 0")
    if max_book_skew_sec <= 0:
        raise ConfigError("execution.max_book_skew_sec must be > 0")

    return Config(
        symbol=symbol,
        hedge_venue=hedge_venue,
        primary_venue=primary_venue,
        entropy=entropy,
        hedge=hedge,
        midline_bps=float(thr["midline_bps"]),
        upper_bps=upper,
        lower_bps=lower,
        close_upper_bps=close_upper,
        close_lower_bps=close_lower,
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
        latency_buffer_bps=latency_buffer_bps,
        slippage_buffer_bps=slippage_buffer_bps,
        max_book_skew_sec=max_book_skew_sec,
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
