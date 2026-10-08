"""Config loading: example file, validation, CLI-selected markets.

Run:  python3 -m pytest tests/  (or  python3 tests/test_config.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import ConfigError, load_config  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
EXAMPLE = os.path.join(ROOT, "config.example.yaml")
NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def write_tmp(text: str) -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(text)
    f.close()
    return f.name


MINIMAL = """
thresholds:
  midline_bps: 5.0
  upper_bps: 4.0
  lower_bps: 3.0
"""


def load(yaml_text: str, symbol="SNDK", hedge="lighter-rh"):
    return load_config(write_tmp(yaml_text), NO_ENV,
                       symbol=symbol, hedge_venue=hedge)


def test_example_config_loads():
    cfg = load_config(EXAMPLE, NO_ENV,
                      symbol="SNDK", hedge_venue="lighter-rh")
    assert cfg.symbol == "SNDK"
    assert cfg.entropy.kind == "hl" and cfg.entropy.hl_dex == "io"
    assert cfg.hedge_venue == "lighter-rh"
    assert cfg.hedge.kind == "lighter"
    assert cfg.hedge.lighter_profile.chain_id == 466324
    assert cfg.entropy.symbol == "SNDK" and cfg.hedge.symbol == "SNDK"
    assert cfg.recorder_enabled
    assert cfg.recorder_csv == "logs/SNDK/minutes.csv"
    assert cfg.trades_csv == "logs/SNDK/trades.csv"
    assert cfg.log_file == "logs/SNDK/engine.log"
    assert cfg.dashboard and cfg.log_file


def test_minimal_defaults():
    cfg = load(MINIMAL, hedge="lighter")
    assert cfg.midline_bps == 5.0 and cfg.upper_bps == 4.0 and cfg.lower_bps == 3.0
    assert cfg.close_upper_bps == 4.0 and cfg.close_lower_bps == 3.0
    assert cfg.hedge.label == "LIGHTER"
    assert cfg.hedge.lighter_profile.chain_id == 304
    assert cfg.take_fraction == 0.5          # defaults kick in
    assert cfg.recorder_enabled is True
    assert cfg.entropy.symbol == cfg.hedge.symbol == "SNDK"


def test_independent_market_symbols_keep_pair_log_paths():
    cfg = load(MINIMAL + """
entropy:
  symbol: ANTH
hedge:
  symbol: ANTHROPIC
""", symbol="ANTH")
    assert cfg.entropy.symbol == "ANTH"
    assert cfg.hedge.symbol == "ANTHROPIC"
    assert cfg.symbol == "ANTH"
    assert cfg.recorder_csv == "logs/ANTH/minutes.csv"
    assert cfg.trades_csv == "logs/ANTH/trades.csv"
    assert cfg.log_file == "logs/ANTH/engine.log"


def test_one_market_symbol_override():
    cfg = load(MINIMAL + '\nhedge:\n  symbol: " ANTHROPIC "\n',
               symbol="ANTH", hedge="tradexyz")
    assert cfg.entropy.symbol == "ANTH"
    assert cfg.hedge.symbol == "ANTHROPIC"


def test_empty_market_symbol_rejected():
    for section in ("entropy", "hedge"):
        expect_error(MINIMAL + f'\n{section}:\n  symbol: " "\n',
                     f"{section}.symbol")


def test_execution_buffers_and_close_bands():
    cfg = load("""
thresholds:
  midline_bps: 5.0
  upper_bps: 8.0
  lower_bps: 7.0
  close_upper_bps: 2.0
  close_lower_bps: 3.0
execution:
  latency_buffer_bps: 1.5
  slippage_buffer_bps: 2.5
  max_book_skew_sec: 0.2
""")
    assert (cfg.close_upper_bps, cfg.close_lower_bps) == (2.0, 3.0)
    assert (cfg.latency_buffer_bps, cfg.slippage_buffer_bps) == (1.5, 2.5)
    assert cfg.max_book_skew_sec == 0.2


def test_tradexyz_hedge():
    cfg = load(MINIMAL, hedge="tradexyz")
    assert cfg.hedge.kind == "hl" and cfg.hedge.hl_dex == "xyz"
    assert cfg.hedge.label == "XYZ"


def test_arcus_config_and_network(monkeypatch):
    monkeypatch.delenv("ARCUS_ACCOUNT_ADDRESS", raising=False)
    monkeypatch.delenv("ARCUS_API_SIGNING_KEY", raising=False)
    monkeypatch.delenv("ARCUS_ACCOUNT_INDEX", raising=False)
    text = MINIMAL + '\nhedge:\n  symbol: ETH-USD\n  taker_fee_bps: 1.0\n'
    cfg = load(text, symbol="ETH", hedge="arcus")
    assert cfg.hedge.kind == "arcus" and cfg.hedge.label == "ARCUS"
    assert cfg.hedge.symbol == "ETH-USD" and cfg.hedge.arcus_network == "mainnet"
    assert cfg.hedge.arcus_creds.account_index == 0
    assert cfg.creds_complete is False
    cfg = load(text + '\narcus:\n  network: testnet\n', symbol="ETH", hedge="arcus")
    assert cfg.hedge.arcus_network == "testnet"
    expect_error(text + '\narcus:\n  network: bad\n', "arcus.network", hedge="arcus")
    expect_error(MINIMAL, "hedge.taker_fee_bps", hedge="arcus")
    for fee in ("-1", ".nan", ".inf"):
        expect_error(MINIMAL + f'\nhedge:\n  taker_fee_bps: {fee}\n',
                     "hedge.taker_fee_bps", hedge="arcus")
    monkeypatch.setenv("ARCUS_ACCOUNT_INDEX", "10")
    expect_error(text, "ARCUS_ACCOUNT_INDEX", hedge="arcus")


def test_hyperliquid_core_dex_is_allowed():
    cfg = load(MINIMAL + '\nentropy:\n  dex: ""\n')
    assert cfg.entropy.hl_dex == ""


def expect_error(yaml_text: str, needle: str, **kw):
    try:
        load(yaml_text, **kw)
    except ConfigError as e:
        assert needle in str(e), f"{needle!r} not in {e}"
        return
    raise AssertionError(f"expected ConfigError containing {needle!r}")


def test_unknown_key_rejected():
    expect_error(MINIMAL + "\nthresholdz:\n  x: 1\n",
                 "unknown config key 'thresholdz'")
    expect_error(MINIMAL + "\nsizing:\n  take_fractionn: 0.5\n",
                 "sizing.take_fractionn")


def test_markets_no_longer_config_keys():
    # Top-level symbol / hedge_venue remain CLI-only. Per-venue symbols
    # are configured under entropy / hedge.
    expect_error("symbol: SNDK\n" + MINIMAL, "unknown config key 'symbol'")
    expect_error("hedge_venue: tradexyz\n" + MINIMAL,
                 "unknown config key 'hedge_venue'")


def test_bad_cli_markets():
    expect_error(MINIMAL, "--hedge", hedge="binance")
    expect_error(MINIMAL, "--symbol", symbol="")


def test_missing_thresholds():
    expect_error("recorder:\n  enabled: true\n", "thresholds.")


def test_nonpositive_band():
    expect_error("thresholds:\n"
                 "  midline_bps: 5\n  upper_bps: 0\n  lower_bps: 3\n",
                 "must be > 0")


def test_negative_slippage_is_rejected():
    expect_error(MINIMAL + "\nexecution:\n  leg_slippage_bps: -1\n",
                 "slippage limits must be >= 0")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
