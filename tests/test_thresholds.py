import csv
from datetime import datetime

from entropy_arb.thresholds import daily_window, load_window_rows, prune_csv, suggest


def test_daily_window_is_previous_to_current_shanghai_0800():
    window = daily_window(datetime.fromisoformat("2026-10-10T08:00:00+08:00"))
    assert window.start.isoformat() == "2026-10-09T08:00:00+08:00"
    assert window.end.isoformat() == "2026-10-10T08:00:00+08:00"


def test_suggest_and_prune_use_the_same_window(tmp_path):
    window = daily_window(datetime.fromisoformat("2026-10-10T08:00:00+08:00"))
    path = tmp_path / "minutes.csv"
    fields = ["minute_ts", "premium_close_bps", "sell_edge_max_bps",
              "buy_edge_max_bps", "samples"]
    rows = []
    for index in range(40):
        ts = window.start_ts + index * 60
        rows.append({"minute_ts": ts, "premium_close_bps": 2,
                     "sell_edge_max_bps": 12 + index % 2,
                     "buy_edge_max_bps": -8 - index % 2, "samples": 60})
    rows += [{"minute_ts": window.start_ts - 60, "premium_close_bps": 99,
              "sell_edge_max_bps": 99, "buy_edge_max_bps": 99, "samples": 60}]
    rows += [{"minute_ts": window.end_ts + 60, "premium_close_bps": 3,
              "sell_edge_max_bps": 13, "buy_edge_max_bps": -9, "samples": 60}]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    loaded = load_window_rows(path, window)
    result = suggest(loaded, fees_bps=2.0, window=window)
    assert result.rows == 40
    assert result.midline_bps == 2.0
    assert result.upper_bps >= 1.0 and result.lower_bps >= 1.0
    assert prune_csv(path, window) == 1
    assert len(load_window_rows(path, window)) == 40
    with path.open(newline="") as handle:
        assert sum(1 for _ in csv.DictReader(handle)) == 41
