"""Daily threshold calculation and retention for recorded minute bars.

The automatic job uses a fixed local-day window: 08:00 yesterday through
08:00 today in Asia/Shanghai.  Keeping this logic out of the CLI makes the
web-console scheduler use exactly the same calculation as manual analysis.
"""
from __future__ import annotations

import csv
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
HEADER = [
    "minute_ts", "time_utc", "entropy_bid", "entropy_ask", "hedge_bid",
    "hedge_ask", "premium_open_bps", "premium_high_bps", "premium_low_bps",
    "premium_close_bps", "premium_mean_bps", "premium_std_bps",
    "sell_edge_mean_bps", "sell_edge_max_bps", "buy_edge_mean_bps",
    "buy_edge_max_bps", "samples",
]


class ThresholdDataError(ValueError):
    """The requested window cannot safely produce a threshold update."""


@dataclass(frozen=True)
class ThresholdWindow:
    start: datetime
    end: datetime

    @property
    def start_ts(self) -> float:
        return self.start.timestamp()

    @property
    def end_ts(self) -> float:
        return self.end.timestamp()

    @property
    def label(self) -> str:
        return self.end.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class ThresholdSuggestion:
    midline_bps: float
    upper_bps: float
    lower_bps: float
    rows: int
    span_hours: float
    window: ThresholdWindow


def daily_window(now: Optional[datetime] = None) -> ThresholdWindow:
    """Return the most recently completed 08:00–08:00 Shanghai window."""
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    local = now.astimezone(SHANGHAI)
    end = local.replace(hour=8, minute=0, second=0, microsecond=0)
    if local < end:
        end -= timedelta(days=1)
    return ThresholdWindow(start=end - timedelta(days=1), end=end)


def _pctl(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * q / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return values[lo]
    return values[lo] * (hi - k) + values[hi] * (k - lo)


def load_window_rows(path, window: ThresholdWindow,
                     min_samples: int = 10) -> list[dict]:
    """Load valid minute rows in ``[window.start, window.end)``."""
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            try:
                ts = float(raw["minute_ts"])
                samples = int(raw["samples"])
                if not window.start_ts <= ts < window.end_ts or samples < min_samples:
                    continue
                row = {
                    "ts": ts,
                    "prem": float(raw["premium_close_bps"]),
                    "sell_max": float(raw["sell_edge_max_bps"]),
                    "buy_max": float(raw["buy_edge_max_bps"]),
                }
                if not all(math.isfinite(value) for value in row.values()):
                    continue
                rows.append(row)
            except (KeyError, TypeError, ValueError):
                continue
    rows.sort(key=lambda row: row["ts"])
    return rows


def suggest(rows: Iterable[dict], fees_bps: float, window: ThresholdWindow,
            min_rows: int = 30) -> ThresholdSuggestion:
    rows = list(rows)
    if len(rows) < min_rows:
        raise ThresholdDataError(
            f"only {len(rows)} valid minute rows in {window.label} window; "
            f"at least {min_rows} are required")
    if not math.isfinite(fees_bps) or fees_bps < 0:
        raise ThresholdDataError("combined taker fees must be a finite non-negative number")
    prem = sorted(row["prem"] for row in rows)
    midline = round(_pctl(prem, 50), 1) or 0.0
    sell_room = [row["sell_max"] - midline - fees_bps for row in rows]
    buy_room = [row["buy_max"] + midline - fees_bps for row in rows]
    upper = max(round(_pctl(sell_room, 90) * 2) / 2, 1.0)
    lower = max(round(_pctl(buy_room, 90) * 2) / 2, 1.0)
    if not all(math.isfinite(value) for value in (midline, upper, lower)):
        raise ThresholdDataError("calculated thresholds are not finite")
    first, last = rows[0]["ts"], rows[-1]["ts"]
    span_hours = max(0.0, (last - first) / 3600.0 + 1 / 60.0)
    return ThresholdSuggestion(midline, upper, lower, len(rows), span_hours, window)


def prune_csv(path: str | Path, window: ThresholdWindow) -> int:
    """Remove only rows older than the successfully applied window start.

    The recorder may already have written a few rows after 08:00 by the time
    the scheduler finishes.  Those rows belong to the next daily window and
    must be retained.
    """
    path = Path(path)
    if not path.exists():
        return 0
    kept: list[list[str]] = []
    removed = 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return 0
        for row in reader:
            try:
                ts = float(row[0])
            except (IndexError, ValueError):
                removed += 1
                continue
            if ts >= window.start_ts:
                kept.append(row)
            else:
                removed += 1
    if not removed:
        return 0
    fd, temporary = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header or HEADER)
            writer.writerows(kept)
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return removed
