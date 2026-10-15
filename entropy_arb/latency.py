"""WS timing diagnostics; cross-machine deltas include clock skew."""
from collections import deque
from datetime import datetime
import math
import time


def timestamp_seconds(value):
    """Accept Unix seconds/ms/us/ns or timezone-qualified ISO timestamps."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        try:
            stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            number = stamp.timestamp() if stamp.tzinfo is not None else float("nan")
        except (ValueError, TypeError, OverflowError, OSError):
            return None
    if not math.isfinite(number):
        return None
    if number >= 1e17:
        number /= 1e9
    elif number >= 1e14:
        number /= 1e6
    elif number >= 1e11:
        number /= 1e3
    # Reject sequence ids, relative durations, and implausible epoch units.
    return number if 946684800 <= number < 4102444800 else None


class BookLatency:
    def __init__(self):
        self.received_at = self.received_mono = None
        self.source_ts = self.data_ts = None
        self.timestamp_field = None
        self.apply_ms = None
        self.delays = deque(maxlen=512)
        self.messages = 0

    def record(self, source, *, received_at=None, received_mono=None,
               timestamp_field=None, data_timestamp=None):
        now_mono = time.perf_counter()
        self.received_at = time.time() if received_at is None else received_at
        self.received_mono = now_mono if received_mono is None else received_mono
        self.source_ts = timestamp_seconds(source)
        self.data_ts = timestamp_seconds(data_timestamp)
        self.timestamp_field = timestamp_field if self.source_ts is not None else None
        self.apply_ms = max(0.0, (now_mono - self.received_mono) * 1000)
        self.messages += 1
        if self.source_ts is not None:
            self.delays.append((self.received_at - self.source_ts) * 1000)

    def snapshot(self, now=None):
        now = time.time() if now is None else now
        delay = ((self.received_at - self.source_ts) * 1000
                 if self.source_ts is not None else None)
        data_delay = ((self.received_at - self.data_ts) * 1000
                      if self.data_ts is not None else None)
        samples = sorted(self.delays)
        return dict(
            received_at=self.received_at, exchange_ts=self.source_ts,
            data_ts=self.data_ts, timestamp_field=self.timestamp_field,
            delay_ms=delay, data_delay_ms=data_delay,
            source_age_ms=(now - self.source_ts) * 1000 if self.source_ts is not None else None,
            publish_gap_ms=(self.source_ts - self.data_ts) * 1000
            if self.source_ts is not None and self.data_ts is not None else None,
            apply_ms=self.apply_ms,
            local_wait_ms=max(0.0, (time.perf_counter() - self.received_mono) * 1000)
            if self.received_mono is not None else None,
            p50_ms=samples[(len(samples) - 1) // 2] if samples else None,
            p95_ms=samples[math.ceil(.95 * len(samples)) - 1] if samples else None,
            max_ms=samples[-1] if samples else None, samples=len(samples), messages=self.messages,
            clock_warning=any(value is not None and value < -1 for value in (delay, data_delay)),
        )


def timing_text(name, timing):
    def number(key, decimals=1):
        value = timing[key]
        return "unknown" if value is None else f"{value:.{decimals}f}"
    return (f"{name} field={timing['timestamp_field'] or 'unknown'} "
            f"exchange_ts={number('exchange_ts', 6)} recv_ts={number('received_at', 6)} "
            f"data_ts={number('data_ts', 6)} "
            f"delay={number('delay_ms')}ms data_delay={number('data_delay_ms')}ms "
            f"publish_gap={number('publish_gap_ms')}ms apply={number('apply_ms')}ms "
            f"local_wait={number('local_wait_ms')}ms source_age={number('source_age_ms')}ms "
            f"p50={number('p50_ms')}ms p95={number('p95_ms')}ms "
            f"max={number('max_ms')}ms samples={timing['samples']} "
            f"clock_warning={timing['clock_warning']}")
