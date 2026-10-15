"""WS timing diagnostics; cross-machine deltas include clock skew."""
from collections import deque
from datetime import datetime
import math
import time


class StaleMarketData(RuntimeError):
    """No order has been sent; the candidate aged out before submission."""


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
        self.data_delays = deque(maxlen=512)
        self.source_gaps = deque(maxlen=512)
        self.receive_gaps = deque(maxlen=512)
        self.source_gap_ms = self.receive_gap_ms = self.data_gap_ms = None
        self.messages = 0

    def record(self, source, *, received_at=None, received_mono=None,
               timestamp_field=None, data_timestamp=None):
        now_mono = time.perf_counter()
        previous_source, previous_data = self.source_ts, self.data_ts
        previous_received = self.received_mono
        self.received_at = time.time() if received_at is None else received_at
        self.received_mono = now_mono if received_mono is None else received_mono
        self.source_ts = timestamp_seconds(source)
        self.data_ts = timestamp_seconds(data_timestamp)
        self.source_gap_ms = ((self.source_ts - previous_source) * 1000
                              if self.source_ts is not None and previous_source is not None else None)
        self.data_gap_ms = ((self.data_ts - previous_data) * 1000
                            if self.data_ts is not None and previous_data is not None else None)
        self.receive_gap_ms = ((self.received_mono - previous_received) * 1000
                               if previous_received is not None else None)
        if self.source_gap_ms is not None:
            self.source_gaps.append(self.source_gap_ms)
        if self.receive_gap_ms is not None:
            self.receive_gaps.append(self.receive_gap_ms)
        self.timestamp_field = timestamp_field if self.source_ts is not None else None
        self.apply_ms = max(0.0, (now_mono - self.received_mono) * 1000)
        self.messages += 1
        if self.source_ts is not None:
            self.delays.append((self.received_at - self.source_ts) * 1000)
        stamps = [ts for ts in (self.source_ts, self.data_ts) if ts is not None]
        if stamps:
            self.data_delays.append((self.received_at - min(stamps)) * 1000)

    def entry_age_ms(self):
        """Data age now: age on receipt + local monotonic elapsed time.

        Use the older of publication and committer times. Do not let a new
        envelope disguise old data or a wall-clock adjustment rejuvenate it.
        """
        stamps = [ts for ts in (self.source_ts, self.data_ts) if ts is not None]
        if not stamps or self.received_at is None or self.received_mono is None:
            return None
        return ((self.received_at - min(stamps)) * 1000
                + max(0.0, time.perf_counter() - self.received_mono) * 1000)

    def receive_delay_ms(self):
        """Publication-to-application delay of the latest accepted book frame."""
        return ((self.received_at - self.source_ts) * 1000
                if self.received_at is not None and self.source_ts is not None else None)

    def entry_block(self, max_age_ms, max_delay_ms=0):
        """Two independent gates. A quiet book doesn't increase receive delay."""
        if max_age_ms <= 0 and max_delay_ms <= 0:
            return None
        def blocked(kind, reason):
            return dict(kind=kind, reason=reason)
        # Small sub-ms rounding differences are possible; seconds of clock
        # error must never make all stale messages qualify as fresh.
        if self.received_at is not None and any(
                self.received_at - stamp < -.005 for stamp in (self.source_ts, self.data_ts)
                if stamp is not None):
            return blocked("clock", "exchange timestamp is in the future; check clock synchronization")
        delay = self.receive_delay_ms()
        if max_delay_ms > 0:
            if delay is None:
                return blocked("missing_ws_timestamp", "missing WS message timestamp")
            if round(delay, 3) > max_delay_ms:
                return blocked("ws_delay", f"WS receive delay {delay:.1f}ms > {max_delay_ms:g}ms")
        age = self.entry_age_ms()
        if max_age_ms > 0 and age is None:
            return blocked("missing_book_timestamp", "missing exchange timestamp for book age")
        # Epoch floats have sub-microsecond rounding noise.
        if max_age_ms > 0 and round(age, 3) > max_age_ms:
            return blocked("book_age", f"book data age {age:.1f}ms > {max_age_ms:g}ms")
        return None

    def entry_block_reason(self, max_age_ms, max_delay_ms=0):
        block = self.entry_block(max_age_ms, max_delay_ms)
        return block["reason"] if block else None

    def snapshot(self, now=None):
        now = time.time() if now is None else now
        delay = self.receive_delay_ms()
        data_delay = ((self.received_at - self.data_ts) * 1000
                      if self.data_ts is not None else None)
        samples = sorted(self.delays)
        data_samples = sorted(self.data_delays)
        source_gaps, receive_gaps = sorted(self.source_gaps), sorted(self.receive_gaps)
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
            p99_ms=samples[math.ceil(.99 * len(samples)) - 1] if samples else None,
            ws_over_100ms_pct=100 * sum(round(value, 3) > 100 for value in samples) / len(samples)
            if samples else None,
            source_gap_ms=self.source_gap_ms, receive_gap_ms=self.receive_gap_ms,
            data_gap_ms=self.data_gap_ms,
            source_gap_p95_ms=source_gaps[math.ceil(.95 * len(source_gaps)) - 1] if source_gaps else None,
            source_gap_p99_ms=source_gaps[math.ceil(.99 * len(source_gaps)) - 1] if source_gaps else None,
            receive_gap_p95_ms=receive_gaps[math.ceil(.95 * len(receive_gaps)) - 1] if receive_gaps else None,
            receive_gap_p99_ms=receive_gaps[math.ceil(.99 * len(receive_gaps)) - 1] if receive_gaps else None,
            entry_age_ms=self.entry_age_ms(),
            data_p50_ms=data_samples[(len(data_samples) - 1) // 2] if data_samples else None,
            data_p95_ms=data_samples[math.ceil(.95 * len(data_samples)) - 1] if data_samples else None,
            data_p99_ms=data_samples[math.ceil(.99 * len(data_samples)) - 1] if data_samples else None,
            data_over_100ms_pct=100 * sum(round(value, 3) > 100 for value in data_samples) / len(data_samples)
            if data_samples else None,
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
            f"p99={number('p99_ms')}ms ws_over_100ms={number('ws_over_100ms_pct')}% "
            f"max={number('max_ms')}ms samples={timing['samples']} "
            f"source_gap={number('source_gap_ms')}ms recv_gap={number('receive_gap_ms')}ms "
            f"data_gap={number('data_gap_ms')}ms "
            f"source_gap_p95={number('source_gap_p95_ms')}ms source_gap_p99={number('source_gap_p99_ms')}ms "
            f"recv_gap_p95={number('receive_gap_p95_ms')}ms recv_gap_p99={number('receive_gap_p99_ms')}ms "
            f"entry_age={number('entry_age_ms')}ms data_p50={number('data_p50_ms')}ms "
            f"data_p95={number('data_p95_ms')}ms data_p99={number('data_p99_ms')}ms "
            f"data_over_100ms={number('data_over_100ms_pct')}% "
            f"clock_warning={timing['clock_warning']}")
