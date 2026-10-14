"""Confirmed fill costs for one BPS cycle, independent of session totals."""
from collections import deque
import math

from .execution_stats import ExecutionStats


class CycleCosts:
    def __init__(self):
        self.cash = 0.0
        self.open_notional = 0.0  # Primary opening notional; denominator for net return bps.
        self.started_at = 0.0
        self.known = True
        self.stats = ExecutionStats()

    def inherit(self, positions, now):
        self.known = False
        self.started_at = now
        self.stats.lots = {key: deque([[qty, None]]) for key, qty in positions.items() if qty}

    def record(self, key, is_buy, qty, price, fee, before_position, step, primary_open, now):
        if qty <= 0:
            return
        self.stats.record(key, is_buy, qty, price, before_position, step)
        if not self.started_at:
            self.started_at = now
        if price is None or not math.isfinite(price) or price <= 0 or not self.stats.realized_known:
            self.known = False
        if price is not None and math.isfinite(price) and price > 0:
            self.cash += qty * price * ((-1 - fee) if is_buy else (1 - fee))
            if primary_open:
                self.open_notional += qty * price

    def dump(self):
        return dict(cash=self.cash, open_notional=self.open_notional,
                    started_at=self.started_at, known=self.known,
                    lots={key: list(lots) for key, lots in self.stats.lots.items()})

    @classmethod
    def restore(cls, raw, positions, tolerance):
        result = cls()
        for key in ('cash', 'open_notional', 'started_at'):
            value = raw[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError('invalid BPS cost checkpoint')
            setattr(result, key, value)
        if (type(raw['known']) is not bool or result.open_notional < 0 or result.started_at <= 0
                or (raw['known'] and result.open_notional <= 0)):
            raise ValueError('invalid BPS cost checkpoint')
        result.known = raw['known']
        for key, position in positions.items():
            lots = raw['lots'].get(key, [])
            for qty, price in lots:
                if not math.isfinite(qty) or (price is not None and (not math.isfinite(price) or price <= 0)):
                    raise ValueError('invalid BPS cost lot')
                if result.known and price is None:
                    raise ValueError('missing BPS cost price')
            if abs(sum(qty for qty, _ in lots) - position) > tolerance:
                raise ValueError('BPS cost quantity differs from position')
            result.stats.lots[key] = deque(lots)
        return result
