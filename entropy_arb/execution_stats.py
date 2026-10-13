"""Per-run fill accounting; realised PnL uses FIFO within each venue."""
from collections import deque
import math


class ExecutionStats:
    def __init__(self):
        self.openings = 0
        self.lots = {}
        self.volumes = {}
        self.unknown_volume = set()
        self.gross_realized = 0.0
        self.realized_known = True
        self.unresolved = False

    def record(self, key, is_buy, qty, price, before_position, step):
        if qty <= 0:
            return
        sign = 1 if is_buy else -1
        known_price = price is not None and math.isfinite(price) and price > 0
        if known_price:
            self.volumes[key] = self.volumes.get(key, 0.0) + qty * price
        else:
            price = None
            self.unknown_volume.add(key)
        if key not in self.lots:
            self.lots[key] = deque()
            if abs(before_position) >= step / 2:
                # Inherited positions have no session fill cost. Never guess it.
                self.lots[key].append([before_position, None])
        lots = self.lots[key]
        if abs(sum(lot[0] for lot in lots) - before_position) > step / 2 + 1e-10:
            self.realized_known = False  # external/reconciled inventory changed
            lots.clear()
            if abs(before_position) >= step / 2:
                lots.append([before_position, None])
        remaining = qty
        while remaining > 1e-12 and lots and lots[0][0] * sign < 0:
            old_qty, old_price = lots[0]
            closed = min(remaining, abs(old_qty))
            if price is None or old_price is None:
                self.realized_known = False
            else:
                self.gross_realized += closed * (price - old_price) * (1 if old_qty > 0 else -1)
            remaining -= closed
            leftover = abs(old_qty) - closed
            if leftover <= 1e-12:
                lots.popleft()
            else:
                lots[0][0] = math.copysign(leftover, old_qty)
        if remaining > 1e-12:
            lots.append([sign * remaining, price])

    def snapshot(self, engine):
        volumes = {v.key: None if v.key in self.unknown_volume else self.volumes.get(v.key, 0.0)
                   for v in engine.venues.values()}
        volume = sum(volumes.values()) if all(v is not None for v in volumes.values()) else None
        fees = engine.total_fees_usd
        gross = self.gross_realized if self.realized_known and not self.unresolved else None
        net = gross - fees if gross is not None else None
        unrealized = 0.0
        for venue in engine.venues.values():
            lots = self.lots.get(venue.key, ())
            if abs(sum(lot[0] for lot in lots) - venue.position) > engine._step / 2 + 1e-10:
                unrealized = None
                break
            mid = venue.book.mid()
            if any(price is None for _, price in lots) or (lots and mid is None):
                unrealized = None
                break
            unrealized += sum(qty * (mid - price) for qty, price in lots)
        if self.unresolved:
            unrealized = None
        return dict(openings=self.openings, volume_by_venue=volumes, volume_usd=volume,
                    fees_usd=fees, gross_realized_usd=gross, net_realized_usd=net,
                    unrealized_usd=unrealized,
                    loss_per_10000_usd=(-net / volume * 10000 if net is not None and volume else None),
                    complete=gross is not None and volume is not None,
                    fee_basis='configured_rates', scope='session')
