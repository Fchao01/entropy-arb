import asyncio
from types import SimpleNamespace

import pytest

from entropy_arb.execution_stats import ExecutionStats


def session():
    venues = {key: SimpleNamespace(key=key, position=0, book=SimpleNamespace(mid=lambda: 100))
              for key in ('primary', 'hedge')}
    engine = SimpleNamespace(venues=venues, _step=.001, total_fees_usd=0)
    return engine, ExecutionStats()


def fill(engine, stats, key, side, qty, price, fee=0):
    venue = engine.venues[key]
    stats.record(key, side == 'buy', qty, price, venue.position, engine._step)
    venue.position += qty if side == 'buy' else -qty
    engine.total_fees_usd += fee


def test_open_cash_difference_is_not_realized_profit_and_close_uses_both_legs():
    eng, stats = session()
    fill(eng, stats, 'primary', 'buy', 1, 100, .1)
    fill(eng, stats, 'hedge', 'sell', 1, 101, .101)
    opened = stats.snapshot(eng)
    assert opened['gross_realized_usd'] == 0
    assert opened['net_realized_usd'] == pytest.approx(-.201)
    assert opened['unrealized_usd'] == 1
    fill(eng, stats, 'primary', 'sell', 1, 102, .102)
    fill(eng, stats, 'hedge', 'buy', 1, 102.5, .1025)
    closed = stats.snapshot(eng)
    assert closed['volume_by_venue'] == dict(primary=202, hedge=203.5)
    assert closed['volume_usd'] == 405.5
    assert closed['gross_realized_usd'] == .5
    assert closed['net_realized_usd'] == pytest.approx(.0945)
    assert closed['loss_per_10000_usd'] == pytest.approx(-.0945 / 405.5 * 10000)
    assert closed['unrealized_usd'] == 0


def test_fifo_partial_close_and_reversal():
    eng, stats = session()
    fill(eng, stats, 'primary', 'buy', 1, 100)
    fill(eng, stats, 'primary', 'buy', 2, 110)
    fill(eng, stats, 'primary', 'sell', 2, 105)
    assert stats.snapshot(eng)['gross_realized_usd'] == 0  # +5 and -5
    fill(eng, stats, 'primary', 'sell', 2, 120)
    assert stats.snapshot(eng)['gross_realized_usd'] == 10
    assert list(stats.lots['primary']) == [[-1, 120]]


def test_unknown_inherited_cost_or_fill_price_is_not_invented():
    eng, stats = session()
    eng.venues['primary'].position = 2
    fill(eng, stats, 'primary', 'sell', 1, 110)
    assert stats.snapshot(eng)['net_realized_usd'] is None
    fill(eng, stats, 'hedge', 'buy', 1, None)
    unknown = stats.snapshot(eng)
    assert unknown['volume_usd'] is None and unknown['unrealized_usd'] is None
    assert unknown['loss_per_10000_usd'] is None


def test_new_session_resets_and_zero_volume_has_no_wear_rate():
    eng, stats = session()
    stats.openings = 2
    assert ExecutionStats().snapshot(eng)['openings'] == 0
    assert stats.snapshot(eng)['loss_per_10000_usd'] is None
    stats.unresolved = True
    assert stats.snapshot(eng)['net_realized_usd'] is None


def test_engine_counts_actual_open_batches_but_not_zero_fill_or_exit(tmp_path):
    from test_bps_strategy import engine_at, execute, books
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        await execute(eng, eng._scan(__import__('time').time()))
        assert eng.execution_stats.openings == 1
        assert eng.execution_stats.snapshot(eng)['gross_realized_usd'] == 0
        books(eng, -5.5)
        while not eng.bps_strategy.flat():
            await execute(eng, eng._scan(__import__('time').time()))
            books(eng, -5.5)
        result = eng.execution_stats.snapshot(eng)
        assert result['openings'] == 1 and result['gross_realized_usd'] > 0
        assert result['unrealized_usd'] == 0
        assert result['volume_usd'] == pytest.approx(eng._total_volume())
    asyncio.run(run())
