"""Aged WS books update normally but cannot create new trading exposure."""
import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from entropy_arb.latency import BookLatency, StaleMarketData
from test_engine import make_engine, run_scan
from test_bps_strategy import engine_at, books, execute


STAMP = 1792022400.125


def stamp(book, age=50, data_age=None, elapsed=0):
    book.latency.record((STAMP - age / 1000) * 1000, received_at=STAMP,
                        received_mono=time.perf_counter() - elapsed / 1000,
                        timestamp_field="timestamp",
                        data_timestamp=(STAMP - data_age / 1000) * 1e6 if data_age is not None else None)


def engine():
    eng = make_engine(midline=0, upper=1, lower=1)
    eng.cfg.max_ws_age_ms = 100
    eng.entropy.set_book(101, 102)
    eng.hedge.set_book(99, 100)
    for venue in eng.venues.values():
        stamp(venue.book)
        venue.volume_usd = 0
        venue.px_round = lambda px, round_up: px
    return eng


def test_age_includes_local_wait_and_older_committer_time():
    book = BookLatency()
    book.record((STAMP - .02) * 1000, received_at=STAMP,
                received_mono=time.perf_counter() - .09,
                data_timestamp=(STAMP - .2) * 1e6)
    assert book.entry_age_ms() >= 290
    assert "data age" in book.entry_block_reason(100)


@pytest.mark.parametrize("age,blocked", [(50, False), (100.5, True), (500, True)])
def test_gate_fresh_and_delayed_data(age, blocked):
    book = BookLatency()
    book.record((STAMP - age / 1000) * 1000, received_at=STAMP)
    assert bool(book.entry_block_reason(100)) is blocked


def test_missing_future_clock_and_explicit_disable():
    book = BookLatency()
    assert "missing" in book.entry_block_reason(100)
    book.record((STAMP + 2) * 1000, received_at=STAMP)
    assert "future" in book.entry_block_reason(100)
    assert book.entry_block_reason(0) is None


def test_stale_diff_is_merged_and_heartbeat_does_not_bypass_gate():
    eng = engine()
    book = eng.entropy.book
    book.apply_lighter(dict(bids=[dict(price="103", size="2")]), snapshot=False)
    stamp(book, age=300)
    book.touch()
    assert book.best_bid() == 103
    assert book.is_fresh(10)
    assert "300" in book.latency.entry_block_reason(100)
    assert run_scan(eng) is None
    assert eng.ws_gate_blocks > 0
    stamp(book)
    assert run_scan(eng) is not None


def test_new_envelope_cannot_hide_old_market_data():
    eng = engine()
    stamp(eng.entropy.book, age=10, data_age=250)
    assert run_scan(eng) is None


def test_one_old_leg_blocks_both_orders_before_sending():
    async def run():
        eng = engine()
        eng.ensure_async_state()
        plan, _ = eng._plan(eng.hedge, eng.entropy, 100)
        stamp(eng.entropy.book, age=200)
        eng._timed_send = AsyncMock()
        with pytest.raises(StaleMarketData):
            await eng._execute(eng.hedge, eng.entropy, plan)
        eng._timed_send.assert_not_awaited()
        assert eng.trades == 0
    asyncio.run(run())


def test_final_recheck_after_price_rounding_does_not_send_one_leg():
    async def run():
        eng = engine()
        eng.ensure_async_state()
        plan, _ = eng._plan(eng.hedge, eng.entropy, 100)
        def round_then_age(px, round_up):
            stamp(eng.entropy.book, age=200)
            return px
        eng.entropy.px_round = round_then_age
        eng._timed_send = AsyncMock()
        with pytest.raises(StaleMarketData):
            await eng._execute(eng.hedge, eng.entropy, plan)
        eng._timed_send.assert_not_awaited()
    asyncio.run(run())


def test_only_strict_reductions_can_bypass_guard():
    eng = engine()
    plan, _ = eng._plan(eng.hedge, eng.entropy, 100)
    eng.hedge.position, eng.entropy.position = -plan.qty, plan.qty
    eng.entropy.book.latency = BookLatency()
    assert eng._entry_data_allowed(eng.hedge, eng.entropy, plan)
    eng.hedge.position /= 2
    assert not eng._entry_data_allowed(eng.hedge, eng.entropy, plan)


def test_maker_quote_becomes_invalid_when_either_leg_ages():
    eng = engine()
    eng.cfg.primary_order_type = "maker"
    plan, _ = eng._plan(eng.hedge, eng.entropy, 100)
    stamp(eng.hedge.book, age=200)
    assert not eng._maker_quote_valid(eng.hedge, eng.entropy, plan, plan.sell_limit)


def test_bps_blocks_open_but_allows_reduce_only_exit(tmp_path):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.cfg.max_ws_age_ms = 100
        assert eng._scan(time.time()) is None  # no exchange timestamp
        for venue in eng.venues.values():
            stamp(venue.book)
        await execute(eng, eng._scan(time.time()))
        assert eng.bps_strategy.entries == 1
        books(eng, -5.5)
        for venue in eng.venues.values():
            stamp(venue.book, age=200)
        candidate = eng._scan(time.time())
        assert candidate[2].action == "bps_close"
        await execute(eng, candidate)
    asyncio.run(run())


def test_bps_checkpoint_aging_aborts_cleanly_without_inventing_a_cycle(tmp_path):
    async def run():
        eng = engine_at(tmp_path)
        eng.ensure_async_state()
        eng.cfg.max_ws_age_ms = 100
        for venue in eng.venues.values():
            stamp(venue.book)
        candidate = eng._scan(time.time())
        before = eng.bps_strategy.before_execution
        def checkpoint_then_age(plan):
            before(plan)
            stamp(eng.entropy.book, age=200)
        with patch.object(eng.bps_strategy, "before_execution", side_effect=checkpoint_then_age):
            await execute(eng, candidate)
        assert eng.bps_strategy.entries == 0 and not eng.bps_strategy.pending
        assert eng.bps_strategy.locked_entry is None
        assert eng.bps_strategy.last_closed_at == 0
        assert not eng.halted and eng.trades == 0
        assert all(not eng._vlock(v.key).locked() for v in eng.venues.values())
        for venue in eng.venues.values():
            venue.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_percentiles_include_rejected_slow_samples():
    timing = BookLatency()
    for age in range(1, 101):
        timing.record((STAMP - age / 1000) * 1000, received_at=STAMP)
    timing.record((STAMP - .5) * 1000, received_at=STAMP)
    stats = timing.snapshot()
    assert stats["data_p50_ms"] == pytest.approx(51, abs=.001)
    assert stats["data_p95_ms"] == pytest.approx(96, abs=.001)
    assert stats["data_p99_ms"] == pytest.approx(100, abs=.001)
    assert stats["data_over_100ms_pct"] == pytest.approx(100 / 101)
