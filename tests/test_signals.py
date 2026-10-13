"""Monitor signals match each strategy's price basis without changing orders."""
import pytest

from entropy_arb.signals import execution_signals
from test_engine import make_engine
from test_bps_strategy import engine_at


@pytest.mark.parametrize('maker', [False, True])
def test_threshold_signal_matches_dashboard_fees_inventory_and_quotes(maker):
    engine = make_engine(midline=2, upper=4, lower=3)
    engine.markets_ready = True
    engine.cfg.primary_order_type = 'maker' if maker else 'taker'
    engine.cfg.primary.fee_bps = 1.5
    engine.cfg.primary.maker_fee_bps = .25
    engine.hedge.fee_bps = .5
    engine.entropy.set_book(100.1, 100.2)
    engine.hedge.set_book(99.9, 100)
    engine._inv_add_bps = lambda buy, sell: 2
    armed_before = dict(engine._armed)
    result = execution_signals(engine)
    sell, buy = result['rows']
    primary_fee = engine.cfg.primary_fee_bps
    assert result['mode'] == 'threshold'
    assert sell['executable_premium_bps'] == pytest.approx(
        ((100.2 if maker else 100.1) / 100 - 1) * 1e4)
    assert buy['executable_premium_bps'] == pytest.approx(
        (99.9 / (100.1 if maker else 100.2) - 1) * 1e4)
    assert sell['hurdle_bps'] == pytest.approx(6 + primary_fee + .5 + 2)
    assert buy['hurdle_bps'] == pytest.approx(1 + primary_fee + .5 + 2)
    for row in result['rows']:
        assert row['trigger_premium_bps'] == row['executable_premium_bps']
        assert row['gap_bps'] == pytest.approx(row['trigger_premium_bps'] - row['hurdle_bps'])
    assert engine._armed == armed_before


@pytest.mark.parametrize('long', [False, True])
@pytest.mark.parametrize('maker', [False, True])
def test_bps_uses_midprice_targets_and_taker_close(tmp_path, long, maker):
    engine = engine_at(tmp_path, maker=maker, long=long)
    engine.markets_ready = True
    engine.cfg.primary.fee_bps = 100
    result = execution_signals(engine)
    opening, closing = result['rows']
    assert result['mode'] == 'bps'
    assert opening['trigger_premium_bps'] == pytest.approx(-9.5 if long else 9.5)
    assert opening['hurdle_bps'] == (-9 if long else 9)
    assert opening['comparison'] == ('<=' if long else '>=')
    assert opening['gap_bps'] == pytest.approx(.5)
    assert closing['gap_bps'] == pytest.approx(-3.5)
    assert opening['has_position'] is False
    assert opening['executable_premium_bps'] != opening['trigger_premium_bps']
    assert closing['order_type'] == 'taker'
    buy, sell = (engine.hedge, engine.entropy) if long else (engine.entropy, engine.hedge)
    assert closing['executable_premium_bps'] == pytest.approx(
        (sell.book.best_bid() / buy.book.best_ask() - 1) * 1e4)
    strategy = engine.bps_strategy
    strategy.entries, strategy.locked_entry, strategy.locked_exit = 1, -10 if long else 10, -7 if long else 7
    strategy.entry, strategy.exit = -100, 100  # live rolling levels cannot replace this cycle
    frozen = execution_signals(engine)['rows']
    assert frozen[0]['hurdle_bps'] == strategy.locked_entry
    assert frozen[1]['hurdle_bps'] == strategy.locked_exit
    assert frozen[1]['has_position'] is True


def test_waiting_for_markets_or_rolling_levels_never_invents_signal(tmp_path):
    engine = engine_at(tmp_path)
    engine.markets_ready = False
    assert execution_signals(engine) is None
    engine.markets_ready = True
    engine.bps_strategy.entry = engine.bps_strategy.exit = None
    for row in execution_signals(engine)['rows']:
        assert row['hurdle_bps'] is None
        assert row['gap_bps'] is None


def test_status_snapshot_exports_signals_for_record_only_monitor(tmp_path):
    from entropy_arb.status import snapshot
    engine = engine_at(tmp_path)
    engine.markets_ready = True
    engine.record_only = True
    for venue, conf in zip(engine.venues.values(), (engine.cfg.primary, engine.cfg.hedge)):
        venue.conf = conf
        venue.equity = venue.free = None
    result = snapshot(engine)
    assert result['execution_signals'] == execution_signals(engine)
    assert result['execution_signals']['rows'][0]['primary_buy'] is True
