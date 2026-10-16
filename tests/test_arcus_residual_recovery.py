"""Screenshot regression: Arcus -0.4883306, RH +0.4759; fake quotes/orders only."""
import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from entropy_arb.reduction import reduction_rules
from entropy_arb.venue_arcus import ArcusVenue
from test_bps_strategy import engine_at
from test_arcus import signed_venue


def paired_inventory(tmp_path, monkeypatch, *, swapped=False, cap=50, opening_minimum=10):
    monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
    eng = engine_at(tmp_path, long=swapped)
    eng.ensure_async_state()
    eng.cfg.max_order_notional, eng.cfg.min_order_notional = cap, opening_minimum
    arcus, rh = (eng.hedge, eng.entropy) if swapped else (eng.entropy, eng.hedge)
    arcus.name, arcus.size_decimals = 'ARCUS', 7
    arcus.min_base, arcus.min_quote = .001, 5
    arcus.reduce_only_min_quote = ArcusVenue.reduce_only_min_quote
    arcus.position, rh.position = -.4883306, .4759
    # Synthetic ~$771 quotes: the excess is worth < $10, but exceeds Arcus'
    # quantity minimum. This is not evidence of the screenshot's actual price.
    for v in eng.venues.values():
        v.set_book(771.50, 771.51)
        v.has_open_orders = AsyncMock(return_value=False)
        v.fetch_position = AsyncMock(side_effect=lambda v=v: v.position)
    strategy = eng.bps_strategy
    strategy.entries, strategy.pending = 1, True
    strategy.locked_entry, strategy.locked_exit = ((-9, -6) if swapped else (9, 6))
    strategy.save()
    eng.bps_strategy = BpsStrategy(eng)
    return eng, arcus, rh


@pytest.mark.parametrize('swapped', [True, False])
@pytest.mark.parametrize('opening_minimum', [10, 100])
def test_screenshot_inventory_recovers_in_one_exact_arcus_reduction(tmp_path, monkeypatch, swapped, opening_minimum):
    async def run():
        eng, arcus, rh = paired_inventory(tmp_path, monkeypatch, swapped=swapped, opening_minimum=opening_minimum)
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        eng._check_starting_positions()
        eng.bps_strategy.restore()
        arcus.send_taker.assert_awaited_once()
        args = arcus.send_taker.call_args.kwargs
        assert args['qty'] == .0124306
        assert args['is_buy'] and args['reduce_only']
        assert arcus.position == pytest.approx(-.4759)
        assert rh.position == .4759
        rh.send_taker.assert_not_awaited()
        assert not eng.halted and not eng.bps_strategy.pending
    asyncio.run(run())


def test_same_arcus_rules_apply_to_runtime_repairs(tmp_path, monkeypatch):
    async def run():
        eng, arcus, rh = paired_inventory(tmp_path, monkeypatch)
        eng.bps_strategy.costs.inherit({v.key: v.position for v in eng.venues.values()}, time.time())
        await eng._maybe_hedge()
        assert eng.bps_strategy.net_balanced()
        assert arcus.send_taker.call_args.kwargs['qty'] == .0124306
        rh.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_real_cap_below_quantity_minimum_stays_blocked_with_numbers(tmp_path, monkeypatch, caplog):
    async def run():
        eng, arcus, rh = paired_inventory(tmp_path, monkeypatch, cap=.5)
        assert await eng.bps_strategy.recover_pending()
        with pytest.raises(RuntimeError) as error:
            await eng.bps_strategy.finish_startup_recovery()
        message = str(error.value)
        for text in ('ARCUS', '-0.0124306', '保护价', '减仓最小数量 0.001',
                     '减仓最小金额 $0.0000', '单笔上限 $0.5000', '保留 pending'):
            assert text in message
        assert '[BPS RECOVERY BLOCKED]' in caplog.text
        assert eng.bps_strategy.pending
        arcus.send_taker.assert_not_awaited()
        rh.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_arcus_reduction_rules_match_signed_request_validation():
    v = signed_venue()
    assert reduction_rules(v, .0001, 100) == (1e-7, .001, 0)
    request = v._place_request(is_buy=True, qty=.001, limit_px=771.51,
                               reduce_only=True, client_id='small-reduction')
    assert request['payload']['reduceOnly']
    with pytest.raises(ValueError, match='quantity outside market limits'):
        v._place_request(is_buy=True, qty=.0005, limit_px=771.51,
                         reduce_only=True, client_id='below-base-minimum')
