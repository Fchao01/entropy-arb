"""Startup repairs only excess known-account exposure, never grows a leg."""
import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from entropy_arb.bps_strategy import BpsStrategy
from test_bps_strategy import engine_at


def pending_pair(tmp_path, monkeypatch, primary=.1434, hedge=-.1023):
    monkeypatch.setattr('entropy_arb.bps_strategy.PENDING_RECOVERY_RECHECK_SEC', 0)
    eng = engine_at(tmp_path)
    eng.ensure_async_state()
    for v, position in zip(eng.venues.values(), (primary, hedge)):
        v.position = position
        v.min_quote = 1  # Synthetic ETH quantities, books priced at $100.
        v.has_open_orders = AsyncMock(return_value=False)
        v.fetch_position = AsyncMock(side_effect=lambda v=v: v.position)
    eng.cfg.min_order_notional = 1
    strategy = eng.bps_strategy
    strategy.pending = True
    strategy.entries = 3
    strategy.locked_entry, strategy.locked_exit = -9, -6
    strategy.save()
    eng.bps_strategy = BpsStrategy(eng)
    return eng


@pytest.mark.parametrize('primary,hedge,side', [(.1434, -.1023, 'entropy'), (.1023, -.1434, 'hedge')])
def test_restart_repairs_only_excess_and_preserves_pair(tmp_path, monkeypatch, primary, hedge, side):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch, primary, hedge)
        original = eng.bps_strategy.path.read_text()
        assert await eng.bps_strategy.recover_pending()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
        await eng.bps_strategy.finish_startup_recovery()
        reduced = eng.venues[side]
        reduced.send_taker.assert_awaited_once()
        call = reduced.send_taker.call_args.kwargs
        assert call['qty'] == pytest.approx(.0411)
        assert call['reduce_only'] and call['is_buy'] is (side == 'hedge')
        other = eng.hedge if side == 'entropy' else eng.entropy
        other.send_taker.assert_not_awaited()
        assert eng.entropy.position == pytest.approx(.1023)
        assert eng.hedge.position == pytest.approx(-.1023)
        assert not eng.bps_strategy.pending
        restored = BpsStrategy(eng)
        restored.restore()
        assert restored.locked_exit == -6 and restored.entries == 3
        assert not restored.costs.known
        archive, = restored.path.parent.glob('*.recovered-*.bak')
        assert archive.read_text() == original
    asyncio.run(run())


def test_balanced_pending_pair_can_resume_without_flattening(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch, .1023, -.1023)
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
        eng.bps_strategy.restore()
        assert not eng.bps_strategy.pending
    asyncio.run(run())


def test_eth_repair_respects_100_usd_cap_without_stranding_a_small_tail(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        eng.entropy.set_book(2433.4, 2433.41)
        eng.hedge.set_book(2434, 2434.01)
        eng.entropy.min_base, eng.hedge.min_base = .005, .002
        eng.entropy.min_quote = eng.hedge.min_quote = eng.cfg.min_order_notional = 10
        eng.cfg.max_order_notional = 100
        assert await eng.bps_strategy.recover_pending()
        await eng.bps_strategy.finish_startup_recovery()
        calls = eng.entropy.send_taker.call_args_list
        assert len(calls) == 2
        assert sum(c.kwargs['qty'] for c in calls) == pytest.approx(.0411)
        assert all(c.kwargs['qty'] >= .005 and c.kwargs['qty'] * c.kwargs['limit_px'] <= 100 for c in calls)
        assert eng.entropy.position == pytest.approx(.1023)
        assert not eng.bps_strategy.pending
    asyncio.run(run())


def test_changing_positions_during_preflight_never_send_orders(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        eng.entropy.fetch_position.side_effect = [.1434, .1845]
        with pytest.raises(RuntimeError, match='两次实时持仓不同'):
            await eng.bps_strategy.recover_pending()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
        assert json.loads(eng.bps_strategy.path.read_text())['pending']
    asyncio.run(run())


def test_wrong_direction_is_never_automatically_modified(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch, -.1434, .1023)
        with pytest.raises(RuntimeError, match='方向与配置不同'):
            await eng.bps_strategy.recover_pending()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
    asyncio.run(run())


def test_confirmed_partial_startup_repair_only_sends_remaining_excess(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        assert await eng.bps_strategy.recover_pending()
        eng.entropy.send_taker.side_effect = [
            dict(status='partial', filled_base=.02, avg_px=100, err=None, unresolved=False),
            dict(status='filled', filled_base=.0211, avg_px=100, err=None, unresolved=False)]
        await eng.bps_strategy.finish_startup_recovery()
        assert [c.kwargs['qty'] for c in eng.entropy.send_taker.call_args_list] == pytest.approx([.0411, .0211])
        assert eng.entropy.position == pytest.approx(-eng.hedge.position)
    asyncio.run(run())


def test_failed_or_unconfirmed_startup_repair_preserves_pending(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        assert await eng.bps_strategy.recover_pending()
        eng.entropy.send_taker = AsyncMock(return_value=dict(
            status='timeout', filled_base=0, avg_px=None, err=None, unresolved=True))
        with pytest.raises(RuntimeError, match='未确认或失败'):
            await eng.bps_strategy.finish_startup_recovery()
        eng.entropy.send_taker.assert_awaited_once()
        assert json.loads(eng.bps_strategy.path.read_text())['pending']
        assert eng.halted
    asyncio.run(run())


def test_open_order_or_changed_position_never_triggers_repair(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        assert await eng.bps_strategy.recover_pending()
        eng.entropy.has_open_orders.return_value = True
        with pytest.raises(RuntimeError, match='未成交订单'):
            await eng.bps_strategy.finish_startup_recovery()
        for v in eng.venues.values():
            v.send_taker.assert_not_awaited()
        assert json.loads(eng.bps_strategy.path.read_text())['pending']
    asyncio.run(run())


def test_saved_own_maker_is_canceled_and_confirmed_before_positions(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        eng.entropy.kind = 'lighter'
        eng.active_maker = dict(venue=eng.entropy.name, client_id='1234', price=100, qty=.0411)
        eng.bps_strategy.pending = True
        eng.bps_strategy.save()
        eng.active_maker = None
        eng.entropy.maker_state = AsyncMock(side_effect=[
            dict(terminal=False, filled_base=0, status='open'),
            dict(terminal=True, filled_base=.0411, status='filled')])
        eng.entropy.cancel_maker = AsyncMock()
        assert await eng.bps_strategy.recover_pending()
        eng.entropy.cancel_maker.assert_awaited_once()
        assert eng.entropy.cancel_maker.call_args.args[0].client_id == 1234
        assert eng.entropy.maker_state.await_count == 2
        await eng.bps_strategy.finish_startup_recovery()
        assert not eng.bps_strategy.pending
    asyncio.run(run())


def test_engine_recovers_imbalance_before_starting_strategy(tmp_path, monkeypatch):
    async def run():
        eng = pending_pair(tmp_path, monkeypatch)
        monkeypatch.setattr(type(eng.cfg), 'creds_complete', property(lambda cfg: True))
        original = dict(eng.venues)
        eng._make_venue = lambda conf: original[conf.key]
        for conf in (eng.cfg.primary, eng.cfg.hedge):
            v = original[conf.key]
            v.conf, v.kind = conf, conf.kind
            v.load_market, v.close = AsyncMock(), AsyncMock()
            v.init_signer = Mock()
            v.start_tasks = Mock(return_value=[])
        eng._step = .0001
        async def strategy():
            assert not eng.bps_strategy.pending
            assert eng.entropy.position == pytest.approx(-eng.hedge.position)
            eng.stop.set()
        eng._strategy_loop = strategy
        eng.cfg.recorder_enabled = False
        await eng._run_inner()
        eng.entropy.send_taker.assert_awaited_once()
        assert eng.entropy.position == pytest.approx(.1023)
    asyncio.run(run())
