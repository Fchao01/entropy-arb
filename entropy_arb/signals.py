"""Read-only signal values for the web monitor; never used to place orders."""
from __future__ import annotations


def executable_premium(engine, buy, sell, *, maker):
    buy_price, sell_price = buy.book.best_ask(), sell.book.best_bid()
    if maker:
        if buy is engine.entropy:
            buy_price = buy.book.best_bid()
        else:
            sell_price = sell.book.best_ask()
    if not buy_price or not sell_price:
        return None
    return (sell_price / buy_price - 1) * 1e4


def execution_signals(engine):
    if not engine.markets_ready:
        return None
    primary, hedge = engine.entropy, engine.hedge
    maker = engine.cfg.primary_order_type == 'maker'
    strategy = engine.bps_strategy
    rows = []
    if strategy:
        state = strategy.snapshot()
        for action, primary_buy, target, comparison in (
            ('open', strategy.long, state['entry_bps'], '<=' if strategy.long else '>='),
            ('close', not strategy.long, state['exit_bps'], '>=' if strategy.long else '<='),
        ):
            trigger = strategy.signal(closing=action == 'close')
            basis = 'executable_premium_bps' if strategy.executable else 'mid_premium_bps'
            if strategy.executable:
                comparison = '>='
            buy, sell = (primary, hedge) if primary_buy else (hedge, primary)
            action_maker = maker and (action == 'open' or engine.cfg.primary_maker_only)
            gap = None if trigger is None or target is None else (
                target - trigger if comparison == '<=' else trigger - target)
            rows.append(dict(action=action, primary_buy=primary_buy, buy=buy.name, sell=sell.name,
                             order_type='maker' if action_maker else 'taker',
                             executable_premium_bps=executable_premium(
                                 engine, buy, sell, maker=action_maker),
                             trigger_premium_bps=trigger, hurdle_bps=target, trigger_basis=basis,
                             comparison=comparison, gap_bps=gap,
                             has_position=bool(strategy.entries),
                             closing=bool(strategy.closing)))
    else:
        spread = engine.cfg.strategy_mode == 'spread'
        for primary_buy in (False, True):
            buy, sell = (primary, hedge) if primary_buy else (hedge, primary)
            premium = executable_premium(engine, buy, sell, maker=maker)
            hurdle = engine._eff_threshold(buy, sell)
            if not spread:
                hurdle += engine.cfg.primary_fee_bps + hedge.fee_bps
            rows.append(dict(action='trade', primary_buy=primary_buy, buy=buy.name, sell=sell.name,
                             order_type='maker' if maker else 'taker',
                             executable_premium_bps=premium, trigger_premium_bps=premium,
                             hurdle_bps=hurdle, comparison='>' if spread else '>=',
                             gap_bps=None if premium is None else premium - hurdle))
    return dict(mode=engine.cfg.strategy_mode, rows=rows)
