# pead_strategy/portfolio.py
import pandas as pd
import pandas_market_calendars as mcal
from config import MAX_POSITION_PCT, HOLD_DAYS

_NYSE = mcal.get_calendar('NYSE')


def get_portfolio_weights(active_symbols, max_pos=MAX_POSITION_PCT):
    """
    Return weight dict for active_symbols plus SPY fill.

    Rules:
      n == 0           -> 100% SPY
      n <= 14          -> min(7%, 1/n) per position; SPY fills remainder
      n > 14           -> 1/n each; SPY = 0
    """
    n = len(active_symbols)
    if n == 0:
        return {'SPY': 1.0}

    pos_weight  = min(max_pos, 1.0 / n)
    spy_weight  = max(0.0, 1.0 - pos_weight * n)
    weights     = {sym: pos_weight for sym in active_symbols}
    if spy_weight > 1e-6:
        weights['SPY'] = spy_weight
    return weights


def get_active_positions(trades_df, as_of_date):
    """Return list of symbols currently in position as of as_of_date."""
    if trades_df.empty:
        return []
    as_of  = pd.Timestamp(as_of_date)
    active = trades_df[pd.to_datetime(trades_df['entry_date']) <= as_of]
    return active['symbol'].tolist()


def check_exits(trades_df, as_of_date):
    """
    Return list of symbols where >= HOLD_DAYS trading days have elapsed since entry.

    Counting rule: entry day is day 0; after HOLD_DAYS trading days the position expires.
    E.g. entry Monday, HOLD_DAYS=20 -> exit on the open of the 21st trading day.
    """
    if trades_df.empty:
        return []

    as_of  = pd.Timestamp(as_of_date)
    exits  = []

    for _, trade in trades_df.iterrows():
        entry    = pd.Timestamp(trade['entry_date'])
        schedule = _NYSE.schedule(start_date=entry, end_date=as_of)
        # len(schedule) - 1 = number of trading days elapsed since entry
        if len(schedule) - 1 >= HOLD_DAYS:
            exits.append(trade['symbol'])

    return exits
