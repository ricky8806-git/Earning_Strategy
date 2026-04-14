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

    pos_weight = min(max_pos, 1.0 / n)
    spy_weight = max(0.0, 1.0 - pos_weight * n)
    weights    = {sym: pos_weight for sym in active_symbols}
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


def _get_latest_close(prices_dict, symbol, as_of_date):
    """
    Return the most recent daily close for symbol on or before as_of_date.
    Returns None if symbol not in prices_dict or no rows qualify.
    """
    df = prices_dict.get(symbol)
    if df is None or df.empty:
        return None
    as_of = pd.Timestamp(as_of_date).date()
    rows  = df[pd.to_datetime(df['date']).dt.date <= as_of]
    if rows.empty:
        return None
    return float(rows.iloc[-1]['close'])


def check_exits(trades_df, prices_dict, as_of_date):
    """
    Return list of exit dicts for positions that should be closed as of as_of_date.

    Each dict has keys: symbol (str), reason ('time' | 'stop_loss').

    Exit conditions (evaluated in priority order):
      1. Time exit   — 20 NYSE trading days elapsed since entry_date
      2. Stop loss   — latest daily close <= trade['stop_price']
                       (stop loss is checked on close, not intraday)

    Args:
        trades_df:    DataFrame with columns: symbol, entry_date, stop_price, ...
        prices_dict:  {symbol: prices_df} where prices_df has columns: date, close
        as_of_date:   date to evaluate exits against
    """
    if trades_df.empty:
        return []

    as_of = pd.Timestamp(as_of_date)
    exits = []

    for _, trade in trades_df.iterrows():
        entry    = pd.Timestamp(trade['entry_date'])
        schedule = _NYSE.schedule(start_date=entry, end_date=as_of)
        # len(schedule) - 1 = trading days elapsed since entry
        if len(schedule) - 1 >= HOLD_DAYS:
            exits.append({'symbol': trade['symbol'], 'reason': 'time'})
            continue

        # Stop loss: check latest close against stored stop_price
        stop_price    = trade['stop_price']
        current_close = _get_latest_close(prices_dict, trade['symbol'], as_of_date)
        if (current_close is not None
                and pd.notna(stop_price)
                and current_close <= float(stop_price)):
            exits.append({'symbol': trade['symbol'], 'reason': 'stop_loss'})

    return exits
