# pead_strategy/signals.py
import pandas as pd
from config import EPS_BEAT_MIN_PCT, DAY0_RET_MIN, VOLUME_MULT


def compute_features(prices_df):
    """
    Enrich a prices DataFrame with forward/backward-looking columns needed for signal detection.

    Input columns: date, open, high, low, close, volume
    Added columns:
        prior_close  — previous trading day's close
        avg20_vol    — 20-day rolling average volume ending the day BEFORE current row
        day0_ret     — (close - prior_close) / prior_close
        d1_close     — next trading day's close
        d1_volume    — next trading day's volume
        d1_open      — next trading day's open (entry price for D0 trigger)
        d2_open      — two trading days' open (entry price for D1 trigger)
        d1_date      — next trading day's date
        d2_date      — two trading days' date
    """
    df = prices_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    df['prior_close'] = df['close'].shift(1)
    # shift(1) on rolling mean: average ends at the day *before* current row (no look-ahead)
    df['avg20_vol']   = df['volume'].rolling(20).mean().shift(1)
    df['day0_ret']    = (df['close'] - df['prior_close']) / df['prior_close']
    df['d1_close']    = df['close'].shift(-1)
    df['d1_volume']   = df['volume'].shift(-1)
    df['d1_open']     = df['open'].shift(-1)
    df['d2_open']     = df['open'].shift(-2)
    df['d1_date']     = df['date'].shift(-1)
    df['d2_date']     = df['date'].shift(-2)

    return df
