# pead_strategy/signals.py
import pandas as pd
from config import EPS_BEAT_MIN_PCT, DAY0_RET_MIN, VOLUME_MULT, STOP_LOSS_PCT


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


_SIGNALS_COLS = ['symbol', 'earnings_date', 'entry_date', 'entry_open',
                 'eps_beat_pct', 'trigger_day', 'stop_price']


def _empty_signals():
    return pd.DataFrame(columns=_SIGNALS_COLS)


def build_signals(events_df, prices_df):
    """
    Merge earnings events onto enriched price features and apply all entry filters.

    events_df must have columns: symbol, earnings_date, eps_estimate, eps_actual
    prices_df must have columns: date, open, high, low, close, volume

    Filters applied:
      1. EPS beat >= EPS_BEAT_MIN_PCT (skip zero/near-zero estimates)
      2. D0 trigger: day0_ret >= DAY0_RET_MIN AND D0 volume >= avg20_vol * VOLUME_MULT
         -> entry on D+1 open
      3. D1 trigger (only if D0 did not fire): D1 close vs prior_close >= DAY0_RET_MIN
         AND D1 volume >= avg20_vol * VOLUME_MULT -> entry on D+2 open

    Returns DataFrame with columns: symbol, earnings_date, entry_date, entry_open,
                                    eps_beat_pct, trigger_day
    """
    featured = compute_features(prices_df)
    featured['date'] = pd.to_datetime(featured['date'])

    events = events_df.copy()
    events['earnings_date'] = pd.to_datetime(events['earnings_date'])

    merged = events.merge(featured, left_on='earnings_date', right_on='date', how='inner')
    if merged.empty:
        return _empty_signals()

    # Drop rows where estimate is zero or near-zero to avoid division issues
    merged = merged[merged['eps_estimate'].abs() > 0.001].copy()
    if merged.empty:
        return _empty_signals()

    merged['eps_beat_pct'] = (
        (merged['eps_actual'] - merged['eps_estimate'])
        / merged['eps_estimate'].abs()
        * 100
    )
    merged = merged[merged['eps_beat_pct'] >= EPS_BEAT_MIN_PCT]
    if merged.empty:
        return _empty_signals()

    parts = []

    # --- D0 trigger ---
    # Return from pre-earnings close >= 3% AND D0 volume >= 2x avg20
    d0_mask = (
        (merged['day0_ret'] >= DAY0_RET_MIN)
        & (merged['volume'] >= merged['avg20_vol'] * VOLUME_MULT)
    )
    if d0_mask.any():
        d0 = merged[d0_mask].copy()
        d0['trigger_day'] = 'd0'
        d0['entry_date']  = d0['d1_date']   # Enter on D+1 open
        d0['entry_open']  = d0['d1_open']
        parts.append(d0)

    # --- D1 trigger (only for rows NOT already captured by D0) ---
    # D1 close vs pre-earnings close >= 3% AND D1 volume >= 2x avg20
    d1_ret = (merged['d1_close'] - merged['prior_close']) / merged['prior_close']
    d1_mask = (
        ~d0_mask
        & (d1_ret >= DAY0_RET_MIN)
        & (merged['d1_volume'] >= merged['avg20_vol'] * VOLUME_MULT)
    )
    if d1_mask.any():
        d1 = merged[d1_mask].copy()
        d1['trigger_day'] = 'd1'
        d1['entry_date']  = d1['d2_date']   # Enter on D+2 open
        d1['entry_open']  = d1['d2_open']
        parts.append(d1)

    if not parts:
        return _empty_signals()

    result = pd.concat(parts, ignore_index=True)
    result['stop_price'] = result['entry_open'] * (1 - STOP_LOSS_PCT)
    return result[_SIGNALS_COLS].reset_index(drop=True)


def get_miss_reason(events_df, prices_df, trigger_type):
    """
    Return a short human-readable string describing the first filter that blocked
    a signal.  Called only when build_signals() already returned empty.

    Possible return values (may be combined with '+'):
        no_price_match   — earnings date not found in price data
        zero_estimate    — EPS estimate is zero/near-zero
        eps_low:<X>%     — EPS beat below threshold
        price_low:<X>%   — day0 (or d1) return below threshold
        vol_low:<X>x     — volume below 2× avg20
    """
    featured = compute_features(prices_df)
    featured['date'] = pd.to_datetime(featured['date'])
    events = events_df.copy()
    events['earnings_date'] = pd.to_datetime(events['earnings_date'])

    merged = events.merge(featured, left_on='earnings_date', right_on='date', how='inner')
    if merged.empty:
        return 'no_price_match'

    merged = merged[merged['eps_estimate'].abs() > 0.001].copy()
    if merged.empty:
        return 'zero_estimate'

    merged['eps_beat_pct'] = (
        (merged['eps_actual'] - merged['eps_estimate'])
        / merged['eps_estimate'].abs() * 100
    )
    row = merged.iloc[0]

    if row['eps_beat_pct'] < EPS_BEAT_MIN_PCT:
        return f'eps_low:{row["eps_beat_pct"]:.1f}%'

    # EPS passed — diagnose price/volume gate
    if trigger_type == 'd0':
        parts = []
        if row['day0_ret'] < DAY0_RET_MIN:
            parts.append(f'price_low:{row["day0_ret"] * 100:.1f}%')
        avg_vol = row['avg20_vol'] if row['avg20_vol'] > 0 else float('nan')
        vol_ratio = row['volume'] / avg_vol if avg_vol == avg_vol else 0
        if vol_ratio < VOLUME_MULT:
            parts.append(f'vol_low:{vol_ratio:.1f}x')
        return '+'.join(parts) if parts else 'unknown'
    else:  # d1
        d1_ret = (row['d1_close'] - row['prior_close']) / row['prior_close'] if row['prior_close'] else 0
        avg_vol = row['avg20_vol'] if row['avg20_vol'] > 0 else float('nan')
        vol_ratio = row['d1_volume'] / avg_vol if avg_vol == avg_vol else 0
        parts = []
        if d1_ret < DAY0_RET_MIN:
            parts.append(f'price_low:{d1_ret * 100:.1f}%')
        if vol_ratio < VOLUME_MULT:
            parts.append(f'vol_low:{vol_ratio:.1f}x')
        return '+'.join(parts) if parts else 'unknown'
