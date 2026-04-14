# pead_strategy/tests/test_signals.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
import numpy as np


def _make_prices(n=25, base_close=100.0, base_vol=1_000_000):
    """Return n rows of synthetic daily prices with incrementing values."""
    dates = pd.bdate_range('2024-01-02', periods=n)
    return pd.DataFrame({
        'date':   dates.date,
        'open':   np.linspace(base_close - 1, base_close + n - 2, n),
        'high':   np.linspace(base_close + 4, base_close + n + 3, n),
        'low':    np.linspace(base_close - 5, base_close + n - 6, n),
        'close':  np.linspace(base_close, base_close + n - 1, n),
        'volume': [base_vol + i * 10_000 for i in range(n)],
    })


def test_compute_features_adds_required_columns():
    from signals import compute_features
    prices = _make_prices(25)
    result = compute_features(prices)

    required = ['prior_close', 'avg20_vol', 'day0_ret', 'd1_close',
                'd1_volume', 'd1_open', 'd2_open', 'd1_date', 'd2_date']
    for col in required:
        assert col in result.columns, f"Missing column: {col}"


def test_compute_features_prior_close_is_previous_close():
    from signals import compute_features
    prices = _make_prices(5)
    result = compute_features(prices)

    # Row 1 prior_close should equal row 0 close
    assert result['prior_close'].iloc[1] == pytest.approx(prices['close'].iloc[0])


def test_compute_features_avg20_vol_nan_for_first_20():
    from signals import compute_features
    prices = _make_prices(25)
    result = compute_features(prices)

    # First 20 rows cannot have a full 20-day lookback
    for i in range(20):
        assert pd.isna(result['avg20_vol'].iloc[i]), f"Row {i} avg20_vol should be NaN"
    # Row 21 (index 20) has 20 days of prior history
    assert pd.notna(result['avg20_vol'].iloc[20])


def test_compute_features_day0_ret_formula():
    from signals import compute_features
    prices = _make_prices(5)
    result = compute_features(prices)

    expected = (prices['close'].iloc[2] - prices['close'].iloc[1]) / prices['close'].iloc[1]
    assert result['day0_ret'].iloc[2] == pytest.approx(expected)


def test_compute_features_d1_close_is_next_row():
    from signals import compute_features
    prices = _make_prices(5)
    result = compute_features(prices)

    # d1_close at row 1 should equal close at row 2
    assert result['d1_close'].iloc[1] == pytest.approx(prices['close'].iloc[2])


def test_compute_features_d1_date_is_next_date():
    from signals import compute_features
    prices = _make_prices(5)
    result = compute_features(prices)

    # d1_date at row 0 should be the date of row 1
    assert result['d1_date'].iloc[0] == pd.Timestamp(prices['date'].iloc[1])


def _make_earnings_event(earnings_date, eps_estimate, eps_actual):
    surprise_pct = (
        (eps_actual - eps_estimate) / abs(eps_estimate) * 100
        if eps_estimate != 0.0 else float('nan')
    )
    return pd.DataFrame([{
        'symbol':       'TEST',
        'earnings_date': earnings_date,
        'eps_estimate':  eps_estimate,
        'eps_actual':    eps_actual,
        'surprise_pct':  surprise_pct,
    }])


def _make_prices_with_big_move(earnings_idx=21, n=25):
    """
    25 days of prices. On earnings_idx, close jumps >3% from prior_close
    and volume is 3x the prior average.
    """
    dates = pd.bdate_range('2024-01-02', periods=n)
    closes = [100.0] * n
    closes[earnings_idx] = 104.5          # +4.5% from prior close of 100
    opens  = [c - 0.5 for c in closes]
    volumes = [1_000_000] * n
    volumes[earnings_idx] = 3_000_000     # 3x average
    volumes[earnings_idx + 1] = 2_500_000 # high D+1 volume too
    return pd.DataFrame({
        'date':   dates.date,
        'open':   opens,
        'high':   [c + 2 for c in closes],
        'low':    [c - 2 for c in closes],
        'close':  closes,
        'volume': volumes,
    })


def test_build_signals_returns_correct_columns():
    from signals import build_signals
    prices = _make_prices_with_big_move()
    dates  = pd.bdate_range('2024-01-02', periods=25)
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.15)

    result = build_signals(events, prices)
    assert list(result.columns) == ['symbol', 'earnings_date', 'entry_date', 'entry_open', 'eps_beat_pct', 'trigger_day']


def test_build_signals_d0_trigger_captured():
    from signals import build_signals
    prices = _make_prices_with_big_move(earnings_idx=21)
    dates  = pd.bdate_range('2024-01-02', periods=25)
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.15)

    result = build_signals(events, prices)
    assert len(result) == 1
    assert result.iloc[0]['trigger_day'] == 'd0'
    assert result.iloc[0]['eps_beat_pct'] == pytest.approx(15.0)


def test_build_signals_d1_trigger_when_d0_flat():
    """D0 flat but D1 closes +3%+ over pre-earnings close. Entry on D+2 open."""
    from signals import build_signals
    dates  = pd.bdate_range('2024-01-02', periods=25)
    closes = [100.0] * 25
    closes[21] = 100.5   # D0: only +0.5%, does NOT trigger D0
    closes[22] = 104.2   # D1: +4.2% from pre-earnings 100 -> triggers D1
    volumes = [1_000_000] * 25
    volumes[21] = 900_000   # D0 volume low
    volumes[22] = 2_500_000 # D1 volume is 2.5x average

    prices = pd.DataFrame({
        'date':   dates.date,
        'open':   [c - 0.5 for c in closes],
        'high':   [c + 2   for c in closes],
        'low':    [c - 2   for c in closes],
        'close':  closes,
        'volume': volumes,
    })
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.12)

    result = build_signals(events, prices)
    assert len(result) == 1
    assert result.iloc[0]['trigger_day'] == 'd1'


def test_build_signals_rejects_insufficient_eps_beat():
    from signals import build_signals
    prices = _make_prices_with_big_move()
    dates  = pd.bdate_range('2024-01-02', periods=25)
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.05)  # only 5%

    result = build_signals(events, prices)
    assert result.empty


def test_build_signals_rejects_zero_eps_estimate():
    from signals import build_signals
    prices = _make_prices_with_big_move()
    dates  = pd.bdate_range('2024-01-02', periods=25)
    events = _make_earnings_event(dates[21].date(), eps_estimate=0.0, eps_actual=0.15)

    result = build_signals(events, prices)
    assert result.empty


def test_build_signals_rejects_low_volume():
    from signals import build_signals
    dates  = pd.bdate_range('2024-01-02', periods=25)
    closes = [100.0] * 25
    closes[21] = 104.5  # +4.5% -- would trigger on return alone
    volumes = [1_000_000] * 25
    volumes[21] = 1_500_000  # Only 1.5x -- below 2x threshold

    prices = pd.DataFrame({
        'date':   dates.date,
        'open':   [c - 0.5 for c in closes],
        'high':   [c + 2   for c in closes],
        'low':    [c - 2   for c in closes],
        'close':  closes,
        'volume': volumes,
    })
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.15)

    result = build_signals(events, prices)
    assert result.empty


def test_build_signals_entry_date_is_d1_for_d0_trigger():
    from signals import build_signals
    prices = _make_prices_with_big_move(earnings_idx=21)
    dates  = pd.bdate_range('2024-01-02', periods=25)
    events = _make_earnings_event(dates[21].date(), eps_estimate=1.00, eps_actual=1.15)

    result = build_signals(events, prices)
    assert len(result) == 1
    # Entry date should be the day AFTER earnings (D+1), confirmed as D0 trigger
    assert result.iloc[0]['entry_date'] == pd.Timestamp(dates[22])
