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
