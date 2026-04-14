# pead_strategy/tests/test_data.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
from unittest.mock import patch


def test_get_sp500_symbols_filters_to_target_sectors():
    mock_df = pd.DataFrame({
        'Symbol': ['AAPL', 'JPM', 'JNJ', 'XOM', 'MSFT', 'UNH'],
        'Name':   ['Apple', 'JPMorgan', 'J&J', 'Exxon', 'Microsoft', 'UnitedHealth'],
        'Sector': [
            'Information Technology',
            'Financials',
            'Health Care',
            'Energy',
            'Information Technology',
            'Health Care',
        ],
    })
    with patch('data.pd.read_csv', return_value=mock_df):
        from data import get_sp500_symbols
        result = get_sp500_symbols()

    assert set(result) == {'AAPL', 'JPM', 'JNJ', 'MSFT', 'UNH'}
    assert 'XOM' not in result


def test_get_sp500_symbols_returns_list():
    mock_df = pd.DataFrame({
        'Symbol': ['AAPL'],
        'Name':   ['Apple'],
        'Sector': ['Information Technology'],
    })
    with patch('data.pd.read_csv', return_value=mock_df):
        from data import get_sp500_symbols
        result = get_sp500_symbols()

    assert isinstance(result, list)


def _make_mock_ohlcv():
    """Helper: 3-row yfinance-style DataFrame with DatetimeIndex."""
    idx = pd.to_datetime(['2024-01-10', '2024-01-11', '2024-01-12'])
    idx.name = 'Date'
    return pd.DataFrame({
        'Open':   [100.0, 101.0, 102.0],
        'High':   [105.0, 106.0, 107.0],
        'Low':    [99.0,  100.0, 101.0],
        'Close':  [103.0, 104.0, 105.0],
        'Volume': [1_000_000, 1_100_000, 1_200_000],
    }, index=idx)


def test_get_prices_returns_clean_columns():
    with patch('data.yf.download', return_value=_make_mock_ohlcv()):
        from data import get_prices
        result = get_prices('AAPL', '2024-01-10', '2024-01-12')

    assert list(result.columns) == ['date', 'open', 'high', 'low', 'close', 'volume']
    assert len(result) == 3
    assert result['close'].iloc[0] == 103.0


def test_get_prices_date_column_is_date_type():
    import datetime
    with patch('data.yf.download', return_value=_make_mock_ohlcv()):
        from data import get_prices
        result = get_prices('AAPL', '2024-01-10', '2024-01-12')

    assert isinstance(result['date'].iloc[0], datetime.date)


def test_get_prices_empty_returns_empty_df():
    with patch('data.yf.download', return_value=pd.DataFrame()):
        from data import get_prices
        result = get_prices('FAKE', '2024-01-01', '2024-01-31')

    assert result.empty
    assert list(result.columns) == ['date', 'open', 'high', 'low', 'close', 'volume']


def test_get_spy_prices_calls_get_prices_with_spy():
    with patch('data.yf.download', return_value=_make_mock_ohlcv()) as mock_dl:
        from data import get_spy_prices
        get_spy_prices('2024-01-10', '2024-01-12')

    mock_dl.assert_called_once()
    call_args = mock_dl.call_args
    assert call_args[0][0] == 'SPY'


from unittest.mock import MagicMock


def _make_mock_earnings_df():
    """Simulate yfinance Ticker.earnings_dates output."""
    idx = pd.DatetimeIndex([
        pd.Timestamp('2024-01-29 10:30:00', tz='America/New_York'),
        pd.Timestamp('2023-10-26 10:30:00', tz='America/New_York'),
    ])
    idx.name = 'Earnings Date'
    return pd.DataFrame({
        'EPS Estimate':  [2.10, 1.41],
        'Reported EPS':  [2.18, 1.46],
        'Surprise(%)':   [3.81, 3.55],
    }, index=idx)


def test_get_earnings_returns_standard_columns():
    mock_ticker = MagicMock()
    mock_ticker.earnings_dates = _make_mock_earnings_df()

    with patch('data.yf.Ticker', return_value=mock_ticker):
        from data import get_earnings
        result = get_earnings('AAPL')

    assert list(result.columns) == ['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']
    assert len(result) == 2


def test_get_earnings_strips_timezone():
    import datetime
    mock_ticker = MagicMock()
    mock_ticker.earnings_dates = _make_mock_earnings_df()

    with patch('data.yf.Ticker', return_value=mock_ticker):
        from data import get_earnings
        result = get_earnings('AAPL')

    assert isinstance(result['earnings_date'].iloc[0], datetime.date)


def test_get_earnings_drops_rows_with_missing_eps():
    idx = pd.DatetimeIndex([
        pd.Timestamp('2024-01-29 10:30:00', tz='America/New_York'),
        pd.Timestamp('2023-10-26 10:30:00', tz='America/New_York'),
    ])
    idx.name = 'Earnings Date'
    df_with_nan = pd.DataFrame({
        'EPS Estimate': [2.10, None],
        'Reported EPS': [2.18, None],
        'Surprise(%)':  [3.81, None],
    }, index=idx)

    mock_ticker = MagicMock()
    mock_ticker.earnings_dates = df_with_nan

    with patch('data.yf.Ticker', return_value=mock_ticker):
        from data import get_earnings
        result = get_earnings('AAPL')

    assert len(result) == 1


def test_get_earnings_returns_empty_on_none():
    mock_ticker = MagicMock()
    mock_ticker.earnings_dates = None

    with patch('data.yf.Ticker', return_value=mock_ticker):
        from data import get_earnings
        result = get_earnings('FAKE')

    assert result.empty
    assert list(result.columns) == ['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']
