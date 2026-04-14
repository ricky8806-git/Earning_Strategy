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
