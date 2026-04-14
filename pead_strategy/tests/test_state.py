# pead_strategy/tests/test_state.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
from unittest.mock import patch


def test_save_and_load_round_trip(tmp_path):
    state_file = str(tmp_path / 'state.json')
    trades = pd.DataFrame([{
        'symbol':      'AAPL',
        'entry_date':  '2024-01-15',
        'entry_price': 185.20,
        'eps_beat_pct': 12.5,
        'earnings_date': '2024-01-14',
    }])

    with patch('state.STATE_FILE', state_file):
        from state import save_state, load_state
        save_state(trades)
        loaded = load_state()

    assert len(loaded) == 1
    assert loaded.iloc[0]['symbol']      == 'AAPL'
    assert float(loaded.iloc[0]['entry_price']) == pytest.approx(185.20)


def test_load_state_missing_file_returns_empty(tmp_path):
    state_file = str(tmp_path / 'missing.json')
    with patch('state.STATE_FILE', state_file):
        from state import load_state
        result = load_state()

    assert result.empty
    assert 'symbol' in result.columns


def test_save_state_persists_multiple_trades(tmp_path):
    state_file = str(tmp_path / 'state.json')
    trades = pd.DataFrame([
        {'symbol': 'AAPL', 'entry_date': '2024-01-10',
         'entry_price': 180.0, 'eps_beat_pct': 11.0, 'earnings_date': '2024-01-09'},
        {'symbol': 'MSFT', 'entry_date': '2024-01-12',
         'entry_price': 395.0, 'eps_beat_pct': 14.0, 'earnings_date': '2024-01-11'},
    ])
    with patch('state.STATE_FILE', state_file):
        from state import save_state, load_state
        save_state(trades)
        loaded = load_state()

    assert len(loaded) == 2
    assert set(loaded['symbol'].tolist()) == {'AAPL', 'MSFT'}


def test_save_empty_state(tmp_path):
    state_file = str(tmp_path / 'state.json')
    empty = pd.DataFrame(columns=['symbol', 'entry_date', 'entry_price', 'eps_beat_pct', 'earnings_date'])
    with patch('state.STATE_FILE', state_file):
        from state import save_state, load_state
        save_state(empty)
        loaded = load_state()

    assert loaded.empty
