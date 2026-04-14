# pead_strategy/tests/test_portfolio.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
from datetime import date


def _make_trades(*entries):
    """entries: list of (symbol, entry_date_str) tuples."""
    return pd.DataFrame([
        {'symbol': sym, 'entry_date': ed, 'entry_price': 100.0,
         'stop_price': 90.0, 'eps_beat_pct': 12.0, 'earnings_date': ed}
        for sym, ed in entries
    ])


# ---- get_portfolio_weights ----

def test_weights_single_position():
    from portfolio import get_portfolio_weights
    w = get_portfolio_weights(['AAPL'])
    assert w['AAPL'] == pytest.approx(0.07)
    assert w['SPY']  == pytest.approx(0.93)
    assert sum(w.values()) == pytest.approx(1.0)


def test_weights_10_positions():
    from portfolio import get_portfolio_weights
    syms = [f'S{i}' for i in range(10)]
    w = get_portfolio_weights(syms)
    for s in syms:
        assert w[s] == pytest.approx(0.07)
    assert w['SPY'] == pytest.approx(0.30)
    assert sum(w.values()) == pytest.approx(1.0)


def test_weights_14_positions_spy_near_zero():
    from portfolio import get_portfolio_weights
    syms = [f'S{i}' for i in range(14)]
    w = get_portfolio_weights(syms)
    # 14 * 7% = 98%, SPY = 2%
    assert w['SPY'] == pytest.approx(0.02, abs=1e-6)


def test_weights_15_positions_equal_weight_no_spy():
    from portfolio import get_portfolio_weights
    syms = [f'S{i}' for i in range(15)]
    w = get_portfolio_weights(syms)
    for s in syms:
        assert w[s] == pytest.approx(1/15, rel=1e-5)
    assert 'SPY' not in w


def test_weights_empty_all_spy():
    from portfolio import get_portfolio_weights
    w = get_portfolio_weights([])
    assert w == {'SPY': 1.0}


# ---- get_active_positions ----

def test_get_active_positions_returns_symbols():
    from portfolio import get_active_positions
    trades = _make_trades(('AAPL', '2024-01-10'), ('MSFT', '2024-01-11'))
    result = get_active_positions(trades, date(2024, 1, 15))
    assert set(result) == {'AAPL', 'MSFT'}


def test_get_active_positions_empty_trades():
    from portfolio import get_active_positions
    trades = pd.DataFrame(columns=['symbol', 'entry_date', 'entry_price', 'eps_beat_pct', 'earnings_date'])
    result = get_active_positions(trades, date(2024, 1, 15))
    assert result == []



# ---- check_exits (updated: prices_dict + [{symbol, reason}] return) ----

def _make_prices_dict(symbol, closes, start_date='2024-01-10'):
    """Build a minimal {symbol: prices_df} dict for check_exits tests."""
    dates = pd.bdate_range(start_date, periods=len(closes))
    df = pd.DataFrame({'date': dates.date, 'close': closes})
    return {symbol: df}


def test_check_exits_time_exit_triggers_after_20_trading_days():
    import pandas_market_calendars as mcal
    from portfolio import check_exits

    nyse     = mcal.get_calendar('NYSE')
    start    = date(2024, 1, 10)
    schedule = nyse.schedule(start_date=str(start), end_date='2024-03-31')
    exit_day = schedule.index[20].date()

    trades      = _make_trades(('AAPL', str(start)))
    prices_dict = _make_prices_dict('AAPL', [95.0] * 25)  # above stop_price=90
    result      = check_exits(trades, prices_dict, exit_day)

    symbols = [e['symbol'] for e in result]
    reasons = [e['reason']  for e in result]
    assert 'AAPL' in symbols
    assert reasons[symbols.index('AAPL')] == 'time'


def test_check_exits_time_exit_does_not_trigger_early():
    import pandas_market_calendars as mcal
    from portfolio import check_exits

    nyse     = mcal.get_calendar('NYSE')
    start    = date(2024, 1, 10)
    schedule = nyse.schedule(start_date=str(start), end_date='2024-03-31')
    early    = schedule.index[10].date()

    trades      = _make_trades(('AAPL', str(start)))
    prices_dict = _make_prices_dict('AAPL', [95.0] * 15)
    result      = check_exits(trades, prices_dict, early)

    symbols = [e['symbol'] for e in result]
    assert 'AAPL' not in symbols


def test_check_exits_stop_loss_triggers_when_close_at_or_below_stop():
    from portfolio import check_exits

    start       = date(2024, 1, 10)
    trades      = _make_trades(('AAPL', str(start)))
    # stop_price=90.0 in _make_trades; close=89.0 triggers stop
    prices_dict = _make_prices_dict('AAPL', [100.0] * 5 + [89.0], start_date='2024-01-10')
    as_of       = list(prices_dict['AAPL']['date'])[-1]

    result  = check_exits(trades, prices_dict, as_of)
    symbols = [e['symbol'] for e in result]
    reasons = [e['reason']  for e in result]
    assert 'AAPL' in symbols
    assert reasons[symbols.index('AAPL')] == 'stop_loss'


def test_check_exits_stop_loss_does_not_trigger_above_stop():
    from portfolio import check_exits

    start       = date(2024, 1, 10)
    trades      = _make_trades(('AAPL', str(start)))
    # close=91.0 > stop_price=90.0 — should NOT trigger
    prices_dict = _make_prices_dict('AAPL', [100.0] * 5 + [91.0], start_date='2024-01-10')
    as_of       = list(prices_dict['AAPL']['date'])[-1]

    result  = check_exits(trades, prices_dict, as_of)
    symbols = [e['symbol'] for e in result]
    assert 'AAPL' not in symbols


def test_check_exits_stop_loss_triggers_exactly_at_stop_price():
    from portfolio import check_exits

    start       = date(2024, 1, 10)
    trades      = _make_trades(('AAPL', str(start)))
    # close exactly equals stop_price=90.0
    prices_dict = _make_prices_dict('AAPL', [100.0] * 5 + [90.0], start_date='2024-01-10')
    as_of       = list(prices_dict['AAPL']['date'])[-1]

    result  = check_exits(trades, prices_dict, as_of)
    symbols = [e['symbol'] for e in result]
    assert 'AAPL' in symbols


def test_check_exits_empty_trades_returns_empty_list():
    from portfolio import check_exits
    trades = pd.DataFrame(columns=['symbol', 'entry_date', 'entry_price',
                                   'stop_price', 'eps_beat_pct', 'earnings_date'])
    result = check_exits(trades, {}, date(2024, 3, 1))
    assert result == []
