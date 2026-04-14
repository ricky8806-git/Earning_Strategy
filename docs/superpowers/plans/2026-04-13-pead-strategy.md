# PEAD Trading Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a live PEAD (Post-Earnings Announcement Drift) trading strategy that scans S&P 500 IT/Health Care/Financials stocks for earnings beats, enters positions at next-day open, holds 20 trading days, and keeps the remainder in SPY.

**Architecture:** Six focused modules with single responsibilities — data.py fetches raw data, signals.py detects qualifying events, portfolio.py calculates weights, broker.py executes via Alpaca, state.py persists open trades, and main.py orchestrates the daily run. scheduler.py triggers main.py at market open via a timezone-aware loop.

**Tech Stack:** Python 3.10+, yfinance 0.2.x, pandas, pandas-market-calendars (NYSE calendar), alpaca-py (paper trading), schedule, pytz, pytest, pytest-mock

---

## File Map

| File | Responsibility |
|---|---|
| `pead_strategy/config.py` | All constants and API credentials |
| `pead_strategy/data.py` | S&P 500 universe, OHLCV prices, earnings data |
| `pead_strategy/signals.py` | Feature computation, signal filtering |
| `pead_strategy/portfolio.py` | Active position tracking, weight allocation, exit detection |
| `pead_strategy/state.py` | Load/save open trades to state.json |
| `pead_strategy/broker.py` | Alpaca account, orders, rebalance execution |
| `pead_strategy/main.py` | Daily orchestration runner |
| `pead_strategy/scheduler.py` | Timezone-aware daily trigger (9:31 AM ET + 6:00 PM ET) |
| `pead_strategy/backtest_validate.py` | Replay last 30 days, print signals found |
| `pead_strategy/state.json` | Persisted open trades (created on first run) |
| `pead_strategy/trades_log.csv` | Append-only audit trail |
| `pead_strategy/requirements.txt` | Pinned dependencies |
| `pead_strategy/tests/test_data.py` | Unit tests for data.py |
| `pead_strategy/tests/test_signals.py` | Unit tests for signals.py |
| `pead_strategy/tests/test_portfolio.py` | Unit tests for portfolio.py |
| `pead_strategy/tests/test_state.py` | Unit tests for state.py |
| `pead_strategy/tests/test_broker.py` | Unit tests for broker.py (all mocked) |
| `README.md` | Setup and run instructions |

---

## Task 1: Project Scaffold + Config

**Files:**
- Create: `pead_strategy/config.py`
- Create: `pead_strategy/requirements.txt`
- Create: `pead_strategy/tests/__init__.py`
- Create: `pead_strategy/__init__.py`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p pead_strategy/tests
touch pead_strategy/__init__.py pead_strategy/tests/__init__.py
```

- [ ] **Step 2: Write requirements.txt**

```
yfinance>=0.2.37
pandas>=2.0.0
pandas-market-calendars>=4.3.3
alpaca-py>=0.25.0
schedule>=1.2.2
pytz>=2024.1
requests>=2.31.0
pytest>=7.4.0
pytest-mock>=3.12.0
```

- [ ] **Step 3: Install dependencies**

```bash
pip install -r pead_strategy/requirements.txt
```

Expected: All packages install without error.

- [ ] **Step 4: Write config.py**

```python
# pead_strategy/config.py
ALPACA_API_KEY    = "PK7DIKPAX7ROEFTQDIVVDX74FC"
ALPACA_SECRET_KEY = "G1trxNG5J5P9QzeinvbCLogG6g4TYQibfGqhfVTQS1hp"
ALPACA_BASE_URL   = "https://paper-api.alpaca.markets"

MAX_POSITION_PCT  = 0.07
HOLD_DAYS         = 20
EPS_BEAT_MIN_PCT  = 10.0
DAY0_RET_MIN      = 0.03
VOLUME_MULT       = 2.0

TARGET_SECTORS = ["Information Technology", "Health Care", "Financials"]
SP500_URL      = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
LOG_FILE       = "trades_log.csv"
STATE_FILE     = "state.json"
```

- [ ] **Step 5: Verify import**

```bash
cd pead_strategy && python -c "from config import MAX_POSITION_PCT; print(MAX_POSITION_PCT)"
```

Expected: `0.07`

- [ ] **Step 6: Commit**

```bash
git add pead_strategy/
git commit -m "feat: scaffold PEAD strategy project with config and requirements"
```

---

## Task 2: Data Layer — S&P 500 Universe

**Files:**
- Create: `pead_strategy/data.py`
- Create: `pead_strategy/tests/test_data.py`

- [ ] **Step 1: Write failing test for get_sp500_symbols**

```python
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
```

- [ ] **Step 2: Run test to confirm failure**

```bash
cd pead_strategy && pytest tests/test_data.py::test_get_sp500_symbols_filters_to_target_sectors -v
```

Expected: `ImportError` or `ModuleNotFoundError` — data.py does not exist yet.

- [ ] **Step 3: Implement get_sp500_symbols in data.py**

```python
# pead_strategy/data.py
import pandas as pd
import yfinance as yf
from config import TARGET_SECTORS, SP500_URL


def get_sp500_symbols(sectors=None):
    """Fetch S&P 500 constituents from GitHub CSV and filter to target sectors."""
    if sectors is None:
        sectors = TARGET_SECTORS
    df = pd.read_csv(SP500_URL)
    return df[df['Sector'].isin(sectors)]['Symbol'].tolist()
```

- [ ] **Step 4: Run test to confirm pass**

```bash
cd pead_strategy && pytest tests/test_data.py -k "sp500" -v
```

Expected: 2 tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/data.py pead_strategy/tests/test_data.py
git commit -m "feat: add get_sp500_symbols with sector filter"
```

---

## Task 3: Data Layer — OHLCV Prices

**Files:**
- Modify: `pead_strategy/data.py`
- Modify: `pead_strategy/tests/test_data.py`

- [ ] **Step 1: Add failing tests for get_prices and get_spy_prices**

Append to `pead_strategy/tests/test_data.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_data.py -k "prices or spy" -v
```

Expected: All FAIL — functions not defined.

- [ ] **Step 3: Implement get_prices and get_spy_prices**

Append to `pead_strategy/data.py`:

```python
def get_prices(symbol, start, end):
    """Fetch daily OHLCV. Returns DataFrame with columns: date, open, high, low, close, volume."""
    df = yf.download(symbol, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        return pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    # Flatten MultiIndex columns that yfinance may produce for single-ticker downloads
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]

    df = df.reset_index()
    # Normalise the index column name (yfinance uses 'Date' or 'Datetime')
    for candidate in ('date', 'datetime'):
        if candidate in df.columns:
            df = df.rename(columns={candidate: 'date'})
            break

    df['date'] = pd.to_datetime(df['date']).dt.date
    return df[['date', 'open', 'high', 'low', 'close', 'volume']].reset_index(drop=True)


def get_spy_prices(start, end):
    """Convenience wrapper: prices for SPY."""
    return get_prices('SPY', start, end)
```

- [ ] **Step 4: Run tests to confirm pass**

```bash
cd pead_strategy && pytest tests/test_data.py -k "prices or spy" -v
```

Expected: 4 tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/data.py pead_strategy/tests/test_data.py
git commit -m "feat: add get_prices and get_spy_prices with MultiIndex handling"
```

---

## Task 4: Data Layer — Earnings

**Files:**
- Modify: `pead_strategy/data.py`
- Modify: `pead_strategy/tests/test_data.py`

- [ ] **Step 1: Add failing tests for get_earnings**

Append to `pead_strategy/tests/test_data.py`:

```python
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


def test_get_earnings_returns_empty_on_exception():
    mock_ticker = MagicMock()
    mock_ticker.earnings_dates = None  # yfinance can return None

    with patch('data.yf.Ticker', return_value=mock_ticker):
        from data import get_earnings
        result = get_earnings('FAKE')

    assert result.empty
    assert list(result.columns) == ['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_data.py -k "earnings" -v
```

Expected: All FAIL.

- [ ] **Step 3: Implement get_earnings**

Append to `pead_strategy/data.py`:

```python
_EARNINGS_EMPTY = pd.DataFrame(
    columns=['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']
)


def get_earnings(symbol):
    """Fetch earnings history from yfinance. Returns DataFrame with standardised columns."""
    ticker = yf.Ticker(symbol)
    try:
        df = ticker.earnings_dates
        if df is None or (hasattr(df, 'empty') and df.empty):
            return _EARNINGS_EMPTY.copy()

        df = df.reset_index()

        # Rename columns robustly regardless of exact yfinance column names
        col_map = {}
        for col in df.columns:
            lower = col.lower().strip()
            if 'date' in lower:
                col_map[col] = 'earnings_date'
            elif 'estimate' in lower:
                col_map[col] = 'eps_estimate'
            elif 'reported' in lower or 'actual' in lower:
                col_map[col] = 'eps_actual'
            elif 'surprise' in lower:
                col_map[col] = 'surprise_pct'
        df = df.rename(columns=col_map)

        # Strip timezone so .dt.date works cleanly
        df['earnings_date'] = (
            pd.to_datetime(df['earnings_date'])
            .dt.tz_localize(None)
            .dt.date
        )
        df = df.dropna(subset=['eps_estimate', 'eps_actual'])
        return df[['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']].reset_index(drop=True)

    except Exception:
        return _EARNINGS_EMPTY.copy()
```

- [ ] **Step 4: Run all data tests**

```bash
cd pead_strategy && pytest tests/test_data.py -v
```

Expected: All tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/data.py pead_strategy/tests/test_data.py
git commit -m "feat: add get_earnings with robust column normalisation"
```

---

## Task 5: Signal Engine — compute_features

**Files:**
- Create: `pead_strategy/signals.py`
- Create: `pead_strategy/tests/test_signals.py`

- [ ] **Step 1: Write failing tests**

```python
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
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_signals.py -v
```

Expected: All FAIL — signals.py does not exist.

- [ ] **Step 3: Implement compute_features**

```python
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
```

- [ ] **Step 4: Run tests to confirm pass**

```bash
cd pead_strategy && pytest tests/test_signals.py -k "compute_features" -v
```

Expected: All PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/signals.py pead_strategy/tests/test_signals.py
git commit -m "feat: add compute_features with no-lookahead avg20_vol and forward price columns"
```

---

## Task 6: Signal Engine — build_signals

**Files:**
- Modify: `pead_strategy/signals.py`
- Modify: `pead_strategy/tests/test_signals.py`

- [ ] **Step 1: Add failing tests for build_signals**

Append to `pead_strategy/tests/test_signals.py`:

```python
def _make_earnings_event(earnings_date, eps_estimate, eps_actual):
    return pd.DataFrame([{
        'symbol':       'TEST',
        'earnings_date': earnings_date,
        'eps_estimate':  eps_estimate,
        'eps_actual':    eps_actual,
        'surprise_pct':  (eps_actual - eps_estimate) / abs(eps_estimate) * 100,
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
    closes[22] = 104.2   # D1: +4.2% from pre-earnings 100 → triggers D1
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
    closes[21] = 104.5  # +4.5% — would trigger on return alone
    volumes = [1_000_000] * 25
    volumes[21] = 1_500_000  # Only 1.5x — below 2x threshold

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
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_signals.py -k "build_signals" -v
```

Expected: All FAIL — build_signals not defined.

- [ ] **Step 3: Implement build_signals**

Append to `pead_strategy/signals.py`:

```python
_SIGNALS_COLS = ['symbol', 'earnings_date', 'entry_date', 'entry_open', 'eps_beat_pct', 'trigger_day']


def _empty_signals():
    return pd.DataFrame(columns=_SIGNALS_COLS)


def build_signals(events_df, prices_df):
    """
    Merge earnings events onto enriched price features and apply all entry filters.

    events_df must have columns: symbol, earnings_date, eps_estimate, eps_actual
    prices_df must have columns: date, open, high, low, close, volume

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
    return result[_SIGNALS_COLS].reset_index(drop=True)
```

- [ ] **Step 4: Run all signal tests**

```bash
cd pead_strategy && pytest tests/test_signals.py -v
```

Expected: All PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/signals.py pead_strategy/tests/test_signals.py
git commit -m "feat: add build_signals with D0/D1 triggers, EPS beat, and volume filters"
```

---

## Task 7: Portfolio Engine

**Files:**
- Create: `pead_strategy/portfolio.py`
- Create: `pead_strategy/tests/test_portfolio.py`

- [ ] **Step 1: Write failing tests**

```python
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
         'eps_beat_pct': 12.0, 'earnings_date': ed}
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


# ---- check_exits ----

def test_check_exits_triggers_after_20_trading_days():
    import pandas_market_calendars as mcal
    from portfolio import check_exits

    nyse     = mcal.get_calendar('NYSE')
    start    = date(2024, 1, 10)
    schedule = nyse.schedule(start_date=str(start), end_date='2024-03-31')
    # 21st row (index 20) = 20 trading days have elapsed since entry
    exit_day = schedule.index[20].date()

    trades = _make_trades(('AAPL', str(start)))
    result = check_exits(trades, exit_day)
    assert 'AAPL' in result


def test_check_exits_does_not_trigger_early():
    import pandas_market_calendars as mcal
    from portfolio import check_exits

    nyse     = mcal.get_calendar('NYSE')
    start    = date(2024, 1, 10)
    schedule = nyse.schedule(start_date=str(start), end_date='2024-03-31')
    early    = schedule.index[10].date()  # Only 10 days elapsed

    trades = _make_trades(('AAPL', str(start)))
    result = check_exits(trades, early)
    assert 'AAPL' not in result


def test_check_exits_empty_trades():
    from portfolio import check_exits
    trades = pd.DataFrame(columns=['symbol', 'entry_date', 'entry_price', 'eps_beat_pct', 'earnings_date'])
    result = check_exits(trades, date(2024, 3, 1))
    assert result == []
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_portfolio.py -v
```

Expected: All FAIL.

- [ ] **Step 3: Implement portfolio.py**

```python
# pead_strategy/portfolio.py
import pandas as pd
import pandas_market_calendars as mcal
from config import MAX_POSITION_PCT, HOLD_DAYS

_NYSE = mcal.get_calendar('NYSE')


def get_portfolio_weights(active_symbols, max_pos=MAX_POSITION_PCT):
    """
    Return weight dict for active_symbols plus SPY fill.

    Rules:
      n == 0           → 100% SPY
      n <= 14          → min(7%, 1/n) per position; SPY fills remainder
      n > 14           → 1/n each; SPY = 0
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
    E.g. entry Monday, HOLD_DAYS=20 → exit on the open of the 21st trading day.
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
```

- [ ] **Step 4: Run all portfolio tests**

```bash
cd pead_strategy && pytest tests/test_portfolio.py -v
```

Expected: All PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/portfolio.py pead_strategy/tests/test_portfolio.py
git commit -m "feat: add portfolio weight allocation and NYSE-calendar exit detection"
```

---

## Task 8: State Management

**Files:**
- Create: `pead_strategy/state.py`
- Create: `pead_strategy/tests/test_state.py`

- [ ] **Step 1: Write failing tests**

```python
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
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_state.py -v
```

Expected: All FAIL.

- [ ] **Step 3: Implement state.py**

```python
# pead_strategy/state.py
import json
import pandas as pd
from pathlib import Path
from config import STATE_FILE

_COLUMNS = ['symbol', 'entry_date', 'entry_price', 'eps_beat_pct', 'earnings_date']


def load_state():
    """Load open trades from STATE_FILE. Returns empty DataFrame if file absent."""
    path = Path(STATE_FILE)
    if not path.exists():
        return pd.DataFrame(columns=_COLUMNS)

    with open(path) as f:
        data = json.load(f)

    trades = data.get('open_trades', [])
    if not trades:
        return pd.DataFrame(columns=_COLUMNS)

    return pd.DataFrame(trades)[_COLUMNS]


def save_state(trades_df):
    """Persist open trades to STATE_FILE."""
    records = []
    for row in trades_df.to_dict('records'):
        record = dict(row)
        # Ensure dates are ISO strings, not datetime objects
        for key in ('entry_date', 'earnings_date'):
            val = record.get(key)
            if val is not None and hasattr(val, 'isoformat'):
                record[key] = val.isoformat()
            elif val is not None:
                record[key] = str(val)
        records.append(record)

    with open(STATE_FILE, 'w') as f:
        json.dump({'open_trades': records}, f, indent=2, default=str)
```

- [ ] **Step 4: Run all state tests**

```bash
cd pead_strategy && pytest tests/test_state.py -v
```

Expected: All PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/state.py pead_strategy/tests/test_state.py
git commit -m "feat: add state management with load/save JSON persistence"
```

---

## Task 9: Broker Integration (Alpaca)

**Files:**
- Create: `pead_strategy/broker.py`
- Create: `pead_strategy/tests/test_broker.py`

- [ ] **Step 1: Write failing tests (all mocked — never hits real API)**

```python
# pead_strategy/tests/test_broker.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from unittest.mock import MagicMock, patch, call


def _mock_position(symbol, market_value):
    p = MagicMock()
    p.symbol       = symbol
    p.market_value = str(market_value)
    p.qty          = '10'
    return p


def _mock_account(cash='10000', portfolio_value='100000'):
    a = MagicMock()
    a.cash            = cash
    a.portfolio_value = portfolio_value
    return a


def test_get_account_returns_floats():
    mock_client = MagicMock()
    mock_client.get_account.return_value       = _mock_account('15000', '100000')
    mock_client.get_all_positions.return_value = []

    with patch('broker._get_client', return_value=mock_client):
        from broker import get_account
        result = get_account()

    assert result['cash']            == pytest.approx(15000.0)
    assert result['portfolio_value'] == pytest.approx(100000.0)
    assert result['positions']       == {}


def test_get_current_positions_maps_symbols():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = [
        _mock_position('AAPL', 7000),
        _mock_position('SPY',  93000),
    ]

    with patch('broker._get_client', return_value=mock_client):
        from broker import get_current_positions
        result = get_current_positions(client=mock_client)

    assert set(result.keys()) == {'AAPL', 'SPY'}


def test_place_order_buy_submits_correct_side():
    from alpaca.trading.enums import OrderSide
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import place_order
        place_order('AAPL', 'buy', 5000.0, client=mock_client)

    mock_client.submit_order.assert_called_once()
    req = mock_client.submit_order.call_args[0][0]
    assert req.side    == OrderSide.BUY
    assert req.notional == pytest.approx(5000.0)
    assert req.symbol  == 'AAPL'


def test_place_order_sell_submits_correct_side():
    from alpaca.trading.enums import OrderSide
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import place_order
        place_order('AAPL', 'sell', 3000.0, client=mock_client)

    req = mock_client.submit_order.call_args[0][0]
    assert req.side == OrderSide.SELL


def test_close_position_calls_alpaca():
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import close_position
        close_position('AAPL', client=mock_client)

    mock_client.close_position.assert_called_once_with('AAPL')


def test_rebalance_closes_positions_not_in_target():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = [_mock_position('AAPL', 7000)]

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'SPY': 1.0}, 100_000, client=mock_client)

    mock_client.close_position.assert_called_once_with('AAPL')


def test_rebalance_buys_new_target_position():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = []  # No existing positions

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'SPY': 0.93, 'AAPL': 0.07}, 100_000, client=mock_client)

    # Should have placed buy orders for both SPY and AAPL
    calls = mock_client.submit_order.call_args_list
    symbols_ordered = [c[0][0].symbol for c in calls]
    assert 'SPY'  in symbols_ordered
    assert 'AAPL' in symbols_ordered


def test_rebalance_skips_tiny_diff():
    """Differences under $10 should not trigger an order."""
    mock_client = MagicMock()
    # AAPL already at $7000, target is also ~$7000 (7% of $100k)
    mock_client.get_all_positions.return_value = [_mock_position('AAPL', 7000)]

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'AAPL': 0.07}, 100_000, client=mock_client)

    # Diff = 7000 - 7000 = 0, no order
    mock_client.submit_order.assert_not_called()
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
cd pead_strategy && pytest tests/test_broker.py -v
```

Expected: All FAIL.

- [ ] **Step 3: Implement broker.py**

```python
# pead_strategy/broker.py
from alpaca.trading.client  import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums   import OrderSide, TimeInForce
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY


def _get_client():
    return TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)


def get_account():
    """Return dict with cash, portfolio_value, and positions."""
    client = _get_client()
    acct   = client.get_account()
    return {
        'cash':            float(acct.cash),
        'portfolio_value': float(acct.portfolio_value),
        'positions':       get_current_positions(client=client),
    }


def get_current_positions(client=None):
    """Return {symbol: shares} for all open positions."""
    if client is None:
        client = _get_client()
    return {p.symbol: int(p.qty) for p in client.get_all_positions()}


def place_order(symbol, side, notional, client=None):
    """Place a market order by notional dollar amount."""
    if client is None:
        client = _get_client()
    req = MarketOrderRequest(
        symbol        = symbol,
        notional      = round(notional, 2),
        side          = OrderSide.BUY if side == 'buy' else OrderSide.SELL,
        time_in_force = TimeInForce.DAY,
    )
    return client.submit_order(req)


def close_position(symbol, client=None):
    """Close entire position in a symbol. Swallows errors (position may already be closed)."""
    if client is None:
        client = _get_client()
    try:
        client.close_position(symbol)
    except Exception as e:
        print(f"[broker] Warning: could not close {symbol}: {e}")


def rebalance(target_weights, portfolio_value, client=None):
    """
    Bring the portfolio in line with target_weights.

    1. Close positions not in target_weights.
    2. For each target position, buy/sell the difference between
       current market value and desired notional.
    """
    if client is None:
        client = _get_client()

    current = {p.symbol: float(p.market_value) for p in client.get_all_positions()}

    # Close positions that are no longer in the target
    for symbol in list(current.keys()):
        if symbol not in target_weights:
            close_position(symbol, client)

    # Adjust target positions
    for symbol, weight in target_weights.items():
        target_notional  = portfolio_value * weight
        current_notional = current.get(symbol, 0.0)
        diff             = target_notional - current_notional

        if diff > 10:            # Need to buy more
            place_order(symbol, 'buy',  diff, client)
        elif diff < -10:         # Need to sell some
            place_order(symbol, 'sell', abs(diff), client)
```

- [ ] **Step 4: Run all broker tests**

```bash
cd pead_strategy && pytest tests/test_broker.py -v
```

Expected: All PASSED.

- [ ] **Step 5: Commit**

```bash
git add pead_strategy/broker.py pead_strategy/tests/test_broker.py
git commit -m "feat: add Alpaca broker integration with position-diff rebalancing"
```

---

## Task 10: Main Runner

**Files:**
- Create: `pead_strategy/main.py`

- [ ] **Step 1: Write main.py**

```python
# pead_strategy/main.py
"""
Daily runner — designed to execute at 9:31 AM ET after market open.

Flow:
  1. Load state (open trades)
  2. Check exits → close expired positions
  3. Scan yesterday's earnings for new signals
  4. Merge new signals into open trades
  5. Calculate target weights
  6. Rebalance via Alpaca
  7. Save updated state
  8. Log all actions to trades_log.csv
"""
import csv
import logging
from datetime import date, timedelta

import pandas as pd

from broker    import close_position, get_account, rebalance
from data      import get_earnings, get_prices, get_sp500_symbols
from portfolio import check_exits, get_active_positions, get_portfolio_weights
from signals   import build_signals
from state     import load_state, save_state
from config    import LOG_FILE

logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)

_LOOKBACK_DAYS = 90   # Days of price history to fetch for signal computation


def _append_log(row_date, symbol, action, price, eps_beat, reason=''):
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([row_date, symbol, action, price, eps_beat, reason])


def run():
    today     = date.today()
    yesterday = today - timedelta(days=1)
    log.info(f"=== PEAD daily run: {today} ===")

    # 1. Load persisted state
    trades_df = load_state()
    log.info(f"Loaded {len(trades_df)} open trades")

    # 2. Exit positions past HOLD_DAYS
    exited = check_exits(trades_df, today)
    for sym in exited:
        log.info(f"EXIT  {sym} (20 trading days elapsed)")
        close_position(sym)
        trades_df = trades_df[trades_df['symbol'] != sym].reset_index(drop=True)
        _append_log(today, sym, 'EXIT', None, None)

    # 3. Scan yesterday's earnings for new signals
    symbols    = get_sp500_symbols()
    new_trades = []

    for sym in symbols:
        try:
            earnings = get_earnings(sym)
            if earnings.empty:
                continue

            # Keep only yesterday's announcement
            earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date'])
            recent = earnings[earnings['earnings_date'].dt.date == yesterday]
            if recent.empty:
                continue

            recent = recent.copy()
            recent['symbol'] = sym

            price_start = (yesterday - timedelta(days=_LOOKBACK_DAYS)).isoformat()
            price_end   = (today + timedelta(days=3)).isoformat()
            prices      = get_prices(sym, price_start, price_end)
            if prices.empty:
                log.warning(f"No price data for {sym}; skipping")
                _append_log(today, sym, 'SKIP', None, None, 'no_price_data')
                continue

            signals = build_signals(recent, prices)
            if signals.empty:
                _append_log(today, sym, 'SCAN_MISS', None,
                            recent['eps_actual'].iloc[0] if len(recent) else None,
                            'no_signal')
                continue

            for _, row in signals.iterrows():
                entry_date = row['entry_date']
                if hasattr(entry_date, 'date'):
                    entry_date = entry_date.date()
                new_trade = {
                    'symbol':        sym,
                    'entry_date':    str(entry_date),
                    'entry_price':   row['entry_open'],
                    'eps_beat_pct':  row['eps_beat_pct'],
                    'earnings_date': str(yesterday),
                }
                new_trades.append(new_trade)
                log.info(f"SIGNAL {sym}  eps_beat={row['eps_beat_pct']:.1f}%  trigger={row['trigger_day']}")
                _append_log(today, sym, 'ENTRY', row['entry_open'], row['eps_beat_pct'])

        except Exception as exc:
            log.warning(f"Error processing {sym}: {exc}")

    # 4. Append new trades to state
    if new_trades:
        new_df    = pd.DataFrame(new_trades)
        trades_df = pd.concat([trades_df, new_df], ignore_index=True)

    # 5. Calculate target weights
    active         = get_active_positions(trades_df, today)
    target_weights = get_portfolio_weights(active)
    log.info(f"Active positions: {active}")
    log.info(f"Target weights:   {target_weights}")

    # 6. Rebalance
    account         = get_account()
    portfolio_value = account['portfolio_value']
    rebalance(target_weights, portfolio_value)
    log.info(f"Rebalance complete (portfolio_value={portfolio_value:.2f})")

    # 7. Save state
    save_state(trades_df)
    log.info("State saved")


if __name__ == '__main__':
    run()
```

- [ ] **Step 2: Smoke-test import**

```bash
cd pead_strategy && python -c "import main; print('main.py imports OK')"
```

Expected: `main.py imports OK` (no ImportError).

- [ ] **Step 3: Commit**

```bash
git add pead_strategy/main.py
git commit -m "feat: add main daily runner with exit/scan/rebalance flow"
```

---

## Task 11: Scheduler

**Files:**
- Create: `pead_strategy/scheduler.py`

- [ ] **Step 1: Write scheduler.py**

```python
# pead_strategy/scheduler.py
"""
Timezone-aware scheduler.

  09:31 ET — run main.py (market open runner)
  18:00 ET — run a pre-scan to log potential next-day entries

Runs as a long-lived process; checks time every 30 seconds.
The loop approach is used (vs the 'schedule' library) because the schedule
library does not natively support timezone-aware scheduling.
"""
import subprocess
import sys
import time
from datetime import datetime

import pytz

ET = pytz.timezone('America/New_York')


def _et_now():
    return datetime.now(ET)


def _run_script(script):
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"[scheduler] {script} exited with code {result.returncode}")


def _prescan():
    """Evening pre-scan: log what signals would trigger tomorrow."""
    print(f"[scheduler] Evening pre-scan at {_et_now().strftime('%H:%M %Z')}")
    subprocess.run(
        [sys.executable, '-c',
         'from data import get_sp500_symbols; '
         'symbols = get_sp500_symbols(); '
         f'print(f"Pre-scan: {{len(symbols)}} symbols in universe")'],
        capture_output=False,
    )


def main():
    print(f"[scheduler] Started at {_et_now()}")
    print("[scheduler] Will run main.py at 09:31 ET and pre-scan at 18:00 ET")

    last_open_run  = None
    last_close_run = None

    while True:
        now   = _et_now()
        today = now.date()

        if now.hour == 9 and now.minute == 31 and last_open_run != today:
            print(f"[scheduler] Triggering market-open run at {now}")
            _run_script('main.py')
            last_open_run = today

        if now.hour == 18 and now.minute == 0 and last_close_run != today:
            print(f"[scheduler] Triggering evening pre-scan at {now}")
            _prescan()
            last_close_run = today

        time.sleep(30)


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Smoke-test import**

```bash
cd pead_strategy && python -c "import scheduler; print('scheduler.py imports OK')"
```

Expected: `scheduler.py imports OK`

- [ ] **Step 3: Commit**

```bash
git add pead_strategy/scheduler.py
git commit -m "feat: add timezone-aware ET scheduler for 9:31 and 18:00 triggers"
```

---

## Task 12: Backtest Validator

**Files:**
- Create: `pead_strategy/backtest_validate.py`

- [ ] **Step 1: Write backtest_validate.py**

```python
# pead_strategy/backtest_validate.py
"""
Replay the last N days using yfinance data and print all signals found.

Usage:
    python backtest_validate.py           # last 30 days (default)
    python backtest_validate.py --days 60

Compare the output to any research backtest CSV to validate that the live
signal engine reproduces the same signals.
"""
import argparse
from datetime import date, timedelta

import pandas as pd

from data    import get_earnings, get_prices, get_sp500_symbols
from signals import build_signals

_LOOKBACK_DAYS = 90   # Price history window for avg20_vol computation


def run_backtest(days: int = 30, output_csv: str = 'backtest_signals.csv'):
    end_date   = date.today()
    start_date = end_date - timedelta(days=days)
    price_from = start_date - timedelta(days=_LOOKBACK_DAYS)

    symbols = get_sp500_symbols()
    print(f"Backtest: {start_date} → {end_date}  |  {len(symbols)} symbols")

    all_signals    = []
    all_considered = []  # Includes rejected signals for auditability

    for i, sym in enumerate(symbols):
        if i % 20 == 0:
            print(f"  Progress: {i}/{len(symbols)} ...")

        try:
            earnings = get_earnings(sym)
            if earnings.empty:
                continue

            earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date'])
            window = earnings[
                (earnings['earnings_date'].dt.date >= start_date)
                & (earnings['earnings_date'].dt.date <= end_date)
            ]
            if window.empty:
                continue

            window = window.copy()
            window['symbol'] = sym

            prices = get_prices(sym, str(price_from), str(end_date + timedelta(days=5)))
            if prices.empty:
                continue

            signals = build_signals(window, prices)

            # Log all earnings events considered (including rejected)
            for _, row in window.iterrows():
                eps_est = row['eps_estimate']
                eps_act = row['eps_actual']
                beat    = ((eps_act - eps_est) / abs(eps_est) * 100) if abs(eps_est) > 0.001 else None
                matched = (
                    not signals.empty
                    and (signals['earnings_date'] == row['earnings_date']).any()
                )
                all_considered.append({
                    'symbol':        sym,
                    'earnings_date': row['earnings_date'].date(),
                    'eps_estimate':  eps_est,
                    'eps_actual':    eps_act,
                    'eps_beat_pct':  round(beat, 2) if beat is not None else None,
                    'signal':        matched,
                })

            if not signals.empty:
                all_signals.append(signals)

        except Exception as exc:
            print(f"  Warning {sym}: {exc}")

    # ---------- Results ----------
    considered_df = pd.DataFrame(all_considered)

    print(f"\n{'='*60}")
    print(f"Events scanned : {len(considered_df)}")
    print(f"Signals found  : {len(all_signals)}")

    if all_signals:
        result = pd.concat(all_signals, ignore_index=True).sort_values('earnings_date')
        print(f"\nSignals:\n{result[['symbol','earnings_date','entry_date','eps_beat_pct','trigger_day']].to_string(index=False)}")
        result.to_csv(output_csv, index=False)
        print(f"\nSaved to {output_csv}")
    else:
        print("No signals found in date range.")

    audit_csv = output_csv.replace('.csv', '_audit.csv')
    considered_df.to_csv(audit_csv, index=False)
    print(f"Audit trail saved to {audit_csv}")

    return all_signals


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PEAD backtest validator')
    parser.add_argument('--days', type=int, default=30, help='Days to look back (default: 30)')
    parser.add_argument('--output', default='backtest_signals.csv', help='Output CSV path')
    args = parser.parse_args()
    run_backtest(args.days, args.output)
```

- [ ] **Step 2: Smoke-test import**

```bash
cd pead_strategy && python -c "import backtest_validate; print('backtest_validate.py imports OK')"
```

Expected: `backtest_validate.py imports OK`

- [ ] **Step 3: Commit**

```bash
git add pead_strategy/backtest_validate.py
git commit -m "feat: add backtest_validate.py with audit trail for all signals considered"
```

---

## Task 13: Full Test Suite Run

- [ ] **Step 1: Run all tests**

```bash
cd pead_strategy && pytest tests/ -v --tb=short
```

Expected: All tests PASSED. No failures.

- [ ] **Step 2: Fix any failures before continuing**

If any test fails, read the error, identify the cause (type mismatch, missing import, column name inconsistency), fix the relevant file, and re-run until all pass.

- [ ] **Step 3: Commit any fixes**

```bash
git add -u
git commit -m "fix: resolve test failures from full suite run"
```

---

## Task 14: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README.md**

```markdown
# PEAD Trading Strategy

Live Post-Earnings Announcement Drift strategy for S&P 500 IT / Health Care / Financials stocks.

## Strategy Summary

- **Universe**: S&P 500 — Information Technology, Health Care, Financials sectors only
- **Entry**: EPS beats estimate by ≥10%, Day-0 or Day-1 return vs prior close ≥+3%, volume ≥2× 20-day average → enter at next open
- **Sizing**: Equal weight, capped at 7% per position; remainder in SPY at all times
- **Exit**: 20 NYSE trading days after entry (NYSE calendar)
- **Broker**: Alpaca (paper mode by default)

## Setup

### 1. Install dependencies

```bash
cd pead_strategy
pip install -r requirements.txt
```

### 2. Configure API keys

Edit `config.py` and update:

```python
ALPACA_API_KEY    = "your_paper_api_key"
ALPACA_SECRET_KEY = "your_paper_secret_key"
```

Get paper trading credentials at: https://app.alpaca.markets → Paper Trading → API Keys

### 3. Run tests

```bash
pytest tests/ -v
```

All tests mock external dependencies — no API calls or market data needed.

## Running in Paper Mode

### One-time run (manual)

```bash
cd pead_strategy
python main.py
```

Runs the full daily cycle: check exits → scan earnings → rebalance.

### Scheduled (automatic)

```bash
cd pead_strategy
python scheduler.py
```

Runs as a long-lived process, triggering:
- **09:31 ET** — main.py (market open, place orders)
- **18:00 ET** — evening pre-scan log

Keep the terminal open (or run in a background service / screen session).

### Validate against backtest

```bash
cd pead_strategy
python backtest_validate.py --days 30
```

Outputs `backtest_signals.csv` (signals found) and `backtest_signals_audit.csv` (all events considered with rejection reasons). Compare against any research backtest CSV to confirm the live engine matches.

## Switching to Live Trading

1. In `config.py`, change `ALPACA_BASE_URL` to:
   ```python
   ALPACA_BASE_URL = "https://api.alpaca.markets"
   ```
2. In `broker.py`, change `_get_client()` to:
   ```python
   return TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=False)
   ```
3. Replace paper API keys with live API keys in `config.py`.

**Important**: Run in paper mode for at least 2–4 weeks and validate backtest_validate.py output before going live.

## File Reference

| File | Purpose |
|---|---|
| `config.py` | API keys, strategy parameters |
| `data.py` | S&P 500 universe, prices, earnings |
| `signals.py` | Feature computation, signal detection |
| `portfolio.py` | Position tracking, weight allocation, exit detection |
| `broker.py` | Alpaca order execution |
| `state.py` | Persist open trades to state.json |
| `main.py` | Daily orchestration |
| `scheduler.py` | Timezone-aware cron trigger |
| `backtest_validate.py` | Validate live signals vs backtest |
| `state.json` | Current open trades (auto-managed) |
| `trades_log.csv` | Append-only audit trail |
```

- [ ] **Step 2: Commit README**

```bash
git add README.md
git commit -m "docs: add README with setup, paper mode, and live switch instructions"
```

---

## Self-Review

### Spec Coverage

| Spec requirement | Task |
|---|---|
| get_sp500_symbols filtering IT/HC/Fin | Task 2 |
| get_earnings with earnings_date, eps_estimate, eps_actual, surprise_pct | Task 4 |
| get_prices / get_spy_prices with date, open, high, low, close, volume | Task 3 |
| compute_features with all 9 added columns | Task 5 |
| build_signals: EPS beat ≥10%, D0/D1 return ≥3%, volume ≥2× avg20 | Task 6 |
| Entry at next-day open after signal confirmed | Task 6 (entry_date = d1_date / d2_date) |
| Skip zero/negative EPS estimates | Task 6 (abs(eps_estimate) > 0.001 guard) |
| NYSE calendar trading-day counting | Task 7 (pandas_market_calendars) |
| Equal weight capped at 7%, SPY fills remainder | Task 7 |
| n > 14 → equal weight, SPY = 0 | Task 7 |
| 20 trading-day fixed hold | Task 7 |
| Alpaca get_account, get_current_positions, place_order, close_position, rebalance | Task 9 |
| rebalance: close not-in-target, resize target positions | Task 9 |
| Load/save state.json | Task 8 |
| Daily runner at 9:31 ET | Tasks 10, 11 |
| 6:00 PM ET pre-scan | Task 11 |
| Log all actions to trades_log.csv | Task 10 |
| Reject rejected signals with reason | Tasks 10, 12 |
| backtest_validate.py replaying last 30 days | Task 12 |
| README with setup/paper/live instructions | Task 14 |

No gaps found.

### Type Consistency Check

- `compute_features` returns `d1_date` / `d2_date` as `pd.Timestamp` (from `df['date'].shift(-1)`) — `build_signals` assigns these to `entry_date` column — `main.py` calls `.date()` on them if they have the attribute. ✓
- `check_exits` uses `pd.Timestamp(trade['entry_date'])` — state stores `entry_date` as ISO string — consistent. ✓
- `get_portfolio_weights` returns `float` weights — `rebalance` multiplies by `portfolio_value` which is `float`. ✓
- `get_current_positions` returns `int` qty — `rebalance` reads `market_value` not qty for diff calc. ✓
