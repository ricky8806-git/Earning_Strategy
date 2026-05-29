# pead_strategy/data.py
import pandas as pd
import yfinance as yf
from config import TARGET_SECTORS, SP500_URL


def get_sp500_symbols(sectors=None):
    """Fetch S&P 500 constituents from GitHub CSV and filter to target sectors."""
    if sectors is None:
        sectors = TARGET_SECTORS
    df = pd.read_csv(SP500_URL)
    return df[df['GICS Sector'].isin(sectors)]['Symbol'].tolist()


_PRICES_EMPTY = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])


def get_prices(symbol, start, end):
    """Fetch daily OHLCV. Returns DataFrame with columns: date, open, high, low, close, volume."""
    df = yf.download(symbol, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        return _PRICES_EMPTY.copy()

    # Flatten MultiIndex columns (yfinance ≥ 0.2 / 1.x returns (Price, Ticker) MultiIndex).
    # Take only the first level so ('Adj Close', 'AAPL') → 'adj close', etc.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]

    # yfinance 1.x includes 'adj close' even with auto_adjust=True — drop it.
    df = df.drop(columns=[c for c in df.columns if 'adj' in c], errors='ignore')

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]

    # After reset_index the date index may surface as 'date', 'datetime', or 'index'
    # (the last happens when the DatetimeIndex has name=None, which yfinance 1.x does).
    for alias in ('datetime', 'index'):
        if alias in df.columns and 'date' not in df.columns:
            df = df.rename(columns={alias: 'date'})
            break

    if 'date' not in df.columns:
        return _PRICES_EMPTY.copy()

    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.date

    required = ['date', 'open', 'high', 'low', 'close', 'volume']
    if not all(c in df.columns for c in required):
        return _PRICES_EMPTY.copy()

    return df[required].reset_index(drop=True)


def get_spy_prices(start, end):
    """Convenience wrapper: prices for SPY."""
    return get_prices('SPY', start, end)


_EARNINGS_EMPTY = pd.DataFrame(
    columns=['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']
)


def get_earnings(symbol):
    """Fetch earnings history from yfinance. Returns DataFrame with standardised columns.

    Rows where eps_actual is NaN are kept — they represent very recent announcements
    where yfinance hasn't yet populated the actual figure (data lag).  Callers must
    check for NaN eps_actual before computing the beat ratio.
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)
    ticker = yf.Ticker(symbol)
    try:
        # .earnings_dates uses a JSON endpoint (no lxml needed) and returns 12 quarters,
        # which is far more history than the 2-day scan window requires.
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
        # Only require eps_estimate (needed for beat ratio); keep NaN eps_actual rows
        # so very recent earnings dates survive to be matched by the scan window check.
        df = df.dropna(subset=['eps_estimate'])
        return df[['earnings_date', 'eps_estimate', 'eps_actual', 'surprise_pct']].reset_index(drop=True)

    except Exception as exc:
        _log.warning(f"get_earnings({symbol}) failed: {exc}")
        return _EARNINGS_EMPTY.copy()
