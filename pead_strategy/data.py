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
    # Normalise column names after reset_index (index becomes 'Date' or 'Datetime')
    df.columns = [c.lower() for c in df.columns]

    # Rename 'datetime' to 'date' if needed
    if 'datetime' in df.columns:
        df = df.rename(columns={'datetime': 'date'})

    df['date'] = pd.to_datetime(df['date']).dt.date
    return df[['date', 'open', 'high', 'low', 'close', 'volume']].reset_index(drop=True)


def get_spy_prices(start, end):
    """Convenience wrapper: prices for SPY."""
    return get_prices('SPY', start, end)


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
