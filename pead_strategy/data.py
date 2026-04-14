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
