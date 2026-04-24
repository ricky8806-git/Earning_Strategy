# pead_strategy/config.py
import os

ALPACA_API_KEY    = os.environ.get('ALPACA_API_KEY', '')
ALPACA_SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY', '')
ALPACA_BASE_URL   = "https://paper-api.alpaca.markets"

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')

MAX_POSITION_PCT      = 0.07
HOLD_DAYS             = 20
REBALANCE_TOLERANCE   = 0.02  # only trim/top-up a position if it drifts beyond this band
EPS_BEAT_MIN_PCT  = 10.0
DAY0_RET_MIN      = 0.03
VOLUME_MULT       = 2.0
STOP_LOSS_PCT     = 0.10

TARGET_SECTORS   = ["Information Technology", "Health Care", "Financials"]
SYMBOLS_EXCLUDE  = {'BRK.B'}  # symbols with no yfinance earnings data
SP500_URL      = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
LOG_FILE       = "trades_log.csv"
STATE_FILE     = "state.json"
