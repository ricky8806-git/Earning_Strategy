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
