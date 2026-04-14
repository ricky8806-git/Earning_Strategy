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

Edit `pead_strategy/config.py` and update:

```python
ALPACA_API_KEY    = "your_paper_api_key"
ALPACA_SECRET_KEY = "your_paper_secret_key"
```

Get paper trading credentials at: https://app.alpaca.markets → Paper Trading → API Keys

### 3. Run tests

```bash
cd pead_strategy
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

1. In `pead_strategy/config.py`, change `ALPACA_BASE_URL` to:
   ```python
   ALPACA_BASE_URL = "https://api.alpaca.markets"
   ```
2. In `pead_strategy/broker.py`, change `_get_client()` to:
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
