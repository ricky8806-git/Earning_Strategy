# pead_strategy/main.py
"""
Daily runner — designed to execute at 9:31 AM ET after market open.

Flow:
  1. Load state (open trades)
  2. Check exits -> close expired positions
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
