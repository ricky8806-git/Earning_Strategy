# pead_strategy/main.py
"""
Daily runner — designed to execute at 9:31 AM ET after market open.

Flow:
  1. Load state (open trades)
  2. Fetch latest prices for all open positions (needed for stop loss check)
  3. Check exits — time (20 trading days) or stop loss (close <= stop_price)
  4. Close any exited positions via broker
  5. Scan yesterday's earnings for new signals
  6. Merge new signals into open trades
  7. Calculate target weights
  8. Rebalance via Alpaca
  9. Save updated state
  10. Log all actions to trades_log.csv
"""
import csv
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from broker    import close_position, get_account, rebalance
from data      import get_earnings, get_prices, get_sp500_symbols
from portfolio import check_exits, get_active_positions, get_portfolio_weights
from signals   import build_signals
from state     import load_state, save_state
from config    import LOG_FILE

logging.basicConfig(
    level  = logging.INFO,
    format = '%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)

_LOOKBACK_DAYS       = 90   # Days of price history to fetch for signal computation
_STOP_PRICE_LOOKBACK = 30   # Days of price history to fetch for stop loss check


def _append_log(row_date, symbol, action, price, eps_beat, reason=''):
    path      = Path(LOG_FILE)
    write_hdr = not path.exists()
    with open(path, 'a', newline='') as f:
        writer = csv.writer(f)
        if write_hdr:
            writer.writerow(['date', 'symbol', 'action', 'price', 'eps_beat_pct', 'reason'])
        writer.writerow([row_date, symbol, action, price, eps_beat, reason])


def _fetch_prices_for_positions(symbols, today):
    """Fetch recent OHLCV for a list of symbols. Returns {symbol: prices_df}."""
    prices_dict = {}
    start       = str(today - timedelta(days=_STOP_PRICE_LOOKBACK))
    end         = str(today)
    for sym in symbols:
        try:
            df = get_prices(sym, start, end)
            if not df.empty:
                prices_dict[sym] = df
        except Exception as exc:
            log.warning(f"Could not fetch prices for {sym}: {exc}")
    return prices_dict


def run():
    from portfolio import _NYSE
    today = date.today()
    # Find the most recent prior trading session (handles weekends + holidays)
    sched     = _NYSE.schedule(
        start_date=str(today - timedelta(days=10)),
        end_date=str(today - timedelta(days=1)),
    )
    yesterday = sched.index[-1].date() if not sched.empty else today - timedelta(days=1)
    log.info(f"=== PEAD daily run: {today} ===")

    # 1. Load persisted state
    trades_df = load_state()
    log.info(f"Loaded {len(trades_df)} open trades")

    # 2. Fetch latest prices for open positions (for stop loss check)
    active_symbols = trades_df['symbol'].tolist() if not trades_df.empty else []
    prices_dict    = _fetch_prices_for_positions(active_symbols, today)

    # 3. Check exits (time exit OR stop loss)
    exited = check_exits(trades_df, prices_dict, today)
    for exit_info in exited:
        sym    = exit_info['symbol']
        reason = exit_info['reason']
        log.info(f"EXIT {sym} reason={reason}")
        close_position(sym)
        trades_df = trades_df[trades_df['symbol'] != sym].reset_index(drop=True)
        action = 'EXIT_TIME' if reason == 'time' else 'EXIT_STOP'
        _append_log(today, sym, action, None, None, reason)

    # 4. Scan yesterday's earnings for new signals
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
                    'stop_price':    row['stop_price'],
                    'eps_beat_pct':  row['eps_beat_pct'],
                    'earnings_date': str(yesterday),
                }
                new_trades.append(new_trade)
                log.info(f"SIGNAL {sym}  eps_beat={row['eps_beat_pct']:.1f}%  "
                         f"trigger={row['trigger_day']}  stop={row['stop_price']:.2f}")
                _append_log(today, sym, 'ENTRY', row['entry_open'], row['eps_beat_pct'])

        except Exception as exc:
            log.warning(f"Error processing {sym}: {exc}")

    # 5. Append new trades to state
    if new_trades:
        new_df    = pd.DataFrame(new_trades)
        trades_df = pd.concat([trades_df, new_df], ignore_index=True)

    # 6. Calculate target weights
    active         = get_active_positions(trades_df, today)
    target_weights = get_portfolio_weights(active)
    log.info(f"Active positions: {active}")
    log.info(f"Target weights:   {target_weights}")

    # 7. Rebalance
    account         = get_account()
    portfolio_value = account['portfolio_value']
    rebalance(target_weights, portfolio_value)
    log.info(f"Rebalance complete (portfolio_value={portfolio_value:.2f})")

    # 8. Save state
    save_state(trades_df)
    log.info("State saved")


if __name__ == '__main__':
    run()
