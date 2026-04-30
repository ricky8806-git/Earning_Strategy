# pead_strategy/backfill.py
"""
One-off catchup for signals missed during the lxml outage (Apr 25-29 2026).

The daily strategy was blind for five consecutive runs because yfinance's
earnings_dates endpoint started requiring lxml, which was not in requirements.txt.
This script rescans the three affected earnings dates (Apr 24, 25, 28) with both
D0 and D1 triggers, then enters any qualifying stocks at today's price.

Entry price = today's latest close (can't buy at the historical open).
Stop price  = 10% below today's entry price.
Entry date  = today (so the 20-day hold clock starts from today).
"""
import csv
import logging
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from broker import get_account, rebalance
from data import get_earnings, get_prices, get_sp500_symbols
from portfolio import _NYSE, get_active_positions, get_portfolio_weights
from signals import build_signals, get_miss_reason
from state import load_state, save_state
from config import LOG_FILE, GITHUB_TOKEN, STOP_LOSS_PCT

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

for _noisy in ('yfinance', 'peewee'):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

# Earnings dates that were missed due to the lxml dependency gap
BACKFILL_EARNINGS_DATES = [
    date(2026, 4, 24),
    date(2026, 4, 25),
    date(2026, 4, 28),
]

_LOOKBACK_DAYS = 90
today = date.today()

repo_root = Path(__file__).resolve().parent.parent


def _append_log(row_date, symbol, action, price, eps_beat, reason=''):
    path = Path(LOG_FILE)
    write_hdr = not path.exists()
    with open(path, 'a', newline='') as f:
        writer = csv.writer(f)
        if write_hdr:
            writer.writerow(['date', 'symbol', 'action', 'price', 'eps_beat_pct', 'reason'])
        writer.writerow([row_date, symbol, action, price, eps_beat, reason])


def _push_state():
    if not GITHUB_TOKEN:
        log.warning("GITHUB_TOKEN empty — skipping push")
        return

    def _git(*args):
        try:
            return subprocess.run(
                ['git', *args], cwd=repo_root,
                capture_output=True, text=True, timeout=60,
            )
        except subprocess.TimeoutExpired:
            log.warning(f"git {args[0]} timed out")
            from types import SimpleNamespace
            return SimpleNamespace(returncode=1, stderr='timeout')

    _git('config', 'user.email', 'pead-bot@auto')
    _git('config', 'user.name', 'PEAD Bot')
    _git('add', 'pead_strategy/state.json', 'pead_strategy/trades_log.csv')

    if _git('diff', '--cached', '--quiet').returncode == 0:
        log.info("No state changes to push")
        return

    commit = _git('commit', '-m', f'chore: backfill state update {today} [skip ci]')
    if commit.returncode != 0:
        log.warning(f"git commit failed: {commit.stderr.strip()}")
        return

    push_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/ricky8806-git/Earning_Strategy.git"
    push = _git('push', push_url, 'main')
    if push.returncode != 0:
        log.warning(f"git push failed: {push.stderr.strip()}")


def run_backfill():
    trades_df = load_state()
    already_open = set(trades_df['symbol'].tolist())
    log.info(f"=== PEAD backfill: {today} ===")
    log.info(f"Current open positions: {sorted(already_open)}")

    symbols = get_sp500_symbols()
    price_start = str(date(2026, 4, 1))
    price_end = str(today + timedelta(days=3))

    new_trades = []

    for earnings_date in BACKFILL_EARNINGS_DATES:
        log.info(f"--- Scanning earnings on {earnings_date} ---")

        for sym in symbols:
            if sym in already_open:
                continue
            try:
                earnings = get_earnings(sym)
                if earnings.empty:
                    continue

                earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date'])
                recent = earnings[earnings['earnings_date'].dt.date == earnings_date]
                if recent.empty:
                    continue

                # Skip if eps_actual not yet available
                if recent['eps_actual'].isna().all():
                    log.info(f"  {sym}: earnings on {earnings_date} but no eps_actual yet")
                    continue
                recent = recent.dropna(subset=['eps_actual'])

                prices = get_prices(sym, price_start, price_end)
                if prices.empty:
                    log.warning(f"  {sym}: no price data")
                    continue

                recent = recent.copy()
                recent['symbol'] = sym

                signals = build_signals(recent, prices)
                if signals.empty:
                    reason = get_miss_reason(recent, prices, 'd0')
                    log.info(f"  BACKFILL_MISS {sym} ({earnings_date}): {reason}")
                    _append_log(today, sym, 'BACKFILL_MISS', None,
                                recent['eps_actual'].iloc[0] if len(recent) else None,
                                f'{reason},earnings={earnings_date}')
                    continue

                # Use the highest-conviction signal (prefer D0 over D1)
                sig = signals[signals['trigger_day'] == 'd0']
                if sig.empty:
                    sig = signals[signals['trigger_day'] == 'd1']
                row = sig.iloc[0]
                trigger = row['trigger_day']

                # Get today's closing price as the actual entry price
                prices_sorted = prices.sort_values('date')
                current_price = float(prices_sorted.iloc[-1]['close'])

                historical_entry = row['entry_open']
                # Skip if stock has already fallen through its would-be stop loss
                if (pd.notna(historical_entry) and historical_entry > 0
                        and current_price < historical_entry * (1 - STOP_LOSS_PCT)):
                    log.info(
                        f"  BACKFILL_SKIP {sym}: current {current_price:.2f} already below "
                        f"historical stop {historical_entry * (1 - STOP_LOSS_PCT):.2f}"
                    )
                    _append_log(today, sym, 'BACKFILL_SKIP', current_price,
                                row['eps_beat_pct'],
                                f'below_stop,earnings={earnings_date}')
                    continue

                stop = round(current_price * (1 - STOP_LOSS_PCT), 4)
                new_trade = {
                    'symbol':        sym,
                    'entry_date':    str(today),
                    'entry_price':   current_price,
                    'stop_price':    stop,
                    'eps_beat_pct':  row['eps_beat_pct'],
                    'earnings_date': str(earnings_date),
                }
                new_trades.append(new_trade)
                already_open.add(sym)
                log.info(
                    f"  BACKFILL_ENTRY {sym}  earnings={earnings_date}  trigger={trigger}  "
                    f"eps_beat={row['eps_beat_pct']:.1f}%  entry={current_price:.2f}  stop={stop:.2f}"
                )
                _append_log(today, sym, f'BACKFILL_{trigger.upper()}',
                            current_price, row['eps_beat_pct'],
                            f'earnings={earnings_date}')

            except Exception as exc:
                log.warning(f"  Error processing {sym} for {earnings_date}: {exc}")

    if not new_trades:
        log.info("No new backfill signals found.")
        _push_state()
        return

    new_df = pd.DataFrame(new_trades)
    trades_df = pd.concat([trades_df, new_df], ignore_index=True)

    active = get_active_positions(trades_df, today)
    target_weights = get_portfolio_weights(active)
    log.info(f"Active positions after backfill: {active}")
    log.info(f"Target weights: {target_weights}")

    try:
        account = get_account()
        portfolio_value = account['portfolio_value']
        rebalance(target_weights, portfolio_value)
        log.info(f"Rebalance complete (portfolio_value={portfolio_value:.2f})")
    except Exception as exc:
        log.error(f"Broker unreachable — live rebalance skipped: {exc}")
        log.info(f"[DRY-RUN] Target weights: {target_weights}")

    save_state(trades_df)
    log.info("State saved.")
    _push_state()


if __name__ == '__main__':
    run_backfill()
