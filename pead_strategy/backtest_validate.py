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
    print(f"Backtest: {start_date} -> {end_date}  |  {len(symbols)} symbols")

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
    total_signals = sum(len(s) for s in all_signals)
    print(f"Signals found  : {total_signals}")

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
