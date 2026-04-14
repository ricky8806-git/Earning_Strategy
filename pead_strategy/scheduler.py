# pead_strategy/scheduler.py
"""
Timezone-aware scheduler.

  09:31 ET — run main.py (market open runner)
  18:00 ET — run a pre-scan to log potential next-day entries

Runs as a long-lived process; checks time every 30 seconds.
The loop approach is used (vs the 'schedule' library) because the schedule
library does not natively support timezone-aware scheduling.
"""
import os
import subprocess
import sys
import time
from datetime import datetime

import pytz
import pandas_market_calendars as mcal
_NYSE_SCHED = mcal.get_calendar('NYSE')


def _is_nyse_trading_day(dt):
    """Return True if dt's date is an NYSE trading day."""
    d = dt.strftime('%Y-%m-%d')
    return not _NYSE_SCHED.schedule(start_date=d, end_date=d).empty


ET = pytz.timezone('America/New_York')


def _et_now():
    return datetime.now(ET)


def _run_script(script):
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"[scheduler] {script} exited with code {result.returncode}")


def _prescan():
    """Evening pre-scan: scan today's earnings and log D0 triggers for tomorrow's open."""
    print(f"[scheduler] Evening pre-scan at {_et_now().strftime('%H:%M %Z')}")
    subprocess.run(
        [sys.executable, '-c', '''
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else ".")
from datetime import date, timedelta
import pandas as pd
from data import get_sp500_symbols, get_earnings, get_prices
from signals import build_signals

today = date.today()
symbols = get_sp500_symbols()
print(f"[prescan] Scanning {len(symbols)} symbols for today earnings ({today})...")
found = 0
for sym in symbols:
    try:
        earnings = get_earnings(sym)
        if earnings.empty:
            continue
        earnings["earnings_date"] = pd.to_datetime(earnings["earnings_date"])
        recent = earnings[earnings["earnings_date"].dt.date == today]
        if recent.empty:
            continue
        recent = recent.copy()
        recent["symbol"] = sym
        prices = get_prices(sym, str(today - timedelta(days=90)), str(today + timedelta(days=3)))
        if prices.empty:
            continue
        signals = build_signals(recent, prices)
        if not signals.empty:
            for _, row in signals.iterrows():
                print(f"[prescan] SIGNAL {sym} eps_beat={row['eps_beat_pct']:.1f}% trigger={row['trigger_day']} entry={row['entry_date']}")
                found += 1
    except Exception as e:
        pass
print(f"[prescan] Done. {found} signals identified for tomorrow.")
'''],
        capture_output=False,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )


def main():
    print(f"[scheduler] Started at {_et_now()}")
    print("[scheduler] Will run main.py at 09:31 ET and pre-scan at 18:00 ET")

    last_open_run  = None
    last_close_run = None

    while True:
        now   = _et_now()
        today = now.date()

        if now.hour == 9 and now.minute == 31 and last_open_run != today and _is_nyse_trading_day(now):
            print(f"[scheduler] Triggering market-open run at {now}")
            _run_script('main.py')
            last_open_run = today

        if now.hour == 18 and now.minute == 0 and last_close_run != today:
            print(f"[scheduler] Triggering evening pre-scan at {now}")
            _prescan()
            last_close_run = today

        time.sleep(30)


if __name__ == '__main__':
    main()
