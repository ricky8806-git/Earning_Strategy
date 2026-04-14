# pead_strategy/scheduler.py
"""
Timezone-aware scheduler.

  09:31 ET — run main.py (market open runner)
  18:00 ET — run a pre-scan to log potential next-day entries

Runs as a long-lived process; checks time every 30 seconds.
The loop approach is used (vs the 'schedule' library) because the schedule
library does not natively support timezone-aware scheduling.
"""
import subprocess
import sys
import time
from datetime import datetime

import pytz

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
    """Evening pre-scan: log what signals would trigger tomorrow."""
    print(f"[scheduler] Evening pre-scan at {_et_now().strftime('%H:%M %Z')}")
    subprocess.run(
        [sys.executable, '-c',
         'from data import get_sp500_symbols; '
         'symbols = get_sp500_symbols(); '
         f'print(f"Pre-scan: {{len(symbols)}} symbols in universe")'],
        capture_output=False,
    )


def main():
    print(f"[scheduler] Started at {_et_now()}")
    print("[scheduler] Will run main.py at 09:31 ET and pre-scan at 18:00 ET")

    last_open_run  = None
    last_close_run = None

    while True:
        now   = _et_now()
        today = now.date()

        if now.hour == 9 and now.minute == 31 and last_open_run != today:
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
