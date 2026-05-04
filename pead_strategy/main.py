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
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from broker    import close_position, get_account, rebalance
from data      import get_earnings, get_prices, get_sp500_symbols
from portfolio import check_exits, get_active_positions, get_portfolio_weights
from signals   import build_signals, get_miss_reason
from state     import load_state, save_state
from config    import LOG_FILE, GITHUB_TOKEN

logging.basicConfig(
    level  = logging.INFO,
    format = '%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)

# yfinance emits noisy ERROR-level messages when Yahoo Finance blocks the IP;
# suppress those so strategy-level ERRORs remain readable.
for _noisy in ('yfinance', 'peewee'):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

_LOOKBACK_DAYS       = 90   # Days of price history to fetch for signal computation
_STOP_PRICE_LOOKBACK = 45   # Days of price history to fetch for stop loss check


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


repo_root = Path(__file__).resolve().parent.parent


def _connectivity_report():
    """
    Quick pre-flight check of key API endpoints.

    Yahoo Finance uses server-side IP allowlisting; if this host is not allowed,
    ALL yfinance calls (prices + earnings) will fail with HTTP 403.  The fix is
    either to run the strategy from an allowlisted IP, or to set the env var
    YF_OAUTH2_REFRESH_TOKEN to a valid Yahoo Finance OAuth refresh token so
    yfinance can authenticate without a crumb fetch.

    Alpaca paper API has the same restriction.  The fix is to run from an
    allowlisted IP or to contact Alpaca support.

    Returns (yahoo_ok: bool, alpaca_ok: bool).
    """
    import urllib.request, urllib.error

    def _probe(name, url):
        try:
            urllib.request.urlopen(url, timeout=6)
            log.info(f"  connectivity OK: {name}")
            return True
        except urllib.error.HTTPError as e:
            body = e.read(200).decode(errors='replace')
            if 'allowlist' in body.lower():
                log.error(
                    f"  connectivity BLOCKED: {name} — "
                    f"HTTP {e.code}, this IP is not in {name}'s allowlist.  "
                    f"{'Set YF_OAUTH2_REFRESH_TOKEN env var with a valid Yahoo Finance OAuth token, or ' if 'yahoo' in name.lower() else ''}"
                    f"run the strategy from an allowlisted IP or use a VPN/proxy."
                )
            else:
                log.warning(f"  connectivity degraded: {name} — HTTP {e.code}")
            return False
        except Exception as exc:
            log.warning(f"  connectivity check failed: {name} — {exc}")
            return False

    log.info("Pre-flight connectivity check:")
    yahoo_ok  = _probe('Yahoo Finance', 'https://query2.finance.yahoo.com')
    alpaca_ok = _probe('Alpaca paper API', 'https://paper-api.alpaca.markets')
    return yahoo_ok, alpaca_ok


def _push_state():
    """Commit state.json and trades_log.csv back to the remote repo."""
    if not GITHUB_TOKEN:
        log.warning("GITHUB_TOKEN is empty — skipping git push")
        return

    def _git(*args):
        try:
            return subprocess.run(
                ['git', *args],
                cwd=repo_root,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            log.warning(f"git {args[0]} timed out after 60s")
            from types import SimpleNamespace
            return SimpleNamespace(returncode=1, stderr='timeout')

    _git('config', 'user.email', 'pead-bot@auto')
    _git('config', 'user.name', 'PEAD Bot')
    _git('add', 'pead_strategy/state.json', 'pead_strategy/trades_log.csv')

    diff = _git('diff', '--cached', '--quiet')
    if diff.returncode == 0:
        log.info("No state changes to push")
        return

    commit_msg = f'chore: state update {date.today()} [skip ci]'
    commit = _git('commit', '-m', commit_msg)
    if commit.returncode != 0:
        log.warning(f"git commit failed: {commit.stderr.strip()}")
        return

    push_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/ricky8806-git/Earning_Strategy.git"
    push = _git('push', push_url, 'main')
    if push.returncode != 0:
        log.warning(f"git push failed: {push.stderr.strip()}")


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
    yahoo_ok, alpaca_ok = _connectivity_report()

    # 1. Load persisted state
    trades_df = load_state()
    log.info(f"Loaded {len(trades_df)} open trades")

    # 2. Fetch latest prices for open positions (for stop loss check)
    active_symbols = trades_df['symbol'].tolist() if not trades_df.empty else []
    prices_dict    = _fetch_prices_for_positions(active_symbols, today)
    missing_prices = [s for s in active_symbols if s not in prices_dict]
    if missing_prices:
        log.warning(
            f"Price data unavailable for {len(missing_prices)}/{len(active_symbols)} positions "
            f"— stop-loss check is degraded (positions will not be force-exited): {missing_prices}"
        )

    # 3. Check exits (time exit OR stop loss)
    exited = check_exits(trades_df, prices_dict, today)
    seen_exits: set = set()
    for exit_info in exited:
        sym    = exit_info['symbol']
        reason = exit_info['reason']
        if sym in seen_exits:
            continue
        seen_exits.add(sym)
        log.info(f"EXIT {sym} reason={reason}")
        try:
            close_position(sym)
        except Exception as exc:
            log.error(f"Could not close {sym} via broker (unreachable?): {exc} — recording exit in state anyway")
        trades_df = trades_df[trades_df['symbol'] != sym].reset_index(drop=True)
        action = 'EXIT_TIME' if reason == 'time' else 'EXIT_STOP'
        _append_log(today, sym, action, None, None, reason)

    # 4. Scan for new signals
    #
    # Two scan windows, each restricted to the trigger type that has valid data at 9:31 AM:
    #   yesterday    → D0 trigger only  (D1 close unavailable; entry = today's open)
    #   two_days_ago → D1 trigger only  (yesterday's full close = D1; entry = today's open)
    two_days_ago = sched.index[-2].date() if len(sched) >= 2 else yesterday - timedelta(days=1)
    scan_plan    = [(yesterday, 'd0'), (two_days_ago, 'd1')]

    symbols      = get_sp500_symbols()
    new_trades   = []
    already_open = set(trades_df['symbol'].tolist())

    # --- Data quality gate ---
    # Prefetch all earnings before scanning so a systemic fetch failure (missing
    # dependency, Yahoo rate-limit, etc.) is caught and logged before any analysis.
    scan_candidates = [s for s in symbols if s not in already_open]
    log.info(f"Prefetching earnings data for {len(scan_candidates)} symbols...")
    earnings_cache: dict = {}
    for sym in scan_candidates:
        data = get_earnings(sym)
        if not data.empty:
            earnings_cache[sym] = data

    fetched      = len(earnings_cache)
    total        = len(scan_candidates)
    success_rate = fetched / total if total else 1.0
    log.info(
        f"Earnings prefetch: {fetched}/{total} symbols returned data "
        f"({success_rate * 100:.0f}% success rate)"
    )
    if success_rate < 0.20:
        likely_cause = (
            "this IP is not in Yahoo Finance's allowlist (HTTP 403). "
            "Fix: set YF_OAUTH2_REFRESH_TOKEN env var with a valid Yahoo Finance OAuth "
            "refresh token, or run from an allowlisted IP / VPN."
            if not yahoo_ok else "yfinance returned no data for most symbols"
        )
        log.critical(
            f"DATA QUALITY GATE FAILED: only {success_rate * 100:.0f}% of symbols have "
            f"earnings data — likely cause: {likely_cause}  "
            f"New-signal scan aborted; existing positions will still be managed."
        )
        scan_candidates = []  # skip the signal scan entirely

    for sym in scan_candidates:
        earnings = earnings_cache.get(sym)
        if earnings is None:
            _append_log(today, sym, 'SKIP_NO_DATA', None, None, 'no_earnings_data')
            continue

        try:
            earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date'])

            price_start = (two_days_ago - timedelta(days=_LOOKBACK_DAYS)).isoformat()
            price_end   = (today + timedelta(days=3)).isoformat()
            prices      = None  # lazy-load once we find a relevant earnings date

            for scan_date, trigger_type in scan_plan:
                recent = earnings[earnings['earnings_date'].dt.date == scan_date]
                if recent.empty:
                    continue

                # yfinance data lag: date present but eps_actual not yet populated
                if recent['eps_actual'].isna().all():
                    _append_log(today, sym, 'SCAN_MISS', None, None, 'no_eps_actual_yet')
                    continue

                recent = recent.dropna(subset=['eps_actual'])

                if prices is None:
                    prices = get_prices(sym, price_start, price_end)
                    if prices.empty:
                        log.warning(f"No price data for {sym}; skipping")
                        _append_log(today, sym, 'SKIP', None, None, 'no_price_data')
                        break

                recent = recent.copy()
                recent['symbol'] = sym

                signals = build_signals(recent, prices)
                signals = signals[signals['trigger_day'] == trigger_type]
                if signals.empty:
                    reason = get_miss_reason(recent, prices, trigger_type)
                    _append_log(today, sym, 'SCAN_MISS', None,
                                recent['eps_actual'].iloc[0] if len(recent) else None,
                                reason)
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
                        'earnings_date': str(scan_date),
                    }
                    new_trades.append(new_trade)
                    already_open.add(sym)  # prevent double-entry across scan windows
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

    # 7. Rebalance — save state and push regardless of broker availability
    try:
        account         = get_account()
        portfolio_value = account['portfolio_value']
        rebalance(target_weights, portfolio_value)
        log.info(f"Rebalance complete (portfolio_value={portfolio_value:.2f})")
    except Exception as exc:
        log.error(f"Broker unreachable — live rebalance skipped: {exc}")
        log.info(f"[DRY-RUN] Target weights: {target_weights}")
    finally:
        # 8. Save state and push to git even when the broker is unavailable
        save_state(trades_df)
        log.info("State saved")
        _push_state()


if __name__ == '__main__':
    run()
