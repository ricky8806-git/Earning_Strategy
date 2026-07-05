#!/usr/bin/env python3
"""
pead_live/analysis.py
Generates run_plan.json for today's Robinhood live trading run.
Uses pead_strategy modules for signal logic; reads/writes pead_live/state.json.

Run locally each morning before market open:
    cd /path/to/Earning_Strategy
    python pead_live/analysis.py

The script generates run_plan.json and pushes it to git so the cloud
Robinhood execution agent can pick it up automatically.
"""
import json
import logging
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / 'pead_strategy'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

for _noisy in ('yfinance', 'peewee'):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

_LOOKBACK_DAYS = 90
_PRICE_LOOKBACK_DAYS = 45
_MAX_INITIAL_SCAN = 100  # symbols to probe for data-quality gate


def _load_live_state():
    path = Path(__file__).parent / 'state.json'
    if not path.exists():
        return []
    with open(path) as f:
        data = json.load(f)
    return data.get('open_trades', [])


def run():
    import pandas as pd
    import pandas_market_calendars as mcal
    from portfolio import check_exits, get_active_positions, get_portfolio_weights
    from data import get_earnings, get_prices, get_sp500_symbols
    from signals import build_signals, get_miss_reason
    near_misses = []  # [(sym, scan_date, trigger_type, reason)]

    today = date.today()
    nyse = mcal.get_calendar('NYSE')

    sched = nyse.schedule(
        start_date=str(today - timedelta(days=30)),
        end_date=str(today - timedelta(days=1)),
    )
    if sched.empty:
        log.error("No recent NYSE schedule — aborting")
        sys.exit(1)

    yesterday = sched.index[-1].date()
    two_days_ago = sched.index[-2].date() if len(sched) >= 2 else yesterday - timedelta(days=1)
    log.info(f"=== PEAD live analysis: {today} ===")
    log.info(f"Scanning earnings from {two_days_ago} (d1) and {yesterday} (d0)")

    # ── Load state ───────────────────────────────────────────────────────────
    open_trades = _load_live_state()
    log.info(f"Loaded {len(open_trades)} open trades from state.json")

    if open_trades:
        trades_df = pd.DataFrame(open_trades)
        for col in ('symbol', 'entry_date', 'entry_price', 'stop_price', 'eps_beat_pct', 'earnings_date'):
            if col not in trades_df.columns:
                trades_df[col] = float('nan')
    else:
        trades_df = pd.DataFrame(columns=['symbol', 'entry_date', 'entry_price',
                                           'stop_price', 'eps_beat_pct', 'earnings_date'])

    # ── Fetch prices for stop-loss checks ────────────────────────────────────
    active_symbols = trades_df['symbol'].tolist() if not trades_df.empty else []
    prices_dict = {}
    for sym in active_symbols:
        try:
            df = get_prices(sym,
                            str(today - timedelta(days=_PRICE_LOOKBACK_DAYS)),
                            str(today))
            if not df.empty:
                prices_dict[sym] = df
        except Exception as exc:
            log.warning(f"Could not fetch prices for {sym}: {exc}")

    # ── Check exits ───────────────────────────────────────────────────────────
    exits = []
    seen_exits = set()
    for exit_info in check_exits(trades_df, prices_dict, today):
        sym = exit_info['symbol']
        if sym in seen_exits:
            continue
        seen_exits.add(sym)
        exits.append(exit_info)
        log.info(f"EXIT {sym}  reason={exit_info['reason']}")

    remaining_trades_df = trades_df[~trades_df['symbol'].isin(seen_exits)].copy()

    # ── Scan for new signals ──────────────────────────────────────────────────
    new_entries = []
    already_open = set(remaining_trades_df['symbol'].tolist())

    try:
        symbols = get_sp500_symbols()
        scan_candidates = [s for s in symbols if s not in already_open]
        log.info(f"Scanning {len(scan_candidates)} symbols for new signals...")

        # Data-quality gate: probe first _MAX_INITIAL_SCAN symbols
        probe_set = scan_candidates[:_MAX_INITIAL_SCAN]
        earnings_cache = {}
        for sym in probe_set:
            try:
                data = get_earnings(sym)
                if not data.empty:
                    earnings_cache[sym] = data
            except Exception:
                pass

        fetched = len(earnings_cache)
        total = len(probe_set)
        success_rate = fetched / total if total > 0 else 0.0
        log.info(f"Earnings probe: {fetched}/{total} = {success_rate*100:.0f}%")

        if success_rate < 0.20:
            log.critical(
                f"DATA QUALITY GATE FAILED ({success_rate*100:.0f}%) — "
                "new-signal scan aborted; existing positions will still be managed."
            )
            scan_candidates = []
        else:
            # Fetch remaining symbols
            for sym in scan_candidates[_MAX_INITIAL_SCAN:]:
                try:
                    data = get_earnings(sym)
                    if not data.empty:
                        earnings_cache[sym] = data
                except Exception:
                    pass

            scan_plan = [(yesterday, 'd0'), (two_days_ago, 'd1')]

            for sym, earnings in earnings_cache.items():
                if sym in already_open:
                    continue
                try:
                    earnings = earnings.copy()
                    earnings['earnings_date'] = pd.to_datetime(earnings['earnings_date'])
                    price_start = str(two_days_ago - timedelta(days=_LOOKBACK_DAYS))
                    price_end = str(today + timedelta(days=3))
                    prices = None

                    for scan_date, trigger_type in scan_plan:
                        recent = earnings[earnings['earnings_date'].dt.date == scan_date]
                        if recent.empty:
                            continue
                        if recent['eps_actual'].isna().all():
                            continue
                        recent = recent.dropna(subset=['eps_actual'])

                        if prices is None:
                            prices = get_prices(sym, price_start, price_end)
                            if prices.empty:
                                break

                        recent = recent.copy()
                        recent['symbol'] = sym
                        signals = build_signals(recent, prices)
                        signals = signals[signals['trigger_day'] == trigger_type]

                        if signals.empty:
                            reason = get_miss_reason(recent, prices, trigger_type)
                            near_misses.append((sym, scan_date, trigger_type, reason))
                        else:
                            for _, row in signals.iterrows():
                                entry_dt = row['entry_date']
                                if hasattr(entry_dt, 'date'):
                                    entry_dt = entry_dt.date()
                                new_entries.append({
                                    'symbol': sym,
                                    'entry_date': str(entry_dt),
                                    'entry_price': float(row['entry_open']),
                                    'stop_price': float(row['stop_price']),
                                    'eps_beat_pct': float(row['eps_beat_pct']),
                                    'earnings_date': str(scan_date),
                                    'price_ret_pct': (float(row['price_ret_pct'])
                                                      if row.get('price_ret_pct', '') != '' else None),
                                    'vol_mult': (float(row['vol_mult'])
                                                 if row.get('vol_mult', '') != '' else None),
                                })
                                already_open.add(sym)
                                log.info(f"SIGNAL {sym}  eps_beat={row['eps_beat_pct']:.1f}%  "
                                         f"trigger={trigger_type}")
                except Exception as exc:
                    log.warning(f"Error scanning {sym}: {exc}")

        if near_misses:
            log.info(f"Near-misses ({len(near_misses)} stocks had earnings but no signal):")
            for sym, scan_date, trigger, reason in near_misses:
                log.info(f"  SKIP {sym:6s}  date={scan_date}  trigger={trigger}  reason={reason}")

    except Exception as exc:
        log.warning(f"New-entry scan failed: {exc} — proceeding with no new entries")

    # ── Build updated state and target weights ────────────────────────────────
    final_trades = (remaining_trades_df.to_dict('records') if not remaining_trades_df.empty else [])
    final_trades += new_entries

    if final_trades:
        final_df = pd.DataFrame(final_trades)
    else:
        final_df = pd.DataFrame(columns=['symbol', 'entry_date'])

    active = get_active_positions(final_df, today)
    target_weights = get_portfolio_weights(active)
    log.info(f"Active positions: {active}")
    log.info(f"Target weights: {target_weights}")

    # ── Serialise updated_state (ensure JSON-safe types) ─────────────────────
    def _safe(v):
        if hasattr(v, 'isoformat'):
            return v.isoformat()
        if isinstance(v, float) and (v != v):  # NaN
            return None
        return v

    serialised_trades = [
        {k: _safe(v) for k, v in t.items()} for t in final_trades
    ]

    plan = {
        'run_date': str(today),
        'exits': exits,
        'new_entries': new_entries,
        'target_weights': target_weights,
        'updated_state': {'open_trades': serialised_trades},
    }

    plan_path = Path(__file__).parent / 'run_plan.json'
    with open(plan_path, 'w') as f:
        json.dump(plan, f, indent=2, default=str)

    log.info(f"run_plan.json written: {len(exits)} exits, {len(new_entries)} new entries, "
             f"{len(target_weights)} target symbols")

    _push_plan(today)


def _push_plan(today):
    """Commit run_plan.json and push to git so the cloud execution agent picks it up."""
    github_token = os.environ.get('GITHUB_TOKEN', '')
    if not github_token:
        log.warning("GITHUB_TOKEN not set — skipping git push (run_plan.json saved locally only)")
        return

    repo_root = Path(__file__).resolve().parent.parent

    def _git(*args, timeout=120):
        try:
            return subprocess.run(
                ['git', *args], cwd=repo_root,
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            log.warning(f"git {args[0]} timed out after {timeout}s")
            # Return a fake failed result so callers can handle it uniformly
            result = subprocess.CompletedProcess(args, returncode=1)
            result.stdout = ''
            result.stderr = f'timed out after {timeout}s'
            return result

    # Fix "dubious ownership" when runner service account differs from repo owner
    _git('config', '--global', '--add', 'safe.directory', str(repo_root).replace('\\', '/'))
    _git('config', 'user.email', 'pead-live-bot@auto')
    _git('config', 'user.name', 'PEAD Live Agent')
    _git('add', 'pead_live/run_plan.json', 'pead_live/state.json')

    diff = _git('diff', '--cached', '--quiet')
    if diff.returncode == 0:
        log.info("run_plan.json unchanged — nothing to push")
        return

    commit = _git('commit', '-m', f'chore: run_plan {today} [skip ci]')
    if commit.returncode != 0:
        log.warning(f"git commit failed: {commit.stderr.strip()}")
        return

    push_url = f"https://x-access-token:{github_token}@github.com/ricky8806-git/Earning_Strategy.git"
    push = _git('push', push_url, 'HEAD:main', timeout=180)
    if push.returncode != 0:
        log.warning(f"git push failed, trying rebase: {push.stderr.strip()[:120]}")
        _git('fetch', push_url, 'main:refs/remotes/origin/main', timeout=180)
        _git('rebase', 'refs/remotes/origin/main')
        push2 = _git('push', push_url, 'HEAD:main', timeout=180)
        if push2.returncode != 0:
            log.warning(f"git push retry failed: {push2.stderr.strip()[:120]}")
        else:
            log.info("run_plan.json pushed to git (after rebase)")
    else:
        log.info("run_plan.json pushed to git")

if __name__ == '__main__':
    run()
