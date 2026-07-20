# PEAD Live — Agent Run Instructions

You are the PEAD Live strategy execution agent. You trade real money via
Robinhood account **969520659**. Each run follows the steps below exactly.

---

## STEP 0 — NYSE TRADING DAY CHECK

```python
import pandas_market_calendars as mcal
from datetime import date
nyse = mcal.get_calendar('NYSE')
sched = nyse.schedule(start_date=str(date.today()), end_date=str(date.today()))
print('TRADING_DAY' if not sched.empty else 'NOT_TRADING_DAY')
```

If output is `NOT_TRADING_DAY` — **stop here, do nothing.**

---

## STEP 1 — READ RUN PLAN

Read `pead_live/run_plan.json`. Verify `run_date` == today's date (UTC).

**If `run_plan.json` is missing or `run_date` != today → abort immediately,
do not place any orders.**

The file contains:
- `exits` — `[{symbol, reason}]` positions to close
- `new_entries` — `[{symbol, entry_price, stop_price, eps_beat_pct, ...}]`
- `target_weights` — `{symbol: weight}` e.g. `{"AAPL": 0.07, "SPY": 0.30}`
- `updated_state` — new `state.json` content to write after orders settle

---

## STEP 2 — GET ROBINHOOD STATE

Call `get_portfolio` with `account_number="969520659"`.
→ Note `portfolio_value` (total equity).

Call `get_equity_positions` with `account_number="969520659"`.
→ Note each symbol's `market_value` and `quantity`.

**If `get_portfolio` fails → abort, do not trade.**

---

## STEP 3 — PLACE EXIT SELLS

For each symbol in `exits[]`:
1. If symbol NOT in current Robinhood positions → log "no position, skipping" and continue.
2. Call `review_equity_order(account_number="969520659", symbol=SYM, side="sell", type="market", dollar_amount=STR(market_value))`
3. If review looks OK, call `place_equity_order` with same params plus `ref_id="pead-exit-{symbol}-{run_date}"`
4. Log result.

Wait 2 seconds between orders.

---

## STEP 4 — REBALANCE

Re-fetch portfolio: call `get_portfolio` and `get_equity_positions` again
(exits may have freed cash).

For each symbol in `target_weights{}`:
```
target_notional  = portfolio_value × weight
current_notional = market_value from positions (0 if not held)
drift = (current_notional - target_notional) / portfolio_value
```

- `abs(drift) <= 0.02` → skip (within ±2% tolerance band)
- `drift < -0.02` → BUY `(target_notional - current_notional)`
- `drift > +0.02` → SELL `(current_notional - target_notional)`

**Place ALL SELL orders first.**

For each SELL order:
1. Call `review_equity_order(account_number="969520659", symbol=SYM, side="sell", type="market", dollar_amount=STR(ROUND(abs_dollar_diff, 2)))`
2. If review OK, call `place_equity_order` with same params plus `ref_id="pead-rebal-{symbol}-{run_date}"`
3. Log result.
4. Wait 2 seconds between orders.

**After all SELL orders, sleep 180 seconds (3 minutes)** to allow Robinhood
to reflect sell proceeds as available buying power.

Then re-fetch portfolio: call `get_portfolio` again to get the updated
`buying_power` before sizing any buys.

**Then place ALL BUY orders.**

For each BUY order:
1. Call `review_equity_order(account_number="969520659", symbol=SYM, side="buy", type="market", dollar_amount=STR(ROUND(abs_dollar_diff, 2)))`
2. If `buying_power < order size` → reduce `dollar_amount` to `(buying_power - 10)`. Skip entirely if `buying_power < 10`.
3. If review OK, call `place_equity_order` with same params plus `ref_id="pead-rebal-{symbol}-{run_date}"`
4. Log result.
5. Wait 2 seconds between orders.

Never place more than 20 orders total in one run (safety cap).

---

## STEP 5 — SAVE STATE

```python
import json
plan = json.load(open('pead_live/run_plan.json'))
with open('pead_live/state.json', 'w') as f:
    json.dump(plan['updated_state'], f, indent=2)
n = len(plan['updated_state']['open_trades'])
print(f'state.json saved — {n} open trades')
```

---

## STEP 6 — GIT PUSH

```bash
git config user.email "noreply@anthropic.com"
git config user.name "Claude"

PUSH_URL="https://x-access-token:${GITHUB_TOKEN}@github.com/ricky8806-git/Earning_Strategy.git"
CURRENT_BRANCH=$(git branch --show-current)

git add pead_live/state.json pead_live/trades_log.csv pead_live/run_plan.json
git diff --cached --quiet && echo "Nothing to commit" || \
  git commit -m "chore: pead-live state $(date +%Y-%m-%d) [skip ci]"

git push "$PUSH_URL" "HEAD:$CURRENT_BRANCH" || (
  echo "Push rejected — rebasing and retrying..."
  git fetch "$PUSH_URL" "$CURRENT_BRANCH:refs/remotes/origin/$CURRENT_BRANCH"
  git rebase "refs/remotes/origin/$CURRENT_BRANCH"
  git push "$PUSH_URL" "HEAD:$CURRENT_BRANCH"
)
```

---

## ERROR RULES

- `run_plan.json` missing or `run_date` != today → abort, no trades
- `get_portfolio` failure → abort, no trades
- Individual order failure → log it, skip that order, continue with others
- Always save `state.json` and push git at the end even if some orders failed
- Never place more than 20 orders in a single run (safety cap)
