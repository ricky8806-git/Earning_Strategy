# pead_strategy/state.py
import json
import pandas as pd
from pathlib import Path
from config import STATE_FILE

_COLUMNS = ['symbol', 'entry_date', 'entry_price', 'eps_beat_pct', 'earnings_date']


def load_state():
    """Load open trades from STATE_FILE. Returns empty DataFrame if file absent."""
    path = Path(STATE_FILE)
    if not path.exists():
        return pd.DataFrame(columns=_COLUMNS)

    with open(path) as f:
        data = json.load(f)

    trades = data.get('open_trades', [])
    if not trades:
        return pd.DataFrame(columns=_COLUMNS)

    return pd.DataFrame(trades)[_COLUMNS]


def save_state(trades_df):
    """Persist open trades to STATE_FILE."""
    records = []
    for row in trades_df.to_dict('records'):
        record = dict(row)
        # Ensure dates are ISO strings, not datetime objects
        for key in ('entry_date', 'earnings_date'):
            val = record.get(key)
            if val is not None and hasattr(val, 'isoformat'):
                record[key] = val.isoformat()
            elif val is not None:
                record[key] = str(val)
        records.append(record)

    with open(STATE_FILE, 'w') as f:
        json.dump({'open_trades': records}, f, indent=2, default=str)
