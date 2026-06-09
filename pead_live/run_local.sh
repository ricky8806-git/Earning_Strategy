#!/bin/bash
# pead_live/run_local.sh
# Runs the PEAD live analysis from your local machine.
# Called by cron every weekday at 7 AM PST.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOGFILE="$REPO_DIR/pead_live/run_local.log"

# Load secrets from .env file (never committed to git)
ENV_FILE="$REPO_DIR/pead_live/.env"
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a; source "$ENV_FILE"; set +a
else
    echo "$(date): ERROR — $ENV_FILE not found. Run: cp pead_live/.env.example pead_live/.env and fill in your token." >> "$LOGFILE"
    exit 1
fi

# Find python3 — check common locations used by Homebrew / pyenv / system
for PY in python3 /usr/local/bin/python3 /opt/homebrew/bin/python3 /usr/bin/python3; do
    if command -v "$PY" &>/dev/null; then
        PYTHON="$PY"
        break
    fi
done

if [[ -z "${PYTHON:-}" ]]; then
    echo "$(date): ERROR — python3 not found" >> "$LOGFILE"
    exit 1
fi

echo "$(date): Starting PEAD live analysis (python=$PYTHON)" >> "$LOGFILE"
cd "$REPO_DIR"
"$PYTHON" pead_live/analysis.py >> "$LOGFILE" 2>&1
echo "$(date): Done (exit $?)" >> "$LOGFILE"
