#!/bin/bash
# pead_live/install_cron.sh
# One-time setup: adds the PEAD analysis job to your crontab.
# Run once from your local machine:  bash pead_live/install_cron.sh

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$REPO_DIR/pead_live/run_local.sh"

chmod +x "$SCRIPT"

# 7:00 AM PST = 15:00 UTC (standard time) / 14:00 UTC (daylight time)
# We schedule at 15:00 UTC so it fires at 7 AM PST / 8 AM PDT year-round.
CRON_LINE="0 15 * * 1-5 $SCRIPT"

# Add only if not already present
if crontab -l 2>/dev/null | grep -qF "$SCRIPT"; then
    echo "Cron job already installed — nothing changed."
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "Cron job installed:"
    echo "  $CRON_LINE"
fi

echo ""
echo "To verify:  crontab -l"
echo "To view log: tail -f $REPO_DIR/pead_live/run_local.log"
echo "To remove:  crontab -e  (delete the line containing run_local.sh)"
