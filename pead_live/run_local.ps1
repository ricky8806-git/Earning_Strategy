# pead_live/run_local.ps1
# Run manually from PowerShell if you ever want to trigger analysis outside the schedule:
#   cd C:\path\to\Earning_Strategy
#   .\pead_live\run_local.ps1

$ErrorActionPreference = "Stop"
$repoDir = Split-Path -Parent $PSScriptRoot
$logFile = Join-Path $repoDir "pead_live\run_local.log"

# Load token from .env
$envFile = Join-Path $repoDir "pead_live\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.+)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
        }
    }
} else {
    Write-Error "Missing $envFile -- copy pead_live\.env.example to pead_live\.env and fill in GITHUB_TOKEN"
}

# NYSE trading-day guard — skip weekends and holidays early
$tradingDay = python -c @"
import pandas_market_calendars as mcal
from datetime import date
nyse = mcal.get_calendar('NYSE')
sched = nyse.schedule(start_date=str(date.today()), end_date=str(date.today()))
print('YES' if not sched.empty else 'NO')
"@
if ($tradingDay -ne 'YES') {
    Add-Content $logFile "$(Get-Date): Not a trading day — skipping"
    exit 0
}

Add-Content $logFile "$(Get-Date): Starting PEAD live analysis"
Set-Location $repoDir
python pead_live\analysis.py 2>&1 | Tee-Object -FilePath $logFile -Append
Add-Content $logFile "$(Get-Date): Done"
