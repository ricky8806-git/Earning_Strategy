# pead_live/install_task_windows.ps1
#
# One-time setup: registers the PEAD analysis job in Windows Task Scheduler.
# Run ONCE from an elevated (Administrator) PowerShell:
#
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   cd C:\path\to\Earning_Strategy
#   .\pead_live\install_task_windows.ps1
#
# Prerequisites:
#   1. Python must be installed and 'python' reachable in your PATH
#   2. pead_live\.env must exist with GITHUB_TOKEN=ghp_...
#
# The task runs Monday–Friday at 06:30 AM Pacific Time.
# It will run whether or not you are logged on, and will NOT require AC power
# (the machine is woken at 06:25 by a separate wake task).

$ErrorActionPreference = "Stop"

# ── Paths ────────────────────────────────────────────────────────────────────
$repoDir  = (Get-Item "$PSScriptRoot\..").FullName
$script   = Join-Path $repoDir "pead_live\run_local.ps1"
$logFile  = Join-Path $repoDir "pead_live\run_local.log"
$taskName = "PEAD-Live-Analysis"

# ── Verify prerequisites ─────────────────────────────────────────────────────
if (-not (Test-Path $script)) {
    Write-Error "run_local.ps1 not found at: $script"
    exit 1
}

$envFile = Join-Path $repoDir "pead_live\.env"
if (-not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile`nCopy pead_live\.env.example to pead_live\.env and add GITHUB_TOKEN."
    exit 1
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
$python = if ($pythonCmd) { $pythonCmd.Source } else { $null }
if (-not $python) {
    Write-Error "'python' not found in PATH. Install Python and ensure it is in your PATH."
    exit 1
}
Write-Host "Python found: $python"

# ── Build the action ─────────────────────────────────────────────────────────
# PowerShell must be called with -NonInteractive and -ExecutionPolicy Bypass
# so the task runs without a desktop session.
$psExe   = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$psArgs  = "-NonInteractive -ExecutionPolicy Bypass -File `"$script`""

$action  = New-ScheduledTaskAction -Execute $psExe -Argument $psArgs -WorkingDirectory $repoDir

# ── Trigger: Mon–Fri at 06:30 AM local time ──────────────────────────────────
$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At "06:30"

# ── Settings ─────────────────────────────────────────────────────────────────
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -MultipleInstances  IgnoreNew `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable                  # run even if the scheduled time was missed
    # NOTE: DisallowStartIfOnBatteries defaults to $true — we override below

# Override the battery restriction — the laptop is woken externally at 06:25
# and may still be on battery for a few seconds; we don't want the task to skip.
$settings.DisallowStartIfOnBatteries = $false
$settings.StopIfGoingOnBatteries     = $false

# ── Principal: run as current user, whether logged on or not ─────────────────
$principal = New-ScheduledTaskPrincipal `
    -UserId    $env:USERNAME `
    -LogonType S4U `           # run without storing password; works for local accounts
    -RunLevel  Highest

# ── Register (or update) ─────────────────────────────────────────────────────
$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "Removed existing task '$taskName'"
}

Register-ScheduledTask `
    -TaskName   $taskName `
    -Action     $action `
    -Trigger    $trigger `
    -Settings   $settings `
    -Principal  $principal `
    -Description "PEAD Live: runs analysis.py Mon-Fri at 06:30 PT, pushes run_plan.json to git"

Write-Host ""
Write-Host "Task '$taskName' registered successfully."
Write-Host ""
Write-Host "Verify:      Get-ScheduledTask -TaskName '$taskName' | Select-Object *"
Write-Host "Run now:     Start-ScheduledTask -TaskName '$taskName'"
Write-Host "View log:    Get-Content -Tail 50 '$logFile'"
Write-Host "Remove:      Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"
Write-Host ""
Write-Host "IMPORTANT: The 06:25 wake task must also have 'Wake the computer' checked."
Write-Host "           Check: Task Scheduler > [your wake task] > Conditions tab."
