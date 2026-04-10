param(
    [string]$TaskName = "DailyBikeMonitor",
    [string]$Time = "09:00",
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"

$workspace = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $workspace "daily_bike_monitor.py"
$logDir = Join-Path $workspace "monitor_data\\logs"
$logPath = Join-Path $logDir "daily_bike_monitor.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$action = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$scriptPath`" --save-raw-html >> `"$logPath`" 2>&1" `
    -WorkingDirectory $workspace

$trigger = New-ScheduledTaskTrigger -Daily -At $Time
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Daily bike inventory monitor with diff output." `
    -Force | Out-Null

Write-Host "Registered scheduled task '$TaskName' at $Time"
