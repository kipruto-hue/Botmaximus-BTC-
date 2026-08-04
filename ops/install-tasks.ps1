<#
.SYNOPSIS
    Makes the supervisor start automatically, so it survives reboots.

.DESCRIPTION
    The desktop equivalent of `systemctl enable`. Without it the supervisor only
    runs while someone has a terminal open -- which is exactly the gap that has
    been losing data: the collector stops at a reboot or a sleep and nobody
    notices until a backtest is refused for coverage.

    Tries a Scheduled Task first (survives logoff, restarts itself if it exits).
    Registering one needs admin, so on failure it falls back to a shortcut in the
    Startup folder, which needs nothing and starts the supervisor at logon.

    The daily backup is NOT a separate task: it needs the same admin rights, so
    the supervisor runs it inline instead. On the VPS the split is proper --
    botmaximus-backup.timer owns it (ops/systemd/).

    Both paths start at LOGON, not at boot. An unattended reboot leaves the
    collector down until someone signs in. That is the honest limit of what a
    desktop without admin can do, and the reason the VPS path exists: this is a
    stopgap that stops the bleeding, not the fix.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File ops\install-tasks.ps1 -BackupDest D:\botmaximus-backups
    powershell -ExecutionPolicy Bypass -File ops\install-tasks.ps1 -Remove
#>
[CmdletBinding()]
param(
    [string] $BackupDest = "D:\botmaximus-backups",
    [int]    $Keep       = 7,
    [switch] $Remove
)

$ErrorActionPreference = "Stop"
$root      = Split-Path -Parent $PSScriptRoot
$python    = Join-Path $root "server\.venv\Scripts\python.exe"
$supervise = Join-Path $PSScriptRoot "supervise.ps1"
$TASK      = "BOTMAXIMUS Supervisor"
$startup   = [Environment]::GetFolderPath("Startup")
$lnk       = Join-Path $startup "BOTMAXIMUS Supervisor.lnk"

if ($Remove) {
    if (Get-ScheduledTask -TaskName $TASK -ErrorAction SilentlyContinue) {
        try { Unregister-ScheduledTask -TaskName $TASK -Confirm:$false; Write-Host "removed scheduled task: $TASK" }
        catch { Write-Warning "could not remove scheduled task (needs admin): $_" }
    }
    if (Test-Path $lnk) { Remove-Item $lnk -Force; Write-Host "removed startup shortcut" }
    return
}

if (-not (Test-Path $python)) { throw "python not found at $python" }

# Parse-check before installing. A syntax error in supervise.ps1 makes it exit
# instantly and silently -- no log line, no window, nothing -- which looks
# exactly like "installed and running fine" until you notice the data stopped.
# That is the same class of failure the supervisor itself exists to catch.
$parseErrors = $null
[void][System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path $supervise), [ref]$null, [ref]$parseErrors)
if ($parseErrors -and $parseErrors.Count) {
    throw "supervise.ps1 has $($parseErrors.Count) parse error(s), refusing to install: " +
          (($parseErrors | ForEach-Object { $_.Message }) -join '; ')
}

New-Item -ItemType Directory -Force -Path $BackupDest | Out-Null

# A backup on the same physical device as the database survives an accidental
# drop and nothing else. Disk loss is the failure that takes the whole 2-year
# history, and most of it -- liquidations, order book, open interest past the
# venue's 30-day window -- cannot be re-fetched at any price.
$dbDisk  = (Get-Partition -DriveLetter $root.Substring(0,1) -ErrorAction SilentlyContinue).DiskNumber
$bakDisk = (Get-Partition -DriveLetter $BackupDest.Substring(0,1) -ErrorAction SilentlyContinue).DiskNumber
if ($null -ne $dbDisk -and $dbDisk -eq $bakDisk) {
    Write-Warning "BackupDest is on the SAME physical disk as the database (disk $dbDisk)."
    Write-Warning "That is not disk-failure protection. Point -BackupDest at another device."
} else {
    Write-Host "backup destination is on physical disk $bakDisk (database is on $dbDisk)"
    Write-Host "NOTE: still the same machine. Theft, fire or a wiped box takes both."
    Write-Host "      Sync $BackupDest to a remote for that."
}

$args = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$supervise`" -BackupDest `"$BackupDest`" -BackupKeep $Keep"
$installed = $false

try {
    $action  = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $args -WorkingDirectory $root
    $trigger = New-ScheduledTaskTrigger -AtLogOn
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
        -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero)      # runs forever, never killed
    Register-ScheduledTask -TaskName $TASK -Action $action -Trigger $trigger `
        -Settings $settings -Force `
        -Description "Keeps MongoDB and the BOTMAXIMUS collector running; restarts on stale feeds; daily verified backup." | Out-Null
    Write-Host "registered scheduled task: $TASK (at logon, auto-restart)"
    $installed = $true
}
catch {
    Write-Warning "scheduled task needs admin ($($_.Exception.Message.Trim())) - falling back to a Startup shortcut"
}

if (-not $installed) {
    $sh = New-Object -ComObject WScript.Shell
    $s = $sh.CreateShortcut($lnk)
    $s.TargetPath  = "powershell.exe"
    $s.Arguments   = $args
    $s.WorkingDirectory = $root
    $s.WindowStyle = 7                              # minimised
    $s.Description = "BOTMAXIMUS supervisor"
    $s.Save()
    Write-Host "created startup shortcut: $lnk"
    Write-Host "NOTE: a Startup shortcut does not restart the supervisor if it exits."
    Write-Host "      Re-run this script from an elevated shell for the scheduled task."
}

Write-Host ""
Write-Host "Start it now without waiting for a logon:"
Write-Host "  Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-WindowStyle','Hidden','-File','$supervise' "
