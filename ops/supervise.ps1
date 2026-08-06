<#
.SYNOPSIS
    Keeps MongoDB and the BOTMAXIMUS collector running, and restarts the
    collector when it goes silent rather than only when it dies.

.DESCRIPTION
    The Windows counterpart to the systemd units in ops/systemd/. Same restart
    policy, same freshness probe (ops/healthcheck.py) -- deliberately, so the
    desktop and the VPS cannot drift into behaving differently.

    Why freshness and not just liveness: the collector's known failure mode is
    a socket that stays connected while pushing nothing (the 2026-04-23 Binance
    routing migration acked SUBSCRIBE and delivered no /market data). A
    liveness-only supervisor sees that as healthy. Liquidations and order book
    have no history endpoint, so every silent hour is data that never comes back.

    Restarts are rate-limited. A tight crash-restart loop would hammer Binance
    with reconnects and risk a rate-limit ban -- which would cost far more
    coverage than the outage being recovered from.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File ops\supervise.ps1
    powershell -ExecutionPolicy Bypass -File ops\supervise.ps1 -Once
#>
[CmdletBinding()]
param(
    [int]    $IntervalSec       = 30,     # how often to probe
    [int]    $StaleStrikes      = 4,      # consecutive bad probes before restarting
    [int]    $StartGraceSec     = 180,    # quiet period after a start (backfill runs first)
    [int]    $MaxRestartsPerHr  = 6,      # ceiling; above this, alert and stop trying
    [string] $ApiUrl            = "http://127.0.0.1:8300",
    # Daily backup lives here rather than in its own scheduled task because
    # registering a task needs admin on this machine and the supervisor does not.
    # On the VPS this is handled by botmaximus-backup.timer instead and these
    # parameters go unused -- see ops/systemd/.
    [string] $BackupDest        = "D:\botmaximus-backups",
    # MUST match config.db_name. The venue migration moved live data to
    # botmaximus_bybit while this defaulted to the old botmaximus, so the daily
    # backup silently protected the retired Binance dataset and left the live
    # one uncovered -- a backup that runs, verifies and reports success while
    # covering the wrong database is worse than none, because it is trusted.
    [string] $DbName            = "botmaximus_bybit",
    [int]    $BackupHourUtc     = 3,
    [int]    $BackupKeep        = 7,
    [switch] $NoBackup,
    # A supervisor that only logs problems is indistinguishable from one that
    # died -- both produce silence. The heartbeat makes "alive and everything is
    # fine" an observable state rather than an absence.
    [int]    $HeartbeatMin      = 30,
    [switch] $Once                        # single probe, for testing
)

$ErrorActionPreference = "Stop"
$root    = Split-Path -Parent $PSScriptRoot
$python  = Join-Path $root "server\.venv\Scripts\python.exe"
$logFile = Join-Path $root "data\supervisor.log"
New-Item -ItemType Directory -Force -Path (Split-Path $logFile) | Out-Null

function Write-Log {
    param([string]$Level, [string]$Message)
    $line = "{0} {1,-5} {2}" -f (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ"), $Level, $Message
    Add-Content -Path $logFile -Value $line -Encoding utf8
    Write-Host $line
}

function Test-Port {
    param([int]$Port)
    $null -ne (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
}

function Start-Mongo {
    if (Test-Port 27017) { return }
    Write-Log INFO "mongod not listening - starting"
    Start-Process powershell -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $root 'start-mongo.ps1') -WindowStyle Hidden
    for ($i = 0; $i -lt 30; $i++) {
        if (Test-Port 27017) { Write-Log INFO "mongod up"; return }
        Start-Sleep -Seconds 1
    }
    Write-Log ERROR "mongod did not come up within 30s"
}

function Get-ServerProcess {
    # The collector is `python -m botmaximus.main` from the project venv. Match on
    # the venv path so a different Python on this machine is never mistaken for it.
    Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -like "*$python*" -and $_.CommandLine -like "*botmaximus.main*" } |
        Select-Object -First 1
}

function Start-Server {
    $existing = Get-ServerProcess
    if ($existing) {
        Write-Log INFO "stopping collector pid $($existing.ProcessId)"
        Stop-Process -Id $existing.ProcessId -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 3
    }
    Write-Log INFO "starting collector"
    # Capture the collector's own output. Without this it goes nowhere: the
    # process is started hidden and detached, so a startup failure -- a bad
    # venue fetch, a Mongo refusal, an import error -- produces a collector that
    # simply is not there, with no trace of why. That cost real debugging time.
    $out = Join-Path $root "data\collector.log"
    $err = Join-Path $root "data\collector.err"
    Start-Process -FilePath $python -ArgumentList '-m','botmaximus.main' `
        -WorkingDirectory (Join-Path $root "server") -WindowStyle Hidden `
        -RedirectStandardOutput $out -RedirectStandardError $err
    $script:lastStart = Get-Date
    $script:strikes = 0
    $script:restarts += ,(Get-Date)
}

function Invoke-DailyBackup {
    if ($NoBackup) { return }
    $now = (Get-Date).ToUniversalTime()
    if ($now.Hour -lt $BackupHourUtc) { return }
    $stampFile = Join-Path $root "data\.last-backup"
    $today = $now.ToString("yyyy-MM-dd")
    if ((Test-Path $stampFile) -and ((Get-Content $stampFile -Raw).Trim() -eq $today)) { return }

    Write-Log INFO "running daily backup of $DbName -> $BackupDest"
    # The dump verifies itself and only prunes after verifying, so a failed
    # backup can never delete the last good one. Failure is logged and retried
    # tomorrow rather than being fatal -- a backup problem must not take the
    # collector down with it, because uptime protects data that no backup can
    # recover (liquidations and order book have no history endpoint).
    # Local Continue: in PowerShell 5.1 a native command's stderr, redirected
    # with 2>&1, becomes an ErrorRecord, and under the script's
    # ErrorActionPreference=Stop that is TERMINATING. A stray Python warning on
    # stderr would take the whole supervisor down mid-backup -- silently, since
    # the process simply ends. The backup's own exit code is the real verdict.
    $ErrorActionPreference = "Continue"
    & $python (Join-Path $PSScriptRoot "mongo_backup.py") dump --db $DbName --dest $BackupDest --keep $BackupKeep 2>&1 |
        ForEach-Object { Write-Log INFO "  backup| $_" }
    $backupExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($backupExit -eq 0) {
        Set-Content -Path $stampFile -Value $today -Encoding utf8
        Write-Log INFO "backup complete"
    } else {
        Write-Log ERROR "backup FAILED (exit $backupExit) - will retry tomorrow"
    }
}

function Get-RecentRestarts {
    $cutoff = (Get-Date).AddHours(-1)
    $script:restarts = @($script:restarts | Where-Object { $_ -gt $cutoff })
    return $script:restarts.Count
}

# ---------------------------------------------------------------- main loop
$script:strikes       = 0
$script:restarts      = @()
$script:lastStart     = [datetime]::MinValue
$script:lastHeartbeat = [datetime]::MinValue    # so the first healthy probe logs
$halted               = $false

Write-Log INFO "supervisor starting (interval ${IntervalSec}s, ${StaleStrikes} strikes, max ${MaxRestartsPerHr}/hr, db $DbName)"

# A supervisor must announce its own death. It died silently once during this
# build -- logged its start line, then nothing, while the collector it was
# meant to protect stayed down and the data it was meant to protect stopped
# arriving. Anything that ends this process now says so on the way out.
trap {
    Write-Log ERROR "FATAL (trap): $_"
    Write-Log ERROR "  at: $($_.InvocationInfo.PositionMessage -replace "`r?`n", ' ')"
    continue
}

try {
while ($true) {
    try {
        Start-Mongo

        if (-not (Get-ServerProcess)) {
            if ($halted) {
                Write-Log WARN "collector down but restart ceiling reached - not starting"
            } else {
                Write-Log WARN "collector process not running"
                Start-Server
            }
        }
        elseif (((Get-Date) - $script:lastStart).TotalSeconds -lt $StartGraceSec) {
            Write-Log INFO "within start grace period - not probing"
        }
        else {
            $output = & $python (Join-Path $PSScriptRoot "healthcheck.py") --url $ApiUrl 2>&1
            $code = $LASTEXITCODE
            if ($code -eq 0) {
                if ($script:strikes -gt 0) { Write-Log INFO "recovered: $output" }
                elseif (((Get-Date) - $script:lastHeartbeat).TotalMinutes -ge $HeartbeatMin) {
                    Write-Log INFO "heartbeat: $output"
                    $script:lastHeartbeat = Get-Date
                }
                $script:strikes = 0
                $halted = $false
            }
            else {
                $script:strikes++
                Write-Log WARN "unhealthy ($script:strikes/$StaleStrikes, exit $code): $output"
                if ($script:strikes -ge $StaleStrikes) {
                    if ((Get-RecentRestarts) -ge $MaxRestartsPerHr) {
                        # Backing off entirely is the right call: a restart loop
                        # reconnecting to Binance every few minutes risks a
                        # rate-limit ban, which costs more coverage than the
                        # outage it is trying to fix.
                        Write-Log ERROR "restart ceiling ($MaxRestartsPerHr/hr) reached - halting restarts, needs an operator"
                        $halted = $true
                        $script:strikes = 0
                    } else {
                        Write-Log ERROR "restarting collector after $script:strikes bad probes"
                        Start-Server
                    }
                }
            }
        }

        Invoke-DailyBackup
    }
    catch {
        Write-Log ERROR "supervisor error: $_"
    }

    if ($Once) { break }
    Start-Sleep -Seconds $IntervalSec
}
}
finally {
    # Reached on a clean -Once exit, and on ANY unhandled termination. The loop
    # is infinite, so outside -Once this line appearing in the log means the
    # supervisor stopped supervising and nothing is watching the collector.
    if (-not $Once) {
        Write-Log ERROR "supervisor LOOP EXITED - nothing is watching the collector"
    }
}
