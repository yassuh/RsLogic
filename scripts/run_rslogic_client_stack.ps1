[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$EnvFile = ".env.rsnode-worker",
    [switch]$Once,
    [switch]$DryRun,
    [string]$RepoUrl = "",
    [string]$RepoBranch = "main",
    [bool]$AutoUpdate = $true,
    [int]$RepoUpdateIntervalSeconds = 300
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $RepoRoot) {
    $RepoRoot = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) ".."
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $Message"
}

function Import-RsLogicEnvFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        throw "Environment file not found: $Path"
    }

    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        $parts = $trimmed -split '=', 2
        if ($parts.Count -ne 2) {
            continue
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($name) {
            Set-Item -Path "env:$name" -Value $value
        }
    }
}

function Get-Int {
    param(
        [string]$Name,
        [int]$Default
    )
    $raw = (Get-Item -Path "env:$Name" -ErrorAction SilentlyContinue)?.Value
    if (-not $raw) {
        return $Default
    }
    try {
        return [int][string]$raw
    } catch {
        return $Default
    }
}

function Start-RSNodeWatchdog {
    param(
        [string]$WatchdogPath,
        [string]$NodeExe,
        [string]$NodeDataRoot,
        [string]$RepoPath,
        [string]$RepoBranch,
        [string]$RepoUrl,
        [bool]$AutoUpdate,
        [int]$RepoUpdateIntervalSeconds
    )

    if (-not (Test-Path $WatchdogPath)) {
        throw "Watchdog script missing: $WatchdogPath"
    }

    $args = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        $WatchdogPath
    )

    if ($NodeExe) {
        $args += @("-ExecutablePath", $NodeExe)
    }
    if ($NodeDataRoot) {
        $args += @("-DataRoot", $NodeDataRoot)
    }
    if ($RepoPath) {
        $args += @("-RepoPath", $RepoPath)
    }
    if ($RepoBranch) {
        $args += @("-RepoBranch", $RepoBranch)
    }
    if ($RepoUrl) {
        $args += @("-RepoUrl", $RepoUrl)
    }
    if ($AutoUpdate) {
        $args += @("-AutoUpdate", "true")
    } else {
        $args += @("-AutoUpdate", "false")
    }
    if ($RepoUpdateIntervalSeconds -gt 0) {
        $args += @("-UpdateIntervalSeconds", "$RepoUpdateIntervalSeconds")
    }

    $watchdogHealthUrl = (Get-Item -Path "env:RSLOGIC_RSNODE_WATCHDOG_HEALTH_URL" -ErrorAction SilentlyContinue)?.Value
    if ($watchdogHealthUrl) {
        $args += @("-HealthUrl", $watchdogHealthUrl)
    }

    $watchdogPoll = Get-Int -Name "RSLOGIC_RSNODE_WATCHDOG_POLL_SECONDS" -Default 10
    $watchdogStartupTimeout = Get-Int -Name "RSLOGIC_RSNODE_WATCHDOG_STARTUP_TIMEOUT_SECONDS" -Default 60
    $watchdogRestartDelay = Get-Int -Name "RSLOGIC_RSNODE_WATCHDOG_RESTART_COOLDOWN_SECONDS" -Default 5

    if ($watchdogPoll -gt 0) {
        $args += @("-PollIntervalSeconds", "$watchdogPoll")
    }
    if ($watchdogStartupTimeout -gt 0) {
        $args += @("-StartupTimeoutSeconds", "$watchdogStartupTimeout")
    }
    if ($watchdogRestartDelay -gt 0) {
        $args += @("-RestartCooldownSeconds", "$watchdogRestartDelay")
    }

    Write-Log "Starting RSNode watchdog. Executable=$NodeExe DataRoot=$NodeDataRoot"
    if ($DryRun) {
        Write-Log "DRY RUN: pwsh.exe $($args -join ' ')"
        return $null
    }

    return Start-Process -FilePath "pwsh.exe" -ArgumentList $args -PassThru
}

function Stop-ProcessSafe {
    param([System.Diagnostics.Process]$Process, [string]$Name)

    if ($null -eq $Process) {
        return
    }
    if ($Process.HasExited) {
        return
    }

    Write-Log "Stopping $Name (pid=$($Process.Id))"
    try {
        $Process.CloseMainWindow() | Out-Null
        $Process.WaitForExit(5000) | Out-Null
    } catch {
        # ignore
    }

    if (-not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    }
}

$repoRootPath = [System.IO.Path]::GetFullPath($RepoRoot)
$watchdogScript = Join-Path $repoRootPath "scripts\rsnode_watchdog.ps1"
$stackEnvPath = if ([System.IO.Path]::IsPathRooted($EnvFile)) {
    $EnvFile
} else {
    Join-Path $repoRootPath $EnvFile
}

Import-RsLogicEnvFile -Path $stackEnvPath

$venvPython = (Get-Item -Path "env:RSLOGIC_CLIENT_PYTHON" -ErrorAction SilentlyContinue)?.Value
if (-not $venvPython) {
    $venvPython = Join-Path $repoRootPath ".venv\Scripts\python.exe"
}
$venvPython = (Resolve-Path $venvPython).Path

if (-not (Test-Path $venvPython)) {
    throw "Python executable not found for client: $venvPython"
}

$nodeExecutable = (Get-Item -Path "env:RSLOGIC_RSNODE_EXECUTABLE" -ErrorAction SilentlyContinue)?.Value
$nodeDataRoot = (Get-Item -Path "env:RSLOGIC_RSNODE_DATA_ROOT" -ErrorAction SilentlyContinue)?.Value
$clientWorkers = Get-Int -Name "RSLOGIC_WORKER_COUNT" -Default 1
$clientRestartSeconds = Get-Int -Name "RSLOGIC_CLIENT_RESTART_SECONDS" -Default 8
$repoUrlFromEnv = (Get-Item -Path "env:RSLOGIC_RSNODE_REPO_URL" -ErrorAction SilentlyContinue)?.Value
$repoBranchFromEnv = (Get-Item -Path "env:RSLOGIC_RSNODE_REPO_BRANCH" -ErrorAction SilentlyContinue)?.Value
$repoAutoUpdateRaw = (Get-Item -Path "env:RSLOGIC_RSNODE_AUTO_UPDATE" -ErrorAction SilentlyContinue)?.Value
$repoUpdateInterval = Get-Int -Name "RSLOGIC_RSNODE_REPO_UPDATE_INTERVAL_SECONDS" -Default $RepoUpdateIntervalSeconds

$effectiveRepoUrl = if ($RepoUrl) { $RepoUrl } elseif ($repoUrlFromEnv) { $repoUrlFromEnv } else { "" }
$effectiveRepoBranch = if ($RepoBranch) { $RepoBranch } elseif ($repoBranchFromEnv) { $repoBranchFromEnv } else { "main" }
$effectiveAutoUpdate = if ($repoAutoUpdateRaw) {
    switch -Regex ($repoAutoUpdateRaw.ToString().Trim().ToLowerInvariant()) {
        "^(0|false|off|no)$" { $false }
        "^(1|true|on|yes)$" { $true }
        default { $true }
    }
} else {
    $AutoUpdate
}

$clientArgs = @(
    "-m",
    "rslogic.client.rsnode_client",
    "run",
    "--workers",
    "$clientWorkers"
)
$envVerbose = (Get-Item -Path "env:RSLOGIC_CLIENT_VERBOSE" -ErrorAction SilentlyContinue)?.Value
if ($envVerbose -eq "1" -or $envVerbose -eq "true") {
    $clientArgs += "--verbose"
}

if ($DryRun) {
    Write-Log "DRY RUN: Python=$venvPython ClientArgs=$($clientArgs -join ' ')"
}

    $watchdogProcess = Start-RSNodeWatchdog `
    -WatchdogPath $watchdogScript `
    -NodeExe $nodeExecutable `
    -NodeDataRoot $nodeDataRoot `
    -RepoPath $repoRootPath `
    -RepoBranch $effectiveRepoBranch `
    -RepoUrl $effectiveRepoUrl `
    -AutoUpdate $effectiveAutoUpdate `
    -RepoUpdateIntervalSeconds $repoUpdateInterval
if (-not $watchdogProcess -and -not $DryRun) {
    throw "Failed to launch RSNode watchdog"
}

try {
    while ($true) {
if (-not $DryRun -and $watchdogProcess -and $watchdogProcess.HasExited) {
    Write-Log "RSNode watchdog exited; restarting"
        $watchdogProcess = Start-RSNodeWatchdog `
        -WatchdogPath $watchdogScript `
        -NodeExe $nodeExecutable `
        -NodeDataRoot $nodeDataRoot `
        -RepoPath $repoRootPath `
        -RepoBranch $effectiveRepoBranch `
        -RepoUrl $effectiveRepoUrl `
        -AutoUpdate $effectiveAutoUpdate `
        -RepoUpdateIntervalSeconds $repoUpdateInterval
    }

        Write-Log "Starting rslogic client worker"
        if (-not $DryRun) {
            & $venvPython @clientArgs
        } else {
            Write-Log "DRY RUN: exiting after one dry run cycle"
            break
        }

        if ($Once) {
            break
        }

        Write-Log "Client worker exited. Restarting in $clientRestartSeconds second(s)"
        Start-Sleep -Seconds [Math]::Max($clientRestartSeconds, 1)
    }
} finally {
    Stop-ProcessSafe -Process $watchdogProcess -Name "RSNode watchdog"
}
