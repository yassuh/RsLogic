[CmdletBinding()]
param(
    [Parameter(HelpMessage = "Path to RSNode executable.")]
    [string]$ExecutablePath = "C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe",

    [Parameter(HelpMessage = "Process name portion to use as fallback when resolving instances.")]
    [string]$ProcessName = "RSNode",

    [Parameter(HelpMessage = "Session storage root for RSNode (critical for stable session paths).")]
    [string]$DataRoot = "$env:LOCALAPPDATA\Epic Games\RealityScan\RSNodeData",

    [Parameter(HelpMessage = "Argument name used by RSNode to set dataRoot.")]
    [string]$DataRootArgument = "--dataRoot",

    [Parameter(HelpMessage = "Optional extra startup arguments for RSNode.")]
    [string[]]$AdditionalArguments = @(),

    [Parameter(HelpMessage = "Optional health endpoint. Leave empty to only watch process lifetime.")]
    [string]$HealthUrl = "",

    [Parameter(HelpMessage = "Seconds between checks.")]
    [ValidateRange(1, 3600)]
    [int]$PollIntervalSeconds = 10,

    [Parameter(HelpMessage = "Seconds to wait after launching RSNode before declaring startup failed.")]
    [ValidateRange(1, 600)]
    [int]$StartupTimeoutSeconds = 60,

    [Parameter(HelpMessage = "Seconds to wait between restart attempts.")]
    [ValidateRange(1, 300)]
    [int]$RestartCooldownSeconds = 5,

    [Parameter(HelpMessage = "Maximum restarts before giving up. 0 means unlimited.")]
    [ValidateRange(0, 1000000)]
    [int]$MaxRestarts = 0,

    [Parameter(HelpMessage = "Path to local watchdog log file.")]
    [string]$LogPath = "$env:ProgramData\RsLogic\rsnode-watchdog.log",

    [Parameter(HelpMessage = "Skip HTTP health checks and only ensure process exists.")]
    [switch]$NoHealthCheck,

    [Parameter(HelpMessage = "How long to keep the monitor running before graceful shutdown in seconds.")]
    [int]$MaxRuntimeSeconds = 0,

    [Parameter(HelpMessage = "Path to repository root for periodic self-updates.")]
    [string]$RepoPath = "",

    [Parameter(HelpMessage = "Repository URL used if the repository is missing and can be re-cloned.")]
    [string]$RepoUrl = "",

    [Parameter(HelpMessage = "Branch to track for auto-update checks.")]
    [string]$RepoBranch = "main",

    [Parameter(HelpMessage = "Seconds between repository update checks. 0 disables periodic checks.")]
    [ValidateRange(0, 86400)]
    [int]$UpdateIntervalSeconds = 300,

    [Parameter(HelpMessage = "Enable automatic repository update checks.")]
    [bool]$AutoUpdate = $true,

    [Parameter(HelpMessage = "Restart RSNode when repository is updated.")]
    [bool]$RestartOnUpdate = $true
)

# Startup note:
# RealityScan node server uses `dataRoot` for session storage.
# README notes default path %LOCALAPPDATA%\Epic Games\RealityScan\RSNodeData.
# This watchdog keeps RSNode alive and launches it with:
# "C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe" --dataRoot "<path>" [extra args]

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $line = "[$timestamp] $Message"
    Write-Host $line
    $logDir = Split-Path -Path $LogPath -Parent
    if ($logDir -and -not (Test-Path $logDir)) {
        try {
            New-Item -ItemType Directory -Path $logDir -Force | Out-Null
        } catch {
            return
        }
    }
    try {
        Add-Content -Path $LogPath -Value $line
    } catch {
        # File logging is optional. Keep monitor alive even if this path is unavailable.
        return
    }
}

function Quote-Arg {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return '""'
    }
    if ($Value -match '[\s"|]') {
        return ('"' + ($Value -replace '"', '""') + '"')
    }
    return $Value
}

function Get-StartupCommand {
    $args = @()
    if ($DataRoot) {
        $args += $DataRootArgument
        $args += $DataRoot
    }
    if ($AdditionalArguments) {
        $args += $AdditionalArguments
    }

    $argText = ($args | ForEach-Object { Quote-Arg $_ }) -join " "
    return ("\"{0}\" {1}" -f $ExecutablePath, $argText).Trim()
}

function Get-RSNodeProcesses {
    $executableFull = $null
    if (Test-Path $ExecutablePath) {
        try {
            $executableFull = (Get-Item $ExecutablePath).FullName
        } catch {
            $executableFull = $null
        }
    }

    $processes = @()
    try {
        $processes = Get-Process -Name $ProcessName -ErrorAction Stop
    } catch {
        return @()
    }

    if (-not $executableFull) {
        return $processes
    }

    $matched = @()
    foreach ($process in $processes) {
        try {
            $path = (Resolve-Path $process.Path).Path
            if ($path -eq $executableFull) {
                $matched += $process
            }
        } catch {
            # Some processes do not expose .Path under restricted execution context.
        }
    }

    if ($matched.Count -gt 0) {
        return $matched
    }

    # Fallback to process-name matching if path could not be resolved.
    return $processes
}

function Start-RSNode {
    $arguments = @()
    if ($DataRoot) {
        $arguments += $DataRootArgument
        $arguments += $DataRoot
    }
    if ($AdditionalArguments) {
        $arguments += $AdditionalArguments
    }

    try {
        Write-Log "Starting RSNode with: $(Get-StartupCommand)"
        $proc = Start-Process -FilePath $ExecutablePath -ArgumentList $arguments -PassThru -WindowStyle Hidden
        Write-Log "RSNode launched pid=$($proc.Id)"
        return $proc.Id
    } catch {
        Write-Log "Failed to launch RSNode: $($_.Exception.Message)"
        return 0
    }
}

function Stop-RSNodeProcesses {
    param([array]$Processes)

    if ($Processes.Count -eq 0) {
        return
    }

    foreach ($proc in $Processes) {
        try {
            Write-Log "Stopping RSNode pid=$($proc.Id)"
            Stop-Process -Id $proc.Id -Force
        } catch {
            Write-Log "Unable to stop pid=$($proc.Id): $($_.Exception.Message)"
        }
    }

    $deadline = (Get-Date).AddSeconds(15)
    do {
        $alive = Get-RSNodeProcesses
        if (-not $alive) {
            break
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $deadline)

    foreach ($proc in (Get-RSNodeProcesses)) {
        try {
            Write-Log "Force killing stubborn RSNode pid=$($proc.Id)"
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        } catch {
            Write-Log "Force kill failed for pid=$($proc.Id): $($_.Exception.Message)"
        }
    }
}

function Test-Health {
    param([string]$Url)

    if ($NoHealthCheck -or [string]::IsNullOrWhiteSpace($Url)) {
        return $true
    }

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -Method Get -TimeoutSec 2
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300)
    } catch {
        return $false
    }
}

function Resolve-RepoPath {
    if ([string]::IsNullOrWhiteSpace($RepoPath)) {
        $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
        $candidate = Join-Path $scriptDir ".."
        if (Test-Path (Join-Path $candidate "pyproject.toml")) {
            return (Resolve-Path $candidate).Path
        }
        return $candidate
    }

    try {
        return (Resolve-Path $RepoPath).Path
    } catch {
        $parent = Split-Path -Path $RepoPath -Parent
        if ([string]::IsNullOrWhiteSpace($parent)) {
            throw "Invalid RepoPath: $RepoPath"
        }
        return (Resolve-Path $parent).Path
    }
}

function Get-RepoHead {
    param([string]$Path)

    try {
        $hash = & git -C $Path rev-parse HEAD 2>&1
        if ($LASTEXITCODE -ne 0) {
            return ""
        }
        return $hash.ToString().Trim()
    } catch {
        return ""
    }
}

function Update-Repository {
    param(
        [string]$Path,
        [string]$Url,
        [string]$Branch
    )

    if (-not (Test-Path $Path)) {
        if (-not $Url) {
            return $false
        }
        $parent = Split-Path -Path $Path -Parent
        if ($parent -and -not (Test-Path $parent)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }
        Write-Log "Repo path missing; cloning repository into: $Path"
        & git clone --branch $Branch $Url $Path
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to clone repository at $Path"
        }
        return $true
    }

    $gitDir = Join-Path $Path ".git"
    if (-not (Test-Path $gitDir)) {
        if (-not $Url) {
            Write-Log "Repo path is not a git checkout and RepoUrl is empty. Skipping update."
            return $false
        }

        if ((Get-ChildItem -Path $Path -Force -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0) {
            throw "Cannot clone repository into non-empty non-git path: $Path"
        }

        Write-Log "Cloning repository before updates: $Url"
        & git clone --branch $Branch $Url $Path
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to clone repository at $Path"
        }
        return $true
    }

    $before = Get-RepoHead -Path $Path
    if (-not $before) {
        Write-Log "Unable to read git HEAD for $Path. Skipping repo update."
        return $false
    }

    Write-Log "Checking for repository updates in $Path (branch=$Branch)"

    & git -C $Path fetch --all --prune
    if ($LASTEXITCODE -ne 0) {
        throw "git fetch failed for $Path"
    }

    & git -C $Path checkout $Branch
    if ($LASTEXITCODE -ne 0) {
        throw "git checkout $Branch failed for $Path"
    }

    & git -C $Path pull --ff-only
    if ($LASTEXITCODE -ne 0) {
        throw "git pull --ff-only failed for $Path"
    }

    $after = Get-RepoHead -Path $Path
    return ($before -ne $after)
}

function Try-UpdateRepository {
    param([string]$Path, [string]$Url, [string]$Branch)

    if (-not $AutoUpdate) {
        return $false
    }
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-Log "Auto-update skipped: git executable not found."
        return $false
    }

    return Update-Repository -Path $Path -Url $Url -Branch $Branch
}

function Test-PathExistsOrCreate {
    if (-not (Test-Path $ExecutablePath)) {
        throw "Executable not found: $ExecutablePath"
    }

    $dataRootParent = Split-Path -Path $DataRoot -Parent
    if ($DataRoot -and -not (Test-Path $DataRoot)) {
        Write-Log "Creating dataRoot directory: $DataRoot"
        New-Item -ItemType Directory -Path $DataRoot -Force | Out-Null
    } elseif ($dataRootParent -and -not (Test-Path $dataRootParent)) {
        New-Item -ItemType Directory -Path $dataRootParent -Force | Out-Null
    }
}

function Ensure-Singleton {
    $mutexName = "Global\RsLogic.RSNode.Watchdog"
    $new = New-Object System.Threading.Mutex($false, $mutexName, [ref]$created)
    if (-not $created) {
        Write-Log "Another rsnode_watchdog instance is already running. Exiting."
        exit 0
    }
    return $new
}

Set-Variable -Name resolvedRepoPath -Value (Resolve-RepoPath) -Scope Script
$lastRepoUpdateCheck = (Get-Date).AddSeconds(-[Math]::Max($UpdateIntervalSeconds, 1))
$mutex = Ensure-Singleton
try {
    Test-PathExistsOrCreate

    Write-Log "Starting RSNode watchdog"
    Write-Log "Monitored executable: $ExecutablePath"
    Write-Log "Process name filter: $ProcessName"
    Write-Log "Configured startup command: $(Get-StartupCommand)"
    Write-Log "Health URL: $(if ($HealthUrl) { $HealthUrl } else { '<disabled>' })"
    Write-Log "Auto-update: $AutoUpdate; RepoPath: $resolvedRepoPath; RepoUrl: $RepoUrl; Branch: $RepoBranch; IntervalSecs: $UpdateIntervalSeconds"

    $startups = 0
    $startedAt = Get-Date

    while ($true) {
        if ($MaxRuntimeSeconds -gt 0 -and (Get-Date) -gt $startedAt.AddSeconds($MaxRuntimeSeconds)) {
            Write-Log "Maximum runtime reached. Exiting watchdog."
            break
        }

        if ($AutoUpdate -and $UpdateIntervalSeconds -gt 0 -and (Get-Date) -gt $lastRepoUpdateCheck.AddSeconds($UpdateIntervalSeconds)) {
            $lastRepoUpdateCheck = Get-Date
            try {
                $updated = Try-UpdateRepository -Path $resolvedRepoPath -Url $RepoUrl -Branch $RepoBranch
                if ($updated) {
                    Write-Log "Repository updated."
                    $running = Get-RSNodeProcesses
                    if ($running.Count -gt 0) {
                        if ($RestartOnUpdate) {
                            Write-Log "Restarting RSNode due to update."
                            Stop-RSNodeProcesses -Processes $running
                            Start-Sleep -Seconds $RestartCooldownSeconds
                            continue
                        }
                        Write-Log "RSNode left running despite repo update."
                    }
                }
            } catch {
                Write-Log "Repo update failed: $($_.Exception.Message)"
            }
        }

        $processes = Get-RSNodeProcesses

        if ($processes.Count -eq 0) {
            if ($MaxRestarts -gt 0 -and $startups -ge $MaxRestarts) {
                Write-Log "Max restart limit reached ($MaxRestarts). Exiting."
                break
            }

            Write-Log "RSNode is not running. Starting instance."
            $pid = Start-RSNode
            if ($pid -le 0) {
                Write-Log "Startup failed. Retrying in $RestartCooldownSeconds seconds."
                Start-Sleep -Seconds $RestartCooldownSeconds
                continue
            }

            $startups += 1
            $deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
            do {
                Start-Sleep -Milliseconds 500
                if (Test-Health -Url $HealthUrl) {
                    Write-Log "RSNode health check passed after start."
                    break
                }
            } while ((Get-Date) -lt $deadline)

            if (-not (Test-Health -Url $HealthUrl)) {
                Write-Log "Startup timeout reached after $StartupTimeoutSeconds seconds. Restarting."
                $running = Get-RSNodeProcesses
                Stop-RSNodeProcesses -Processes $running
                Start-Sleep -Seconds $RestartCooldownSeconds
            }

            continue
        }

        if ($processes.Count -gt 1) {
            $ordered = $processes | Sort-Object StartTime
            $keep = $ordered[-1]
            $extra = $ordered | Where-Object { $_.Id -ne $keep.Id }
            if ($extra.Count -gt 0) {
                Write-Log "Detected $($extra.Count) extra RSNode process(es). Keeping pid=$($keep.Id), stopping duplicates."
                Stop-RSNodeProcesses -Processes $extra
            }
        }

        if (-not (Test-Health -Url $HealthUrl)) {
            Write-Log "Health check failed. Restarting RSNode."
            Stop-RSNodeProcesses -Processes $processes
            Start-Sleep -Seconds $RestartCooldownSeconds
            continue
        }

        Start-Sleep -Seconds $PollIntervalSeconds
    }
} finally {
    try {
        $mutex.ReleaseMutex() | Out-Null
        $mutex.Dispose()
    } catch {}
}
