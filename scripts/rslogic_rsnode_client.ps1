[CmdletBinding()]
param(
    [string]$RepoUrl = "https://github.com/yassuh/RsLogic.git",
    [string]$RepoPath = "",
    [string]$RepoBranch = "main",
    [string]$PythonExecutable = "python",
    [string]$VenvPath = "",
    [string]$EnvFileName = ".env.rsnode-worker",
    [string]$NodeExecutable = "C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe",
    [string]$NodeDataRoot = "$env:LOCALAPPDATA\Epic Games\RealityScan\RSNodeData",

    [string]$RedisUrl = "",
    [string]$RedisHost = "localhost",
    [int]$RedisPort = 6379,
    [string]$RedisDb = "0",
    [string]$RedisPassword = "",

    [string]$ControlCommandQueue = "rslogic:control:commands",
    [string]$ControlResultQueue = "rslogic:control:results",
    [string]$QueueKey = "rslogic:jobs:queue",
    [string]$SdkBaseUrl = "http://localhost:8000",
    [string]$SdkClientId = "",
    [string]$SdkAppToken = "",
    [string]$SdkAuthToken = "",
    [string]$ServerHost = "",

    [int]$WorkerCount = 1,
    [int]$ClientWorkers = 1,
    [int]$ClientRestartSeconds = 8,
    [int]$WatchdogPollSeconds = 10,
    [int]$WatchdogStartupTimeoutSeconds = 60,
    [int]$WatchdogRestartCooldownSeconds = 5,
    [int]$WatchdogRepoUpdateIntervalSeconds = 300,
    [string]$WatchdogHealthUrl = "",
    [bool]$AutoUpdate = $true,

    [switch]$NoPull,
    [switch]$NoDeps,
    [switch]$StartNow,
    [switch]$StartDetached,
    [switch]$CreateStartupTask,
    [string]$StartupTaskName = "RsLogic.RSNodeClient",
    [switch]$NoPromptForSecrets,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-ScriptRoot {
    return Split-Path -Parent $MyInvocation.MyCommand.Path
}

if (-not $RepoPath) {
    $candidateFromScript = Join-Path (Resolve-ScriptRoot) ".."
    if (Test-Path (Join-Path $candidateFromScript "pyproject.toml")) {
        $defaultRepoPath = $candidateFromScript
    } else {
        $defaultRepoPath = Join-Path $env:ProgramData "RsLogic\RsLogic"
    }
    $RepoPath = $defaultRepoPath
}

function Write-Step {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $Message"
}

function Invoke-OrThrow {
    param([scriptblock]$Action, [string]$ErrorMessage)
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "$ErrorMessage (exit code: $LASTEXITCODE)"
    }
}

function Ensure-Tool {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Build-RedisUrl {
    param(
        [string]$Explicit,
        [string]$Host,
        [int]$Port,
        [string]$Database,
        [string]$Password
    )

    if ($Explicit -and $Explicit.Trim()) {
        return $Explicit.Trim()
    }

    $cleanHost = $Host.Trim()
    if (-not $cleanHost) {
        $cleanHost = "localhost"
    }

    $escapedPassword = if ($Password) { [System.Uri]::EscapeDataString($Password) } else { "" }
    if ($escapedPassword) {
        return ("redis://:{0}@{1}:{2}/{3}" -f $escapedPassword, $cleanHost, $Port, $Database)
    }
    return ("redis://{0}:{1}/{2}" -f $cleanHost, $Port, $Database)
}

function Resolve-Required {
    param(
        [string]$PromptText,
        [string]$Current
    )
    if ($Current) {
        return $Current
    }
    if ($NoPromptForSecrets) {
        return ""
    }
    return (Read-Host $PromptText)
}

function Write-EnvFile {
    param(
        [string]$Path,
        [hashtable]$Values
    )

    $lines = @(
        "# RsLogic RSNode worker environment."
        "RSLOGIC_APP_NAME=RsLogic RSNode Worker"
        "RSLOGIC_DEFAULT_GROUP_NAME=default-group"
        "RSLOGIC_QUEUE_BACKEND=redis"
        "RSLOGIC_QUEUE_START_LOCAL_WORKERS=false"
        "RSLOGIC_WORKER_COUNT=$($Values.worker_count)"
        "RSLOGIC_REDIS_URL=$($Values.redis_url)"
        "RSLOGIC_REDIS_QUEUE_KEY=$($Values.queue_key)"
        "RSLOGIC_CONTROL_COMMAND_QUEUE=$($Values.control_command_queue)"
        "RSLOGIC_CONTROL_RESULT_QUEUE=$($Values.control_result_queue)"
        "RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS=7200"
        "RSLOGIC_CONTROL_RESULT_TTL_SECONDS=3600"
        "RSLOGIC_CONTROL_BLOCK_TIMEOUT_SECONDS=2"

        "RSLOGIC_RSTOOLS_MODE=remote"
        "RSLOGIC_RSTOOLS_SDK_BASE_URL=$($Values.sdk_base_url)"
        "RSLOGIC_RSTOOLS_SDK_CLIENT_ID=$($Values.sdk_client_id)"
        "RSLOGIC_RSTOOLS_SDK_APP_TOKEN=$($Values.sdk_app_token)"
        "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN=$($Values.sdk_auth_token)"
        "RSLOGIC_LOG_LEVEL=INFO"
        "RSLOGIC_LOG_FORMAT=%(asctime)s %(levelname)s %(name)s: %(message)s"
        ""
        "# Installer/runtime helpers (read by run_rslogic_client_stack.ps1)"
        "RSLOGIC_CLIENT_PYTHON=$($Values.python_path)"
        "RSLOGIC_CLIENT_RESTART_SECONDS=$($Values.client_restart_seconds)"
        "RSLOGIC_CLIENT_VERBOSE=1"
        "RSLOGIC_RSNODE_EXECUTABLE=$($Values.node_executable)"
        "RSLOGIC_RSNODE_DATA_ROOT=$($Values.node_data_root)"
        "RSLOGIC_RSNODE_WATCHDOG_POLL_SECONDS=$($Values.watchdog_poll_seconds)"
        "RSLOGIC_RSNODE_WATCHDOG_STARTUP_TIMEOUT_SECONDS=$($Values.watchdog_startup_timeout_seconds)"
        "RSLOGIC_RSNODE_WATCHDOG_RESTART_COOLDOWN_SECONDS=$($Values.watchdog_restart_cooldown_seconds)"
        "RSLOGIC_RSNODE_REPO_URL=$RepoUrl"
        "RSLOGIC_RSNODE_REPO_BRANCH=$RepoBranch"
        "RSLOGIC_RSNODE_AUTO_UPDATE=$($Values.repo_auto_update)"
        "RSLOGIC_RSNODE_REPO_UPDATE_INTERVAL_SECONDS=$($Values.watchdog_repo_update_interval_seconds)"
    )

    if ($Values.watchdog_health_url) {
        $lines += "RSLOGIC_RSNODE_WATCHDOG_HEALTH_URL=$($Values.watchdog_health_url)"
    }

    Set-Content -Path $Path -Encoding UTF8 -Value ($lines -join "`r`n")
}

Write-Step "Starting RsLogic RSNode worker install/setup"

Ensure-Tool -Name $PythonExecutable

$repoPath = [System.IO.Path]::GetFullPath($RepoPath)
$venvResolved = if ($VenvPath) { $VenvPath } else { Join-Path $repoPath ".venv" }
$venvResolved = [System.IO.Path]::GetFullPath($venvResolved)
$envFilePath = Join-Path $repoPath $EnvFileName

if (Test-Path $repoPath) {
    $hasPyproject = Test-Path (Join-Path $repoPath "pyproject.toml")
    $hasGitCheckout = Test-Path (Join-Path $repoPath ".git")
    if (-not $hasPyproject -and -not $hasGitCheckout) {
        if ((Get-ChildItem -Path $repoPath -Force | Measure-Object).Count -gt 0) {
            throw "RepoPath exists but is not a git checkout and missing pyproject.toml: $repoPath"
        }
    }
} else {
    New-Item -ItemType Directory -Path (Split-Path -Parent $repoPath) -Force | Out-Null
    New-Item -ItemType Directory -Path $repoPath -Force | Out-Null
}

$hasPyproject = Test-Path (Join-Path $repoPath "pyproject.toml")
$hasGitCheckout = Test-Path (Join-Path $repoPath ".git")
$repoCheckoutPath = $repoPath

if (-not $hasPyproject -and -not $hasGitCheckout) {
    if (-not $RepoUrl) {
        throw "RepoPath points to a non-repo destination and no RepoUrl was provided."
    }

    Ensure-Tool -Name git
    Write-Step "Cloning repo from $RepoUrl to $repoPath"
    if (-not $DryRun) {
        Invoke-OrThrow -Action {
            git clone --branch $RepoBranch $RepoUrl $repoPath
        } -ErrorMessage "Failed to clone repository"
    }
    $hasPyproject = Test-Path (Join-Path $repoPath "pyproject.toml")
    $hasGitCheckout = Test-Path (Join-Path $repoPath ".git")
}

if ($hasGitCheckout -and -not $NoPull -and -not $DryRun) {
    Ensure-Tool -Name git
    Write-Step "Updating repository at $repoPath (branch=$RepoBranch)"
    Invoke-OrThrow -Action {
        git -C $repoPath fetch --all --prune
    } -ErrorMessage "Failed to fetch repository"
    Invoke-OrThrow -Action {
        git -C $repoPath checkout $RepoBranch
    } -ErrorMessage "Failed to checkout branch $RepoBranch"
    Invoke-OrThrow -Action {
        git -C $repoPath pull --ff-only
    } -ErrorMessage "Failed to pull latest changes"
}

if (-not (Test-Path (Join-Path $repoCheckoutPath "pyproject.toml"))) {
    throw "Could not locate rslogic repository at $repoCheckoutPath"
}

$repoRoot = $repoCheckoutPath
Set-Location $repoRoot

if ($ServerHost) {
    $resolvedServerHost = $ServerHost.Trim()
    if ($resolvedServerHost) {
        if (-not $RedisHost -or $RedisHost -eq "localhost") {
            $RedisHost = $resolvedServerHost
        }
        if (-not $SdkBaseUrl -or $SdkBaseUrl -eq "http://localhost:8000") {
            $SdkBaseUrl = "http://{0}:8000" -f $resolvedServerHost
        }
    }
}

if (-not (Test-Path $venvResolved)) {
    Write-Step "Creating virtual environment: $venvResolved"
    if (-not $DryRun) {
        Invoke-OrThrow -Action {
            & $PythonExecutable -m venv $venvResolved
        } -ErrorMessage "Failed to create virtual environment"
    }
}

$pythonPath = Join-Path $venvResolved "Scripts\python.exe"

if (-not $NoDeps -and -not $DryRun) {
    Write-Step "Installing project dependencies in venv"
    Invoke-OrThrow -Action {
        & $pythonPath -m pip install --upgrade pip
    } -ErrorMessage "Failed to upgrade pip"
    Invoke-OrThrow -Action {
        & $pythonPath -m pip install -e .
    } -ErrorMessage "Failed to install this package in editable mode"
}

$sdkClientId = Resolve-Required -Current $SdkClientId -PromptText "RSLOGIC_RSTOOLS_SDK_CLIENT_ID"
$sdkAppToken = Resolve-Required -Current $SdkAppToken -PromptText "RSLOGIC_RSTOOLS_SDK_APP_TOKEN"
$sdkAuthToken = Resolve-Required -Current $SdkAuthToken -PromptText "RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN"

$redisUrlResolved = Build-RedisUrl -Explicit $RedisUrl -Host $RedisHost -Port $RedisPort -Database $RedisDb -Password $RedisPassword

$envValues = @{
    worker_count               = $WorkerCount
    redis_url                  = $redisUrlResolved
    queue_key                  = $QueueKey
    control_command_queue      = $ControlCommandQueue
    control_result_queue       = $ControlResultQueue
    sdk_base_url               = $SdkBaseUrl
    sdk_client_id              = $sdkClientId
    sdk_app_token              = $sdkAppToken
    sdk_auth_token             = $sdkAuthToken
    python_path                = $pythonPath
    client_restart_seconds     = $ClientRestartSeconds
    node_executable            = $NodeExecutable
    node_data_root             = $NodeDataRoot
    watchdog_poll_seconds      = $WatchdogPollSeconds
    watchdog_startup_timeout_seconds = $WatchdogStartupTimeoutSeconds
    watchdog_restart_cooldown_seconds = $WatchdogRestartCooldownSeconds
    watchdog_health_url        = $WatchdogHealthUrl
    watchdog_repo_update_interval_seconds = $WatchdogRepoUpdateIntervalSeconds
    repo_auto_update          = if ($AutoUpdate) { "true" } else { "false" }
}

if ($WorkerCount -lt 1) {
    throw "WorkerCount must be at least 1"
}
if ($ClientWorkers -lt 1) {
    Write-Step "ClientWorkers was less than 1. Using WorkerCount instead."
    $ClientWorkers = $WorkerCount
}

if ($ClientWorkers -ne $WorkerCount) {
    Write-Step "Using client worker count=$ClientWorkers for rslogic-client"
    $envValues.worker_count = $ClientWorkers
}

Write-Step "Writing RSNode worker environment to $envFilePath"
if (-not $DryRun) {
    Write-EnvFile -Path $envFilePath -Values $envValues
}

$stackScript = Join-Path $repoRoot "scripts\run_rslogic_client_stack.ps1"
if (-not (Test-Path $stackScript)) {
    throw "Missing stack script: $stackScript"
}

Write-Step "Install completed. Env file: $envFilePath"
Write-Step "Start command: pwsh -File `"$stackScript`" -RepoRoot `"$repoRoot`" -EnvFile `"$EnvFileName`""

if ($CreateStartupTask) {
    if ($DryRun) {
        Write-Step "DRY RUN: would create startup task '$StartupTaskName'"
    } else {
        $taskCommand = "pwsh.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$stackScript`" -RepoRoot `"$repoRoot`" -EnvFile `"$EnvFileName`" -RepoUrl `"$RepoUrl`" -RepoBranch `"$RepoBranch`""
        if (-not $AutoUpdate) {
            $taskCommand += " -AutoUpdate false"
        }
        if ($WatchdogRepoUpdateIntervalSeconds -gt 0) {
            $taskCommand += " -RepoUpdateIntervalSeconds $WatchdogRepoUpdateIntervalSeconds"
        }
        Write-Step "Creating/replacing startup task: $StartupTaskName"
        $taskArgs = @(
            "/Create",
            "/F",
            "/TN", $StartupTaskName,
            "/SC", "ONLOGON",
            "/RL", "HIGHEST",
            "/IT",
            "/TR", $taskCommand
        )
        Invoke-OrThrow -Action {
            schtasks.exe @taskArgs
        } -ErrorMessage "Failed to create scheduled task. Open PowerShell as admin and retry, or start manually."
    }
}

if ($StartNow) {
    $stackArg = @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        $stackScript,
        "-RepoRoot",
        $repoRoot,
        "-EnvFile",
        $EnvFileName,
        "-RepoUrl",
        $RepoUrl,
        "-RepoBranch",
        $RepoBranch,
        "-AutoUpdate",
        "$AutoUpdate",
        "-RepoUpdateIntervalSeconds",
        "$WatchdogRepoUpdateIntervalSeconds"
    )

    if ($StartDetached) {
        Write-Step "Starting detached RSNode stack process"
        if ($DryRun) {
            Write-Step "DRY RUN: pwsh.exe $($stackArg -join ' ')"
        } else {
            Start-Process -FilePath "pwsh.exe" -ArgumentList $stackArg -WindowStyle Hidden | Out-Null
            Write-Step "Detached supervisor started. Check process list for run_rslogic_client_stack.ps1"
        }
    } else {
        Write-Step "Running supervisor in current console"
        if ($DryRun) {
            Write-Step "DRY RUN: command would run in foreground"
        } else {
            & "pwsh.exe" @stackArg
        }
    }
}
