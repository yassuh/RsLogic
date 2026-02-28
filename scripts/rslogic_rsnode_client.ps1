[CmdletBinding()]
param(
    [string]$RepoUrl = "https://github.com/yassuh/RsLogic.git",
    [string]$RepoBranch = "main",
    [string]$RepoPath = "",
    [string]$PythonExecutable = "python",
    [string]$VenvPath = "",
    [string]$NodeExecutable = "C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe",
    [string]$NodeDataRoot = "",
    [string]$NodeDataRootArgument = "-dataRoot",
    [string[]]$NodeArguments = @(),
    [string]$RedisUrl = "",
    [string]$RedisHost = "localhost",
    [int]$RedisPort = 9002,
    [string]$RedisDb = "0",
    [string]$RedisPassword = "",
    [string]$ControlCommandQueue = "rslogic:control:commands",
    [string]$ControlResultQueue = "rslogic:control:results",
    [string]$QueueKey = "rslogic:jobs:queue",
    [string]$ServerHost = "192.168.193.56",
    [string]$SdkBaseUrl = "http://localhost:8000",
    [string]$SdkClientId = "",
    [string]$SdkAppToken = "",
    [string]$SdkAuthToken = "",
    [int]$ClientWorkers = 1,
    [int]$NodePollSeconds = 10,
    [int]$NodeStartupTimeoutSeconds = 60,
    [int]$RepoUpdateIntervalSeconds = 300,
    [int]$LoopSleepSeconds = 8,
    [int]$ClientRestartDelaySeconds = 8,
    [int]$NodeRestartDelaySeconds = 5,
    [string]$NodeHealthUrl = "",
    [string]$LogPath = "$env:ProgramData\RsLogic\rsnode-orchestrator.log",
    [switch]$NoAutoUpdate,
    [switch]$NoPull,
    [switch]$NoDeps,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ProgressPreference = "SilentlyContinue"

$scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $PSCommandPath }
if (-not $NodeDataRoot) {
    $NodeDataRoot = Join-Path $env:LOCALAPPDATA "Epic Games\RealityScan\RSNodeData"
}

$resolvedRepoPath = ""
if ($RepoPath) {
    $resolvedRepoPath = [System.IO.Path]::GetFullPath($RepoPath)
} else {
    $scriptParent = (Resolve-Path (Join-Path $scriptRoot "..")).Path
    if (Test-Path (Join-Path $scriptParent "pyproject.toml")) {
        $resolvedRepoPath = $scriptParent
    } else {
        $resolvedRepoPath = [System.IO.Path]::GetFullPath((Join-Path $env:ProgramData "RsLogic\RsLogic"))
    }
}

if (-not $VenvPath) {
    $VenvPath = Join-Path $resolvedRepoPath ".venv"
}
$resolvedVenvPath = [System.IO.Path]::GetFullPath($VenvPath)
$envFilePath = Join-Path $resolvedRepoPath ".env.rsnode-worker"
$installHeadFile = Join-Path $resolvedVenvPath ".rslogic_install_head.txt"
$nodeLogPrefix = "RsLogic RSNode client orchestrator"
$loopStartTime = Get-Date
$cancelRequested = $false
$script:basePythonExecutable = ""

try {
    if ([System.Console].GetEvent("CancelKeyPress")) {
        [void][System.Console]::add_CancelKeyPress({
            param([object]$Sender, [System.ConsoleCancelEventArgs]$Args)
            $script:cancelRequested = $true
            $Args.Cancel = $true
        })
        $bootstrapMsg = "[{0}] [INFO] Registered Ctrl+C handler for graceful shutdown." -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff")
        Write-Host $bootstrapMsg
    }
} catch {
    $bootstrapMsg = "[{0}] [WARN] Ctrl+C handler not available in this PowerShell host; graceful shutdown via Ctrl+C may be limited." -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff")
    Write-Host $bootstrapMsg
}

$resolvedServerHost = $ServerHost.Trim()
if ($resolvedServerHost -and $resolvedServerHost -ne "localhost") {
    if (-not $PSBoundParameters.ContainsKey("RedisHost") -or -not $RedisHost -or $RedisHost -eq "localhost") {
        $RedisHost = $resolvedServerHost
    }
    if (-not $PSBoundParameters.ContainsKey("SdkBaseUrl") -or -not $SdkBaseUrl -or $SdkBaseUrl -eq "http://localhost:8000") {
        $SdkBaseUrl = "http://$resolvedServerHost:8000"
    }
}

if (-not $LogPath) {
    $LogPath = Join-Path $env:ProgramData "RsLogic\rsnode-orchestrator.log"
}
$logDir = Split-Path -Parent $LogPath
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}
$alreadyRunning = $false
$nodeStdOutPath = Join-Path $logDir "rsnode-stdout.log"
$nodeStdErrPath = Join-Path $logDir "rsnode-stderr.log"
$clientStdOutPath = Join-Path $logDir "rslogic-client-stdout.log"
$clientStdErrPath = Join-Path $logDir "rslogic-client-stderr.log"
$script:nodeStopReason = "not-started"
$script:clientStopReason = "not-started"

if (-not (Test-Path (Join-Path $resolvedRepoPath ".git"))) {
    $gitHint = if (Test-Path $resolvedRepoPath) { "Directory exists but is not a git repository: $resolvedRepoPath" } else { "No local repository found at $resolvedRepoPath" }
    Write-Host "$nodeLogPrefix | $gitHint"
}

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")

    $line = "[{0}] [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"), $Level, $Message
    Write-Host $line
    try {
        Add-Content -Path $LogPath -Value $line
    } catch {
        # Logging should not stop the orchestrator.
    }
}

function Get-RecentLogTail {
    param([string]$Path, [int]$LineCount = 20)
    if (-not (Test-Path $Path)) {
        return ""
    }
    try {
        $lines = Get-Content -Path $Path -Tail $LineCount -ErrorAction Stop
        if (-not $lines) {
            return ""
        }
        return $lines -join "`n"
    } catch {
        return ""
    }
}

function Ensure-Tool {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Build-RedisUrl {
    param([string]$Explicit, [string]$RedisHost, [int]$Port, [string]$Database, [string]$Password)
    if ($Explicit) {
        return $Explicit
    }

    $cleanHost = $RedisHost.Trim()
    if (-not $cleanHost) {
        $cleanHost = "localhost"
    }

    $escapedPassword = if ($Password) { [System.Uri]::EscapeDataString($Password) } else { "" }
    if ($escapedPassword) {
        return "redis://:{0}@{1}:{2}/{3}" -f $escapedPassword, $cleanHost, $Port, $Database
    }
    return "redis://{0}:{1}/{2}" -f $cleanHost, $Port, $Database
}

function Resolve-PythonCandidate {
    param([string]$Candidate)

    if ([string]::IsNullOrWhiteSpace($Candidate)) {
        return ""
    }

    $candidatePath = ""
    try {
        if (Test-Path $Candidate -PathType Leaf) {
            $candidatePath = (Resolve-Path $Candidate -ErrorAction SilentlyContinue).Path
        } else {
            $resolvedCommand = Get-Command $Candidate -CommandType Application -ErrorAction SilentlyContinue
            if ($resolvedCommand -and $resolvedCommand.Source) {
                $candidatePath = $resolvedCommand.Source
            }
        }
    } catch {
        return ""
    }

    if (-not $candidatePath -or -not (Test-Path $candidatePath)) {
        return ""
    }

    try {
        $leaf = (Split-Path -Leaf $candidatePath).ToLowerInvariant()
        if ($leaf -eq "py.exe") {
            $probe = & $candidatePath -3 -c "import sys; print(sys.executable)"
        } else {
            $probe = & $candidatePath -c "import sys; print(sys.executable)"
        }
        if ($LASTEXITCODE -ne 0) {
            return ""
        }

        $probePath = $probe.ToString().Trim()
        if ($probePath -and (Test-Path $probePath)) {
            return $probePath
        }
        return ""
    } catch {
        return ""
    }
}

function Get-SystemPythonExecutable {
    if ($script:basePythonExecutable) {
        return $script:basePythonExecutable
    }

    $candidates = @()
    if ($PythonExecutable) {
        $candidates += $PythonExecutable
    }
    $candidates += @("py", "python3", "python")

    foreach ($candidate in $candidates) {
        $resolved = Resolve-PythonCandidate -Candidate $candidate
        if ($resolved) {
            $script:basePythonExecutable = $resolved
            return $resolved
        }
    }

    throw "Python executable could not be resolved. Tried: $($candidates -join ', ')"
}

function Get-RepoHead {
    param([string]$Path)
    try {
        $head = & git -C $Path rev-parse HEAD
        if ($LASTEXITCODE -ne 0) {
            return ""
        }
        return $head.ToString().Trim()
    } catch {
        return ""
    }
}

function Ensure-Repository {
    if (-not (Test-Path $resolvedRepoPath)) {
        New-Item -ItemType Directory -Path $resolvedRepoPath -Force | Out-Null
    }

    if (-not (Test-Path (Join-Path $resolvedRepoPath ".git"))) {
        if (-not $RepoUrl) {
            throw "RepoPath points to a non-repo location and no RepoUrl was provided."
        }
        if ((Get-ChildItem -Path $resolvedRepoPath -Force -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0 -and -not $DryRun) {
            $backupName = "{0}.invalid-{1:yyyyMMddHHmmss}" -f $resolvedRepoPath, (Get-Date)
            Write-Log "Existing directory at $resolvedRepoPath is not a valid repo. Backing it up to $backupName and recreating."
            Move-Item -Path $resolvedRepoPath -Destination $backupName -Force
            New-Item -ItemType Directory -Path $resolvedRepoPath -Force | Out-Null
        }

        if (-not $NoPull -and -not $DryRun) {
            Write-Log "Cloning $RepoUrl -> $resolvedRepoPath ($RepoBranch)"
            & git clone --branch $RepoBranch $RepoUrl $resolvedRepoPath
            if ($LASTEXITCODE -ne 0) {
                throw "Git clone failed for $RepoUrl"
            }
        } else {
            throw "Repository missing at $resolvedRepoPath and cloning is disabled with -NoPull."
        }
        return $true
    }

    if (Test-Path (Join-Path $resolvedRepoPath "pyproject.toml")) {
        return Update-Repository
    }

    if ($NoPull) {
        throw "Invalid repository checkout at ${resolvedRepoPath}: missing pyproject.toml"
    }

    Write-Log "Existing repository at ${resolvedRepoPath} is invalid (missing pyproject.toml). Rebuilding."
    $backupName = "{0}.invalid-{1:yyyyMMddHHmmss}" -f $resolvedRepoPath, (Get-Date)
    if (-not $DryRun) {
        Move-Item -Path $resolvedRepoPath -Destination $backupName -Force
        New-Item -ItemType Directory -Path $resolvedRepoPath -Force | Out-Null
        Write-Log "Backed up invalid checkout to $backupName."
        Write-Log "Cloning $RepoUrl -> $resolvedRepoPath ($RepoBranch)"
        & git clone --branch $RepoBranch $RepoUrl $resolvedRepoPath
        if ($LASTEXITCODE -ne 0) {
            throw "Git clone failed for $RepoUrl"
        }
        return $true
    }

    return $true
}

function Update-Repository {
    if ($NoPull) {
        return $false
    }

    Write-Log "Checking for updates from $RepoUrl (branch=$RepoBranch)"
    $before = Get-RepoHead -Path $resolvedRepoPath
    & git -C $resolvedRepoPath fetch --all --prune
    if ($LASTEXITCODE -ne 0) {
        throw "git fetch failed."
    }

    & git -C $resolvedRepoPath checkout $RepoBranch
    if ($LASTEXITCODE -ne 0) {
        throw "git checkout $RepoBranch failed."
    }

    & git -C $resolvedRepoPath pull --ff-only
    if ($LASTEXITCODE -ne 0) {
        throw "git pull --ff-only failed."
    }

    $after = Get-RepoHead -Path $resolvedRepoPath
    return ($before -and $after -and $before -ne $after)
}

function Ensure-Venv {
    param([string]$PythonExecutable)

    if (-not (Test-Path $resolvedVenvPath)) {
        Write-Log "Creating python virtual environment at $resolvedVenvPath"
        if (-not $DryRun) {
            & $PythonExecutable -m venv $resolvedVenvPath
            if ($LASTEXITCODE -ne 0) {
                throw "Could not create virtual environment at $resolvedVenvPath"
            }
        }
    }
}

function Get-PythonExecutable {
    $path = Join-Path $resolvedVenvPath "Scripts\python.exe"
    if (Test-Path $path) {
        return $path
    }
    return Get-SystemPythonExecutable
}

function Install-ProjectDeps {
    if ($NoDeps) {
        Write-Log "Skipping dependency install due -NoDeps."
        return
    }
    if (-not (Test-Path (Join-Path $resolvedRepoPath "pyproject.toml"))) {
        throw "Cannot install dependencies without repository pyproject.toml at $resolvedRepoPath"
    }

    $python = Get-PythonExecutable
    Write-Log "Installing RsLogic into $resolvedVenvPath (python=$python)"
    if ($DryRun) {
        Write-Log "DRY RUN: pip install -e ."
        return
    }
    Push-Location $resolvedRepoPath
    try {
        & $python -m pip install --upgrade pip
        if ($LASTEXITCODE -ne 0) {
            throw "pip upgrade failed"
        }
        & $python -m pip install -e .
        if ($LASTEXITCODE -ne 0) {
            throw "pip install -e . failed"
        }
    } finally {
        Pop-Location
    }
}

function Test-NeedsDependencyInstall {
    param([string]$CurrentHead)

    if (-not (Test-Path (Join-Path $resolvedVenvPath "Scripts\python.exe"))) {
        return $true
    }
    if ($NoDeps) {
        return $false
    }
    if (-not (Test-Path $installHeadFile)) {
        return $true
    }

    try {
        $installedHead = (Get-Content -Path $installHeadFile -Raw).Trim()
        return $installedHead -ne $CurrentHead
    } catch {
        return $true
    }
}

function Set-DependencyInstallMarker {
    param([string]$CurrentHead)
    if ($DryRun) {
        return
    }
    Set-Content -Path $installHeadFile -Encoding UTF8 -Value $CurrentHead
}

function Build-RSNodeEnv {
    param([string]$RedisUrlValue, [string]$PythonPath)

    $values = @{
        RSLOGIC_APP_NAME = "RsLogic RSNode Worker"
        RSLOGIC_DEFAULT_GROUP_NAME = "default-group"
        RSLOGIC_QUEUE_BACKEND = "redis"
        RSLOGIC_REDIS_URL = $RedisUrlValue
        RSLOGIC_REDIS_QUEUE_KEY = $QueueKey
        RSLOGIC_CONTROL_COMMAND_QUEUE = $ControlCommandQueue
        RSLOGIC_CONTROL_RESULT_QUEUE = $ControlResultQueue
        RSLOGIC_CONTROL_BLOCK_TIMEOUT_SECONDS = "2"
        RSLOGIC_CONTROL_RESULT_TTL_SECONDS = "3600"
        RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS = "7200"

        RSLOGIC_WORKER_COUNT = [string]$ClientWorkers
        RSLOGIC_RSTOOLS_MODE = "remote"
        RSLOGIC_RSTOOLS_SDK_BASE_URL = $SdkBaseUrl
        RSLOGIC_RSTOOLS_SDK_CLIENT_ID = $SdkClientId
        RSLOGIC_RSTOOLS_SDK_APP_TOKEN = $SdkAppToken
        RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN = $SdkAuthToken

        RSLOGIC_LOG_LEVEL = "INFO"
        RSLOGIC_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        RSLOGIC_CLIENT_RESTART_SECONDS = [string]$ClientRestartDelaySeconds
        RSLOGIC_CLIENT_PYTHON = $PythonPath
        RSLOGIC_RSNODE_EXECUTABLE = $NodeExecutable
        RSLOGIC_RSNODE_DATA_ROOT = $NodeDataRoot
        RSLOGIC_RSNODE_WATCHDOG_POLL_SECONDS = [string]$NodePollSeconds
        RSLOGIC_RSNODE_WATCHDOG_STARTUP_TIMEOUT_SECONDS = [string]$NodeStartupTimeoutSeconds
        RSLOGIC_RSNODE_WATCHDOG_RESTART_COOLDOWN_SECONDS = [string]$NodeRestartDelaySeconds
        RSLOGIC_RSNODE_REPO_URL = $RepoUrl
        RSLOGIC_RSNODE_REPO_BRANCH = $RepoBranch
        RSLOGIC_RSNODE_AUTO_UPDATE = $(-not $NoAutoUpdate).ToString().ToLowerInvariant()
        RSLOGIC_RSNODE_REPO_UPDATE_INTERVAL_SECONDS = [string]$RepoUpdateIntervalSeconds
        RSLOGIC_RSNODE_WATCHDOG_HEALTH_URL = $NodeHealthUrl
    }
    if ($NodeHealthUrl) {
        $values["RSLOGIC_RSNODE_WATCHDOG_HEALTH_URL"] = $NodeHealthUrl
    }

    $lines = @()
    foreach ($entry in $values.GetEnumerator()) {
        if ($entry.Value -ne $null -and $entry.Value -ne "") {
            $lines += "{0}={1}" -f $entry.Key, $entry.Value
        }
    }
    Set-Content -Path $envFilePath -Encoding UTF8 -Value $lines
    return $values
}

function Test-ProcessAlive {
    param([System.Diagnostics.Process]$Process)
    if (-not $Process) {
        return $false
    }
    try {
        if ($Process.HasExited) {
            return $false
        }
        Get-Process -Id $Process.Id -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Stop-ManagedProcess {
    param([System.Diagnostics.Process]$Process, [string]$Name)
    if (-not (Test-ProcessAlive $Process)) {
        return
    }
    Write-Log "Stopping $Name (pid=$($Process.Id))"
    try {
        $Process.CloseMainWindow() | Out-Null
        $Process.WaitForExit(3000) | Out-Null
    } catch {
        # ignore
    }
    if (-not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    }
}

function Stop-Processes {
    param([System.Diagnostics.Process]$NodeProcess, [System.Diagnostics.Process]$ClientProcess)
    Stop-ManagedProcess -Process $ClientProcess -Name "rslogic-client"
    Stop-ManagedProcess -Process $NodeProcess -Name "RSNode.exe"
}

function Get-NodeHealth {
    if (-not $NodeHealthUrl) {
        return $true
    }
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $NodeHealthUrl -Method Get -TimeoutSec 2
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300)
    } catch {
        return $false
    }
}

function Start-RSNode {
    if (-not (Test-Path $NodeExecutable)) {
        throw "RSNode executable missing: $NodeExecutable"
    }

    if (-not (Test-Path $NodeDataRoot)) {
        New-Item -ItemType Directory -Path $NodeDataRoot -Force | Out-Null
    }

    if ($DryRun) {
        Write-Log "DRY RUN: skip RSNode launch"
        return $null
    }

    $rootArgCandidates = @()
    if ($NodeDataRoot) {
        if ($NodeDataRootArgument) {
            $rootArgCandidates += $NodeDataRootArgument
        }
        if ($NodeDataRootArgument -eq "--dataRoot") {
            $rootArgCandidates += "-dataRoot"
        } elseif ($NodeDataRootArgument -eq "-dataRoot") {
            $rootArgCandidates += "--dataRoot"
        } else {
            $rootArgCandidates += "--dataRoot"
            $rootArgCandidates += "-dataRoot"
        }
    } else {
        $rootArgCandidates = @("")
    }

    $rootArgCandidates = @($rootArgCandidates | Where-Object { $_ -ne "" } | Select-Object -Unique)
    if (-not $rootArgCandidates) {
        $rootArgCandidates = @("")
    }

    $attempt = 0
    $lastError = ""
    foreach ($rootArg in $rootArgCandidates) {
        $attempt += 1
        $nodeArgs = @()
        if ($NodeDataRoot) {
            $nodeArgs += $rootArg
            $nodeArgs += $NodeDataRoot
        }
        if ($NodeArguments) {
            $nodeArgs += $NodeArguments
        }

        Write-Log "Starting RSNode (attempt=$attempt arg=$rootArg): $NodeExecutable $($nodeArgs -join ' ')"
        try {
            $nodeProcess = Start-Process -FilePath $NodeExecutable -ArgumentList $nodeArgs -PassThru -WindowStyle Hidden -RedirectStandardOutput $nodeStdOutPath -RedirectStandardError $nodeStdErrPath -ErrorAction Stop
            Start-Sleep -Milliseconds 900
            if ($nodeProcess.HasExited) {
                $nodeExitReason = "exit-code=$($nodeProcess.ExitCode)"
                $stderrTail = Get-RecentLogTail -Path $nodeStdErrPath -LineCount 30
                if ($stderrTail) {
                    Write-Log "RSNode exited immediately (attempt $attempt) ($nodeExitReason). stderr tail: $stderrTail" "ERROR"
                } else {
                    Write-Log "RSNode exited immediately (attempt $attempt) ($nodeExitReason)." "ERROR"
                }
                $script:nodeStopReason = "attempt=$attempt-$nodeExitReason"
                if ($attempt -lt $rootArgCandidates.Count) {
                    continue
                }
                return $null
            }
            $script:nodeStopReason = "running"
            return $nodeProcess
        } catch {
            $lastError = "$($_.Exception.Message)"
            Write-Log "RSNode start attempt $attempt failed with arg '$rootArg': $lastError" "WARN"
            $script:nodeStopReason = "attempt=$($attempt):$($lastError)"
            if ($attempt -ge $rootArgCandidates.Count) {
                throw "Failed to start RSNode. Last error: $lastError"
            }
        }
    }
    throw "Failed to start RSNode. Last error: $lastError"
}

function Start-RSLogicClient {
    param([hashtable]$EnvValues, [string]$PythonPath)

    $pythonArgs = @(
        "-m",
        "rslogic.client.rsnode_client",
        "run",
        "--workers",
        [string]$ClientWorkers
    )

    $pythonArgLine = $pythonArgs -join " "
    Write-Log "Starting rslogic-client: $PythonPath $pythonArgLine"
    if ($DryRun) {
        return $null
    }

    $backup = @{}
    foreach ($key in $EnvValues.Keys) {
        $envVar = "env:$key"
        $exists = Get-Item -Path $envVar -ErrorAction SilentlyContinue
        if ($exists) {
            $backup[$key] = $exists.Value
        } else {
            $backup[$key] = $null
        }
        Set-Item -Path $envVar -Value $EnvValues[$key]
    }

    try {
        # Preferred launch path.
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $PythonPath
        $psi.Arguments = $pythonArgLine
        $psi.WorkingDirectory = $resolvedRepoPath
        $psi.UseShellExecute = $false
        $psi.CreateNoWindow = $true

        try {
            $process = New-Object System.Diagnostics.Process
            $process.StartInfo = $psi
            $started = $process.Start()
            if (-not $started) {
                throw "Process.Start() returned false for client process."
            }
            Start-Sleep -Milliseconds 900
            if ($process.HasExited) {
                $processWaitOutput = ""
                try {
                    $processWaitOutput = $process.StandardError.ReadToEnd()
                } catch {
                    # ignore
                }
                $fallbackOutput = ""
                if (-not $processWaitOutput) {
                    $fallbackOutput = Get-RecentLogTail -Path $clientStdErrPath -LineCount 30
                } else {
                    $fallbackOutput = $processWaitOutput
                }
                if ($fallbackOutput) {
                    Write-Log "Client process exited immediately (exit-code=$($process.ExitCode)). stderr: $fallbackOutput" "ERROR"
                } else {
                    Write-Log "Client process exited immediately (exit-code=$($process.ExitCode))."
                }
                $script:clientStopReason = "exit-code=$($process.ExitCode)"
                return $null
            }
            $script:clientStopReason = "running"
            return $process
        } catch {
            Write-Log "Launch attempt 1 failed (ProcessStartInfo). type=$($_.Exception.GetType().FullName) message=$($_.Exception.Message)" "WARN"
        }

        # Fallback 1: direct Start-Process with string arguments.
        try {
            $process = Start-Process -FilePath $PythonPath -ArgumentList $pythonArgLine -PassThru -WindowStyle Hidden -RedirectStandardError $clientStdErrPath -RedirectStandardOutput $clientStdOutPath -ErrorAction Stop
            Start-Sleep -Milliseconds 900
            if ($process.HasExited) {
                $tail = Get-RecentLogTail -Path $clientStdErrPath -LineCount 30
                if ($tail) {
                    Write-Log "Launch attempt 1 returned process that exited immediately (exit-code=$($process.ExitCode)). stderr: $tail" "ERROR"
                } else {
                    Write-Log "Launch attempt 1 returned process that exited immediately (exit-code=$($process.ExitCode))."
                }
                $script:clientStopReason = "exit-code=$($process.ExitCode)"
                return $null
            }
            $script:clientStopReason = "running"
            return $process
        } catch {
            Write-Log "Launch attempt 2 failed (Start-Process string args). type=$($_.Exception.GetType().FullName) message=$($_.Exception.Message)" "WARN"
        }

        # Fallback 2: direct Start-Process with array arguments.
        try {
            $process = Start-Process -FilePath $PythonPath -ArgumentList $pythonArgs -PassThru -WindowStyle Hidden -RedirectStandardError $clientStdErrPath -RedirectStandardOutput $clientStdOutPath -ErrorAction Stop
            Start-Sleep -Milliseconds 900
            if ($process.HasExited) {
                $tail = Get-RecentLogTail -Path $clientStdErrPath -LineCount 30
                if ($tail) {
                    Write-Log "Launch attempt 2 returned process that exited immediately (exit-code=$($process.ExitCode)). stderr: $tail" "ERROR"
                } else {
                    Write-Log "Launch attempt 2 returned process that exited immediately (exit-code=$($process.ExitCode))."
                }
                $script:clientStopReason = "exit-code=$($process.ExitCode)"
                return $null
            }
            $script:clientStopReason = "running"
            return $process
        } catch {
            Write-Log "Launch attempt 3 failed (Start-Process array args). type=$($_.Exception.GetType().FullName) message=$($_.Exception.Message)" "WARN"
        }

        # Fallback 3: CMD wrapper with explicit quoting.
        $cmd = "`"$PythonPath`" $pythonArgLine"
        $cmdPath = Join-Path $env:SystemRoot "System32\cmd.exe"
        try {
            $process = Start-Process -FilePath $cmdPath -ArgumentList @("/d", "/c", $cmd) -PassThru -WindowStyle Hidden -RedirectStandardError $clientStdErrPath -RedirectStandardOutput $clientStdOutPath -ErrorAction Stop
            Start-Sleep -Milliseconds 900
            if ($process.HasExited) {
                $tail = Get-RecentLogTail -Path $clientStdErrPath -LineCount 30
                if ($tail) {
                    Write-Log "Launch attempt 3 returned process that exited immediately (exit-code=$($process.ExitCode)). stderr: $tail" "ERROR"
                } else {
                    Write-Log "Launch attempt 3 returned process that exited immediately (exit-code=$($process.ExitCode))."
                }
                $script:clientStopReason = "exit-code=$($process.ExitCode)"
                return $null
            }
            $script:clientStopReason = "running"
            return $process
        } catch {
            $fallbackMessage = "$($_.Exception.GetType().FullName): $($_.Exception.Message)"
            Write-Log "Launch attempt 4 failed (cmd wrapper). $fallbackMessage" "ERROR"
            throw "All client launch strategies failed. Last error: $fallbackMessage"
        }
    } finally {
        foreach ($key in $backup.Keys) {
            $envVar = "env:$key"
            if ($null -eq $backup[$key]) {
                Remove-Item $envVar -ErrorAction SilentlyContinue
            } else {
                Set-Item -Path $envVar -Value $backup[$key]
            }
        }
    }
}

function Show-Status {
    param([System.Diagnostics.Process]$NodeProcess, [System.Diagnostics.Process]$ClientProcess, [string]$RepoHead, [bool]$AutoUpdate)
    $nodeUp = if (Test-ProcessAlive $NodeProcess) { $NodeProcess.Id } else { "stopped/$script:nodeStopReason" }
    $clientUp = if (Test-ProcessAlive $ClientProcess) { $ClientProcess.Id } else { "stopped/$script:clientStopReason" }
    $health = if (Get-NodeHealth) { "ok" } else { "degraded" }
    $uptime = ((Get-Date) - $loopStartTime).ToString("dd\.hh\:mm\:ss")
    Write-Log "STATUS node=$nodeUp client=$clientUp autoUpdate=$AutoUpdate health=$health repo=$RepoHead uptime=$uptime"
}

function Ensure-Singleton {
    $mutexName = "Global\RsLogic.RSNodeClientOrchestrator"
    try {
        $created = $false
        $mutex = New-Object System.Threading.Mutex($false, $mutexName, [ref]$created)
        if (-not $created) {
            $script:alreadyRunning = $true
            return $null
        }
        if (-not $mutex.WaitOne([TimeSpan]::FromSeconds(3))) {
            $script:alreadyRunning = $true
            $mutex.Dispose()
            return $null
        }
        $script:ownsMutex = $true
        return $mutex
    } catch {
        Write-Log "Singleton lock unavailable. Running without process lock." "WARN"
        $script:ownsMutex = $false
        return $null
    }
}

$script:ownsMutex = $false
$singleton = Ensure-Singleton
$nodeProcess = $null
$clientProcess = $null
$alreadyRunning = [bool]$alreadyRunning
if ($alreadyRunning) {
    Write-Log "Another orchestrator is already running. Exiting this instance."
    exit 0
}
if ($singleton) {
    Write-Log "Acquired singleton lock."
}

try {
    Ensure-Tool -Name git
    $basePython = Get-SystemPythonExecutable

    Write-Log "$nodeLogPrefix bootstrapping"
    Write-Log "Repo path: $resolvedRepoPath"

    $bootstrapHead = Get-RepoHead -Path $resolvedRepoPath
    if ($bootstrapHead) {
        Write-Log "Repository HEAD before bootstrap/update: $bootstrapHead"
    }

    $initialUpdate = Ensure-Repository
    if (-not (Test-Path $envFilePath)) {
        $null = New-Item -ItemType File -Path $envFilePath -Force
    }

    $redisConnection = Build-RedisUrl -Explicit $RedisUrl -RedisHost $RedisHost -Port $RedisPort -Database $RedisDb -Password $RedisPassword
    Write-Log "Resolved Python executable: $basePython"
    Ensure-Venv -PythonExecutable $basePython
    $pythonForClient = Get-PythonExecutable
    if (-not (Test-Path $pythonForClient)) {
        throw "Python executable not found in virtual environment: $pythonForClient"
    }

    $envValues = Build-RSNodeEnv -RedisUrlValue $redisConnection -PythonPath $pythonForClient
    $currentHead = Get-RepoHead -Path $resolvedRepoPath
    $shouldInstall = Test-NeedsDependencyInstall -CurrentHead $currentHead
    if ($shouldInstall) {
        if ($currentHead -and -not $DryRun) {
            Write-Log "Installing dependencies for checkout $currentHead"
        }
        Install-ProjectDeps
        Set-DependencyInstallMarker -CurrentHead $currentHead
    } else {
        Write-Log "Skipping dependency install; environment already initialized for repository commit $currentHead."
    }
    if (-not (Test-Path $pythonForClient)) {
        throw "Python executable not found in venv: $pythonForClient"
    }

    $lastUpdateCheck = Get-Date
    $nextStatus = Get-Date
    Write-Log "Startup complete. Entering watch loop."

    if ($initialUpdate) {
        Write-Log "Repository bootstrap detected; fresh install complete."
    }

    while (-not $cancelRequested) {
        $updated = $false
        if (-not $NoAutoUpdate -and -not $NoPull -and $RepoUpdateIntervalSeconds -gt 0 -and (Get-Date) -ge $lastUpdateCheck.AddSeconds($RepoUpdateIntervalSeconds)) {
            $lastUpdateCheck = Get-Date
            try {
                $updated = Update-Repository
            } catch {
                Write-Log "Update check failed: $($_.Exception.Message)" "WARN"
                $updated = $false
            }
        }

        if ($updated) {
            Write-Log "Repository changed. Running dependency refresh and restarting managed services."
            $updatedHead = Get-RepoHead -Path $resolvedRepoPath
            Ensure-Venv -PythonExecutable $basePython
            $pythonForClient = Get-PythonExecutable
            if (-not (Test-Path $pythonForClient)) {
                throw "Python executable not found in virtual environment after update: $pythonForClient"
            }
            $envValues = Build-RSNodeEnv -RedisUrlValue $redisConnection -PythonPath $pythonForClient
            Install-ProjectDeps
            Set-DependencyInstallMarker -CurrentHead $updatedHead
            Stop-Processes -NodeProcess $nodeProcess -ClientProcess $clientProcess
            $nodeProcess = $null
            $clientProcess = $null
            Write-Log "Repo HEAD now $updatedHead"
        }

        if (-not (Test-ProcessAlive $nodeProcess)) {
            if ($nodeProcess -and $nodeProcess.HasExited) {
                try {
                    $script:nodeStopReason = "exit-code=$($nodeProcess.ExitCode)"
                } catch {
                    $script:nodeStopReason = "terminated"
                }
            }
            $nodeProcess = Start-RSNode
            if ($nodeProcess) {
                $healthy = if ($NodeHealthUrl) { Get-NodeHealth } else { $true }
                if (-not $healthy) {
                    Write-Log "RSNode failed startup health check." "WARN"
                    Stop-ManagedProcess -Process $nodeProcess -Name "RSNode.exe"
                    Start-Sleep -Seconds $NodeRestartDelaySeconds
                    $nodeProcess = $null
                }
            }
        }

        if ($nodeProcess -and -not (Get-NodeHealth)) {
            Write-Log "RSNode health check failed. Restarting RSNode and client."
            Stop-Processes -NodeProcess $nodeProcess -ClientProcess $clientProcess
            $nodeProcess = $null
            $clientProcess = $null
            Start-Sleep -Seconds $NodeRestartDelaySeconds
        }

        if (-not (Test-ProcessAlive $clientProcess)) {
            if ($clientProcess -and $clientProcess.HasExited) {
                try {
                    $script:clientStopReason = "exit-code=$($clientProcess.ExitCode)"
                } catch {
                    $script:clientStopReason = "terminated"
                }
            }
            if ($nodeProcess -and -not (Test-ProcessAlive $nodeProcess)) {
                Start-Sleep -Seconds 1
            }
            $clientProcess = Start-RSLogicClient -EnvValues $envValues -PythonPath $pythonForClient
        }

        if ((Get-Date) -ge $nextStatus) {
            $head = Get-RepoHead -Path $resolvedRepoPath
            Show-Status -NodeProcess $nodeProcess -ClientProcess $clientProcess -RepoHead $head -AutoUpdate (-not $NoAutoUpdate)
            $nextStatus = (Get-Date).AddSeconds([Math]::Max($LoopSleepSeconds, 5))
        }

        Start-Sleep -Seconds $LoopSleepSeconds
    }

    Write-Log "Shutdown requested. Stopping managed processes."
} catch {
    Write-Log "Fatal error: $($_.Exception.GetType().FullName): $($_.Exception.Message)" "ERROR"
    try {
        Write-Log "Exception details: $($_.Exception | Out-String)" "ERROR"
    } catch {
        # ignore if exception formatting fails
    }
    exit 1
} finally {
    try {
        Stop-Processes -NodeProcess $nodeProcess -ClientProcess $clientProcess
        if ($singleton -and $script:ownsMutex) {
            try {
                $singleton.ReleaseMutex() | Out-Null
            } catch {
                Write-Log "Could not release mutex: $($_.Exception.Message)" "WARN"
            } finally {
                try {
                    $singleton.Dispose()
                } catch {
                    # ignore
                }
            }
        }
        Write-Log "Orchestrator stopped."
    } catch {
        Write-Log "Error during cleanup: $($_.Exception.Message)" "WARN"
    }
}
