@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "REPO_URL=https://github.com/yassuh/RsLogic.git"
set "REPO_BRANCH=main"
set "RSLOGIC_SERVER_HOST=192.168.193.56"
set "RSLOGIC_REDIS_PORT=9002"
set "RSLOGIC_SERVER_API_URL=http://192.168.193.56:8000"
set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%ProgramData%\RsLogic\RsLogic"
set "WATCHDOG_LOG=%ProgramData%\RsLogic\rsnode-watchdog.log"
if exist "%SCRIPT_DIR%..\pyproject.toml" (
    for %%P in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fP"
)
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "GIT_AVAILABLE=1"
set "REPO_REACHABLE=0"
set "BOOTSTRAP_NO_PULL="
set "REPAIR_PS=%SCRIPT_DIR%repair_rslogic_rsnode_client.ps1"
set "SKIP_BOOTSTRAP=0"
set "LOG_POLL_COUNT=8"
set "LOG_POLL_SECONDS=3"

if not exist "%PS_EXE%" set "PS_EXE=powershell.exe"

for %%R in ("%REPO_ROOT%") do set "REPO_ROOT=%%~fR\"
set "BOOTSTRAP_PS=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"

echo RsLogic RSNode client installer/runner
set "HAS_VALID_REPO=0"
if exist "%REPO_ROOT%\.git" (
    if exist "%REPO_ROOT%\pyproject.toml" (
        set "HAS_VALID_REPO=1"
    )
)
 
if "%HAS_VALID_REPO%"=="1" (
    set "SKIP_BOOTSTRAP=1"
    echo Repository is already installed at %REPO_ROOT%.
    echo Skipping install/update and ensuring stack is running.
) else (
    echo No valid local repository detected at %REPO_ROOT%.
    echo Running install/bootstrap flow.
    if exist "%BOOTSTRAP_PS%" (
        if not exist "%REPO_ROOT%\pyproject.toml" (
            echo Local bootstrap exists but repo root is not valid. Reinstalling into %REPO_ROOT%.
        )
    ) else (
        echo Bootstrap file not present at local location. Installing into %REPO_ROOT%.
    )
)

if not exist "%REPO_ROOT%" mkdir "%REPO_ROOT%" >nul 2>&1

where git >nul 2>&1
if errorlevel 1 (
    set "GIT_AVAILABLE=0"
    echo WARNING: git not found. Running in local/offline mode.
)

if "%HAS_VALID_REPO%"=="0" (
if "%GIT_AVAILABLE%"=="1" (
    echo Checking connectivity to repository source: %REPO_URL% (%REPO_BRANCH%)
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "git ls-remote --exit-code --heads '%REPO_URL%' '%REPO_BRANCH%' >$null 2>&1"
    if errorlevel 1 (
        set "REPO_REACHABLE=0"
    ) else (
        set "REPO_REACHABLE=1"
    )
)
)

if "%HAS_VALID_REPO%"=="1" (
    set "BOOTSTRAP_NO_PULL=-NoPull"
) else (
    if "%REPO_REACHABLE%"=="1" (
        echo Repository source is reachable. Online mode enabled.
    ) else (
        echo Repository source is not reachable. Offline mode enabled.
        set "BOOTSTRAP_NO_PULL=-NoPull"
    )
)

if "%HAS_VALID_REPO%"=="0" (
    if "%GIT_AVAILABLE%"=="0" (
        echo ERROR: No valid repository found at %REPO_ROOT% and git is unavailable.
        echo Install or sync the repository manually, then rerun this script.
        call :pause_on_error 1
    )

    if "%REPO_REACHABLE%"=="0" (
        echo ERROR: No valid repository found at %REPO_ROOT% and remote source is unreachable.
        echo Ensure network access to the repository source or pre-seed %REPO_ROOT%.
        call :pause_on_error 1
    )

    if exist "%REPO_ROOT%\pyproject.toml" (
        echo Existing folder at %REPO_ROOT% does not contain a valid git checkout. Reinstalling into %REPO_ROOT%.
    )
    if exist "%REPO_ROOT%\*" (
        rmdir /s /q "%REPO_ROOT%" >nul 2>&1
    )
    if not exist "%REPO_ROOT%" mkdir "%REPO_ROOT%" >nul 2>&1
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "git clone --branch '%REPO_BRANCH%' '%REPO_URL%' '%REPO_ROOT%'"
    if errorlevel 1 (
        echo ERROR: Failed to clone repository from %REPO_URL%.
        call :pause_on_error 1
    )

    if exist "%BOOTSTRAP_PS%" (
        if exist "%REPO_ROOT%\pyproject.toml" (
            set "HAS_VALID_REPO=1"
        )
    )
)

if "%HAS_VALID_REPO%"=="0" (
    echo ERROR: Could not create a valid local repository at %REPO_ROOT%.
    call :pause_on_error 1
)


set "BOOTSTRAP_PS=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"
if not exist "%REPAIR_PS%" set "REPAIR_PS=%REPO_ROOT%\scripts\repair_rslogic_rsnode_client.ps1"

if "%HAS_VALID_REPO%"=="0" (
    echo ERROR: Could not locate rslogic repository root at %REPO_ROOT%.
    call :pause_on_error 1
)

if "%SKIP_BOOTSTRAP%"=="0" (
    if exist "%REPAIR_PS%" (
        "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%REPAIR_PS%" -Path "%BOOTSTRAP_PS%"
        if errorlevel 1 (
            echo ERROR: Failed to repair bootstrap script.
            call :pause_on_error 1
        )
    ) else (
        echo WARNING: Missing repair helper script (%REPAIR_PS%).
        echo Continuing without repair. Parse errors will stop startup.
    )
)

if not exist "%BOOTSTRAP_PS%" (
    echo ERROR: Could not install a valid bootstrap script.
    echo Check that the clone completed correctly.
    call :pause_on_error 1
)

if not exist "%REPO_ROOT%\pyproject.toml" (
    echo ERROR: Repository root is invalid at %REPO_ROOT%.
    call :pause_on_error 1
)

if "%SKIP_BOOTSTRAP%"=="1" goto :status_only

"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%BOOTSTRAP_PS%" -RepoUrl "%REPO_URL%" -RepoPath "%REPO_ROOT%" -RepoBranch "%REPO_BRANCH%" -ServerHost "%RSLOGIC_SERVER_HOST%" -RedisHost "%RSLOGIC_SERVER_HOST%" -RedisPort "%RSLOGIC_REDIS_PORT%" -SdkBaseUrl "%RSLOGIC_SERVER_API_URL%" %BOOTSTRAP_NO_PULL% -StartNow -StartDetached -AutoUpdate true
if errorlevel 1 (
    echo.
    echo Install or startup failed.
    call :pause_on_error 1
)

echo.
echo Using repo: %REPO_ROOT%
echo Repository detected locally.
echo.
echo RsLogic RSNode client bootstrap complete.
goto :ensure_stack_running

:status_only
echo.
echo Using repo: %REPO_ROOT%
echo Existing installation detected. Skipping bootstrap/setup.
goto :ensure_stack_running

:show_status
echo.
echo ===== Runtime status =====
if exist "%REPO_ROOT%\.env.rsnode-worker" (
    echo ENV file: %REPO_ROOT%\.env.rsnode-worker
) else (
    echo WARNING: Missing environment file: %REPO_ROOT%\.env.rsnode-worker
)
if exist "%WATCHDOG_LOG%" (
    echo.
    echo ===== Latest rsnode-watchdog log (%WATCHDOG_LOG%) =====
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -Path '%WATCHDOG_LOG%' -Tail 80"
) else (
    echo.
    echo No watchdog log found at: %WATCHDOG_LOG%
)
echo.
echo ===== Process snapshot =====
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*run_rslogic_client_stack.ps1*' -or $_.CommandLine -like '*rsnode_watchdog.ps1*' -or $_.CommandLine -like '*rslogic.client.rsnode_client*' -or $_.Name -eq 'RSNode.exe' } | Select-Object ProcessId, Name, CommandLine | Format-Table -AutoSize"
goto :watchdog_log_loop

:watchdog_log_loop
echo.
echo ===== Live watchdog log watch =====
echo Showing recent log output (%LOG_POLL_COUNT% polls, %LOG_POLL_SECONDS%s interval).
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "$watchdogLog = '%WATCHDOG_LOG%'; $pollCount = %LOG_POLL_COUNT%; $sleepSeconds = %LOG_POLL_SECONDS%; for ($i=1; $i -le $pollCount; $i++) { $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'; if (Test-Path $watchdogLog) { Write-Host \"[$stamp] Poll $i/$pollCount - rsnode-watchdog.log\"; Get-Content -Path $watchdogLog -Tail 18 }; if ($i -lt $pollCount) { Start-Sleep -Seconds $sleepSeconds } }"
goto :hold_terminal

:hold_terminal
echo.
echo.
echo Script completed. Press any key to close.
pause >nul
goto :eof

:pause_on_error
set "ERROR_CODE=%~1"
if not defined ERROR_CODE set "ERROR_CODE=1"
echo.
echo Script exited with code %ERROR_CODE%.
echo Press any key to close.
pause >nul
exit /b %ERROR_CODE%

:ensure_stack_running
if not exist "%REPO_ROOT%\scripts\run_rslogic_client_stack.ps1" (
    echo.
    echo ERROR: Missing run_rslogic_client_stack.ps1 at %REPO_ROOT%\scripts.
    goto :show_status
)

if not exist "%REPO_ROOT%\.env.rsnode-worker" (
    echo.
    echo WARNING: Missing environment file: %REPO_ROOT%\.env.rsnode-worker
    echo Cannot auto-start until .env.rsnode-worker exists.
    goto :show_status
)

"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "if (Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*run_rslogic_client_stack.ps1*' -or $_.CommandLine -like '*rsnode_watchdog.ps1*' -or $_.CommandLine -like '*rslogic.client.rsnode_client*' }) { exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo.
    echo No running RSNode stack processes found. Starting supervisor...
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command " $repoRoot = '%REPO_ROOT%'; $stackScript = Join-Path $repoRoot 'scripts\run_rslogic_client_stack.ps1'; Start-Process -FilePath '%PS_EXE%' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$stackScript,'-RepoRoot',$repoRoot,'-EnvFile','.env.rsnode-worker','-RepoUrl','%REPO_URL%','-RepoBranch','%REPO_BRANCH%','-AutoUpdate','true') -WindowStyle Hidden | Out-Null"
    if errorlevel 1 (
        echo ERROR: Failed to start rslogic client stack.
    ) else (
        echo RSNode client stack launch requested. Watchdog and client should start shortly.
    )
) else (
    echo RSNode stack processes already running.
)
goto :show_status
