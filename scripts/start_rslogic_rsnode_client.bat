@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "REPO_URL=https://github.com/yassuh/RsLogic.git"
set "REPO_BRANCH=main"
set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%ProgramData%\RsLogic\RsLogic"
set "BOOTSTRAP_PS=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "GIT_AVAILABLE=1"
set "REPO_REACHABLE=0"
set "BOOTSTRAP_NO_PULL="
set "REPAIR_PS=%SCRIPT_DIR%repair_rslogic_rsnode_client.ps1"

if not exist "%PS_EXE%" set "PS_EXE=powershell.exe"

rem Prefer a local repository when this launcher is run from inside a checked-out copy.
if exist "%SCRIPT_DIR%\rslogic_rsnode_client.ps1" (
    set "BOOTSTRAP_PS=%SCRIPT_DIR%rslogic_rsnode_client.ps1"
    for %%P in ("%BOOTSTRAP_PS%") do set "REPO_ROOT=%%~dpP\.."
) else if exist "%SCRIPT_DIR%\scripts\rslogic_rsnode_client.ps1" (
    set "BOOTSTRAP_PS=%SCRIPT_DIR%scripts\rslogic_rsnode_client.ps1"
    for %%P in ("%BOOTSTRAP_PS%") do set "REPO_ROOT=%%~dpP\.."
)

for %%R in ("%REPO_ROOT%") do set "REPO_ROOT=%%~fR\"

echo RsLogic RSNode client installer/runner
if exist "%BOOTSTRAP_PS%" (
    if not exist "%REPO_ROOT%\pyproject.toml" (
        echo Local bootstrap exists but repo root is not valid. Reinstalling into %REPO_ROOT%.
    )
) else (
    echo Bootstrap file not present at local location. Installing into %REPO_ROOT%.
)

if not exist "%REPO_ROOT%" mkdir "%REPO_ROOT%" >nul 2>&1

where git >nul 2>&1
if errorlevel 1 (
    set "GIT_AVAILABLE=0"
    echo WARNING: git not found. Running in local/offline mode.
)

if "%GIT_AVAILABLE%"=="1" (
    echo Checking connectivity to repository source: %REPO_URL% (%REPO_BRANCH%)
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "git ls-remote --exit-code --heads '%REPO_URL%' '%REPO_BRANCH%' >$null 2>&1"
    if errorlevel 1 (
        set "REPO_REACHABLE=0"
    ) else (
        set "REPO_REACHABLE=1"
    )
)

if "%REPO_REACHABLE%"=="1" (
    echo Repository source is reachable. Online mode enabled.
) else (
    echo Repository source is not reachable. Offline mode enabled.
    set "BOOTSTRAP_NO_PULL=-NoPull"
)

set "HAS_VALID_REPO=0"
if exist "%REPO_ROOT%\.git" (
    if exist "%REPO_ROOT%\pyproject.toml" (
        set "HAS_VALID_REPO=1"
    )
)

if "%HAS_VALID_REPO%"=="1" (
    if "%REPO_REACHABLE%"=="1" (
        echo Updating local checkout at %REPO_ROOT%.
        "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "git -C '%REPO_ROOT%' fetch --all --prune; if ($?) { git -C '%REPO_ROOT%' checkout '%REPO_BRANCH%' }; if ($?) { git -C '%REPO_ROOT%' pull --ff-only }"
        if errorlevel 1 (
            echo Warning: unable to update local checkout. Continuing with current files.
        )
    ) else (
        echo Skipping repo update while offline.
    )
) else (
    if "%GIT_AVAILABLE%"=="0" (
        echo ERROR: No valid repository found at %REPO_ROOT% and git is unavailable.
        echo Install or sync the repository manually, then rerun this script.
        pause >nul
        exit /b 1
    )

    if "%REPO_REACHABLE%"=="0" (
        echo ERROR: No valid repository found at %REPO_ROOT% and remote source is unreachable.
        echo Ensure network access to the repository source or pre-seed %REPO_ROOT%.
        pause >nul
        exit /b 1
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
        pause >nul
        exit /b 1
    )
)

set "BOOTSTRAP_PS=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"
if exist "%SCRIPT_DIR%\rslogic_rsnode_client.ps1" (
    copy /Y "%SCRIPT_DIR%\rslogic_rsnode_client.ps1" "%BOOTSTRAP_PS%" >nul
)

if exist "%REPAIR_PS%" (
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%REPAIR_PS%" -Path "%BOOTSTRAP_PS%"
) else (
    echo ERROR: Missing repair helper script (%REPAIR_PS%).
    pause >nul
    exit /b 1
)
if not exist "%BOOTSTRAP_PS%" (
    echo ERROR: Could not install a valid bootstrap script.
    echo Check that the clone completed correctly.
    pause >nul
    exit /b 1
)

if not exist "%REPO_ROOT%\pyproject.toml" (
    echo ERROR: Repository root is invalid at %REPO_ROOT%.
    pause >nul
    exit /b 1
)

echo.
echo Using repo: %REPO_ROOT%
if exist "%REPO_ROOT%\pyproject.toml" echo Repository detected locally.

"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%BOOTSTRAP_PS%" -RepoUrl "%REPO_URL%" -RepoPath "%REPO_ROOT%" -RepoBranch "%REPO_BRANCH%" %BOOTSTRAP_NO_PULL% -StartNow -StartDetached -AutoUpdate true
if errorlevel 1 (
    echo.
    echo Install or startup failed. Press any key to close.
    pause >nul
    exit /b 1
)

echo.
echo RsNode client stack started in background.
echo Use Task Manager / schtasks or logs to monitor.
pause >nul
