@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%..\"
if not exist "%REPO_ROOT%pyproject.toml" (
    set "REPO_ROOT=%ProgramData%\RsLogic\RsLogic\"
)
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "RSLOGIC_REPO_URL=https://github.com/yassuh/RsLogic.git"

if not exist "%PS_EXE%" (
    set "PS_EXE=powershell.exe"
)

echo RsLogic RSNode client installer/runner
echo Using repo: %REPO_ROOT%
echo.
if exist "%REPO_ROOT%\pyproject.toml" (
    echo Repository detected locally.
) else (
    echo Repo not found at local path. Installer will clone from %RSLOGIC_REPO_URL%.
    echo.
)

"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%\rslogic_rsnode_client.ps1" -RepoUrl "%RSLOGIC_REPO_URL%" -RepoPath "%REPO_ROOT%" -StartNow -StartDetached -AutoUpdate true
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
