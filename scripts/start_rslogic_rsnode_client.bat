@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
if not exist "%PS_EXE%" set "PS_EXE=powershell.exe"
set "PS_SCRIPT=%SCRIPT_DIR%rslogic_rsnode_client.ps1"

if not exist "%PS_SCRIPT%" (
    echo Error: missing orchestrator script.
    echo Expected: %PS_SCRIPT%
    echo Place start_rslogic_rsnode_client.bat alongside rslogic_rsnode_client.ps1.
    pause
    exit /b 1
)

title RsLogic RSNode Client Orchestrator
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -NoExit -File "%PS_SCRIPT%" %*
