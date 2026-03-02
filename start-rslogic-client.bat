@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "ROOT=%~dp0"
if "%ROOT%"=="" set "ROOT=%CD%\"
set "VENV=%ROOT%.venv"
set "LOG_DIR=%ROOT%logs\client"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "RUNNING_CLIENT_PIDS="

for /f "tokens=2 delims==" %%p in ('wmic process where "name^='python.exe' and CommandLine like '%%rslogic.client.rsnode_client%%'" get ProcessId /value 2^>nul ^| findstr "^ProcessId="') do (
  set "RUNNING_CLIENT_PIDS=!RUNNING_CLIENT_PIDS! %%p"
)
for /f "tokens=2 delims==" %%p in ('wmic process where "name^='pythonw.exe' and CommandLine like '%%rslogic.client.rsnode_client%%'" get ProcessId /value 2^>nul ^| findstr "^ProcessId="') do (
  set "RUNNING_CLIENT_PIDS=!RUNNING_CLIENT_PIDS! %%p"
)
if defined RUNNING_CLIENT_PIDS (
  echo [ERROR] rslogic client already running:%RUNNING_CLIENT_PIDS%
  echo [ERROR] Stop it first: %ROOT%stop-rslogic-client.bat
  exit /b 1
)

set PYTHONUNBUFFERED=1
set "RSLOGIC_ROOT=%ROOT%"
set "PYTHONPATH=%RSLOGIC_ROOT%;%RSLOGIC_ROOT%rslogic\internal_tools\rstool-sdk\src"
set "PY_EXE=%VENV%\Scripts\python.exe"
set "LOG_FILE=%LOG_DIR%\\rslogic-client.log"
if not exist "%PY_EXE%" (
  echo [ERROR] Python executable not found: %PY_EXE%
  echo [ERROR] Re-run installer.bat to recreate the environment.
  exit /b 1
)
"%PY_EXE%" -u -m rslogic.client.rsnode_client 2>>"%LOG_FILE%" 1>>"%LOG_FILE%"
