@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "ROOT=%~dp0"
if "%ROOT%"=="" set "ROOT=%CD%\"

set "VENV=.venv"
if not "%1"=="" set "ROOT=%~f1"

if not exist "%ROOT%" (
  echo [ERROR] invalid root: %ROOT%
  exit /b 1
)

if not exist "%ROOT%\pyproject.toml" (
  echo [ERROR] pyproject.toml not found under %ROOT%
  echo Place installer.bat at the repository root or pass the repo root as argument.
  exit /b 1
)

where uv >nul 2>&1
if errorlevel 1 (
  echo [ERROR] uv not found in PATH. Install uv and retry.
  exit /b 1
)

echo [*] Using Python 3.14t virtual environment in %ROOT%\%VENV%
cd /d "%ROOT%"
if not exist "%ROOT%\%VENV%" (
  uv venv --python 3.14t "%ROOT%\%VENV%"
) else (
  echo [*] Reusing existing %VENV%
)

call "%ROOT%\%VENV%\Scripts\activate.bat"
if errorlevel 1 exit /b 1

echo [*] Installing RsLogic in editable mode
uv pip install -e .
if errorlevel 1 exit /b 1

echo [*] Preparing client.env
if not exist "%ROOT%\client.env" (
  if exist "%ROOT%\client.env.template" (
    copy "%ROOT%\client.env.template" "%ROOT%\client.env" >nul
  ) else if exist "%ROOT%\scripts\client.env.template" (
    copy "%ROOT%\scripts\client.env.template" "%ROOT%\client.env" >nul
  ) else (
    echo [WARN] client.env.template not found
  )
)

if not exist "%ROOT%\client.env" (
  echo [WARN] No client.env found. Create one before running rslogic-client.
)

if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\logs\client" mkdir "%ROOT%\logs\client"

echo [*] Writing start-rslogic-client.bat
> "%ROOT%\start-rslogic-client.bat" echo @echo off
>> "%ROOT%\start-rslogic-client.bat" echo setlocal
>> "%ROOT%\start-rslogic-client.bat" echo set "ROOT=%~dp0"
>> "%ROOT%\start-rslogic-client.bat" echo if "%ROOT%"=="" set "ROOT=%CD%\"
>> "%ROOT%\start-rslogic-client.bat" echo set "VENV=%ROOT%.venv"
>> "%ROOT%\start-rslogic-client.bat" echo set "LOG_DIR=%ROOT%logs\client"
>> "%ROOT%\start-rslogic-client.bat" echo if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
>> "%ROOT%\start-rslogic-client.bat" echo set PYTHONUNBUFFERED=1
>> "%ROOT%\start-rslogic-client.bat" echo set "RSLOGIC_ROOT=%ROOT%"
>> "%ROOT%\start-rslogic-client.bat" echo set "PYTHONPATH=%RSLOGIC_ROOT%;%RSLOGIC_ROOT%rslogic\internal_tools\rstool-sdk\src"
>> "%ROOT%\start-rslogic-client.bat" echo set "PY_EXE=%VENV%\Scripts\python.exe"
>> "%ROOT%\start-rslogic-client.bat" echo if not exist "%PY_EXE%" ^(
>> "%ROOT%\start-rslogic-client.bat" echo   echo [ERROR] Python executable not found: %PY_EXE%
>> "%ROOT%\start-rslogic-client.bat" echo   echo [ERROR] Re-run installer.bat to recreate the environment.
>> "%ROOT%\start-rslogic-client.bat" echo   exit /b 1
>> "%ROOT%\start-rslogic-client.bat" echo ^)
>> "%ROOT%\start-rslogic-client.bat" echo "%PY_EXE%" -m rslogic.client.rsnode_client 1>>"%LOG_DIR%\rslogic-client-stdout.log" 2>>"%LOG_DIR%\rslogic-client-stderr.log"

echo [*] Install complete.
echo [*] Edit %ROOT%\client.env, then run:
echo     %ROOT%\start-rslogic-client.bat
exit /b 0
