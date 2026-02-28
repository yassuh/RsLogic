@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
set "REPO_URL=https://github.com/yassuh/RsLogic.git"
set "REPO_BRANCH=main"
if not defined SystemDrive set "SystemDrive=C:"

if defined ProgramData (
    set "REPO_ROOT=%ProgramData%\RsLogic\RsLogic"
    set "LOG_DIR=%ProgramData%\RsLogic"
) else (
    set "REPO_ROOT=%SystemDrive%\ProgramData\RsLogic\RsLogic"
    set "LOG_DIR=%SystemDrive%\ProgramData\RsLogic"
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
set "LOG_PATH=%LOG_DIR%\rsnode-orchestrator.log"

set "LOCAL_SCRIPT=%SCRIPT_DIR%rslogic_rsnode_client.py"
set "REPO_SCRIPT=%REPO_ROOT%\scripts\rslogic_rsnode_client.py"

set "ORCH_SCRIPT="
if exist "%LOCAL_SCRIPT%" set "ORCH_SCRIPT=%LOCAL_SCRIPT%"
if not defined ORCH_SCRIPT if exist "%REPO_SCRIPT%" set "ORCH_SCRIPT=%REPO_SCRIPT%"

if not exist "%ORCH_SCRIPT%" (
    echo Bootstrapping repository to "%REPO_ROOT%".
    where git >nul 2>nul
    if errorlevel 1 (
        echo Git not found on PATH. Install Git for Windows and retry.
        pause
        exit /b 1
    )

    if exist "%REPO_ROOT%" (
        if exist "%REPO_ROOT%\.git" (
            echo %REPO_ROOT% already has a git checkout.
        ) else (
            echo Existing folder at %REPO_ROOT% is not a git repository, replacing.
            rmdir /s /q "%REPO_ROOT%" >nul 2>&1
        )
    )

    if not exist "%REPO_ROOT%" mkdir "%REPO_ROOT%" >nul 2>&1
    git clone --branch "%REPO_BRANCH%" "%REPO_URL%" "%REPO_ROOT%"
    if errorlevel 1 (
        echo Clone failed from "%REPO_URL%".
        pause
        exit /b 1
    )
    set "ORCH_SCRIPT=%REPO_SCRIPT%"
)

if not exist "%ORCH_SCRIPT%" (
    echo Failed to locate rslogic_rsnode_client.py at "%SCRIPT_DIR%" or "%REPO_ROOT%\scripts".
    pause
    exit /b 1
)

where py >nul 2>nul
if not errorlevel 1 (
    set "PY_EXE=py -3"
) else (
    where python >nul 2>nul
    if errorlevel 1 (
        echo Python not found on PATH. Install Python and retry.
        pause
        exit /b 1
    )
    set "PY_EXE=python"
)

title RsLogic RSNode Client Orchestrator
set "ARGS=%*"
call %PY_EXE% "%ORCH_SCRIPT%" --repo-url "%REPO_URL%" --repo-branch "%REPO_BRANCH%" --repo-root "%REPO_ROOT%" --node-data-root-argument -dataRoot --log-path "%LOG_PATH%" %ARGS%
if errorlevel 1 pause

endlocal
