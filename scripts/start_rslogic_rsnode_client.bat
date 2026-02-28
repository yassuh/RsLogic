@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
if not exist "%PS_EXE%" set "PS_EXE=powershell.exe"
set "REPO_URL=https://github.com/yassuh/RsLogic.git"
set "REPO_BRANCH=main"
set "REPO_ROOT=%ProgramData%\RsLogic\RsLogic"
set "PS_SCRIPT=%SCRIPT_DIR%rslogic_rsnode_client.ps1"
if not exist "%PS_SCRIPT%" set "PS_SCRIPT=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"

if not exist "%PS_SCRIPT%" (
    echo Bootstrap repository to "%REPO_ROOT%".
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'Git is required for first-time bootstrap. Install Git for Windows.' }; if (-not (Test-Path '%REPO_ROOT%')) { New-Item -ItemType Directory -Path '%REPO_ROOT%' -Force | Out-Null }; if (-not (Test-Path '%REPO_ROOT%\.git')) { if ((Test-Path '%REPO_ROOT%\scripts') -or (Get-ChildItem '%REPO_ROOT%' -Force -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0) { Remove-Item -Recurse -Force '%REPO_ROOT%' }; New-Item -ItemType Directory -Path '%REPO_ROOT%' -Force | Out-Null; git clone --branch %REPO_BRANCH% %REPO_URL% '%REPO_ROOT%' }"
    set "PS_SCRIPT=%REPO_ROOT%\scripts\rslogic_rsnode_client.ps1"
)

if not exist "%PS_SCRIPT%" (
    echo Error: missing orchestrator script at %PS_SCRIPT%.
    echo Reinstall from https://github.com/yassuh/RsLogic and place this launcher inside the scripts folder.
    pause
    exit /b 1
)

title RsLogic RSNode Client Orchestrator
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -NoExit -File "%PS_SCRIPT%" -RepoUrl "%REPO_URL%" -RepoBranch "%REPO_BRANCH%" -RepoPath "%REPO_ROOT%" %*
